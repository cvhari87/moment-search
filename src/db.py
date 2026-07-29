"""Postgres (Neon) access layer — the videos manifest, source of truth.

One row per (user's) video; `status` tracks the ingest lifecycle:
pending -> fetching -> sampling -> embedding -> indexed | skipped | failed
(skipped = duplicate (user_id, source_hash); indexed = searchable in Qdrant).
"""
from __future__ import annotations

import os
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import DATABASE_URL, INFLIGHT_STATUSES

_pool: ConnectionPool | None = None
_pool_pid: int | None = None


def pool() -> ConnectionPool:
    """Process-local pool. Prefect runs flows in subprocesses; a child must
    never reuse the parent's SSL connections (corrupts the TLS stream), so a
    fork gets a fresh pool."""
    global _pool, _pool_pid
    if _pool is None or _pool_pid != os.getpid():
        # check= pings each connection before lending it out — Neon silently
        # drops idle SSL connections, which otherwise 500s the first request
        # after a quiet period.
        _pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=5,
                               check=ConnectionPool.check_connection,
                               kwargs={"row_factory": dict_row})
        _pool_pid = os.getpid()
    return _pool


SCHEMA = """
CREATE TABLE IF NOT EXISTS ms_videos (
    id           TEXT PRIMARY KEY,           -- yt_<id> | up_<uuid>
    user_id      TEXT NOT NULL,
    source       TEXT NOT NULL,              -- youtube | upload
    url          TEXT,                       -- YouTube URL (source=youtube)
    storage_key  TEXT,                       -- uploads/<user>/<id>.<ext> (source=upload)
    source_hash  TEXT,                       -- sha256 of the file / yt video id
    title        TEXT,
    status       TEXT NOT NULL DEFAULT 'pending',
    error        TEXT,
    frame_count  INT,
    progress     REAL,                       -- 0..1 within the current stage
    attempts     INT NOT NULL DEFAULT 0,
    embed_version TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ms_videos_user_idx   ON ms_videos (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ms_videos_status_idx ON ms_videos (status);
CREATE INDEX IF NOT EXISTS ms_videos_hash_idx   ON ms_videos (user_id, source_hash);

-- Bring-your-own-model: a tenant's hosted LLM endpoint (vLLM / Ollama / any
-- OpenAI-compatible server, NVIDIA NIM, or Anthropic). When a row exists the
-- read path answers with THIS model instead of the server's LLM_* env config.
CREATE TABLE IF NOT EXISTS ms_user_llms (
    user_id    TEXT PRIMARY KEY,
    provider   TEXT NOT NULL DEFAULT 'openai',  -- openai | nvidia | anthropic
    model      TEXT NOT NULL,
    base_url   TEXT,                            -- e.g. http://my-vllm:8000/v1
    api_key    TEXT,                            -- optional (vLLM often has none)
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Multi-source manifest migration: one shared table for every source kind
-- (video | paper | deck), not a second table — this is what lets the WFQ
-- dispatcher and wfq_claim() generalize to documents with zero rewrite.
-- ADD COLUMN IF NOT EXISTS is idempotent whether the table is brand new or
-- already has rows; the DEFAULT keeps every pre-existing video row valid.
ALTER TABLE ms_videos ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'video';
ALTER TABLE ms_videos ADD COLUMN IF NOT EXISTS uri TEXT;
ALTER TABLE ms_videos ADD COLUMN IF NOT EXISTS chunk_count INT;
CREATE INDEX IF NOT EXISTS ms_videos_kind_idx ON ms_videos (kind);

-- Crash-safety reconciler (Block G): a lease token bumped on every admission
-- (wfq_claim). Every status/progress/attempts write a flow makes carries the
-- generation it was admitted under, gated by this column — so if the
-- reconciler resets a row to 'pending' while an old, still-alive run is mid-
-- write, and a new run gets admitted (bumping generation again) before the
-- old one finishes, the old run's writes silently no-op instead of clobbering
-- the new run's progress. See db.set_status's docstring.
ALTER TABLE ms_videos ADD COLUMN IF NOT EXISTS generation INT NOT NULL DEFAULT 0;
"""

# Advisory-lock key for schema init. Arbitrary but stable — must not collide
# with other pg_advisory_lock callers in this Postgres instance.
_SCHEMA_LOCK_KEY = 833271


def init_schema() -> None:
    """Both api and worker call this independently at boot. Without
    coordination, concurrent first-time `CREATE TABLE IF NOT EXISTS` calls can
    race on Postgres's own system catalogs — observed in practice as
    `UniqueViolation: duplicate key ... pg_type_typname_nsp_index`, not just a
    theoretical risk. A container restart policy can mask the crash (the loser
    just retries and finds the table already there), which makes it look safe
    when it isn't.

    pg_advisory_xact_lock is transaction-scoped: it's held for exactly the
    connection block below and releases automatically on commit OR rollback,
    so a crash mid-migration can never leave the lock stuck.
    """
    with pool().connection() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (_SCHEMA_LOCK_KEY,))
        conn.execute(SCHEMA)


def upsert_pending(video: dict[str, Any]) -> dict:
    """Insert a source (video, paper, or deck) as pending; re-submitting an
    existing id resets it to pending — UNLESS a run is already actively
    in-flight for it, in which case status/error/progress are left alone.

    Without that guard, re-registering something already running (a real
    scenario: eval.py's documents_async check re-submits the locked paper,
    and a user can just double-click Ingest) unconditionally flipped the row
    back to 'pending', and the dispatcher would fairly re-admit it — while
    the ORIGINAL run was still executing. Observed live: two distinct Prefect
    run IDs processing the same doc_id simultaneously, both completing and
    racing to write the row's terminal status last. Deterministic Qdrant
    point IDs made the double-embed harmless, but nothing stopped a slower,
    stale run from overwriting a newer run's 'indexed' with its own (possibly
    'failed') outcome. `kind` defaults to 'video' so every existing caller
    (src/api/videos.py) works unchanged; documents pass kind + uri."""
    video = {"kind": "video", "uri": None, "inflight": list(INFLIGHT_STATUSES), **video}
    with pool().connection() as conn:
        row = conn.execute(
            """
            INSERT INTO ms_videos (id, user_id, source, url, storage_key, source_hash,
                                   title, kind, uri, status)
            VALUES (%(id)s, %(user_id)s, %(source)s, %(url)s, %(storage_key)s,
                    %(source_hash)s, %(title)s, %(kind)s, %(uri)s, 'pending')
            ON CONFLICT (id) DO UPDATE SET
                url = COALESCE(EXCLUDED.url, ms_videos.url),
                storage_key = COALESCE(EXCLUDED.storage_key, ms_videos.storage_key),
                source_hash = COALESCE(EXCLUDED.source_hash, ms_videos.source_hash),
                title = COALESCE(EXCLUDED.title, ms_videos.title),
                uri = COALESCE(EXCLUDED.uri, ms_videos.uri),
                kind = EXCLUDED.kind,
                status = CASE WHEN ms_videos.status = ANY(%(inflight)s)
                              THEN ms_videos.status ELSE 'pending' END,
                error = CASE WHEN ms_videos.status = ANY(%(inflight)s)
                             THEN ms_videos.error ELSE NULL END,
                progress = CASE WHEN ms_videos.status = ANY(%(inflight)s)
                                THEN ms_videos.progress ELSE NULL END,
                updated_at = now()
            RETURNING *
            """,
            video,
        ).fetchone()
    return row


def set_status(video_id: str, status: str, *, error: str | None = None,
               title: str | None = None, frame_count: int | None = None,
               source_hash: str | None = None, embed_version: str | None = None,
               progress: float | None = None, chunk_count: int | None = None,
               generation: int | None = None) -> None:
    """`generation`, when passed, is the lease token (see SCHEMA's comment on
    the column) a flow run was admitted under — the write becomes a no-op if
    the row's generation has since moved on (a reconciler resume + a fresh
    admission happened while this run was still alive). Callers outside a
    flow (the reconciler's own sweep, the /retry endpoint, upsert_pending)
    intentionally omit it: those ARE the events that legitimately change
    admission state, not writers racing to be the last one in.

    Narrow stale-write guard, not a full fencing mechanism on its own: once a row is
    'indexed', NO further write from this function applies to it UNLESS the
    new status is 'pending' (the one legitimate way anything un-terminals an
    indexed row: a fresh, deliberate re-registration through
    upsert_pending()). This is stronger than an earlier version that only
    blocked a direct indexed->failed write: a stale run doesn't necessarily
    fail immediately — it can walk indexed->parsing->embedding->failed,
    clobbering 'indexed' with 'parsing' well before its own eventual
    'failed' write, which the narrower check never saw coming since the row
    was no longer 'indexed' by the time that write happened. Blocking every
    non-'pending' write while current status is 'indexed' closes that: a
    stale run's parsing/chunking/embedding/failed calls all become no-ops
    from the moment a newer run's 'indexed' has landed, not just its last one.

    Still NOT full fencing: this protects the 'indexed' terminal state
    specifically, not general ordering between two concurrent runs (which
    deterministic Qdrant point IDs already make safe for duplicate WORK,
    just not for which run's status write "wins"). A generation/lease token
    per admission, checked by every status write, is the complete answer —
    that's Block G's admission-time resilience work, spanning the
    dispatcher, jobs, and both ingest flows, not a one-function patch."""
    with pool().connection() as conn:
        conn.execute(
            """
            UPDATE ms_videos SET status = %(status)s, error = %(error)s,
                title = COALESCE(%(title)s, title),
                frame_count = COALESCE(%(frame_count)s, frame_count),
                source_hash = COALESCE(%(source_hash)s, source_hash),
                embed_version = COALESCE(%(embed_version)s, embed_version),
                progress = %(progress)s,
                chunk_count = COALESCE(%(chunk_count)s, chunk_count),
                updated_at = now()
            WHERE id = %(video_id)s
              AND (status != 'indexed' OR %(status)s = 'pending')
              AND (%(generation)s::int IS NULL OR generation = %(generation)s::int)
            """,
            {"status": status, "error": error, "title": title, "frame_count": frame_count,
             "source_hash": source_hash, "embed_version": embed_version, "progress": progress,
             "chunk_count": chunk_count, "video_id": video_id, "generation": generation},
        )


def set_progress(video_id: str, progress: float, *, generation: int | None = None) -> None:
    """Same 'indexed' guard as set_status() — a stale run's own progress
    callbacks (video sampling/embedding report progress many times per run)
    shouldn't cosmetically overwrite an already-indexed row's progress
    either, even though a stray progress value is lower-severity than a
    stray status. Same `generation` lease-token gate as set_status()."""
    with pool().connection() as conn:
        conn.execute(
            "UPDATE ms_videos SET progress = %(progress)s, updated_at = now() "
            "WHERE id = %(video_id)s AND status != 'indexed' "
            "AND (%(generation)s::int IS NULL OR generation = %(generation)s::int)",
            {"progress": round(progress, 3), "video_id": video_id, "generation": generation},
        )


class StaleLeaseError(RuntimeError):
    """Raised when a flow discovers — at flow start (bump_attempts) or mid-
    run (check_generation) — that its `generation` lease has been superseded
    by a newer admission (the reconciler swept this row as stale while THIS
    run was still alive, and the dispatcher has since re-admitted it under a
    new generation). This is not an ingest failure: a newer run already owns
    the row and is the one that should finish it. The caller must stop
    immediately, before doing any further destructive/overwriting work
    (deleting or overwriting frames, Qdrant points, or checkpoints) — set_status()'s
    generation guard alone only protects the STATUS COLUMN, it does nothing
    to stop the stale run's actual side effects from clobbering the newer
    run's in-progress state in the meantime. See check_generation()."""


def check_generation(video_id: str, generation: int) -> None:
    """Revalidate a lease mid-task, immediately before a destructive or
    overwriting side effect (deleting existing frames/Qdrant points,
    overwriting a checkpoint, upserting chunks). A stale run can otherwise
    run for minutes past the point its lease was superseded — set_status()'s
    guard silently no-ops its STATUS writes the whole time, but nothing stops
    it from continuing to execute and corrupt shared state UNLESS every
    destructive stage explicitly checks first. Raises StaleLeaseError if the
    row's current generation no longer matches; a no-op if it still does."""
    with pool().connection() as conn:
        row = conn.execute(
            "SELECT generation FROM ms_videos WHERE id = %s", (video_id,)
        ).fetchone()
    current = row["generation"] if row else None
    if current != generation:
        raise StaleLeaseError(
            f"{video_id}: lease generation {generation} superseded "
            f"(current={current!r}) — aborting before further side effects")


def bump_attempts(video_id: str, *, generation: int | None = None) -> int:
    """Same `generation` gate as set_status(). Unlike set_status() — where a
    rejected write silently no-ops because a stray status/progress value is
    low-severity — a rejected bump here means this run's lease was ALREADY
    gone before it did any work, so it raises StaleLeaseError instead of
    quietly returning a number: this is the flow-start check that stops a
    stale run before t_fetch/t_parse even begins. Raises ValueError if the
    row is simply gone (deleted mid-run) — a different, real error, not a
    lease loss."""
    with pool().connection() as conn:
        row = conn.execute(
            "UPDATE ms_videos SET attempts = attempts + 1, updated_at = now() "
            "WHERE id = %(video_id)s "
            "AND (%(generation)s::int IS NULL OR generation = %(generation)s::int) "
            "RETURNING attempts",
            {"video_id": video_id, "generation": generation},
        ).fetchone()
        if row:
            return row["attempts"]
        current = conn.execute(
            "SELECT attempts, generation FROM ms_videos WHERE id = %s", (video_id,)
        ).fetchone()
    if current is None:
        raise ValueError(f"no manifest row for {video_id}")
    if generation is not None and current["generation"] != generation:
        raise StaleLeaseError(
            f"{video_id}: lease generation {generation} superseded "
            f"(current={current['generation']!r}) — aborting before any work")
    return current["attempts"]


def get_video(video_id: str) -> dict | None:
    with pool().connection() as conn:
        return conn.execute("SELECT * FROM ms_videos WHERE id = %s", (video_id,)).fetchone()


def find_duplicate(user_id: str, source_hash: str, exclude_id: str) -> dict | None:
    """An already-indexed video with the same content for the same user."""
    with pool().connection() as conn:
        return conn.execute(
            """
            SELECT * FROM ms_videos
            WHERE user_id = %s AND source_hash = %s AND id <> %s AND status = 'indexed'
            LIMIT 1
            """,
            (user_id, source_hash, exclude_id),
        ).fetchone()


def list_videos(user_id: str, status: str | None = None) -> list[dict]:
    q = "SELECT * FROM ms_videos WHERE user_id = %s"
    params: list = [user_id]
    if status:
        q += " AND status = %s"
        params.append(status)
    q += " ORDER BY created_at DESC"
    with pool().connection() as conn:
        return conn.execute(q, tuple(params)).fetchall()


def videos_by_ids(ids: list[str]) -> dict[str, dict]:
    """Metadata join for search citations (title/url/source live here, not in Qdrant)."""
    if not ids:
        return {}
    with pool().connection() as conn:
        rows = conn.execute("SELECT * FROM ms_videos WHERE id = ANY(%s)", (ids,)).fetchall()
    return {r["id"]: r for r in rows}


def delete_video(video_id: str) -> None:
    with pool().connection() as conn:
        conn.execute("DELETE FROM ms_videos WHERE id = %s", (video_id,))


# ── Fair scheduling (WFQ) ────────────────────────────────────────────────────

def count_inflight() -> int:
    """How many videos currently occupy execution capacity (scheduled/running)."""
    with pool().connection() as conn:
        row = conn.execute(
            "SELECT count(*) AS n FROM ms_videos WHERE status = ANY(%s)",
            (list(INFLIGHT_STATUSES),),
        ).fetchone()
    return row["n"] if row else 0


def wfq_claim(limit: int, *, fair: bool = True) -> list[dict]:
    """Atomically claim up to `limit` pending sources (any kind), flipping
    them pending -> queued. Returns the claimed rows.

    Every source is admitted through here — there is no direct-enqueue path
    anywhere else (see src/api/videos.py, src/api/documents.py) — because a
    direct enqueue plus a background claimer racing the same 'pending' row
    used to schedule two Prefect runs for one source. ENABLE_FAIR_DISPATCH
    only picks the ORDERING below, never whether this is the admission path.

    fair=True  (ENABLE_FAIR_DISPATCH=true, default): round-robin across
      users — rank each user's pending sources by age (row_number
      partitioned by user_id), then order by that rank first, so we take
      everyone's oldest, then everyone's 2nd, ... A user who dumped 50
      videos only gets one slot per round, exactly like the others.
    fair=False (ENABLE_FAIR_DISPATCH=false): plain FIFO by creation time
      across all users — "useful for A/B teaching the difference" per the
      module docstring in src/dispatcher.py.

    The UPDATE ... WHERE status='pending' RETURNING is the atomic claim:
    if two dispatchers race, each row is handed out once.
    """
    if limit <= 0:
        return []
    order_sql = (
        """
        SELECT id FROM (
            SELECT id, row_number() OVER (
                PARTITION BY user_id ORDER BY created_at, id) AS rn
            FROM ms_videos WHERE status = 'pending'
        ) t
        ORDER BY rn, id
        LIMIT %s
        """
        if fair else
        """
        SELECT id FROM ms_videos WHERE status = 'pending'
        ORDER BY created_at, id
        LIMIT %s
        """
    )
    with pool().connection() as conn:
        picked = conn.execute(order_sql, (limit,)).fetchall()
        ids = [r["id"] for r in picked]
        if not ids:
            return []
        return conn.execute(
            """
            UPDATE ms_videos SET status = 'queued', generation = generation + 1, updated_at = now()
            WHERE id = ANY(%s) AND status = 'pending'
            RETURNING id, user_id, kind, generation
            """,
            (ids,),
        ).fetchall()


# ── Crash safety / reconciler (Block G) ──────────────────────────────────────

def reconcile_stale(stale_seconds: float, max_attempts: int) -> list[dict]:
    """Atomically find sources stuck in an in-flight status (queued,
    fetching/sampling, parsing/chunking, embedding) whose updated_at hasn't
    moved in `stale_seconds`, and either reset them to 'pending' (dispatcher
    fairly re-admits, resumes from checkpoint) or, if they've already burned
    through `max_attempts` flow-run attempts, dead-letter them straight to
    'failed' instead of resurrecting a source that just keeps crashing the
    worker. One UPDATE ... RETURNING, no separate SELECT-then-UPDATE — avoids
    a TOCTOU race against a run that finishes (reaches 'indexed') in the gap
    between reading and writing.

    Bumps `generation` in this SAME atomic write — load-bearing, not
    optional. Without it, a row reset to 'pending' keeps the OLD run's
    generation until the dispatcher happens to claim it (wfq_claim() is the
    only other generation-bumping site), which can be seconds away. In that
    window, an old run that's actually still alive (a false-positive
    staleness read, not a real crash) still holds a lease that matches —
    check_generation()/set_status() would let it keep writing, potentially
    racing the dispatcher's next admission or, worse, undoing a dead-letter
    this same call just wrote (status='failed' isn't otherwise generation-
    protected the way 'indexed' is). Bumping here closes that at the
    source: by the time this UPDATE commits, ANY generation an old run is
    still holding is already stale, regardless of whether or when the
    dispatcher re-admits the row."""
    with pool().connection() as conn:
        return conn.execute(
            """
            UPDATE ms_videos SET
                status = CASE WHEN attempts >= %(max_attempts)s THEN 'failed' ELSE 'pending' END,
                error = CASE WHEN attempts >= %(max_attempts)s
                             THEN 'reconciler: exceeded max ingest attempts after repeated staleness resets'
                             ELSE error END,
                generation = generation + 1,
                updated_at = now()
            WHERE status = ANY(%(inflight)s)
              AND updated_at < now() - (%(stale_seconds)s * interval '1 second')
            RETURNING id, status, kind, attempts, generation
            """,
            {"max_attempts": max_attempts, "inflight": list(INFLIGHT_STATUSES),
             "stale_seconds": stale_seconds},
        ).fetchall()


# ── Bring-your-own-model (per-tenant LLM endpoint) ───────────────────────────

def get_user_llm(user_id: str) -> dict | None:
    with pool().connection() as conn:
        return conn.execute("SELECT * FROM ms_user_llms WHERE user_id = %s",
                            (user_id,)).fetchone()


def set_user_llm(user_id: str, *, provider: str, model: str,
                 base_url: str | None, api_key: str | None) -> dict:
    """Upsert a tenant's model endpoint. An empty api_key keeps the stored one
    (so users can change model/URL without re-pasting their secret)."""
    with pool().connection() as conn:
        return conn.execute(
            """
            INSERT INTO ms_user_llms (user_id, provider, model, base_url, api_key)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                provider = EXCLUDED.provider,
                model = EXCLUDED.model,
                base_url = EXCLUDED.base_url,
                api_key = COALESCE(NULLIF(EXCLUDED.api_key, ''), ms_user_llms.api_key),
                updated_at = now()
            RETURNING *
            """,
            (user_id, provider, model, base_url, api_key),
        ).fetchone()


def delete_user_llm(user_id: str) -> None:
    with pool().connection() as conn:
        conn.execute("DELETE FROM ms_user_llms WHERE user_id = %s", (user_id,))
