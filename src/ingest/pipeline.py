"""Per-video ingest pipeline — a Prefect flow of three stage-tasks.

pending -> fetching -> sampling -> embedding -> indexed | skipped | failed

Stages:
  1. fetch    acquire the source into worker scratch (bucket download for
              uploads, yt-dlp for YouTube), hash it, skip duplicates
  2. sample   ffmpeg pipe-to-memory keyframes -> pHash dedup -> thumbnails
              batch-uploaded to object storage
  3. embed    CLIP-embed the surviving frames (batched) -> idempotent Qdrant
              upsert (deterministic IDs, user_id-tagged)

Orchestration: Prefect Cloud. The API triggers a deployment run (src/jobs.py);
worker.py serves this flow and picks runs up. Each task carries its own retry
policy — a completed stage is not re-run when a later one fails and retries.

Postgres remains the business-status source of truth: tasks update the videos
row; Prefect Cloud's UI is the operational view (logs, retries, run history).
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from prefect import flow, task

from .. import db, storage
from ..config import CLIP_BATCH, EMBED_VERSION
from ..rag import vector_store
from ..rag.embeddings import embed_jpegs
from . import fetch as fetch_mod
from .dedup import dedup
from .frames import Frame, sample

_UPLOAD_POOL = 8  # concurrent thumbnail PUTs (I/O-bound)


@task(name="fetch", retries=2, retry_delay_seconds=[30, 120])
def t_fetch(video_id: str, user_id: str, generation: int) -> str:
    """Source video -> worker scratch file; duplicate check via source_hash.

    Returns "" when the content is a duplicate of an already-indexed video for
    this user (row marked 'skipped' — a plain outcome, not a retryable error).
    """
    db.set_status(video_id, "fetching", generation=generation)
    row = db.get_video(video_id)
    if row is None:
        raise ValueError(f"no manifest row for {video_id}")

    if row["source"] == "youtube":
        path, title = fetch_mod.fetch_youtube(row["url"], video_id)
        source_hash = video_id  # the YouTube id IS the content identity
        db.set_status(video_id, "fetching", title=title, source_hash=source_hash, generation=generation)
    else:
        path = fetch_mod.fetch_upload(row["storage_key"], video_id)
        source_hash = fetch_mod.sha256_file(path)
        db.set_status(video_id, "fetching", source_hash=source_hash, generation=generation)

    dup = db.find_duplicate(user_id, source_hash, exclude_id=video_id)
    if dup:
        path.unlink(missing_ok=True)
        db.set_status(video_id, "skipped", error=f"duplicate of {dup['id']}", generation=generation)
        return ""
    return str(path)


@task(name="sample")
def t_sample(video_id: str, user_id: str, path: str, generation: int) -> list[Frame]:
    """Keyframes in memory -> pHash dedup -> thumbnails to object storage."""
    db.set_status(video_id, "sampling", progress=0.0, generation=generation)
    frames = sample(Path(path))
    if not frames:
        raise RuntimeError("No frames could be extracted from the video.")
    kept = dedup(frames)
    print(f"[sample] {video_id}: {len(frames)} sampled -> {len(kept)} after dedup")

    # Revalidate the lease immediately before the first destructive op: a
    # stale run's status writes above would have silently no-op'd already if
    # the reconciler swept and re-admitted this row mid-sample, but nothing
    # stops the delete below unless we check explicitly (see StaleLeaseError).
    db.check_generation(video_id, generation)
    # Idempotent re-run: clear any thumbnails a previous attempt left behind.
    storage.delete_prefix(storage.frame_prefix(user_id, video_id))
    done = 0
    done_lock = threading.Lock()
    # A periodic (every-25th) check left two gaps a live simulation actually
    # measured: staleness went undetected for up to 25 uploads, and even
    # once ONE thread detected it, the other _UPLOAD_POOL threads had no way
    # to know — each was independently marching toward its own 25th multiple,
    # so uploads kept landing (34 total observed) well after the lease was
    # already gone. Fixed two ways together: checked before EVERY put now
    # (not periodically), and `lost` is a shared signal — the instant ANY
    # thread's check fails, every other thread sees it before its own next
    # write and stops, instead of finishing its own lap first.
    lost = threading.Event()

    def _put(i_f: tuple[int, Frame]) -> None:
        nonlocal done
        if lost.is_set():
            return
        i, f = i_f
        try:
            db.check_generation(video_id, generation)
        except db.StaleLeaseError:
            lost.set()
            raise
        storage.put_bytes(storage.frame_key(user_id, video_id, i), f.jpeg, "image/jpeg")
        with done_lock:
            done += 1
            local_done = done
        if local_done % 25 == 0:
            db.set_progress(video_id, local_done / len(kept), generation=generation)

    with ThreadPoolExecutor(max_workers=_UPLOAD_POOL) as ex:
        list(ex.map(_put, enumerate(kept)))
    db.set_progress(video_id, 1.0, generation=generation)
    return kept


@task(name="embed-index", retries=2, retry_delay_seconds=60)
def t_embed_index(video_id: str, user_id: str, frames: list[Frame], generation: int) -> int:
    """Batched CLIP embeddings -> idempotent multi-tenant Qdrant upsert.

    Deliberately does NOT set status 'indexed' — the transcript branch
    (t_transcript) still runs after this in ingest_video, and a worker
    killed in that gap must NOT be left in a terminal status the reconciler
    will never revisit. ingest_video sets 'indexed' once, at the flow
    boundary, after both branches finish."""
    db.set_status(video_id, "embedding", progress=0.0, generation=generation)
    vector_store.ensure_collection()
    # Revalidate before the destructive delete — same reasoning as t_sample's
    # check above; this is the more consequential one since it drops every
    # existing point for this video before re-upserting.
    db.check_generation(video_id, generation)
    vector_store.delete_video(user_id, video_id)  # drop stale points from prior runs

    total = 0
    for start in range(0, len(frames), CLIP_BATCH):
        batch = frames[start:start + CLIP_BATCH]
        vectors = embed_jpegs([f.jpeg for f in batch])
        # Recheck before EACH batch's write, not just once before the loop —
        # a video with many CLIP_BATCH-sized batches can run long enough for
        # a lease to be lost partway through.
        db.check_generation(video_id, generation)
        vector_store.upsert_frames(
            user_id, video_id,
            ids=range(start, start + len(batch)),
            vectors=vectors,
            payloads=[{"user_id": user_id, "video_id": video_id, "ms": f.ms,
                       "idx": start + i, "modality": "frame",
                       "t_start": f.ms / 1000.0, "t_end": f.ms / 1000.0,
                       "embed_version": EMBED_VERSION}
                      for i, f in enumerate(batch)],
        )
        total += len(batch)
        db.set_progress(video_id, total / len(frames), generation=generation)
    return total


@task(name="transcript", retries=1, retry_delay_seconds=30)
def t_transcript(video_id: str, user_id: str, generation: int) -> int:
    """YouTube captions -> time chunks -> bge -> text collection (the 2nd
    branch). Best-effort: uploads have no captions, some videos have none, and
    any failure just leaves the video visual-only — never fails the flow.
    Runs AFTER embed-index (whose delete clears both branches first).

    The broad `except Exception` below is deliberately narrower than it
    looks: a `StaleLeaseError` from check_generation() is re-raised BEFORE
    that handler, not swallowed as "no transcript, visual-only" — a lost
    lease is not a transcript failure, it's a signal to stop entirely, same
    as every other stage."""
    from ..config import ENABLE_TRANSCRIPT, TEXT_EMBED_VERSION
    from ..rag.embeddings import embed_docs
    from .transcript import chunk_cues, fetch_transcript

    if not ENABLE_TRANSCRIPT:
        return 0
    row = db.get_video(video_id) or {}
    if row.get("source") != "youtube" or not row.get("url"):
        return 0  # uploaded files have no caption track
    try:
        chunks = chunk_cues(fetch_transcript(row["url"], video_id))
        if not chunks:
            print(f"[transcript] {video_id}: no captions — visual-only")
            return 0
        vector_store.ensure_text_collection()
        vecs = embed_docs([c["text"] for c in chunks])
        # Revalidate immediately before the Qdrant write — same reasoning as
        # every other destructive/overwriting op in this flow.
        db.check_generation(video_id, generation)
        vector_store.upsert_chunks(user_id, video_id, vecs, payloads=[
            {"user_id": user_id, "video_id": video_id, "modality": "text",
             "t_start": c["t_start"], "t_end": c["t_end"],
             "ms": int(c["t_start"] * 1000), "text": c["text"],
             "embed_version": TEXT_EMBED_VERSION} for c in chunks])
        print(f"[transcript] {video_id}: indexed {len(chunks)} transcript chunks")
        return len(chunks)
    except db.StaleLeaseError:
        raise  # a lost lease is not a "no transcript" outcome — let it stop the flow
    except Exception as exc:
        print(f"[transcript] {video_id}: failed ({type(exc).__name__}: {exc}) — visual-only")
        return 0


@flow(name="ms-ingest-video", log_prints=True, timeout_seconds=3600)
def ingest_video(video_id: str, user_id: str, generation: int) -> dict:
    """`generation` is the lease token db.wfq_claim() minted when this run was
    admitted (src/dispatcher.py -> src/jobs.py) — threaded into every status
    write below so a reconciler-triggered resume (src/reconciler.py) can
    never have a still-alive old run's writes clobber a newer run's progress;
    see db.set_status's docstring for the mechanism.

    'indexed' is set exactly once here, at the flow boundary, AFTER both the
    frame and transcript branches finish — never inside t_embed_index. A
    worker killed between the two would otherwise leave a terminal row the
    reconciler (which only sweeps in-flight statuses) can never recover,
    permanently stuck visual-only even though a transcript existed."""
    path: str | None = None
    try:
        attempt = db.bump_attempts(video_id, generation=generation)
        path = t_fetch(video_id, user_id, generation)
        if not path:  # duplicate — already marked 'skipped' by t_fetch
            print(f"[ingest] {video_id} skipped (duplicate content)")
            return {"video_id": video_id, "skipped": True}
        frames = t_sample(video_id, user_id, path, generation)
        n = t_embed_index(video_id, user_id, frames, generation)
        # Transcript branch AFTER frames (embed-index's delete clears both first).
        t = t_transcript(video_id, user_id, generation)
        db.set_status(video_id, "indexed", frame_count=n,
                      embed_version=EMBED_VERSION, progress=1.0, generation=generation)
        print(f"[ingest] {video_id} indexed: {n} frames + {t} transcript chunks (attempt {attempt})")
        return {"video_id": video_id, "frames": n, "transcript_chunks": t}
    except db.StaleLeaseError as exc:
        # A newer run already owns this row (reconciler swept + re-admitted
        # while this run was still alive) — not an ingest failure. Every
        # status write above already no-op'd or never ran; stop cleanly
        # without touching the row further.
        print(f"[ingest] {video_id}: {exc}")
        return {"video_id": video_id, "stale_lease": True}
    except Exception as exc:
        db.set_status(video_id, "failed", error=f"{type(exc).__name__}: {exc}", generation=generation)
        raise  # Prefect marks the run Failed; full trace in the Cloud UI
    finally:
        if path:  # scratch only — durable copies live in object storage
            Path(path).unlink(missing_ok=True)
