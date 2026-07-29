"""Unit tests for the Block I reliability fixes — stdlib unittest only, no
live stack, no network, no docker (same conventions as
benchmark/test_bench_gates.py, which established that pattern for this
repo). Covers:

  1. src/rag/vector_store.py's _upsert_with_retry — the short local retry
     added to absorb a transient Qdrant connection error
     (ResponseHandlingException wrapping a ConnectTimeout, measured live
     under concurrent ingest) in a few seconds instead of falling through to
     Prefect's own 60s x2 task-level retry.
  2. src/rag/embeddings.py's embed_docs/embed_query routing. embed_docs
     routes to the remote clip service when CLIP_SERVICE_URL is set;
     embed_query deliberately does NOT — it always runs locally, so a
     search query never contends with ingest's bulk embeds for the same
     process's lock. An earlier attempt got this backwards (routed BOTH to
     the remote service, then tried to fix the resulting contention with a
     per-process thread-count knob that was dead code either way) — these
     tests pin the corrected routing down.
  3. src/rag/embeddings.py's _clip_lock/_text_lock separation and
     embed_docs_local's sub-batching.
  4. src/db.py's claim_pending() — verifies the function's own logic
     (advisory lock taken before the inflight count is read, capacity
     computed from that count, claim skipped when no slots remain) against
     a fake connection that records executed SQL in order. Does not
     exercise true cross-process concurrency (that needs a live Postgres,
     out of scope for this stdlib-only suite) — it pins down that the
     function's ordering and arithmetic are what makes cross-process
     serialization possible in the first place.

    python3 -m unittest discover -s tests -p 'test_*.py'
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from qdrant_client.http.exceptions import ResponseHandlingException

from src import db
from src.rag import embeddings, vector_store


class UpsertWithRetryTests(unittest.TestCase):
    def setUp(self):
        patcher = patch("src.rag.vector_store.time.sleep")
        self.mock_sleep = patcher.start()
        self.addCleanup(patcher.stop)

    @staticmethod
    def _points():
        return [MagicMock(name="point")]

    def test_succeeds_first_try_without_retry(self):
        mock_client = MagicMock()
        with patch.object(vector_store, "client", return_value=mock_client):
            vector_store._upsert_with_retry("coll", self._points())
        mock_client.upsert.assert_called_once()
        self.mock_sleep.assert_not_called()

    def test_transient_failure_then_success(self):
        mock_client = MagicMock()
        mock_client.upsert.side_effect = [
            ResponseHandlingException(ConnectionError("handshake timed out")),
            None,
        ]
        with patch.object(vector_store, "client", return_value=mock_client):
            vector_store._upsert_with_retry("coll", self._points())
        self.assertEqual(mock_client.upsert.call_count, 2)
        self.mock_sleep.assert_called_once()

    def test_exhausts_retries_and_raises(self):
        mock_client = MagicMock()
        mock_client.upsert.side_effect = ResponseHandlingException(
            ConnectionError("handshake timed out"))
        with patch.object(vector_store, "client", return_value=mock_client):
            with self.assertRaises(ResponseHandlingException):
                vector_store._upsert_with_retry("coll", self._points())
        self.assertEqual(mock_client.upsert.call_count, vector_store._UPSERT_RETRIES)
        self.assertEqual(self.mock_sleep.call_count, vector_store._UPSERT_RETRIES - 1)

    def test_non_retryable_exception_propagates_immediately(self):
        """A bug in our own payload (e.g. a ValueError) must NOT be treated
        as a transient connection issue — only ResponseHandlingException is
        caught. Retrying a deterministic failure would just waste the
        retry budget and still fail."""
        mock_client = MagicMock()
        mock_client.upsert.side_effect = ValueError("not a connection issue")
        with patch.object(vector_store, "client", return_value=mock_client):
            with self.assertRaises(ValueError):
                vector_store._upsert_with_retry("coll", self._points())
        mock_client.upsert.assert_called_once()
        self.mock_sleep.assert_not_called()

    def test_backoff_is_increasing(self):
        mock_client = MagicMock()
        mock_client.upsert.side_effect = [
            ResponseHandlingException(ConnectionError("x")),
            ResponseHandlingException(ConnectionError("x")),
            None,
        ]
        with patch.object(vector_store, "client", return_value=mock_client):
            vector_store._upsert_with_retry("coll", self._points())
        delays = [call.args[0] for call in self.mock_sleep.call_args_list]
        self.assertEqual(len(delays), 2)
        self.assertLess(delays[0], delays[1])


class EmbedRoutingTests(unittest.TestCase):
    """embed_docs routes to the remote clip service when CLIP_SERVICE_URL is
    set (bulk ingest embedding, throughput-oriented) and to the local model
    only when it isn't. embed_query is different ON PURPOSE: it ALWAYS runs
    locally regardless of CLIP_SERVICE_URL — a search query is one small,
    latency-critical call, and routing it through the same shared process
    (and lock) that ingest's bulk document embeds contend for was measured
    live (Block I) to noticeably slow search during heavy ingest, even
    after splitting the CLIP/BGE locks. Running it in-process instead means
    it never contends with ingest's lock at all — a different process, not
    just a different lock in the same one."""

    def test_embed_docs_uses_remote_service_when_configured(self):
        with patch.object(embeddings.config, "CLIP_SERVICE_URL", "http://clip:8001"), \
             patch.object(embeddings.config, "TEXT_EMBED_PROVIDER", "fastembed"), \
             patch.object(embeddings, "_post", return_value={"vectors": [[0.1]]}) as mock_post, \
             patch.object(embeddings, "embed_docs_local") as mock_local:
            embeddings.embed_docs(["hello"])
        mock_post.assert_called_once()
        mock_local.assert_not_called()

    def test_embed_docs_uses_local_model_when_service_unset(self):
        with patch.object(embeddings.config, "CLIP_SERVICE_URL", ""), \
             patch.object(embeddings.config, "TEXT_EMBED_PROVIDER", "fastembed"), \
             patch.object(embeddings, "_post") as mock_post, \
             patch.object(embeddings, "embed_docs_local", return_value="local") as mock_local:
            embeddings.embed_docs(["hello"])
        mock_local.assert_called_once()
        mock_post.assert_not_called()

    def test_embed_query_stays_local_even_when_clip_service_is_configured(self):
        """The regression this guards against: embed_query used to route to
        the remote clip service whenever CLIP_SERVICE_URL was set, putting
        it behind ingest's bulk embeds on that process's lock. It must NOT
        do that anymore, even though embed_docs still does."""
        with patch.object(embeddings.config, "CLIP_SERVICE_URL", "http://clip:8001"), \
             patch.object(embeddings.config, "TEXT_EMBED_PROVIDER", "fastembed"), \
             patch.object(embeddings, "_post") as mock_post, \
             patch.object(embeddings, "embed_query_local", return_value="local") as mock_local:
            embeddings.embed_query("hello")
        mock_local.assert_called_once()
        mock_post.assert_not_called()

    def test_embed_query_uses_local_model_when_service_unset(self):
        with patch.object(embeddings.config, "CLIP_SERVICE_URL", ""), \
             patch.object(embeddings.config, "TEXT_EMBED_PROVIDER", "fastembed"), \
             patch.object(embeddings, "_post") as mock_post, \
             patch.object(embeddings, "embed_query_local", return_value="local") as mock_local:
            embeddings.embed_query("hello")
        mock_local.assert_called_once()
        mock_post.assert_not_called()

    def test_embed_query_still_uses_openai_when_that_is_the_provider(self):
        """The always-local rule is specific to the fastembed/bge provider —
        TEXT_EMBED_PROVIDER=openai must still call the OpenAI embeddings API,
        not the local bge model, regardless of CLIP_SERVICE_URL."""
        with patch.object(embeddings.config, "CLIP_SERVICE_URL", "http://clip:8001"), \
             patch.object(embeddings.config, "TEXT_EMBED_PROVIDER", "openai"), \
             patch.object(embeddings, "embed_openai", return_value=[[0.1]]) as mock_openai, \
             patch.object(embeddings, "embed_query_local") as mock_local:
            embeddings.embed_query("hello")
        mock_openai.assert_called_once()
        mock_local.assert_not_called()


class EmbedLockSeparationTests(unittest.TestCase):
    """CLIP (torch) and BGE (fastembed) must use separate locks — a shared
    lock would serialize search's own query embed behind unrelated bulk
    ingest document embeds (and vice versa), which was a real, measured
    contributor to the decoupling-ratio SLA gate."""

    def test_clip_and_text_locks_are_distinct_objects(self):
        self.assertIsNot(embeddings._clip_lock, embeddings._text_lock)

    def test_embed_docs_local_sub_batches_and_releases_lock_between_batches(self):
        """A call spanning more than TEXT_EMBED_BATCH texts must acquire and
        release _text_lock more than once — proof the lock isn't held for
        the whole call, which is what lets a concurrent query embed
        interleave instead of queuing behind an entire large document."""
        texts = ["chunk"] * (embeddings.config.TEXT_EMBED_BATCH * 2 + 1)
        acquire_count = 0
        real_lock = embeddings._text_lock

        class CountingLock:
            def __enter__(self_inner):
                nonlocal acquire_count
                acquire_count += 1
                return real_lock.__enter__()

            def __exit__(self_inner, *exc):
                return real_lock.__exit__(*exc)

        mock_model = MagicMock()
        mock_model.embed.side_effect = lambda batch: [[0.0]] * len(batch)
        with patch.object(embeddings, "_text_lock", CountingLock()), \
             patch.object(embeddings, "_text_model", return_value=mock_model):
            embeddings.embed_docs_local(texts)
        self.assertEqual(acquire_count, 3)  # ceil(65 / 32) == 3
        self.assertEqual(mock_model.embed.call_count, 3)


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _FakeConnection:
    """Records every executed statement (normalized + params) in order and
    returns canned rows keyed by statement shape — enough to drive
    claim_pending() through its real control flow without a live Postgres."""

    def __init__(self, *, inflight_count: int, pending_ids: list[str],
                claimed_rows: list[dict]):
        self.calls: list[tuple[str, tuple]] = []
        self._inflight_count = inflight_count
        self._pending_ids = pending_ids
        self._claimed_rows = claimed_rows

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.calls.append((normalized, params))
        if "pg_advisory_xact_lock" in normalized:
            return _FakeCursor([])
        if "count(*)" in normalized:
            return _FakeCursor([{"n": self._inflight_count}])
        if normalized.startswith("SELECT id FROM"):
            return _FakeCursor([{"id": i} for i in self._pending_ids])
        if normalized.startswith("UPDATE ms_videos"):
            return _FakeCursor(self._claimed_rows)
        raise AssertionError(f"unexpected SQL in claim_pending(): {sql!r}")


class _FakeConnCtx:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def connection(self):
        return _FakeConnCtx(self._conn)


def _call_kind(sql: str) -> str:
    if "pg_advisory_xact_lock" in sql:
        return "lock"
    if "count(*)" in sql:
        return "count"
    if sql.startswith("SELECT id FROM"):
        return "select_pending"
    if sql.startswith("UPDATE"):
        return "update"
    return "other"


class ClaimPendingTests(unittest.TestCase):
    """db.claim_pending() replaces what used to be two separate, unlocked
    reads (count_inflight() then wfq_claim()) — a real race where two
    dispatchers (one per worker replica) could each compute `slots` from the
    same stale inflight count and collectively admit more than
    DISPATCH_MAX_INFLIGHT (found in review). These tests pin down the
    ordering and arithmetic that make cross-process serialization possible;
    they can't exercise the actual cross-process locking behavior itself
    without a live Postgres."""

    def test_takes_advisory_lock_before_reading_inflight_count(self):
        conn = _FakeConnection(inflight_count=2, pending_ids=["a", "b"],
                               claimed_rows=[{"id": "a"}, {"id": "b"},
                                            {"user_id": "u", "kind": "video", "generation": 1}])
        with patch.object(db, "pool", return_value=_FakePool(conn)):
            db.claim_pending(8, fair=True)
        kinds = [_call_kind(sql) for sql, _ in conn.calls]
        self.assertEqual(kinds[0], "lock")
        self.assertEqual(kinds[1], "count")

    def test_computes_slots_from_the_just_locked_count(self):
        conn = _FakeConnection(inflight_count=5, pending_ids=["a", "b", "c"],
                               claimed_rows=[{"id": "a"}, {"id": "b"}, {"id": "c"}])
        with patch.object(db, "pool", return_value=_FakePool(conn)):
            db.claim_pending(8, fair=True)  # cap=8, inflight=5 -> exactly 3 slots
        select_call = next(c for c in conn.calls if _call_kind(c[0]) == "select_pending")
        self.assertEqual(select_call[1], (3,))

    def test_returns_empty_and_never_attempts_a_claim_when_no_slots_remain(self):
        conn = _FakeConnection(inflight_count=8, pending_ids=[], claimed_rows=[])
        with patch.object(db, "pool", return_value=_FakePool(conn)):
            result = db.claim_pending(8, fair=True)  # cap == inflight -> 0 slots
        self.assertEqual(result, [])
        kinds = {_call_kind(sql) for sql, _ in conn.calls}
        self.assertNotIn("select_pending", kinds)
        self.assertNotIn("update", kinds)
        # The lock and the count are still taken — the whole point is that
        # even a tick with nothing to admit still serializes against every
        # other dispatcher's count-then-claim sequence.
        self.assertIn("lock", kinds)
        self.assertIn("count", kinds)

    def test_zero_or_negative_cap_short_circuits_without_any_query(self):
        conn = _FakeConnection(inflight_count=0, pending_ids=[], claimed_rows=[])
        with patch.object(db, "pool", return_value=_FakePool(conn)):
            result = db.claim_pending(0, fair=True)
        self.assertEqual(result, [])
        self.assertEqual(conn.calls, [])

    def test_fair_and_fifo_ordering_use_different_select_sql(self):
        conn_fair = _FakeConnection(inflight_count=0, pending_ids=[], claimed_rows=[])
        conn_fifo = _FakeConnection(inflight_count=0, pending_ids=[], claimed_rows=[])
        with patch.object(db, "pool", return_value=_FakePool(conn_fair)):
            db.claim_pending(1, fair=True)
        with patch.object(db, "pool", return_value=_FakePool(conn_fifo)):
            db.claim_pending(1, fair=False)
        fair_select = next(sql for sql, _ in conn_fair.calls if _call_kind(sql) == "select_pending")
        fifo_select = next(sql for sql, _ in conn_fifo.calls if _call_kind(sql) == "select_pending")
        self.assertIn("row_number()", fair_select)
        self.assertNotIn("row_number()", fifo_select)


if __name__ == "__main__":
    unittest.main()
