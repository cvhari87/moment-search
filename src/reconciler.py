"""Reconciler — the staleness sweep that makes crash recovery real, not
assumed (Block G).

Prefect's own @task(retries=N) only catches an exception raised
*in-process*; it does nothing when the worker's OS process is hard-killed
mid-task (docker kill, OOM, host crash) — the row is simply left sitting in
whatever in-flight status (queued/fetching/sampling/parsing/chunking/
embedding) it last had, with a now-stale updated_at, forever. Nothing inside
that dead process can run to fix it, so recovery has to come from the
outside: this sweep periodically finds sources stuck past
RECONCILE_STALE_S and resets them to 'pending' so the dispatcher fairly
re-admits them. Both ingest flows are checkpointed (src/ingest/document.py's
parse/chunk artifacts; src/ingest/pipeline.py's idempotent full-redo), so the
resumed run either skips straight past already-committed stages or safely
redoes idempotent ones — never duplicates data.

Poison-URI DLQ: a source that keeps getting swept (crashes or times out on
every attempt — a file that panics the parser, say, rather than one that
just fails a clean fetch) stops being resurrected once its total attempts
(db.bump_attempts, counted across resumes) reach MAX_INGEST_ATTEMPTS — the
sweep marks it 'failed' instead of 'pending'. A merely-unfetchable/non-PDF
URI (Trap 3's actual case) doesn't even need this: _fetch_bytes raises
cleanly, the task's own retries exhaust in one flow run, and
ingest_document's except-block already lands it in 'failed' well before it'd
ever go stale (see src/ingest/document.py).
"""
from __future__ import annotations

import threading
import time

from . import config, db


def sweep_once() -> list[dict]:
    """Reset sources stuck in-flight past the staleness threshold back to
    pending (or to failed if they've exhausted their attempt budget).
    Returns the affected rows."""
    rows = db.reconcile_stale(config.RECONCILE_STALE_S, config.MAX_INGEST_ATTEMPTS)
    resumed = [r["id"] for r in rows if r["status"] == "pending"]
    dlq = [r["id"] for r in rows if r["status"] == "failed"]
    if resumed:
        print(f"[reconcile] stale -> pending (will resume from checkpoint): {resumed}")
    if dlq:
        print(f"[reconcile] stale AND out of attempts -> failed (dead-lettered): {dlq}")
    return rows


def run_forever() -> None:
    print(f"[reconcile] sweep on — stale threshold {config.RECONCILE_STALE_S}s, "
          f"tick {config.RECONCILE_INTERVAL_S}s, max attempts {config.MAX_INGEST_ATTEMPTS}")
    while True:
        try:
            sweep_once()
        except Exception as exc:  # never let the sweep thread die
            print(f"[reconcile] error: {type(exc).__name__}: {exc}")
        time.sleep(config.RECONCILE_INTERVAL_S)


def start_in_background() -> None:
    """Start the sweep as a daemon thread — mirrors dispatcher.start_in_background()."""
    threading.Thread(target=run_forever, daemon=True, name="reconciler").start()
