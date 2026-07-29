"""Dispatcher — the ONLY path that ever schedules a Prefect run.

Why this exists: if the API enqueued every source to Prefect at
registration-time, Prefect would run them in submitted order (FIFO) — one
user who uploads 50 videos blocks everyone behind them. Instead, every
source (video, paper, deck) waits `pending` in Postgres and THIS loop
admits them:

  every DISPATCH_INTERVAL_S:
    slots = DISPATCH_MAX_INFLIGHT - (sources currently queued/running)
    claim up to `slots` pending sources (order below) -> pending -> queued
    schedule a Prefect run for each

No other code path ever calls jobs.enqueue_video / jobs.enqueue_document.
Earlier this had a direct-enqueue fallback at registration time when
ENABLE_FAIR_DISPATCH=false (videos only — documents never had one, per
ASSIGNMENT_AGENTS.md non-negotiable #1: ingestion never triggers from the
request path). That was a real bug: enqueue_video() doesn't itself update
status, so the row stayed 'pending' for as long as Prefect took to actually
start the run — easily longer than one dispatcher tick — and this loop
would legitimately re-claim and re-enqueue the same video a second time.
Fixed by removing every bypass: this loop is now the sole admission path,
always running, for every source kind, in both modes.

ENABLE_FAIR_DISPATCH now controls ORDERING only (db.wfq_claim's `fair`
flag): true = round-robin across users (no one starves); false = plain
FIFO by creation time — "useful for A/B teaching the difference," not a
bypass of this loop.

Runs as a background thread in worker.py. With one worker that's exact; with
several, each runs a dispatcher — the atomic claim keeps sources handed out
once, at worst mildly over-admitting (harmless; Prefect still caps execution).
"""
from __future__ import annotations

import threading
import time

from . import config, db, jobs


def dispatch_once() -> int:
    """Admit as many pending sources as free capacity allows, in
    ENABLE_FAIR_DISPATCH order. Returns how many were dispatched this tick."""
    slots = config.DISPATCH_MAX_INFLIGHT - db.count_inflight()
    if slots <= 0:
        return 0
    claimed = db.wfq_claim(slots, fair=config.ENABLE_FAIR_DISPATCH)
    for row in claimed:
        try:
            if row.get("kind", "video") == "video":
                jobs.enqueue_video(row["id"], row["user_id"], row["generation"])
            else:
                jobs.enqueue_document(row["id"], row["user_id"], row["kind"], row["generation"])
        except Exception as exc:
            # Couldn't reach Prefect — put it back so it's retried next tick.
            # Gated by the generation wfq_claim just minted for this row, same
            # as any other write: harmless either way since nothing else could
            # have raced a newer admission in the few lines between claim and
            # here, but keeps this call site consistent with every other
            # in-flow write instead of being the one unguarded exception.
            db.set_status(row["id"], "pending", error=f"dispatch: {exc}", generation=row["generation"])
    if claimed:
        print(f"[dispatch] admitted {len(claimed)} source(s) "
              f"({db.count_inflight()}/{config.DISPATCH_MAX_INFLIGHT} in flight)")
    return len(claimed)


def run_forever() -> None:
    order = "fair (WFQ, round-robin across users)" if config.ENABLE_FAIR_DISPATCH else "FIFO (by creation time)"
    print(f"[dispatch] scheduler on [{order}] — max in-flight "
          f"{config.DISPATCH_MAX_INFLIGHT}, tick {config.DISPATCH_INTERVAL_S}s")
    while True:
        try:
            dispatch_once()
        except Exception as exc:  # never let the scheduler thread die
            print(f"[dispatch] error: {type(exc).__name__}: {exc}")
        time.sleep(config.DISPATCH_INTERVAL_S)


def start_in_background() -> None:
    """Start the dispatcher as a daemon thread — always, unconditionally.
    This is the ONLY admission path (see module docstring); ENABLE_FAIR_DISPATCH
    selects ordering inside it, it does not gate whether it runs."""
    threading.Thread(target=run_forever, daemon=True, name="dispatcher").start()
