"""Fair dispatcher — the WFQ scheduler that sits in front of Prefect.

Why this exists: if the API enqueued every video to Prefect at register-time,
Prefect would run them in submitted order (FIFO) — one user who uploads 50
videos blocks everyone behind them. Instead, videos wait `pending` in Postgres
and THIS loop admits them:

  every DISPATCH_INTERVAL_S:
    slots = DISPATCH_MAX_INFLIGHT - (videos currently queued/running)
    claim up to `slots` pending videos in FAIR order (round-robin across users)
    schedule a Prefect run for each

Because only ~capacity videos are ever handed to Prefect at once, the *waiting
line lives in our DB, fairly ordered* (db.wfq_claim) rather than FIFO inside
Prefect. No user can starve the others. Set ENABLE_FAIR_DISPATCH=false to have
VIDEOS fall back to immediate FIFO enqueue at registration time (useful for
A/B teaching the difference) — but this dispatcher loop still always runs
regardless of that flag. Documents have no such bypass (ASSIGNMENT_AGENTS.md
non-negotiable #1: ingestion is never triggered from the request path), so
without this loop running, a pending document would never be picked up at
all under ENABLE_FAIR_DISPATCH=false — a real bug caught by review, not a
theoretical one, since that flag previously stopped the thread from starting.

Runs as a background thread in worker.py. With one worker that's exact; with
several, each runs a dispatcher — the atomic claim keeps videos handed out once,
at worst mildly over-admitting (harmless; Prefect still caps execution).
"""
from __future__ import annotations

import threading
import time

from . import config, db, jobs


def dispatch_once() -> int:
    """Admit as many fairly-chosen pending videos as free capacity allows.
    Returns how many were dispatched this tick."""
    slots = config.DISPATCH_MAX_INFLIGHT - db.count_inflight()
    if slots <= 0:
        return 0
    claimed = db.wfq_claim(slots)
    for row in claimed:
        try:
            if row.get("kind", "video") == "video":
                jobs.enqueue_video(row["id"], row["user_id"])
            else:
                jobs.enqueue_document(row["id"], row["user_id"], row["kind"])
        except Exception as exc:
            # Couldn't reach Prefect — put it back so it's retried next tick.
            db.set_status(row["id"], "pending", error=f"dispatch: {exc}")
    if claimed:
        print(f"[dispatch] admitted {len(claimed)} video(s) "
              f"({db.count_inflight()}/{config.DISPATCH_MAX_INFLIGHT} in flight)")
    return len(claimed)


def run_forever() -> None:
    mode = "fair (WFQ)" if config.ENABLE_FAIR_DISPATCH else "unfair (ENABLE_FAIR_DISPATCH=false — videos bypass this via direct enqueue; documents still route through here)"
    print(f"[dispatch] scheduler on [{mode}] — max in-flight "
          f"{config.DISPATCH_MAX_INFLIGHT}, tick {config.DISPATCH_INTERVAL_S}s")
    while True:
        try:
            dispatch_once()
        except Exception as exc:  # never let the scheduler thread die
            print(f"[dispatch] error: {type(exc).__name__}: {exc}")
        time.sleep(config.DISPATCH_INTERVAL_S)


def start_in_background() -> None:
    """Start the dispatcher as a daemon thread — always, regardless of
    ENABLE_FAIR_DISPATCH. That flag only controls whether VIDEOS bypass the
    queue via a direct enqueue at registration time (src/api/videos.py); it
    is not a toggle for "does background admission run at all". Documents
    always rely on this loop (see module docstring)."""
    threading.Thread(target=run_forever, daemon=True, name="dispatcher").start()
