"""Prefect Cloud trigger layer — the API schedules runs, workers execute them.

One flow ("ms-ingest-video" — the "ms-" prefix keeps it distinct from the
digital-twin-akash flow living in the same Prefect workspace), one deployment
("ingest", registered by worker.py's flow.serve()). The API never imports the
pipeline or its heavy deps (torch, ffmpeg) — it just asks Prefect Cloud to
schedule a run; any live worker picks it up. Retries/backoff live on the
flow's tasks (src/ingest/pipeline.py); failed runs are visible + retryable in
the Prefect Cloud UI.
"""
from __future__ import annotations

from prefect.deployments import run_deployment

INGEST_DEPLOYMENT = "ms-ingest-video/ingest"
INGEST_DOCUMENT_DEPLOYMENT = "ms-ingest-document/ingest"


def enqueue_video(video_id: str, user_id: str, generation: int) -> str:
    """Schedule the ingest flow for one video. Returns the Prefect flow-run id.

    `generation` is the lease token db.wfq_claim() just bumped for this row —
    threaded through to every status write the flow makes (src/db.py's
    generation guard), so a reconciler-triggered resume can never have its
    writes clobbered by a stale, still-running earlier attempt."""
    flow_run = run_deployment(
        name=INGEST_DEPLOYMENT,
        parameters={"video_id": video_id, "user_id": user_id, "generation": generation},
        timeout=0,  # fire-and-forget: don't block the API waiting for the run
        flow_run_name=f"ingest-{video_id}",
    )
    return str(flow_run.id)


def enqueue_document(doc_id: str, user_id: str, kind: str, generation: int) -> str:
    """Schedule the ingest flow for one document (paper or deck). Same
    fire-and-forget shape and generation-token threading as enqueue_video;
    the flow itself is registered by worker.py (see src/ingest/document.py)."""
    flow_run = run_deployment(
        name=INGEST_DOCUMENT_DEPLOYMENT,
        parameters={"doc_id": doc_id, "user_id": user_id, "kind": kind, "generation": generation},
        timeout=0,
        flow_run_name=f"ingest-{doc_id}",
    )
    return str(flow_run.id)
