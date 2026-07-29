"""Ingest worker entrypoint — serves the Prefect flow.

    python -m src.worker

flow.serve() registers the "ms-ingest-video/ingest" deployment in Prefect Cloud
(idempotent) and long-polls for scheduled runs — outbound HTTPS only, no
ports. Scale horizontally by running more replicas of this process; each
executes up to WORKER_CONCURRENCY runs at once.

Sample seeding is NOT done here — it's a one-shot startup gate (seed.py /
src/seeding.py) that the whole stack waits on, so the app never serves a
half-indexed corpus. This worker only handles user uploads + YouTube adds.

Embedding goes to the warm CLIP service when CLIP_SERVICE_URL is set
(docker-compose default); unset, each run loads the model in-process.
"""
import os
import time

from prefect import serve
from prefect.deployments.runner import EntrypointType

from .db import init_schema
from .ingest.document import ingest_document
from .ingest.pipeline import ingest_video


def main():
    init_schema()  # make sure migrations ran before consuming runs
    from .rag import vector_store
    vector_store.ensure_collection()  # up front, not mid-first-ingest
    # Fair scheduler (WFQ): admits pending sources round-robin across users so
    # one bulk uploader can't starve everyone else (src/dispatcher.py). This is
    # now the ONLY path to Prefect for every source kind — no API route ever
    # enqueues directly.
    from . import dispatcher
    dispatcher.start_in_background()
    # Staleness sweep (Block G): recovers sources orphaned by a hard-killed
    # worker process, which Prefect's own task retries never see happen.
    from . import reconciler
    reconciler.start_in_background()
    limit = int(os.getenv("WORKER_CONCURRENCY", "2"))
    # serve() talks to Prefect Cloud on startup; a transient outage (e.g. a 503)
    # used to crash the worker permanently and stop the machine. Self-heal:
    # retry forever so a blip pauses ingest instead of killing the worker.
    # `limit` is a single runner-wide concurrency cap shared across BOTH
    # deployments below (not per-deployment) — same total-concurrency meaning
    # WORKER_CONCURRENCY always had, just now spanning two flows.
    while True:
        try:
            print(f"[worker] serving 'ms-ingest-video/ingest' + "
                  f"'ms-ingest-document/ingest' (concurrency {limit})")
            # entrypoint_type=MODULE_PATH is load-bearing, not cosmetic: Prefect's
            # default (FILE_PATH, e.g. "src/ingest/pipeline.py:ingest_video")
            # unconditionally loads the flow run's subprocess via
            # load_script_as_module() — which executes the file standalone, NOT
            # as part of the `src` package, so every relative import in it
            # (`from .. import db, storage`) raises ImportError at flow-run time.
            # MODULE_PATH stores a dotted entrypoint ("src.ingest.pipeline:
            # ingest_video") instead, which Prefect loads via a normal
            # importlib.import_module() — relative imports resolve correctly
            # because the module is loaded as part of its real package. This
            # broke BOTH flows (video included) the first time multiple
            # deployments were served from one process via to_deployment(); a
            # single flow.serve() call never hit it because it manages its own
            # entrypoint resolution differently.
            serve(
                ingest_video.to_deployment(name="ingest", entrypoint_type=EntrypointType.MODULE_PATH),
                ingest_document.to_deployment(name="ingest", entrypoint_type=EntrypointType.MODULE_PATH),
                limit=limit,
            )
            break  # clean shutdown
        except KeyboardInterrupt:
            break
        except Exception as exc:
            print(f"[worker] serve crashed: {type(exc).__name__}: {exc} — retrying in 15s")
            time.sleep(15)


if __name__ == "__main__":
    main()
