"""MomentSearch — unified API (one service, one port).

Two routers on one FastAPI app (:8000):
  - src/api/videos.py  /api/videos/*  — presigned uploads + registration +
                                        ingest status (Bearer auth)
  - src/api/search.py  public         — / (web UI), /api/ask, /api/config,
                                        local-dev media, /api/health

Heavy processing never happens here — the videos router only schedules Prefect
flow runs; worker.py (separate process, same image) executes the ingest
pipeline. Every durable byte lives in object storage, Qdrant, or Postgres, so
this process is stateless and disposable.

Run:
    uvicorn src.app:app --port 8000
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import config, db
from .api.documents import router as documents_router
from .api.search import router as search_router
from .api.videos import router as videos_router
from .rag import embeddings, vector_store


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_schema()
    # Regenerate the corpus deck PDF from its tracked Python source on every
    # boot — the rendered PDF is never committed (ASSIGNMENT_AGENTS.md
    # non-negotiable #7), so it must exist wherever this process runs: local
    # dev, CI, or the Fly image. Deterministic + idempotent, cheap either way.
    try:
        from corpus.generate_deck import build_deck
        build_deck(config.DATA / "corpus" / "one-index-for-every-source-deck.pdf")
    except Exception as exc:
        print(f"[startup] corpus deck generation failed ({exc!r}) — /corpus/* will 404")
    # Create the Qdrant collection up front (known CLIP dims resolve without
    # loading the model) so a question before the first ingest returns
    # "no moments" instead of a 500. Qdrant being down must not block boot.
    try:
        vector_store.ensure_collection()          # visual (CLIP frames)
        if config.ENABLE_TRANSCRIPT:
            vector_store.ensure_text_collection()  # transcript (bge text)
    except Exception as exc:
        print(f"[startup] Qdrant not ready ({exc!r}) — search degrades to empty results")
    # embed_query() always runs the bge model LOCALLY in this process now
    # (see src/rag/embeddings.py) — even with CLIP_SERVICE_URL set, unlike
    # embed_docs, which still routes bulk ingest embedding to that shared
    # service. A search query is one small, latency-critical call; running
    # it in the same process as ingest's bulk document embeds meant it
    # contended for the same lock (Block I). Warm it now, not on the first
    # question, same reasoning as clip_service.py's own warmup.
    if config.ENABLE_TRANSCRIPT and config.TEXT_EMBED_PROVIDER != "openai":
        try:
            embeddings.embed_query_local("warmup")
            print(f"[startup] local query-embed model {config.TEXT_EMBED_MODEL} warm")
        except Exception as exc:
            print(f"[startup] query-embed warmup failed ({exc!r}) — first search will be slow")
    yield


app = FastAPI(title="MomentSearch", version="1.0.0", lifespan=lifespan)
app.include_router(videos_router)
app.include_router(documents_router)
app.include_router(search_router)
