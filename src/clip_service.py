"""CLIP inference service — one warm model behind a URL.

    uvicorn src.clip_service:app --host 0.0.0.0 --port 8001

The model loads ONCE at boot and stays hot; workers and the API send batches
instead of each flow-run subprocess paying a fresh torch import + weight load
(~15-30s per video). This is the standard model-serving pattern (TEI / Triton
/ OpenAI-embeddings-shaped): inference is a URL, so scaling embedding means
scaling THIS one service — today a CPU container, later the same container on
a GPU machine — while workers stay cheap and stateless.

Wire-up: set CLIP_SERVICE_URL=http://clip:8001 on api + worker (docker-compose
does this by default). Unset, they embed in-process — simple mode, no service.

Endpoints:
  POST /embed/images  {"jpegs_b64": [...]}  -> {"vectors": [[...], ...]}
  POST /embed/text    {"text": "..."}       -> {"vector": [...]}
  GET  /healthz                             -> {"ok", "model", "dim"}
"""
from __future__ import annotations

import base64
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from . import config
from .config import CLIP_MODEL
from .rag import embeddings


@asynccontextmanager
async def lifespan(app: FastAPI):
    dim = embeddings.embedding_dim()  # load CLIP NOW, not on first request
    print(f"[clip] {CLIP_MODEL} warm (dim {dim})")
    # Only warm the local bge model when it's actually the text provider; with
    # TEXT_EMBED_PROVIDER=openai the transcript branch calls OpenAI directly and
    # never touches this service.
    if config.ENABLE_TRANSCRIPT and config.TEXT_EMBED_PROVIDER != "openai":
        embeddings.embed_docs_local(["warmup"])  # load the bge text model too
        print(f"[clip] text model {config.TEXT_EMBED_MODEL} warm")
    yield


app = FastAPI(title="MomentSearch CLIP service", lifespan=lifespan)


class ImagesRequest(BaseModel):
    jpegs_b64: list[str]


class TextRequest(BaseModel):
    text: str


class DocsRequest(BaseModel):
    texts: list[str]


@app.get("/healthz")
def healthz():
    return {"ok": True, "model": CLIP_MODEL, "dim": embeddings.embedding_dim()}


@app.post("/embed/images")
def embed_images(req: ImagesRequest):
    jpegs = [base64.b64decode(j) for j in req.jpegs_b64]
    return {"vectors": embeddings.embed_jpegs_local(jpegs).tolist()}


@app.post("/embed/text")
def embed_text(req: TextRequest):
    return {"vector": embeddings.embed_text_local(req.text).tolist()}


# ── Transcript branch (bge semantic text) ────────────────────────────────────

@app.post("/embed/docs")
def embed_docs(req: DocsRequest):
    return {"vectors": embeddings.embed_docs_local(req.texts).tolist()}


@app.post("/embed/query")
def embed_query(req: TextRequest):
    """DEPRECATED — nothing in this codebase calls this anymore: the API now
    always embeds search queries locally (embeddings.embed_query, see
    src/app.py's startup warmup), never over HTTP, so it never contends with
    this service's bulk document-embedding lock. Kept only as a rolling-
    deployment safety net: during a Fly deploy, an old-image api machine can
    briefly keep serving traffic while a new-image clip machine is already
    up (Fly replaces machines per-process-group, not as one atomic
    cross-group cutover) — that old api code still calls this route. Safe
    to delete once no api machine older than this endpoint's removal is
    running."""
    return {"vector": embeddings.embed_query_local(req.text).tolist()}
