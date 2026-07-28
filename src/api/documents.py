"""Document registration API (papers + decks) — same contract shape as
src/api/videos.py's register endpoint, generalized to a second `kind`.

POST /admin/documents  -> 202 fast (enqueue-and-return, zero parsing here —
                          no PDF library is imported anywhere in this module)
GET  /admin/sources    -> unified video + document listing

Both source kinds land in the SAME `ms_videos` table (see src/db.py) — a
`kind` column, not a second table — so the WFQ dispatcher and db.wfq_claim()
need no rewrite to fairly admit documents alongside videos.
"""
from __future__ import annotations

import hashlib
import re

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from .. import config, db
from .videos import _PUBLIC_FIELDS, require_auth, user_id

router = APIRouter(prefix="/admin", tags=["documents"])

_URI_RE = re.compile(r"^https?://\S+$")
_KINDS = ("paper", "deck")


def _doc_id(uri: str) -> str:
    """Deterministic from the URI (like yt_<video_id> is deterministic from a
    YouTube URL) — re-registering the same URI updates the same row instead
    of piling up duplicates."""
    return "doc_" + hashlib.sha256(uri.encode()).hexdigest()[:12]


class RegisterDocument(BaseModel):
    uri: str
    kind: str
    title: str | None = None


@router.post("/documents", status_code=202, dependencies=[Depends(require_auth)])
def register_document(req: RegisterDocument, uid: str = Depends(user_id)):
    if req.kind not in _KINDS:
        raise HTTPException(400, f"kind must be one of {_KINDS}.")
    uri = req.uri.strip()
    if not _URI_RE.match(uri):
        raise HTTPException(400, "uri must be http(s) — fetchability is checked "
                                 "during ingestion, not at registration time.")
    doc_id = _doc_id(uri)
    row = db.upsert_pending({
        "id": doc_id, "user_id": uid, "source": "document",
        "url": None, "storage_key": None, "source_hash": uri,
        "title": req.title, "kind": req.kind, "uri": uri,
    })
    if config.ENABLE_FAIR_DISPATCH:
        return {"id": row["id"], "status": "pending", "kind": row["kind"]}
    from .. import jobs
    flow_run_id = jobs.enqueue_document(row["id"], uid, row["kind"])
    return {"id": row["id"], "status": row["status"], "kind": row["kind"],
            "flow_run_id": flow_run_id}


_SOURCE_FIELDS = _PUBLIC_FIELDS + ("kind", "uri", "chunk_count")


@router.get("/sources")
def list_sources(uid: str = Depends(user_id)):
    rows = db.list_videos(uid)
    out = []
    for r in rows:
        item = {k: r.get(k) for k in _SOURCE_FIELDS}
        item["pct"] = round((r.get("progress") or 0) * 100) if r.get("progress") is not None else None
        out.append(item)
    return {"sources": out}
