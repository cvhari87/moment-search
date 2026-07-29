"""Document registration API (papers + decks) — same contract shape as
src/api/videos.py's register endpoint, generalized to a second `kind`.

POST /admin/documents         -> register by URL, 202 fast
POST /admin/documents/upload  -> register by direct file upload, 202 fast
GET  /admin/sources           -> unified video + document listing

Both source kinds land in the SAME `ms_videos` table (see src/db.py) — a
`kind` column, not a second table — so the WFQ dispatcher and db.wfq_claim()
need no rewrite to fairly admit documents alongside videos.

The upload path does NOT give ingestion a second, different fetch
mechanism to trust — it writes the bytes to the same place our own corpus
deck already lives (data/corpus/, served by GET /corpus/{name} in
src/api/search.py) and then calls the SAME uri-based registration path
below. One fetch path (src/ingest/document.py's SSRF-hardened
_fetch_bytes), reused, not duplicated — the same reasoning as why the
deck itself is self-hosted there rather than given its own file:// special
case.
"""
from __future__ import annotations

import hashlib
import ipaddress
import re
import urllib.parse

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from .. import config, db, storage
from .videos import _PUBLIC_FIELDS, require_auth, user_id

router = APIRouter(prefix="/admin", tags=["documents"])

_URI_RE = re.compile(r"^https?://\S+$")
_KINDS = ("paper", "deck")
_UPLOAD_MAX_BYTES = config.DOCUMENT_FETCH_MAX_MB * 1024 * 1024


def _obviously_unsafe_host(uri: str) -> bool:
    """Cheap, no-DNS reject for literal blocked-range IPs (loopback,
    link-local — 169.254.169.254 cloud metadata most of all — private
    RFC1918, multicast, reserved) typed directly into the uri — a fast first
    line of defense that stays well inside the sub-300ms budget. Domain
    names pass through untouched, INCLUDING our own allowlisted "api"
    hostname (it isn't a literal IP, so it never reaches this check at all);
    the authoritative, DNS-resolved, allowlist-aware, redirect-safe check
    runs at fetch time in src/ingest/document.py, not here — registration
    must not do a network call."""
    host = (urllib.parse.urlparse(uri).hostname or "").strip("[]")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False  # a domain name, not a literal IP — resolved later
    return (ip.is_loopback or ip.is_link_local or ip.is_private
            or ip.is_multicast or ip.is_unspecified or ip.is_reserved)


def _doc_id(user_id: str, uri: str) -> str:
    """Deterministic from (user_id, uri) — re-registering the same URI as the
    SAME user updates that one row instead of piling up duplicates. Scoped by
    user_id (unlike yt_<video_id>, which the video path leaves unscoped — a
    pre-existing gap there, not one to newly copy here): hashing the URI alone
    would let two different users collide on one manifest row, silently
    handing one tenant's document to another (upsert_pending's ON CONFLICT
    never re-checks or updates user_id)."""
    return "doc_" + hashlib.sha256(f"{user_id}:{uri}".encode()).hexdigest()[:12]


def _register(uid: str, uri: str, kind: str, title: str | None) -> dict:
    """Shared by both registration paths (URL and upload) — validate,
    insert-pending, and return. Unlike the video path, neither ever falls
    back to a direct synchronous enqueue: ASSIGNMENT_AGENTS.md
    non-negotiable #1 is explicit that ingestion work happens on a worker,
    never in the request path. Only the dispatcher's background thread
    (src/dispatcher.py) ever calls jobs.enqueue_document."""
    if kind not in _KINDS:
        raise HTTPException(400, f"kind must be one of {_KINDS}.")
    if not _URI_RE.match(uri):
        raise HTTPException(400, "uri must be http(s) — fetchability is checked "
                                 "during ingestion, not at registration time.")
    if _obviously_unsafe_host(uri):
        raise HTTPException(400, "uri resolves to a non-public address range "
                                 "(loopback / link-local / private / metadata).")
    doc_id = _doc_id(uid, uri)
    row = db.upsert_pending({
        "id": doc_id, "user_id": uid, "source": "document",
        "url": None, "storage_key": None, "source_hash": uri,
        "title": title, "kind": kind, "uri": uri,
    })
    return {"id": row["id"], "status": row["status"], "kind": row["kind"]}


class RegisterDocument(BaseModel):
    uri: str
    kind: str
    title: str | None = None


@router.post("/documents", status_code=202, dependencies=[Depends(require_auth)])
def register_document(req: RegisterDocument, uid: str = Depends(user_id)):
    return _register(uid, req.uri.strip(), req.kind, req.title)


@router.post("/documents/upload", status_code=202, dependencies=[Depends(require_auth)])
async def upload_document(file: UploadFile = File(...), kind: str = Form(...),
                          title: str | None = Form(None), uid: str = Depends(user_id)):
    """Direct file upload — no presign step, unlike videos. A PDF (capped at
    DOCUMENT_FETCH_MAX_MB) is cheap enough to pass through the API process
    itself; presigning exists for videos because those are large enough
    that routing gigabytes through this process would be wasteful, not
    because a bypass is required in principle."""
    if kind not in _KINDS:
        raise HTTPException(400, f"kind must be one of {_KINDS}.")
    data = await file.read(_UPLOAD_MAX_BYTES + 1)
    if len(data) > _UPLOAD_MAX_BYTES:
        raise HTTPException(413, f"File exceeds the {config.DOCUMENT_FETCH_MAX_MB}MB limit.")
    if not data.startswith(b"%PDF"):
        raise HTTPException(415, "Only PDF uploads are accepted.")

    digest = hashlib.sha256(data).hexdigest()[:16]
    if storage.presign_capable():
        key = f"documents/{uid}/{digest}.pdf"
        storage.put_bytes(key, data, "application/pdf")
        uri = storage.presign_get(key)
    else:
        # Reuses the exact local-serving mechanism the corpus deck already
        # depends on (GET /corpus/{name} in src/api/search.py) rather than
        # inventing a second local-file trust path — one fetch mechanism
        # for the ingestion pipeline to reason about, not two.
        filename = f"{uid}_{digest}.pdf"
        storage.put_bytes(f"corpus/{filename}", data, "application/pdf")
        uri = f"http://api:8000/corpus/{filename}"

    return _register(uid, uri, kind, title or file.filename)


_SOURCE_FIELDS = _PUBLIC_FIELDS + ("kind", "uri", "chunk_count")


@router.get("/sources", dependencies=[Depends(require_auth)])
def list_sources(uid: str = Depends(user_id)):
    rows = db.list_videos(uid)
    out = []
    for r in rows:
        item = {k: r.get(k) for k in _SOURCE_FIELDS}
        item["pct"] = round((r.get("progress") or 0) * 100) if r.get("progress") is not None else None
        out.append(item)
    return {"sources": out}
