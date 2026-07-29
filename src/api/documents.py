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
import ipaddress
import re
import urllib.parse

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from .. import db
from .videos import _PUBLIC_FIELDS, require_auth, user_id

router = APIRouter(prefix="/admin", tags=["documents"])

_URI_RE = re.compile(r"^https?://\S+$")
_KINDS = ("paper", "deck")


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
    if _obviously_unsafe_host(uri):
        raise HTTPException(400, "uri resolves to a non-public address range "
                                 "(loopback / link-local / private / metadata).")
    doc_id = _doc_id(uid, uri)
    row = db.upsert_pending({
        "id": doc_id, "user_id": uid, "source": "document",
        "url": None, "storage_key": None, "source_hash": uri,
        "title": req.title, "kind": req.kind, "uri": uri,
    })
    # Unlike the video path, this never falls back to a direct synchronous
    # enqueue: ASSIGNMENT_AGENTS.md non-negotiable #1 is explicit that
    # ingestion work happens on a worker, never in the request path. Always
    # insert-pending-and-return; only the dispatcher's background thread
    # (src/dispatcher.py) ever calls jobs.enqueue_document.
    return {"id": row["id"], "status": row["status"], "kind": row["kind"]}


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
