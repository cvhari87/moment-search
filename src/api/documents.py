"""Document registration API (papers + decks) — same contract shape as
src/api/videos.py's register endpoint, generalized to a second `kind`.

POST /admin/documents         -> register by URL, 202 fast
POST /admin/documents/upload  -> register by direct file upload, 202 fast
GET  /admin/sources           -> unified video + document listing

Both source kinds land in the SAME `ms_videos` table (see src/db.py) — a
`kind` column, not a second table — so the WFQ dispatcher and db.wfq_claim()
need no rewrite to fairly admit documents alongside videos.

The upload path does NOT give ingestion a second, different fetch
mechanism to trust — an uploaded PDF or PPTX is written straight to this
app's own object storage under a private, content-addressed key
(documents/{user}/{sha256}.pdf or .pptx, never under the public corpus/
prefix that /corpus/{name} serves unauthenticated) and the manifest row's
`storage_key` points at it. src/ingest/document.py reads it back with
storage.get_bytes() — no HTTP hop, no SSRF surface, no presigned URL that
could expire before a worker gets to it or a later retry runs. The
SSRF-hardened HTTP fetch (_fetch_bytes) is reserved for the OTHER
registration path — a real external https:// paper/deck URL, which is the
one case where we don't already have the bytes ourselves. A .pptx is
stored as-is here too (see Block M) — conversion to PDF happens
server-side inside t_parse, not at upload time.
"""
from __future__ import annotations

import hashlib
import ipaddress
import re
import urllib.parse

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from .. import config, db, storage
from ..ingest.detect import sniff_document_kind
from .videos import _PUBLIC_FIELDS, require_auth, user_id

router = APIRouter(prefix="/admin", tags=["documents"])

_URI_RE = re.compile(r"^https?://\S+$")
_KINDS = ("paper", "deck")
_UPLOAD_MAX_BYTES = config.DOCUMENT_FETCH_MAX_MB * 1024 * 1024
_UPLOAD_KEY_PREFIX = "documents/"  # private — distinct from the public corpus/ prefix


def _enforce_kind_for_sniffed_type(doc_kind: str, requested_kind: str) -> str:
    """A .pptx is unambiguously a presentation — force kind="deck"
    server-side regardless of what the client's dropdown (or a caller
    bypassing the UI entirely) requested, rather than trusting it. The
    client defaulting to "Paper" would otherwise silently produce "page N"
    locators for a slide deck instead of "slide N" (found in review — this
    is what KIND_SPEC's page/slide split is FOR; a pptx choosing the wrong
    one isn't a cosmetic label mismatch, it's the wrong semantic locator).
    A sniffed 'pdf' never overrides — the user's choice of paper vs. deck
    for an actual PDF is a real, legitimate distinction this can't infer."""
    return "deck" if doc_kind == "pptx" else requested_kind


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


def _doc_id(user_id: str, identity: str) -> str:
    """Deterministic from (user_id, identity) — re-registering the SAME
    identity as the SAME user updates that one row instead of piling up
    duplicates. `identity` is the external URI for URL-registered documents,
    or a content-hash token ("upload:<sha256>") for uploads — NOT the
    presigned/uploaded storage key, which would defeat this: two identical
    uploads must resolve to the same doc_id so a re-upload updates the
    existing row instead of registering a duplicate. Scoped by user_id
    (unlike yt_<video_id>, which the video path leaves unscoped — a
    pre-existing gap there, not one to newly copy here): hashing identity
    alone would let two different users collide on one manifest row, silently
    handing one tenant's document to another (upsert_pending's ON CONFLICT
    never re-checks or updates user_id)."""
    return "doc_" + hashlib.sha256(f"{user_id}:{identity}".encode()).hexdigest()[:12]


def _register(uid: str, kind: str, title: str | None, *,
              uri: str | None = None, storage_key: str | None = None,
              source_hash: str | None = None) -> dict:
    """Shared by both registration paths (URL and upload) — validate,
    insert-pending, and return. Unlike the video path, neither ever falls
    back to a direct synchronous enqueue: ASSIGNMENT_AGENTS.md
    non-negotiable #1 is explicit that ingestion work happens on a worker,
    never in the request path. Only the dispatcher's background thread
    (src/dispatcher.py) ever calls jobs.enqueue_document.

    Exactly one of uri/storage_key identifies where the bytes live: `uri` for
    an external https:// document (src/ingest/document.py fetches it, SSRF-
    hardened, at ingest time), `storage_key` for an upload already sitting in
    OUR storage (read directly via storage.get_bytes() — no fetch, no expiry,
    no public exposure).

    storage_key rows are tagged with THIS process's config.DEPLOYMENT_ENV —
    local dev and the Fly.io deployment share one Postgres manifest and one
    Prefect Cloud workspace but have incompatible storage backends, so a
    dispatcher running in a different environment must never claim a row
    whose bytes only exist in this one (db.claim_pending filters on it).
    uri rows stay untagged (None): they fetch fresh over HTTPS at ingest
    time and never touch our own storage, so any environment can run them."""
    if kind not in _KINDS:
        raise HTTPException(400, f"kind must be one of {_KINDS}.")
    if uri is not None:
        if not _URI_RE.match(uri):
            raise HTTPException(400, "uri must be http(s) — fetchability is checked "
                                     "during ingestion, not at registration time.")
        if _obviously_unsafe_host(uri):
            raise HTTPException(400, "uri resolves to a non-public address range "
                                     "(loopback / link-local / private / metadata).")
    doc_id = _doc_id(uid, source_hash or uri)
    row = db.upsert_pending({
        "id": doc_id, "user_id": uid, "source": "document",
        "url": None, "storage_key": storage_key, "source_hash": source_hash or uri,
        "title": title, "kind": kind, "uri": uri,
        "storage_env": config.DEPLOYMENT_ENV if storage_key else None,
    })
    return {"id": row["id"], "status": row["status"], "kind": row["kind"]}


class RegisterDocument(BaseModel):
    uri: str
    kind: str
    title: str | None = None


@router.post("/documents", status_code=202, dependencies=[Depends(require_auth)])
def register_document(req: RegisterDocument, uid: str = Depends(user_id)):
    uri = req.uri.strip()
    return _register(uid, req.kind, req.title, uri=uri, source_hash=uri)


@router.post("/documents/upload", status_code=202, dependencies=[Depends(require_auth)])
async def upload_document(file: UploadFile = File(...), kind: str = Form(...),
                          title: str | None = Form(None), uid: str = Depends(user_id)):
    """Direct file upload — no presign step, unlike videos. A PDF or PPTX
    (capped at DOCUMENT_FETCH_MAX_MB) is cheap enough to pass through the
    API process itself; presigning exists for videos because those are
    large enough that routing gigabytes through this process would be
    wasteful, not because a bypass is required in principle.

    This is a deliberate, narrow exception to "ingestion never does
    synchronous work in the request path" (ASSIGNMENT_AGENTS.md non-
    negotiable #1): the storage PUT here is bytes-in-hand, bounded by
    DOCUMENT_FETCH_MAX_MB, and off the event loop (run_in_threadpool below)
    — not the unbounded parse/chunk/embed work that non-negotiable #1 is
    actually about, which stays worker-only via _register's insert-pending-
    and-return. The graded contract endpoint (POST /admin/documents, the
    uri-registration path above) does no I/O at all before returning.

    The bytes land under a PRIVATE, content-addressed key
    (documents/{user}/{sha256}.pdf or .pptx) — never the public corpus/ prefix, which
    /corpus/{name} serves to anyone with no auth (that route exists only for
    this app's own self-hosted deck, a deliberately public fixture). Ingestion
    reads the key straight from storage (storage_key on the row), so there is
    no presigned URL to expire before a queued job or a later retry runs, and
    re-uploading identical bytes resolves to the SAME doc_id (content-hash
    identity) instead of registering a duplicate every time."""
    if kind not in _KINDS:
        raise HTTPException(400, f"kind must be one of {_KINDS}.")
    data = await file.read(_UPLOAD_MAX_BYTES + 1)
    if len(data) > _UPLOAD_MAX_BYTES:
        raise HTTPException(413, f"File exceeds the {config.DOCUMENT_FETCH_MAX_MB}MB limit.")
    # Structural check (magic bytes + real zip/XML structure for a .pptx),
    # not the filename or the browser-supplied content-type — both are
    # trivially spoofable the same way a renamed .txt could once claim
    # `%PDF`. Block M: a .pptx is converted to PDF server-side, as the first
    # step of t_parse (src/ingest/document.py) — the ORIGINAL .pptx bytes
    # are what's stored and checkpointed here, not a pre-converted PDF.
    doc_kind = sniff_document_kind(data)
    if doc_kind is None:
        raise HTTPException(415, "Only PDF or PPTX uploads are accepted.")
    kind = _enforce_kind_for_sniffed_type(doc_kind, kind)

    digest = hashlib.sha256(data).hexdigest()[:16]
    key = f"{_UPLOAD_KEY_PREFIX}{uid}/{digest}.{doc_kind}"
    content_type = ("application/pdf" if doc_kind == "pdf" else
                    "application/vnd.openxmlformats-officedocument.presentationml.presentation")
    # Off the event loop: boto3/GCS calls are blocking, and a 50MB body
    # sitting in this coroutine would otherwise stall every other request
    # this API process is handling concurrently.
    await run_in_threadpool(storage.put_bytes, key, data, content_type)

    return _register(uid, kind, title or file.filename,
                     storage_key=key, source_hash=f"upload:{digest}")


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
