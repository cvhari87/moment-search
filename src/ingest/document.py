"""Per-document ingest pipeline (papers, decks) — a Prefect flow of three
stage-tasks, checkpointed from the start.

pending -> parsing -> chunking -> embedding -> indexed | failed

Stages:
  1. parse   fetch the PDF into memory (size-capped, magic-byte-checked),
             extract per-page text with real page numbers -> commit
             docs/{user}/{id}/parsed.json
  2. chunk   page-aware chunking (never spans two pages — the page number
             IS the locator) -> commit docs/{user}/{id}/chunks.json
  3. embed   text-embed the chunks -> idempotent Qdrant upsert into the SAME
             TEXT_COLLECTION transcript chunks already live in

Checkpointing is built in, not retrofitted: each of parse/chunk checks
storage.exists() for its own artifact FIRST and skips straight to loading it
if already committed, so a worker killed between chunk-commit and the
embed/upsert resumes at embed — parse and chunk never re-run.

Unlike src/ingest/pipeline.py's t_embed_index (videos), this does NOT delete
existing points before re-upserting. Two reasons: (1) deterministic point IDs
(vector_store.upsert_chunks, uuid5 of "<doc_id>:text:<i>") already make
re-upserting safe — same chunks in the same order overwrite in place, no
duplication; (2) deleting first would open a real window where a document
that was already indexed and searchable becomes unsearchable mid-re-run —
exactly the window eval.py's own documents_async check can hit, since it
re-submits the locked paper (resetting it to 'pending') and then immediately
queries for a citation with no wait in between.
"""
from __future__ import annotations

import http.client
import ipaddress
import json
import socket
import ssl
import urllib.parse

from prefect import flow, task

from .. import db, storage
from ..config import (
    DOCUMENT_CHUNK_CHARS,
    DOCUMENT_FETCH_ALLOWED_INTERNAL_HOSTS,
    DOCUMENT_FETCH_MAX_MB,
    DOCUMENT_MAX_CHUNKS,
    TEXT_EMBED_VERSION,
)
from ..rag import vector_store
from ..rag.embeddings import embed_docs

_MAX_BYTES = DOCUMENT_FETCH_MAX_MB * 1024 * 1024


def _parsed_key(user_id: str, doc_id: str) -> str:
    return f"docs/{user_id}/{doc_id}/parsed.json"


def _chunks_key(user_id: str, doc_id: str) -> str:
    return f"docs/{user_id}/{doc_id}/chunks.json"


def _is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Anything that isn't a normal, globally-routable public address:
    loopback, link-local (covers every cloud metadata endpoint —
    AWS/GCP/Azure/Fly all serve credentials at 169.254.169.254), RFC1918
    private ranges, multicast, and unspecified/reserved. Unlike the first
    pass at this function, private ranges are now blocked too — a caller
    holding the admin token could otherwise probe any other private-network
    service (internal databases, dashboards, other containers), not just
    the one address this app actually needs. What this app actually needs
    (its own self-hosted deck at a docker-compose/Fly-internal private
    address) is handled separately, by DOCUMENT_FETCH_ALLOWED_INTERNAL_HOSTS
    — an explicit host:port allowlist, not a blanket range exception."""
    return (ip.is_loopback or ip.is_link_local or ip.is_private
            or ip.is_multicast or ip.is_unspecified or ip.is_reserved)


def _validate_host(host: str, port: int) -> str:
    """Resolve `host` and return the single IP the connection will actually
    use. Callers MUST connect to exactly this IP (see the pinned Connection
    classes below) rather than letting the HTTP client re-resolve the
    hostname — otherwise a validated-safe hostname could resolve to a
    different, internal address moments later at connect time (DNS
    rebinding), silently defeating this whole check.

    The allowlist is host:port, not bare host — a hostname-only allowlist
    would let a document uri hit ANY port on that host (other services on
    the same machine/container), not just the one this app actually needs."""
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror as exc:
        raise ValueError(f"cannot resolve host {host!r}: {exc}") from exc
    if f"{host.lower()}:{port}" in DOCUMENT_FETCH_ALLOWED_INTERNAL_HOSTS:
        return infos[0][4][0]
    for *_rest, sockaddr in infos:
        ip = ipaddress.ip_address(sockaddr[0])
        if _is_blocked_ip(ip):
            raise ValueError(
                f"refusing to fetch {host!r}:{port} -> {ip} (not a public address, and "
                f"{host!r}:{port} is not in DOCUMENT_FETCH_ALLOWED_INTERNAL_HOSTS)")
    return infos[0][4][0]


class _PinnedHTTPConnection(http.client.HTTPConnection):
    """Connects to a pre-validated IP instead of letting the stdlib re-resolve
    the hostname at connect time (closes the DNS-rebinding gap noted above)."""

    def __init__(self, ip: str, host: str, port: int, **kwargs):
        super().__init__(host, port, **kwargs)
        self._pinned_ip = ip

    def connect(self):
        self.sock = socket.create_connection((self._pinned_ip, self.port), self.timeout)


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """Same IP pinning, but still presents SNI and validates the certificate
    against the real hostname (self.host) — connecting to a raw IP without
    this would fail TLS verification against any real cert."""

    def __init__(self, ip: str, host: str, port: int, **kwargs):
        super().__init__(host, port, **kwargs)
        self._pinned_ip = ip

    def connect(self):
        sock = socket.create_connection((self._pinned_ip, self.port), self.timeout)
        context = self._context or ssl.create_default_context()
        self.sock = context.wrap_socket(sock, server_hostname=self.host)


def _fetch_bytes(uri: str) -> bytes:
    """Size-capped, magic-byte-checked, SSRF-hardened fetch. Deliberately no
    auth/cookies/special-casing (unlike fetch.py's YouTube path) — every
    document URI is plain http(s), including our own self-hosted
    /corpus/{name}. No redirects are followed at all (any 3xx is treated as
    a failure) — simpler and safer than re-validating each hop."""
    parsed = urllib.parse.urlparse(uri)
    host = parsed.hostname
    if not host:
        raise ValueError(f"uri has no hostname: {uri!r}")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    ip = _validate_host(host, port)
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query

    conn_cls = _PinnedHTTPSConnection if parsed.scheme == "https" else _PinnedHTTPConnection
    conn = conn_cls(ip, host, port, timeout=30)
    try:
        conn.putrequest("GET", path, skip_host=True)
        conn.putheader("Host", host)
        conn.putheader("User-Agent", "MomentSearch/1.0")
        conn.endheaders()
        resp = conn.getresponse()
        if 300 <= resp.status < 400:
            raise ValueError(f"refusing to follow redirect (HTTP {resp.status})")
        if resp.status != 200:
            raise ValueError(f"fetch failed: HTTP {resp.status} {resp.reason}")
        data = resp.read(_MAX_BYTES + 1)
    finally:
        conn.close()

    if len(data) > _MAX_BYTES:
        raise ValueError(f"document exceeds {DOCUMENT_FETCH_MAX_MB}MB fetch limit")
    if not data.startswith(b"%PDF"):
        raise ValueError("fetched content is not a PDF (missing %PDF magic bytes)")
    return data


def _parse_pdf(data: bytes) -> list[dict]:
    """PDF bytes -> [{page, text}], 1-indexed to match real, human page numbers
    (what a citation deep-links to). Imported lazily — nothing at module scope
    pulls in pymupdf, keeping it out of the API process entirely."""
    import fitz  # pymupdf

    doc = fitz.open(stream=data, filetype="pdf")
    try:
        return [{"page": i + 1, "text": doc[i].get_text()} for i in range(len(doc))]
    finally:
        doc.close()


def _chunk_page(text: str, page: int, target_chars: int) -> list[dict]:
    """Greedy word-accumulate up to ~target_chars, same shape as
    transcript.chunk_cues()'s time-accumulate — sized in characters since a
    page has no timeline. Never crosses a page boundary: the page number IS
    the locator, so a chunk spanning two pages would have no single correct
    citation target."""
    words = text.split()
    if not words:
        return []
    chunks: list[dict] = []
    buf: list[str] = []
    length = 0
    for w in words:
        buf.append(w)
        length += len(w) + 1
        if length >= target_chars:
            chunks.append({"page": page, "text": " ".join(buf)})
            buf, length = [], 0
    if buf:
        chunks.append({"page": page, "text": " ".join(buf)})
    return chunks


def _valid_shape(data, *, max_len: int | None = None) -> bool:
    """Base structural check shared by both checkpoint kinds: a non-empty
    list of {page: positive int, text: str}. Syntactically valid JSON like
    `[]`, `{"x": 1}`, or `[1, 2]` all pass json.loads() cleanly but are NOT
    usable checkpoints: an empty list would let t_embed silently upsert zero
    points and still mark the source 'indexed', and a wrong shape would
    raise deep inside the next stage (e.g. `"x"["text"]`) on every retry,
    forever, since nothing regenerates a checkpoint that already "exists"."""
    if not isinstance(data, list) or not data:
        return False
    if max_len is not None and len(data) > max_len:
        return False
    for item in data:
        if not isinstance(item, dict):
            return False
        if not isinstance(item.get("page"), int) or item["page"] <= 0:
            return False
        if not isinstance(item.get("text"), str):
            return False
    return True


def _valid_parsed_pages(data) -> bool:
    """Parsed-page checkpoint: base shape, plus at least ONE page with real
    (non-whitespace) text. Individual blank pages are normal PDF reality
    (a section divider, an image-only page) and must stay — only an
    ALL-blank document is actually unusable."""
    return _valid_shape(data) and any(p["text"].strip() for p in data)


def _valid_chunks(data, *, max_len: int | None) -> bool:
    """Chunk checkpoint: base shape, plus EVERY chunk must have real
    (non-whitespace) text — unlike pages, a chunk's entire reason for
    existing is to carry embeddable content; an empty-text chunk is a
    corrupt/foreign file, not a normal outcome. _chunk_page() never
    produces one in the ordinary path (word-accumulation on a non-blank
    page always yields non-empty text), so this should never trigger on a
    checkpoint this pipeline actually wrote — it's a floor against a
    tampered or foreign file at this key, same as the shape check."""
    return _valid_shape(data, max_len=max_len) and all(c["text"].strip() for c in data)


def _load_checkpoint(key: str, validate) -> list[dict] | None:
    """None if the artifact doesn't exist, is corrupt JSON, OR fails
    `validate` — any of those is treated identically: redo this stage and
    overwrite the checkpoint with a fresh, valid one. put_bytes' atomic
    rename (src/storage.py) means a kill mid-write can no longer leave a
    truncated file behind, but a stale/foreign/hand-edited or logically
    empty/blank file needs the same "doesn't exist" fallback so it can't
    wedge every future retry in a permanent failure loop."""
    if not storage.exists(key):
        return None
    try:
        data = json.loads(storage.get_bytes(key))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        print(f"[checkpoint] {key}: unreadable ({type(exc).__name__}: {exc}) — redoing this stage")
        return None
    if not validate(data):
        print(f"[checkpoint] {key}: wrong shape, empty, or blank — redoing this stage")
        return None
    return data


@task(name="parse-document", retries=2, retry_delay_seconds=[30, 120])
def t_parse(doc_id: str, user_id: str, uri: str) -> list[dict]:
    db.set_status(doc_id, "parsing")
    key = _parsed_key(user_id, doc_id)
    cached = _load_checkpoint(key, _valid_parsed_pages)
    if cached is not None:
        print(f"[parse] {doc_id}: parsed.json already committed — resuming from checkpoint")
        return cached
    pages = _parse_pdf(_fetch_bytes(uri))
    if not any(p["text"].strip() for p in pages):
        raise RuntimeError("No extractable text in document.")
    storage.put_bytes(key, json.dumps(pages).encode(), "application/json")
    return pages


@task(name="chunk-document")
def t_chunk(doc_id: str, user_id: str, pages: list[dict]) -> list[dict]:
    db.set_status(doc_id, "chunking")
    key = _chunks_key(user_id, doc_id)
    cached = _load_checkpoint(key, lambda d: _valid_chunks(d, max_len=DOCUMENT_MAX_CHUNKS))
    if cached is not None:
        print(f"[chunk] {doc_id}: chunks.json already committed — resuming from checkpoint")
        return cached
    chunks: list[dict] = []
    for p in pages:
        chunks.extend(_chunk_page(p["text"], p["page"], DOCUMENT_CHUNK_CHARS))
        if len(chunks) > DOCUMENT_MAX_CHUNKS:
            raise RuntimeError(f"Document exceeds {DOCUMENT_MAX_CHUNKS} chunks "
                               f"(byte cap alone doesn't bound page/text count).")
    if not chunks:
        raise RuntimeError("Chunking produced zero chunks.")
    storage.put_bytes(key, json.dumps(chunks).encode(), "application/json")
    return chunks


@task(name="embed-index-document", retries=2, retry_delay_seconds=60)
def t_embed(doc_id: str, user_id: str, chunks: list[dict], kind: str, uri: str) -> int:
    # Defense in depth: t_chunk's own validation should make this unreachable
    # (empty/wrong-shaped chunk lists are now rejected before they get here),
    # but never let a source reach 'indexed' with nothing actually indexed.
    if not chunks:
        raise RuntimeError("t_embed received zero chunks — refusing to mark indexed.")
    db.set_status(doc_id, "embedding", progress=0.0)
    vector_store.ensure_text_collection()
    vecs = embed_docs([c["text"] for c in chunks])
    if len(vecs) != len(chunks):
        raise RuntimeError(f"embed_docs returned {len(vecs)} vectors for {len(chunks)} chunks "
                           f"— refusing to upsert a mismatched batch.")
    vector_store.upsert_chunks(user_id, doc_id, vecs, payloads=[
        {"user_id": user_id, "video_id": doc_id, "source_id": doc_id, "kind": kind,
         "page": c["page"], "uri": uri, "text": c["text"], "modality": "text",
         "embed_version": TEXT_EMBED_VERSION}
        for c in chunks
    ])
    db.set_status(doc_id, "indexed", chunk_count=len(chunks), progress=1.0)
    return len(chunks)


@flow(name="ms-ingest-document", log_prints=True, timeout_seconds=1800)
def ingest_document(doc_id: str, user_id: str, kind: str) -> dict:
    attempt = db.bump_attempts(doc_id)
    try:
        row = db.get_video(doc_id)
        if row is None:
            raise ValueError(f"no manifest row for {doc_id}")
        uri = row["uri"]
        if not uri:
            raise ValueError(f"{doc_id} has no uri")
        pages = t_parse(doc_id, user_id, uri)
        chunks = t_chunk(doc_id, user_id, pages)
        n = t_embed(doc_id, user_id, chunks, kind, uri)
        print(f"[ingest] {doc_id} indexed: {n} chunks (attempt {attempt})")
        return {"doc_id": doc_id, "chunks": n}
    except Exception as exc:
        db.set_status(doc_id, "failed", error=f"{type(exc).__name__}: {exc}")
        raise  # Prefect marks the run Failed; full trace in the Cloud UI
