"""Per-document ingest pipeline (papers, decks) — a Prefect flow of three
stage-tasks, checkpointed from the start.

pending -> parsing -> chunking -> embedding -> indexed | failed

Stages:
  1. parse   fetch the document into memory (size-capped, structurally
             checked as PDF or PPTX — see sniff_document_kind). A .pptx is
             converted to PDF server-side (headless LibreOffice, its own
             checkpoint -> docs/{user}/{id}/{kind}/{_PARSE_VERSION}/
             converted.pdf) BEFORE extraction, so a .pptx deck and a
             hand-exported .pdf of the same deck converge to the identical
             extraction code path. Then extract per-page text with real
             page numbers -> commit
             docs/{user}/{id}/{kind}/{_PARSE_VERSION}/parsed.json
  2. chunk   page-aware chunking (never spans two pages — the page number
             IS the locator) -> commit
             docs/{user}/{id}/{kind}/{_PARSE_VERSION}/chunks.json
  3. embed   text-embed the chunks -> idempotent Qdrant upsert into the SAME
             TEXT_COLLECTION transcript chunks already live in

Checkpoint keys carry `kind` and `_PARSE_VERSION`, not just doc_id: re-
registering the same bytes (same content-hash doc_id) under a different kind
must not resume from the other kind's checkpoint (paper pages skip the
vision-caption branch entirely; reusing them for a deck would silently keep
raw uncaptioned text), and bumping _PARSE_VERSION is how a change to what
parsing DOES (like adding that caption branch) invalidates every checkpoint
written under the old semantics instead of being silently reused forever.

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

from .. import db, llm, storage
from ..config import (
    DECK_SLIDE_MIN_CHARS,
    DECK_SLIDE_RENDER_DPI,
    DOCUMENT_CHUNK_CHARS,
    DOCUMENT_CONVERT_TIMEOUT_S,
    DOCUMENT_FETCH_ALLOWED_INTERNAL_HOSTS,
    DOCUMENT_FETCH_MAX_MB,
    DOCUMENT_MAX_CHUNKS,
    DOCUMENT_MAX_CONVERTED_MB,
    DOCUMENT_MAX_CONVERTED_PAGES,
    TEXT_EMBED_VERSION,
)
from ..rag import vector_store
from ..rag.embeddings import embed_docs
from .detect import PARSE_VERSION, converted_pdf_key as _converted_pdf_key, sniff_document_kind

_MAX_BYTES = DOCUMENT_FETCH_MAX_MB * 1024 * 1024
_MAX_CONVERTED_BYTES = DOCUMENT_MAX_CONVERTED_MB * 1024 * 1024


class PermanentDocumentError(ValueError):
    """A fetch/validation failure that is deterministic given the same
    input — retrying it changes nothing. Raised by _validate_host and
    _fetch_bytes for an unresolvable/blocked host, a disallowed redirect, an
    oversized body, or content that isn't actually a PDF. t_parse's
    retry_condition_fn (below _fetch_bytes) uses this to skip its normal
    retry budget for these specifically — the alternative (retrying a
    permanently-broken URI twice, 30s then 120s later) is exactly what let
    benchmark/bench.py's 30 example.com probes hold dispatcher slots for
    tens of minutes instead of failing fast. Genuine transient failures
    (connection refused, timeout, DNS hiccup) raise their normal builtin
    exception types instead and keep the full retry budget."""

# Paper and deck are one parameterized code path (parse -> chunk -> embed),
# not two modules. This is the one thing that actually differs: the name of
# the locator field a chunk's payload carries downstream — a page number for
# a paper, a slide number for a deck. Both are still, internally, just "the
# PDF page index" (see _parse_pdf/_chunk_page) — this only renames the field
# at the Qdrant-payload boundary, where eval.py and the UI expect `slide`
# for decks (not `page`).
KIND_SPEC = {"paper": "page", "deck": "slide"}

# Bump whenever _parse_pdf's semantics change (e.g. the deck-caption branch
# added here) so a worker never resumes from a checkpoint written under the
# OLD semantics — without this, an existing deck's committed parsed.json from
# before the caption branch existed would be treated as "already done" and
# the new text-poor-slide captioning would simply never run for it, since
# _load_checkpoint only regenerates on missing/corrupt/invalid, not stale.
# Defined in detect.py (not here) so src/rag/search.py can share the exact
# same value without importing this module's prefect dependency — see
# detect.py's own docstring.
_PARSE_VERSION = PARSE_VERSION


def _parsed_key(user_id: str, doc_id: str, kind: str) -> str:
    # `kind` is part of the identity, not just a column on the row: the same
    # PDF bytes (same doc_id, content-hash identity) re-registered first as a
    # paper then as a deck must NOT reuse the paper's parsed.json — paper
    # pages skip the vision-caption branch entirely, so a deck's checkpoint
    # under the paper's key would silently carry raw (uncaptioned) text.
    return f"docs/{user_id}/{doc_id}/{kind}/{_PARSE_VERSION}/parsed.json"


def _chunks_key(user_id: str, doc_id: str, kind: str) -> str:
    return f"docs/{user_id}/{doc_id}/{kind}/{_PARSE_VERSION}/chunks.json"


def doc_prefix(user_id: str, doc_id: str) -> str:
    """Prefix covering both this document's checkpoints — used by the delete
    endpoint (src/api/videos.py) to purge them in one batch call, the same
    way it already purges a video's frame_prefix()."""
    return f"docs/{user_id}/{doc_id}/"


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
        raise PermanentDocumentError(f"cannot resolve host {host!r}: {exc}") from exc
    if f"{host.lower()}:{port}" in DOCUMENT_FETCH_ALLOWED_INTERNAL_HOSTS:
        return infos[0][4][0]
    for *_rest, sockaddr in infos:
        ip = ipaddress.ip_address(sockaddr[0])
        if _is_blocked_ip(ip):
            raise PermanentDocumentError(
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


def _convert_pptx_to_pdf(data: bytes) -> bytes:
    """Block M: shell out to headless LibreOffice (`soffice`) to convert a
    .pptx deck to PDF bytes, so every downstream stage — page-aware
    chunking, the slide locator, vision-captioning of text-poor slides,
    checkpointing — reuses _parse_pdf completely unchanged; a .pptx deck and
    a hand-exported .pdf of the same deck converge to the identical code
    path one step earlier, not a second extraction implementation via
    python-pptx.

    `-env:UserInstallation` points LibreOffice at a fresh, per-call profile
    directory instead of the default shared one: several flow-run
    subprocesses can call this concurrently (WORKER_CONCURRENCY > 1), and
    headless soffice instances sharing ONE profile directory take a lock on
    it — a second concurrent invocation would fail with "invalid profile"
    or hang waiting for the first to exit, not run in parallel."""
    import pathlib
    import subprocess
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = pathlib.Path(tmp)
        src = tmp_path / "deck.pptx"
        src.write_bytes(data)
        profile_dir = tmp_path / "loprofile"
        try:
            subprocess.run(
                ["soffice", "--headless", "--norestore",
                 f"-env:UserInstallation=file://{profile_dir}",
                 "--convert-to", "pdf", "--outdir", str(tmp_path), str(src)],
                check=True, capture_output=True, timeout=DOCUMENT_CONVERT_TIMEOUT_S,
            )
        except subprocess.CalledProcessError as exc:
            # Deterministic given the same bytes — same input, same
            # LibreOffice, same outcome. Same reasoning _parse_pdf's own
            # except-clause below uses for a corrupt PDF.
            stderr = exc.stderr.decode(errors="replace")[:500] if exc.stderr else ""
            raise PermanentDocumentError(
                f"LibreOffice PPTX->PDF conversion failed: {stderr}") from exc
        except subprocess.TimeoutExpired as exc:
            raise PermanentDocumentError(
                f"LibreOffice PPTX->PDF conversion exceeded "
                f"{DOCUMENT_CONVERT_TIMEOUT_S}s") from exc
        out = tmp_path / "deck.pdf"
        if not out.exists():
            raise PermanentDocumentError(
                "LibreOffice reported success but produced no PDF output")
        output_size = out.stat().st_size
        if output_size > _MAX_CONVERTED_BYTES:
            raise PermanentDocumentError(
                f"converted PDF exceeds {DOCUMENT_MAX_CONVERTED_MB}MB limit "
                f"({output_size} bytes)")
        pdf = out.read_bytes()
        # Validate what we're about to hand downstream AND checkpoint. This
        # runs before t_parse commits converted.pdf, so a truncated or
        # non-PDF output can never be cached as a "valid" checkpoint that
        # every later retry then trusts and reuses (_load_checkpoint's
        # revalidation covers the JSON artifacts, but converted.pdf is raw
        # bytes read back with storage.get_bytes, not validated JSON).
        if not pdf.startswith(b"%PDF"):
            raise PermanentDocumentError(
                "LibreOffice output is not a PDF (missing %PDF magic bytes)")
        return pdf


def _fetch_bytes(uri: str) -> bytes:
    """Size-capped, magic-byte-checked, SSRF-hardened fetch. Deliberately no
    auth/cookies/special-casing (unlike fetch.py's YouTube path) — every
    document URI is plain http(s), including our own self-hosted
    /corpus/{name}. No redirects are followed at all (any 3xx is treated as
    a failure) — simpler and safer than re-validating each hop."""
    parsed = urllib.parse.urlparse(uri)
    host = parsed.hostname
    if not host:
        raise PermanentDocumentError(f"uri has no hostname: {uri!r}")
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
            raise PermanentDocumentError(f"refusing to follow redirect (HTTP {resp.status})")
        if resp.status >= 500 or resp.status == 429:
            # Server error or rate-limit: plausibly transient on an otherwise
            # legitimate host (429 especially — retrying WITH backoff, which
            # t_parse's retry_delay_seconds already provides, is the normal,
            # correct response, not a reason to fail fast). Unlike every
            # other case here, keep the normal task retry budget.
            raise RuntimeError(f"fetch failed: HTTP {resp.status} {resp.reason} (retryable)")
        if resp.status != 200:
            raise PermanentDocumentError(f"fetch failed: HTTP {resp.status} {resp.reason}")
        data = resp.read(_MAX_BYTES + 1)
    finally:
        conn.close()

    if len(data) > _MAX_BYTES:
        raise PermanentDocumentError(f"document exceeds {DOCUMENT_FETCH_MAX_MB}MB fetch limit")
    if sniff_document_kind(data) is None:
        raise PermanentDocumentError(
            "fetched content is not a PDF or PPTX (structural check failed)")
    return data


def _resolve_caption_llm(user_id: str) -> llm.LLMConfig | None:
    """Same tenant-first, server-fallback resolution search.py uses to pick a
    model for answering — reused here so a deck's vision captions render with
    whichever model the user would actually get at query time. Imported
    lazily (like fitz below) to keep document.py's module-level import graph
    light; nothing here runs in the API process."""
    from ..rag.search import resolve_llm

    cfg, _source = resolve_llm(user_id)
    return cfg


def _pdf_page_count(data: bytes) -> int:
    """Cheap: fitz.open just reads the PDF's page table, no per-page
    decoding — safe to call before deciding whether _parse_pdf's real,
    per-page (and possibly per-slide-caption) work is worth doing at all."""
    import fitz  # pymupdf

    doc = fitz.open(stream=data, filetype="pdf")
    try:
        return len(doc)
    finally:
        doc.close()


def _parse_pdf(data: bytes, kind: str, user_id: str) -> list[dict]:
    """PDF bytes -> [{page, text}], 1-indexed to match real, human page
    numbers (what a citation deep-links to; for a deck this is the slide
    number). Imported lazily — nothing at module scope pulls in pymupdf,
    keeping it out of the API process entirely.

    Deck-only: a slide whose extracted text is below DECK_SLIDE_MIN_CHARS
    (a title slide, a diagram-only slide) is rendered to an image and
    captioned by the vision LLM instead of embedding near-nothing. Papers
    never take this branch — a sparse paper page (a figure, a section
    break) is normal PDF reality, not a gap to fill. If no model is
    configured, the slide's (possibly sparse) extracted text is kept as-is
    rather than failing the whole document."""
    import fitz  # pymupdf

    caption_cfg = _resolve_caption_llm(user_id) if kind == "deck" else None

    doc = fitz.open(stream=data, filetype="pdf")
    try:
        pages = []
        for i in range(len(doc)):
            page = doc[i]
            text = page.get_text()
            if kind == "deck" and len(text.strip()) < DECK_SLIDE_MIN_CHARS and caption_cfg is not None:
                try:
                    # pil_tobytes (not tobytes(), which defaults to PNG)
                    # guarantees real JPEG bytes matching the image/jpeg
                    # media type llm.caption_image hardcodes, regardless of
                    # render DPI — tobytes()'s PNG output would otherwise be
                    # mislabeled below LLM_IMAGE_MAX_PX, where _downscale()
                    # passes images through unmodified.
                    image = page.get_pixmap(dpi=DECK_SLIDE_RENDER_DPI).pil_tobytes(format="JPEG")
                    caption = llm.caption_image(image, caption_cfg)
                    if caption.strip():
                        text = caption
                except Exception as exc:
                    print(f"[parse] slide {i + 1}: caption failed "
                          f"({type(exc).__name__}: {exc}) — keeping extracted text")
            pages.append({"page": i + 1, "text": text})
        return pages
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


def _converted_checkpoint_exists(key: str) -> bool:
    """Check a raw converted-PDF checkpoint without downloading it.

    This runs only in the ingestion/resume path. Citation rendering uses
    the persisted manifest key and performs no object-storage probe."""
    meta = storage.head(key)
    if meta is None:
        return False
    checkpoint_size = int(meta.get("size", 0))
    if checkpoint_size > _MAX_CONVERTED_BYTES:
        raise PermanentDocumentError(
            f"converted PDF checkpoint exceeds "
            f"{DOCUMENT_MAX_CONVERTED_MB}MB limit "
            f"({checkpoint_size} bytes)")
    return True


def _skip_retry_on_permanent_error(task, task_run, state) -> bool:
    """t_parse's retry_condition_fn: False (don't retry) when the task
    failed with a PermanentDocumentError — deterministic given the same
    input, so Prefect's retry budget (30s then 120s) would just hold a
    dispatcher slot for tens of minutes across a batch of poison URIs for no
    benefit. True (use the normal retries=2 budget) for anything else — a
    real transient network/connection error, or an unexpected bug, both of
    which a retry might plausibly help with. `state.data` holds the raised
    exception (see prefect.task_engine.SyncTaskRunEngine.can_retry)."""
    return not isinstance(state.data, PermanentDocumentError)


@task(name="parse-document", retries=2, retry_delay_seconds=[30, 120],
     retry_condition_fn=_skip_retry_on_permanent_error)
def t_parse(doc_id: str, user_id: str, uri: str | None, kind: str, generation: int,
           storage_key: str | None = None) -> list[dict]:
    """Get the raw document bytes (PDF or PPTX — see sniff_document_kind) one
    of two ways: `storage_key` (an upload already sitting in OUR storage —
    read directly, no fetch, no expiry, trusted the same way a checkpoint
    read is) takes priority over `uri` (an external https:// document —
    SSRF-hardened HTTP fetch, since we don't already have those bytes
    ourselves). Exactly one is set per row
    (src/api/documents.py's _register)."""
    db.set_status(doc_id, "parsing", generation=generation)
    key = _parsed_key(user_id, doc_id, kind)
    cached = _load_checkpoint(key, _valid_parsed_pages)
    if cached is not None:
        # Upgrade a PPTX checkpoint created before the manifest gained
        # view_storage_key. Check the derivative here in the worker's resume
        # path (not on every search citation); this also covers URI PPTX rows,
        # which have no original storage_key or trustworthy filename.
        conv_key = _converted_pdf_key(user_id, doc_id, kind)
        if kind == "deck" and _converted_checkpoint_exists(conv_key):
            db.set_status(
                doc_id, "parsing", view_storage_key=conv_key,
                generation=generation)
        print(f"[parse] {doc_id}: parsed.json already committed — resuming from checkpoint")
        return cached
    data = storage.get_bytes(storage_key) if storage_key else _fetch_bytes(uri)
    doc_kind = sniff_document_kind(data)
    if doc_kind is None:
        raise PermanentDocumentError(
            "document bytes are neither a PDF nor a PPTX (structural check failed)")
    if doc_kind == "pptx" and kind != "deck":
        # src/api/documents.py forces kind="deck" for a sniffed PPTX at
        # upload time, but a URL registration can't sniff the bytes until
        # THIS fetch — a paper-registered URI that turns out to serve a
        # .pptx must not proceed under kind="paper" (wrong "page N" locators,
        # and checkpoint keys below are namespaced by `kind` — see
        # converted_pdf_key/_parsed_key — so continuing would write a
        # converted.pdf/parsed.json under the wrong kind's path). Permanent:
        # the same URI serves the same bytes on every retry (found in review).
        raise PermanentDocumentError(
            f"fetched document is a PPTX presentation but was registered as "
            f"kind={kind!r} — re-register it with kind=\"deck\".")
    if doc_kind == "pptx":
        # Block M: convert BEFORE _parse_pdf, checkpointed separately from
        # parsed.json — LibreOffice's headless conversion isn't instant for
        # a large deck, so a worker killed after conversion but before
        # parsed.json commits must not pay that cost again on retry.
        conv_key = _converted_pdf_key(user_id, doc_id, kind)
        cached_pdf = None
        # HEAD first: get_bytes() buffers the whole object, so checking its
        # length after download would not protect worker memory from an
        # oversized/corrupt checkpoint.
        if _converted_checkpoint_exists(conv_key):
            candidate = storage.get_bytes(conv_key)
            # Revalidate on READ, same principle as _load_checkpoint's shape
            # check on the JSON artifacts: a stale, hand-edited, or foreign
            # file at this key must fall back to "doesn't exist" (redo the
            # stage) rather than wedge every future retry on bytes pymupdf
            # will reject identically forever.
            if candidate.startswith(b"%PDF"):
                cached_pdf = candidate
            else:
                print(f"[checkpoint] {conv_key}: not a PDF — redoing conversion")
        if cached_pdf is not None:
            print(f"[parse] {doc_id}: converted.pdf already committed — "
                  f"skipping LibreOffice conversion")
            data = cached_pdf
        else:
            data = _convert_pptx_to_pdf(data)
            db.check_generation(doc_id, generation)  # see the identical check below
            storage.put_bytes(conv_key, data, "application/pdf")
        # Checked BEFORE _parse_pdf, not after: a slide count this large is
        # what DOCUMENT_MAX_CHUNKS is already meant to guard against, but
        # that cap is only checked once every page has already been through
        # (possibly vision-captioned by) _parse_pdf — a pathological deck's
        # LLM-captioning cost would already be spent by the time it fires.
        try:
            page_count = _pdf_page_count(data)
        except Exception as exc:
            # Fixed converted bytes will fail page-table inspection the same
            # way on every retry. Classify this consistently with the full
            # _parse_pdf failure below so Prefect does not burn retry delays.
            raise PermanentDocumentError(
                f"failed to inspect converted PDF: "
                f"{type(exc).__name__}: {exc}") from exc
        if page_count > DOCUMENT_MAX_CONVERTED_PAGES:
            raise PermanentDocumentError(
                f"converted deck has {page_count} slides, exceeding the "
                f"{DOCUMENT_MAX_CONVERTED_PAGES}-slide cap")
        # The derivative is durable and structurally readable. Persist its
        # exact key once so every citation can select it without probing
        # object storage on the latency-sensitive query path.
        db.set_status(doc_id, "parsing", view_storage_key=conv_key,
                      generation=generation)
    try:
        pages = _parse_pdf(data, kind, user_id)
    except Exception as exc:
        # A file that starts with %PDF but is corrupt/malformed past that
        # point fails deterministically on every retry — same bytes, same
        # parser, same outcome. fitz's own exception types vary (FileDataError
        # and others), so this catches broadly rather than trying to enumerate
        # them; anything from _parse_pdf given fixed input bytes is permanent.
        raise PermanentDocumentError(
            f"failed to parse PDF: {type(exc).__name__}: {exc}") from exc
    if not any(p["text"].strip() for p in pages):
        raise PermanentDocumentError("No extractable text in document.")
    # Revalidate before committing the checkpoint: a stale run's fetch+parse
    # above was wasted work if its lease is already gone, but writing this
    # artifact would still be a real corruption risk if a newer run had
    # already committed a different (or fresher) parsed.json for this key.
    db.check_generation(doc_id, generation)
    storage.put_bytes(key, json.dumps(pages).encode(), "application/json")
    return pages


@task(name="chunk-document")
def t_chunk(doc_id: str, user_id: str, pages: list[dict], kind: str, generation: int) -> list[dict]:
    db.set_status(doc_id, "chunking", generation=generation)
    key = _chunks_key(user_id, doc_id, kind)
    cached = _load_checkpoint(key, lambda d: _valid_chunks(d, max_len=DOCUMENT_MAX_CHUNKS))
    if cached is not None:
        print(f"[chunk] {doc_id}: chunks.json already committed — resuming from checkpoint")
        return cached
    chunks: list[dict] = []
    for p in pages:
        chunks.extend(_chunk_page(p["text"], p["page"], DOCUMENT_CHUNK_CHARS))
        if len(chunks) > DOCUMENT_MAX_CHUNKS:
            # Deterministic given the same parsed pages — t_chunk currently
            # has no retries configured (so this doesn't retry today either
            # way), but typed correctly in case that ever changes.
            raise PermanentDocumentError(
                f"Document exceeds {DOCUMENT_MAX_CHUNKS} chunks "
                f"(byte cap alone doesn't bound page/text count).")
    if not chunks:
        raise PermanentDocumentError("Chunking produced zero chunks.")
    db.check_generation(doc_id, generation)  # see t_parse's identical check above
    storage.put_bytes(key, json.dumps(chunks).encode(), "application/json")
    return chunks


@task(name="embed-index-document", retries=2, retry_delay_seconds=60)
def t_embed(doc_id: str, user_id: str, chunks: list[dict], kind: str, uri: str, generation: int) -> int:
    # Defense in depth: t_chunk's own validation should make this unreachable
    # (empty/wrong-shaped chunk lists are now rejected before they get here),
    # but never let a source reach 'indexed' with nothing actually indexed.
    if not chunks:
        raise RuntimeError("t_embed received zero chunks — refusing to mark indexed.")
    db.set_status(doc_id, "embedding", progress=0.0, generation=generation)
    vector_store.ensure_text_collection()
    vecs = embed_docs([c["text"] for c in chunks])
    if len(vecs) != len(chunks):
        raise RuntimeError(f"embed_docs returned {len(vecs)} vectors for {len(chunks)} chunks "
                           f"— refusing to upsert a mismatched batch.")
    locator_key = KIND_SPEC.get(kind, "page")
    # Revalidate before the Qdrant write — the most consequential of the
    # three checks, since a stale run upserting stale/wrong text here would
    # silently corrupt a newer run's freshly-indexed content (deterministic
    # point IDs make this an overwrite, not a duplicate, which is exactly
    # why it's dangerous rather than merely wasteful).
    db.check_generation(doc_id, generation)
    vector_store.upsert_chunks(user_id, doc_id, vecs, payloads=[
        {"user_id": user_id, "video_id": doc_id, "source_id": doc_id, "kind": kind,
         locator_key: c["page"], "uri": uri, "text": c["text"], "modality": "text",
         "embed_version": TEXT_EMBED_VERSION}
        for c in chunks
    ])
    db.set_status(doc_id, "indexed", chunk_count=len(chunks), progress=1.0, generation=generation)
    return len(chunks)


@flow(name="ms-ingest-document", log_prints=True, timeout_seconds=1800)
def ingest_document(doc_id: str, user_id: str, kind: str, generation: int) -> dict:
    """`generation` is the lease token db.wfq_claim() minted when this run was
    admitted — see ingest_video's docstring (src/ingest/pipeline.py) for the
    full mechanism; identical here."""
    try:
        attempt = db.bump_attempts(doc_id, generation=generation)
        row = db.get_video(doc_id)
        if row is None:
            raise ValueError(f"no manifest row for {doc_id}")
        uri, storage_key = row["uri"], row.get("storage_key")
        if not uri and not storage_key:
            raise ValueError(f"{doc_id} has no uri or storage_key")
        pages = t_parse(doc_id, user_id, uri, kind, generation, storage_key)
        chunks = t_chunk(doc_id, user_id, pages, kind, generation)
        n = t_embed(doc_id, user_id, chunks, kind, uri, generation)
        print(f"[ingest] {doc_id} indexed: {n} chunks (attempt {attempt})")
        return {"doc_id": doc_id, "chunks": n}
    except db.StaleLeaseError as exc:
        # A newer run already owns this row — not an ingest failure. See
        # ingest_video's identical handling (src/ingest/pipeline.py).
        print(f"[ingest] {doc_id}: {exc}")
        return {"doc_id": doc_id, "stale_lease": True}
    except Exception as exc:
        db.set_status(doc_id, "failed", error=f"{type(exc).__name__}: {exc}", generation=generation)
        raise  # Prefect marks the run Failed; full trace in the Cloud UI
