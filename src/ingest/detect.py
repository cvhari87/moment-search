"""Structural content-type detection + checkpoint-key naming for document
ingestion.

Deliberately its OWN tiny stdlib-only module
rather than living in src/ingest/document.py, which imports prefect at
module scope.
src/api/documents.py (upload-time validation, runs in the API process),
src/rag/search.py (citation URL resolution, ALSO runs in the API process),
and src/ingest/document.py (fetch-time validation + t_parse's conversion
branch, runs in the worker process) all import this — the API process must
never drag prefect in just to reuse a magic-byte check or a checkpoint-key
format string (the same reason fitz/pymupdf is imported lazily inside
document.py's _parse_pdf instead of at module scope).

PARSE_VERSION and converted_pdf_key() are the single source of truth for
the PPTX-conversion checkpoint's path — document.py imports both rather
than redefining them, so the writer (t_parse) and the reader (search.py's
citation-serving _doc_url, which needs to know whether a converted PDF
exists to serve THAT instead of the original uploaded .pptx bytes) can
never drift out of sync on the key format.
"""
from __future__ import annotations

import io
import zipfile
from xml.etree import ElementTree

# A real [Content_Types].xml is a few KB even for a deck with hundreds of
# parts — this is generous headroom, not a realistic ceiling. Bounds BOTH
# an outright zip-bomb single entry (a small compressed blob that expands
# to gigabytes) and pathological input before it ever reaches ElementTree.
_MAX_CONTENT_TYPES_BYTES = 5 * 1024 * 1024
# Guards the archive as a whole: sum of every member's DECLARED uncompressed
# size (read from the zip's own directory, never decompressed to check)
# against a hard cap AND against the compressed archive's own byte length —
# a legitimate .pptx (mostly already-compressed XML + media) rarely exceeds
# a single-digit expansion ratio; a zip bomb is built specifically to blow
# far past that.
_MAX_TOTAL_UNCOMPRESSED_BYTES = 500 * 1024 * 1024
_MAX_EXPANSION_RATIO = 100
_CONTENT_TYPES_NS = "{http://schemas.openxmlformats.org/package/2006/content-types}"
_PRESENTATION_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml")

# Bump whenever document.py's parse semantics change, so a worker never
# resumes from (or search.py never serves) a checkpoint written under OLD
# semantics — see src/ingest/document.py's own docstring for the full
# reasoning (the deck-caption branch is what originally required this).
PARSE_VERSION = "v2"


def converted_pdf_key(user_id: str, doc_id: str, kind: str) -> str:
    """Deterministic storage key for a PPTX deck's LibreOffice-converted PDF
    checkpoint. Written by src.ingest.document.t_parse; read by BOTH t_parse
    (to skip re-conversion after a crash) and viewer_storage_key below (to
    serve the browser a viewable PDF instead of the original .pptx bytes a
    browser can't render inline)."""
    return f"docs/{user_id}/{doc_id}/{kind}/{PARSE_VERSION}/converted.pdf"


def viewer_storage_key(storage_key: str | None,
                       view_storage_key: str | None) -> str | None:
    """Choose citation bytes from manifest fields only.

    Ingestion persists `view_storage_key` after a PPTX derivative has been
    committed and validated. Query-time callers therefore do not need an
    S3/GCS HEAD request per citation. PDF documents have no derivative and
    naturally fall back to their original `storage_key`; URL PDFs keep both
    fields null and callers fall through to `uri`."""
    return view_storage_key or storage_key


def _zip_bomb_guard(zf: zipfile.ZipFile, archive_size: int) -> bool:
    """True if this archive's total DECLARED uncompressed size — read from
    the zip's own central directory, never decompressed to check — is
    within sane bounds for a real .pptx. A legitimate deck (mostly
    already-compressed XML + media) rarely expands more than a handful of
    times; a zip bomb is built specifically to blow far past that on a
    tiny, cheap-to-upload file."""
    total_uncompressed = sum(info.file_size for info in zf.infolist())
    if total_uncompressed > _MAX_TOTAL_UNCOMPRESSED_BYTES:
        return False
    if archive_size > 0 and total_uncompressed / archive_size > _MAX_EXPANSION_RATIO:
        return False
    return True


def _content_types_names_presentation(content_types_xml: bytes) -> bool:
    """Real XML validation of [Content_Types].xml — an actual `<Override>`
    element mapping PartName="/ppt/presentation.xml" to a presentation
    content type, not a byte substring search anywhere in the file. A
    substring match is satisfied by a crafted archive that merely contains
    the text `presentationml.presentation` somewhere (a comment, an
    unrelated part's declaration, padding) without the file actually being
    a valid content-types manifest that maps that part correctly."""
    try:
        root = ElementTree.fromstring(content_types_xml)
    except ElementTree.ParseError:
        return False
    for override in root.iter(f"{_CONTENT_TYPES_NS}Override"):
        if (override.get("PartName") == "/ppt/presentation.xml"
                and override.get("ContentType", "") == _PRESENTATION_CONTENT_TYPE):
            return True
    return False


def sniff_document_kind(data: bytes) -> str | None:
    """'pdf', 'pptx', or None — structural detection, not a caller-supplied
    extension or content-type header (the same reason the original PDF
    check tested `%PDF` magic bytes instead of trusting a filename): a
    renamed .txt claiming to be a .pptx must still be rejected, both at
    upload time (src/api/documents.py) and at fetch time
    (src/ingest/document.py's _fetch_bytes).

    A .pptx is a zip archive, so `PK\\x03\\x04` alone (the zip local-file-
    header signature) isn't distinctive enough — any zip file matches it,
    spoofable the same way a bare extension is. Confirmed structurally: the
    package must actually contain `ppt/presentation.xml` (the main
    presentation part every real .pptx has) AND `[Content_Types].xml` must
    ACTUALLY map that part to a presentation content type (real XML
    parsing — see _content_types_names_presentation — not a substring
    search, which a crafted archive could satisfy without being a genuine
    presentation package). Also guards against a zip bomb: a tiny archive
    whose declared uncompressed size explodes past what any real deck would
    need (see _zip_bomb_guard) is rejected before its [Content_Types].xml
    is ever decompressed."""
    if data.startswith(b"%PDF"):
        return "pdf"
    if not data.startswith(b"PK\x03\x04"):
        return None
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            if not _zip_bomb_guard(zf, len(data)):
                return None
            if "ppt/presentation.xml" not in zf.namelist():
                return None
            info = zf.getinfo("[Content_Types].xml")
            if info.file_size > _MAX_CONTENT_TYPES_BYTES:
                return None
            content_types = zf.read("[Content_Types].xml")
    except (zipfile.BadZipFile, KeyError):
        return None
    return "pptx" if _content_types_names_presentation(content_types) else None
