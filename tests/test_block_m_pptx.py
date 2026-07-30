"""Unit tests for Block M (native PPTX ingestion) — stdlib unittest only, no
live stack, no real soffice invocation (mocked), same conventions as
tests/test_block_i_reliability.py. Covers:

  1. src/ingest/detect.py's sniff_document_kind — real in-memory PDF/PPTX
     byte fixtures (not just "starts with the right 4 bytes"), plus the
     spoofing cases the structural check exists to catch: a foreign OOXML
     document (docx-shaped) claiming to be a presentation, and a zip that
     merely LOOKS like one without the required parts.
  2. src/ingest/document.py's _convert_pptx_to_pdf — success, a failing
     soffice process, a timeout, and the "exited 0 but produced nothing"
     case, all via a mocked subprocess.run so these tests don't need
     LibreOffice installed.
  3. t_parse's conversion-checkpoint logic — converts and commits
     converted.pdf when it isn't there yet, skips conversion entirely (and
     never calls _convert_pptx_to_pdf) when it is.
  4. src/ingest/detect.py's viewer_storage_key — the citation-viewer fix: a
     PPTX deck's storage_key points at the ORIGINAL .pptx bytes, while the
     manifest's view_storage_key points at the converted PDF. Citation
     selection must prefer that persisted derivative without an S3/GCS
     existence probe on every search result.

    python3 -m unittest discover -s tests -p 'test_*.py'
"""
from __future__ import annotations

import io
import subprocess
import unittest
import zipfile
from unittest.mock import MagicMock, patch

from src import db
from src.api.documents import _enforce_kind_for_sniffed_type
from src.ingest import detect, document
from src.ingest.detect import sniff_document_kind

_PRESENTATION_CONTENT_TYPES = (
    '<?xml version="1.0"?>'
    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
    '<Override PartName="/ppt/presentation.xml" '
    'ContentType="application/vnd.openxmlformats-officedocument'
    '.presentationml.presentation.main+xml"/>'
    '</Types>'
)

_WORD_CONTENT_TYPES = (
    '<?xml version="1.0"?>'
    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
    '<Override PartName="/word/document.xml" '
    'ContentType="application/vnd.openxmlformats-officedocument'
    '.wordprocessingml.document.main+xml"/>'
    '</Types>'
)


def _zip_bytes(entries: dict[str, str], *, compress=zipfile.ZIP_STORED) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=compress) as zf:
        for name, content in entries.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _real_pptx_bytes() -> bytes:
    return _zip_bytes({
        "[Content_Types].xml": _PRESENTATION_CONTENT_TYPES,
        "ppt/presentation.xml": "<p:presentation/>",
    })


class SniffDocumentKindTests(unittest.TestCase):
    def test_real_pdf(self):
        self.assertEqual(sniff_document_kind(b"%PDF-1.4\n%rest of a pdf"), "pdf")

    def test_real_pptx(self):
        self.assertEqual(sniff_document_kind(_real_pptx_bytes()), "pptx")

    def test_plain_text_is_neither(self):
        self.assertIsNone(sniff_document_kind(b"just some plain text, not a document at all"))

    def test_renamed_text_file_with_pptx_extension_is_still_rejected(self):
        """The whole point of this check: a caller-supplied extension or
        content-type must not be trusted. This has neither PDF nor zip
        magic bytes, so it must fail regardless of what it claims to be."""
        self.assertIsNone(sniff_document_kind(b"Hello, I am definitely a slide deck"))

    def test_docx_shaped_zip_is_not_pptx(self):
        """A foreign OOXML document (docx) is ALSO a zip with a real
        [Content_Types].xml — the presentationml-specific check must reject
        it, not just any well-formed Office Open XML package."""
        data = _zip_bytes({
            "[Content_Types].xml": _WORD_CONTENT_TYPES,
            "word/document.xml": "<w:document/>",
        })
        self.assertIsNone(sniff_document_kind(data))

    def test_zip_missing_presentation_part_is_not_pptx(self):
        """Content-types claims a presentation, but the actual part is
        missing — the namelist check catches what the content-type string
        alone would miss."""
        data = _zip_bytes({"[Content_Types].xml": _PRESENTATION_CONTENT_TYPES})
        self.assertIsNone(sniff_document_kind(data))

    def test_zip_missing_content_types_entirely_is_not_pptx(self):
        data = _zip_bytes({"ppt/presentation.xml": "<p:presentation/>"})
        self.assertIsNone(sniff_document_kind(data))

    def test_corrupt_zip_with_right_magic_bytes_is_not_pptx(self):
        self.assertIsNone(sniff_document_kind(b"PK\x03\x04" + b"\x00" * 20))

    def test_empty_bytes(self):
        self.assertIsNone(sniff_document_kind(b""))

    # ── Spoofing / resource-exhaustion cases (found in review) ──────────────

    def test_content_type_string_hidden_elsewhere_is_not_enough(self):
        """The original check was a byte-substring search for
        `presentationml.presentation` ANYWHERE in [Content_Types].xml, which
        a crafted archive satisfies without actually mapping
        /ppt/presentation.xml to a presentation content type. Real XML
        parsing must reject this: the magic string appears (as an unrelated
        part's name and in a comment) but no Override maps the presentation
        part."""
        spoofed = (
            '<?xml version="1.0"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<!-- presentationml.presentation -->'
            '<Override PartName="/decoy/presentationml.presentation.xml" '
            'ContentType="text/plain"/>'
            '</Types>'
        )
        data = _zip_bytes({
            "[Content_Types].xml": spoofed,
            "ppt/presentation.xml": "<p:presentation/>",
        })
        self.assertIsNone(sniff_document_kind(data))

    def test_presentation_part_mapped_to_wrong_content_type_is_rejected(self):
        """/ppt/presentation.xml exists and IS declared — but as plain text,
        not a presentation content type. Must not pass."""
        wrong = (
            '<?xml version="1.0"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Override PartName="/ppt/presentation.xml" ContentType="text/plain"/>'
            '</Types>'
        )
        data = _zip_bytes({
            "[Content_Types].xml": wrong,
            "ppt/presentation.xml": "<p:presentation/>",
        })
        self.assertIsNone(sniff_document_kind(data))

    def test_malformed_content_types_xml_is_rejected_not_crashed_on(self):
        data = _zip_bytes({
            "[Content_Types].xml": "<Types><unclosed>",
            "ppt/presentation.xml": "<p:presentation/>",
        })
        self.assertIsNone(sniff_document_kind(data))

    def test_zip_bomb_is_rejected_without_decompressing(self):
        """A small archive whose declared uncompressed size explodes far
        past any real deck's needs must be rejected on the zip DIRECTORY's
        own numbers — never by decompressing to find out. 10MB of zeros
        compresses to a few KB, an expansion ratio no legitimate .pptx
        (already-compressed XML + media) comes close to."""
        bomb = _zip_bytes({
            "[Content_Types].xml": _PRESENTATION_CONTENT_TYPES,
            "ppt/presentation.xml": "<p:presentation/>",
            "bomb.bin": "0" * (10 * 1024 * 1024),
        }, compress=zipfile.ZIP_DEFLATED)
        # Sanity-check the fixture is actually bomb-shaped before asserting.
        self.assertLess(len(bomb), 200 * 1024)
        self.assertIsNone(sniff_document_kind(bomb))

    def test_content_type_with_trailing_suffix_is_not_a_match(self):
        """A prior version compared the content-type with `startswith()`
        against the base type (stripped of its `.main+xml` suffix), which a
        crafted archive satisfies with ANY string sharing that prefix — e.g.
        `...presentationml.presentation.evil` — without it being the real,
        exact content type. Must be exact equality (found in review)."""
        spoofed = (
            '<?xml version="1.0"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Override PartName="/ppt/presentation.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument'
            '.presentationml.presentation.evil"/>'
            '</Types>'
        )
        data = _zip_bytes({
            "[Content_Types].xml": spoofed,
            "ppt/presentation.xml": "<p:presentation/>",
        })
        self.assertIsNone(sniff_document_kind(data))

    def test_legitimately_compressed_pptx_still_passes(self):
        """The zip-bomb guard must not reject a NORMAL compressed .pptx —
        real decks are deflated, and their XML compresses well. This is the
        false-positive guard on the check above."""
        normal = _zip_bytes({
            "[Content_Types].xml": _PRESENTATION_CONTENT_TYPES,
            "ppt/presentation.xml": "<p:presentation/>" + "<!--pad-->" * 200,
        }, compress=zipfile.ZIP_DEFLATED)
        self.assertEqual(sniff_document_kind(normal), "pptx")


class EnforceKindForSniffedTypeTests(unittest.TestCase):
    """A sniffed PPTX must always register as kind=deck server-side,
    regardless of what the client's dropdown (or a direct API caller)
    requested — a plain PDF's requested kind is a real, legitimate choice
    this can't and shouldn't override."""

    def test_pptx_always_forced_to_deck(self):
        self.assertEqual(_enforce_kind_for_sniffed_type("pptx", "paper"), "deck")
        self.assertEqual(_enforce_kind_for_sniffed_type("pptx", "deck"), "deck")

    def test_pdf_keeps_the_requested_kind(self):
        self.assertEqual(_enforce_kind_for_sniffed_type("pdf", "paper"), "paper")
        self.assertEqual(_enforce_kind_for_sniffed_type("pdf", "deck"), "deck")


class ConvertPptxToPdfTests(unittest.TestCase):
    """_convert_pptx_to_pdf shells out to soffice — these tests mock
    subprocess.run so they don't need LibreOffice installed, and verify the
    function's own contract: write the produced PDF's bytes back, or raise
    PermanentDocumentError (deterministic given the same input — retrying a
    broken .pptx changes nothing, same reasoning _parse_pdf's own corrupt-PDF
    handling uses)."""

    def _run_with_mocked_soffice(self, run_side_effect):
        # subprocess is imported LOCALLY inside _convert_pptx_to_pdf (kept
        # out of document.py's module scope), so there is no
        # src.ingest.document.subprocess to patch — patch the real
        # subprocess module's `run` directly; the function's local `import
        # subprocess` binds to that same sys.modules entry either way.
        with patch("subprocess.run", side_effect=run_side_effect):
            return document._convert_pptx_to_pdf(b"fake pptx bytes")

    def test_success_returns_the_produced_pdf_bytes(self):
        def fake_run(cmd, **kwargs):
            # cmd: [..., "--outdir", outdir, src_path] — write deck.pdf next
            # to the input the function itself wrote, matching soffice's
            # real naming (same basename, .pdf extension).
            outdir = cmd[cmd.index("--outdir") + 1]
            (__import__("pathlib").Path(outdir) / "deck.pdf").write_bytes(b"%PDF-converted")
            return MagicMock(returncode=0)

        result = self._run_with_mocked_soffice(fake_run)
        self.assertEqual(result, b"%PDF-converted")

    def test_nonzero_exit_raises_permanent_error(self):
        def fake_run(cmd, **kwargs):
            raise subprocess.CalledProcessError(1, cmd, stderr=b"soffice: conversion failed")

        with self.assertRaises(document.PermanentDocumentError) as ctx:
            self._run_with_mocked_soffice(fake_run)
        self.assertIn("conversion failed", str(ctx.exception))

    def test_timeout_raises_permanent_error(self):
        def fake_run(cmd, **kwargs):
            raise subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 0))

        with self.assertRaises(document.PermanentDocumentError) as ctx:
            self._run_with_mocked_soffice(fake_run)
        self.assertIn("exceeded", str(ctx.exception))

    def test_exit_zero_but_no_output_file_raises_permanent_error(self):
        def fake_run(cmd, **kwargs):
            return MagicMock(returncode=0)  # "succeeds" but writes nothing

        with self.assertRaises(document.PermanentDocumentError) as ctx:
            self._run_with_mocked_soffice(fake_run)
        self.assertIn("no PDF output", str(ctx.exception))

    def test_exit_zero_but_non_pdf_output_raises_permanent_error(self):
        """soffice reports success and DOES write a file at deck.pdf, but
        the content isn't actually a PDF (truncated, corrupted, or wrong
        format entirely) — must not be trusted just because the process
        exited 0 and a file exists at the expected path. This is what
        would let a bad conversion get checkpointed as if it were valid
        (found in review, alongside the checkpoint-read-side guard in
        TParseConversionCheckpointTests)."""
        def fake_run(cmd, **kwargs):
            outdir = cmd[cmd.index("--outdir") + 1]
            (__import__("pathlib").Path(outdir) / "deck.pdf").write_bytes(b"not actually a pdf")
            return MagicMock(returncode=0)

        with self.assertRaises(document.PermanentDocumentError) as ctx:
            self._run_with_mocked_soffice(fake_run)
        self.assertIn("not a PDF", str(ctx.exception))

    def test_oversized_converted_pdf_is_rejected_before_reading_it(self):
        """LibreOffice output can be much larger than the uploaded PPTX.
        Reject it from filesystem metadata before read_bytes() can allocate
        the whole derivative in worker memory."""
        def fake_run(cmd, **kwargs):
            outdir = cmd[cmd.index("--outdir") + 1]
            (__import__("pathlib").Path(outdir) / "deck.pdf").write_bytes(
                b"%PDF-oversized")
            return MagicMock(returncode=0)

        with patch.object(document, "_MAX_CONVERTED_BYTES", 5, create=True):
            with self.assertRaises(document.PermanentDocumentError) as ctx:
                self._run_with_mocked_soffice(fake_run)
        self.assertIn("converted PDF exceeds", str(ctx.exception))

    def test_uses_an_isolated_profile_directory_per_call(self):
        """Concurrent flow-run subprocesses (WORKER_CONCURRENCY > 1) can
        each call this at once — sharing LibreOffice's default profile
        directory across concurrent invocations causes lock contention. The
        -env:UserInstallation flag must be present and point somewhere
        under this call's own temp dir, not a fixed shared path."""
        seen_cmd = {}

        def fake_run(cmd, **kwargs):
            seen_cmd["cmd"] = cmd
            outdir = cmd[cmd.index("--outdir") + 1]
            (__import__("pathlib").Path(outdir) / "deck.pdf").write_bytes(b"%PDF-x")
            return MagicMock(returncode=0)

        self._run_with_mocked_soffice(fake_run)
        profile_args = [a for a in seen_cmd["cmd"] if a.startswith("-env:UserInstallation=")]
        self.assertEqual(len(profile_args), 1)


class TParseConversionCheckpointTests(unittest.TestCase):
    """t_parse's PPTX branch: convert-and-commit when converted.pdf isn't
    checkpointed yet, or load-and-skip when it already is. Calls
    t_parse.fn (Prefect's own escape hatch to the undecorated function) so
    this runs as a plain function call, no Prefect engine/flow context
    needed."""

    def _patched(self, *, conv_checkpoint_exists: bool, raw_data: bytes):
        exists_calls = []

        def fake_exists(key):
            exists_calls.append(key)
            if key.endswith("converted.pdf"):
                return conv_checkpoint_exists
            return False  # parsed.json: force past that checkpoint every time

        def fake_head(key):
            return {"size": len(b"%PDF-already-converted")} if (
                key.endswith("converted.pdf") and conv_checkpoint_exists) else None

        def fake_get_bytes(key):
            if key.endswith("converted.pdf"):
                return b"%PDF-already-converted"
            return raw_data  # the storage_key read (original .pptx bytes)

        return patch.multiple(
            document,
            storage=MagicMock(exists=fake_exists, head=fake_head,
                              get_bytes=fake_get_bytes, put_bytes=MagicMock()),
            db=MagicMock(check_generation=MagicMock(), set_status=MagicMock()),
            _parse_pdf=MagicMock(return_value=[{"page": 1, "text": "hello"}]),
            # The fake "%PDF-..." placeholders below aren't real PDFs fitz
            # can open — _pdf_page_count is exercised on its own in
            # ConvertedPagesCapTests, not re-run against these fixtures.
            _pdf_page_count=MagicMock(return_value=1),
        )

    def test_converts_and_commits_checkpoint_when_none_exists(self):
        with self._patched(conv_checkpoint_exists=False, raw_data=_real_pptx_bytes()), \
             patch.object(document, "_convert_pptx_to_pdf",
                         return_value=b"%PDF-freshly-converted") as mock_convert:
            document.t_parse.fn("doc1", "user1", None, "deck", 1,
                               storage_key="documents/user1/x.pptx")
            mock_convert.assert_called_once()
            put_calls = document.storage.put_bytes.call_args_list
            conv_puts = [c for c in put_calls if c.args[0].endswith("converted.pdf")]
            self.assertEqual(len(conv_puts), 1)
            self.assertEqual(conv_puts[0].args[1], b"%PDF-freshly-converted")
            expected_key = detect.converted_pdf_key("user1", "doc1", "deck")
            persisted = [c for c in document.db.set_status.call_args_list
                         if c.kwargs.get("view_storage_key") == expected_key]
            self.assertEqual(len(persisted), 1)

    def test_skips_conversion_when_checkpoint_already_committed(self):
        with self._patched(conv_checkpoint_exists=True, raw_data=_real_pptx_bytes()), \
             patch.object(document, "_convert_pptx_to_pdf") as mock_convert:
            document.t_parse.fn("doc1", "user1", None, "deck", 1,
                               storage_key="documents/user1/x.pptx")
            mock_convert.assert_not_called()
            # _parse_pdf must have received the CHECKPOINTED bytes, not the
            # raw .pptx bytes — proof the skip path actually loads the
            # checkpoint instead of just skipping the write.
            document._parse_pdf.assert_called_once()
            self.assertEqual(document._parse_pdf.call_args.args[0], b"%PDF-already-converted")

    def test_corrupt_conversion_checkpoint_is_redone_not_trusted(self):
        """A converted.pdf checkpoint that exists but isn't actually a PDF
        (truncated write, foreign file at that key) must be treated the
        same as "doesn't exist" — redo the conversion — rather than handing
        garbage bytes to _parse_pdf forever on every future retry. Mirrors
        _load_checkpoint's identical revalidate-on-read principle for the
        JSON checkpoints (found in review — the converted.pdf checkpoint
        had no equivalent guard)."""
        def fake_exists(key):
            return key.endswith("converted.pdf")  # claims to exist

        def fake_get_bytes(key):
            if key.endswith("converted.pdf"):
                return b"not a pdf at all"  # corrupt
            return _real_pptx_bytes()

        with patch.multiple(
                document,
                storage=MagicMock(exists=fake_exists,
                                  head=lambda key: ({"size": len(b"not a pdf at all")}
                                                    if key.endswith("converted.pdf") else None),
                                  get_bytes=fake_get_bytes,
                                  put_bytes=MagicMock()),
                db=MagicMock(check_generation=MagicMock(), set_status=MagicMock()),
                _parse_pdf=MagicMock(return_value=[{"page": 1, "text": "hello"}]),
                _pdf_page_count=MagicMock(return_value=1)), \
             patch.object(document, "_convert_pptx_to_pdf",
                         return_value=b"%PDF-freshly-converted") as mock_convert:
            document.t_parse.fn("doc1", "user1", None, "deck", 1,
                               storage_key="documents/user1/x.pptx")
            mock_convert.assert_called_once()
            self.assertEqual(document._parse_pdf.call_args.args[0], b"%PDF-freshly-converted")

    def test_oversized_conversion_checkpoint_is_rejected_before_download(self):
        """An existing object must be size-checked through HEAD before
        get_bytes() buffers the entire converted PDF into worker memory."""
        storage_mock = MagicMock()
        storage_mock.exists.return_value = False  # parsed.json is absent
        storage_mock.head.return_value = {"size": 11}
        storage_mock.get_bytes.side_effect = lambda key: (
            _real_pptx_bytes() if key.endswith("x.pptx") else
            self.fail("oversized converted.pdf must not be downloaded"))
        with patch.object(document, "_MAX_CONVERTED_BYTES", 10), patch.multiple(
                document,
                storage=storage_mock,
                db=MagicMock(check_generation=MagicMock(), set_status=MagicMock()),
                _parse_pdf=MagicMock(return_value=[{"page": 1, "text": "hello"}])):
            with self.assertRaises(document.PermanentDocumentError) as ctx:
                document.t_parse.fn("doc1", "user1", None, "deck", 1,
                                   storage_key="documents/user1/x.pptx")
        self.assertIn("checkpoint exceeds", str(ctx.exception))
        storage_mock.get_bytes.assert_called_once_with("documents/user1/x.pptx")

    def test_pdf_upload_never_calls_convert_or_touches_conversion_checkpoint(self):
        """A plain PDF (not a .pptx) must skip the whole conversion branch
        — sniff_document_kind returns 'pdf', not 'pptx'."""
        with self._patched(conv_checkpoint_exists=False, raw_data=b"%PDF-1.4\nreal pdf"), \
             patch.object(document, "_convert_pptx_to_pdf") as mock_convert:
            document.t_parse.fn("doc1", "user1", None, "paper", 1,
                               storage_key="documents/user1/x.pdf")
            mock_convert.assert_not_called()
            conv_puts = [c for c in document.storage.put_bytes.call_args_list
                        if c.args[0].endswith("converted.pdf")]
            self.assertEqual(conv_puts, [])

    def test_url_registered_pptx_mismatched_as_paper_is_rejected(self):
        """src/api/documents.py forces kind="deck" for a sniffed PPTX at
        UPLOAD time, but a URL registration (POST /admin/documents) can't
        sniff the bytes until t_parse actually fetches them — so a paper-
        registered URI that turns out to serve a .pptx must be caught HERE,
        before any checkpoint is written under the wrong kind's path (found
        in review: continuing would produce "page N" locators for a deck,
        and namespace converted.pdf/parsed.json under kind=paper instead of
        kind=deck, silently diverging from where viewer_storage_key and a
        later correctly-kinded retry would look)."""
        with self._patched(conv_checkpoint_exists=False, raw_data=_real_pptx_bytes()), \
             patch.object(document, "_convert_pptx_to_pdf") as mock_convert:
            with self.assertRaises(document.PermanentDocumentError) as ctx:
                document.t_parse.fn("doc1", "user1", None, "paper", 1,
                                   storage_key="documents/user1/x.pptx")
            self.assertIn("deck", str(ctx.exception))
            mock_convert.assert_not_called()
            conv_puts = [c for c in document.storage.put_bytes.call_args_list
                        if c.args[0].endswith("converted.pdf")]
            self.assertEqual(conv_puts, [])

    def test_url_registered_pptx_correctly_kinded_as_deck_still_converts(self):
        """The mismatch check above must not false-positive on the normal,
        correctly-kinded case — a URI registered with kind="deck" that turns
        out to be a real .pptx converts exactly as an upload would (fetched
        via _fetch_bytes since there's no storage_key, unlike an upload)."""
        with self._patched(conv_checkpoint_exists=False, raw_data=_real_pptx_bytes()), \
             patch.object(document, "_fetch_bytes", return_value=_real_pptx_bytes()), \
             patch.object(document, "_convert_pptx_to_pdf",
                         return_value=b"%PDF-freshly-converted") as mock_convert:
            document.t_parse.fn("doc1", "user1", "https://example.com/deck.pptx", "deck", 1)
            mock_convert.assert_called_once()

    def test_cached_url_pptx_backfills_persisted_view_key(self):
        """Rows indexed before view_storage_key was added may resume from
        parsed.json without fetching the source again. Backfill from the
        durable converted checkpoint in the ingest path, including URI
        documents that have no original storage_key."""
        parsed = b'[{"page": 1, "text": "hello"}]'
        conv_key = detect.converted_pdf_key("user1", "doc1", "deck")
        storage_mock = MagicMock()
        storage_mock.exists.return_value = True
        storage_mock.get_bytes.return_value = parsed
        storage_mock.head.return_value = {"size": 123}
        db_mock = MagicMock()
        with patch.multiple(document, storage=storage_mock, db=db_mock):
            result = document.t_parse.fn(
                "doc1", "user1", "https://example.com/deck.pptx", "deck", 1)
        self.assertEqual(result, [{"page": 1, "text": "hello"}])
        persisted = [c for c in db_mock.set_status.call_args_list
                     if c.kwargs.get("view_storage_key") == conv_key]
        self.assertEqual(len(persisted), 1)


def _minimal_pdf_bytes(n_pages: int) -> bytes:
    """A real, fitz-openable PDF with `n_pages` blank pages — used to
    exercise _pdf_page_count and the converted-pages cap against genuine
    PDF structure, not a "%PDF-..." placeholder string."""
    import fitz  # pymupdf

    doc = fitz.open()
    for _ in range(n_pages):
        doc.new_page()
    data = doc.tobytes()
    doc.close()
    return data


class ConvertedPagesCapTests(unittest.TestCase):
    """DOCUMENT_MAX_CONVERTED_PAGES: a converted deck with too many slides
    must be rejected BEFORE _parse_pdf runs — and before any vision-
    captioning cost is paid — not after DOCUMENT_MAX_CHUNKS eventually
    catches it post-parse (found in review)."""

    def _patched_for_conversion(self, converted_pdf: bytes):
        def fake_get_bytes(key):
            return b"" if key.endswith("converted.pdf") else _real_pptx_bytes()

        return patch.multiple(
            document,
            storage=MagicMock(exists=MagicMock(return_value=False),
                              head=MagicMock(return_value=None),
                              get_bytes=fake_get_bytes, put_bytes=MagicMock()),
            db=MagicMock(check_generation=MagicMock(), set_status=MagicMock()),
            _parse_pdf=MagicMock(return_value=[{"page": 1, "text": "hello"}]),
        ), patch.object(document, "_convert_pptx_to_pdf", return_value=converted_pdf)

    def test_pdf_page_count_counts_real_pages(self):
        self.assertEqual(document._pdf_page_count(_minimal_pdf_bytes(3)), 3)

    def test_converted_deck_over_the_cap_is_rejected_before_parsing(self):
        multi, convert_patch = self._patched_for_conversion(_minimal_pdf_bytes(3))
        with patch.object(document, "DOCUMENT_MAX_CONVERTED_PAGES", 2), multi, convert_patch:
            with self.assertRaises(document.PermanentDocumentError) as ctx:
                document.t_parse.fn("doc1", "user1", None, "deck", 1,
                                   storage_key="documents/user1/x.pptx")
            self.assertIn("3 slides", str(ctx.exception))
            document._parse_pdf.assert_not_called()

    def test_converted_deck_within_the_cap_still_parses(self):
        multi, convert_patch = self._patched_for_conversion(_minimal_pdf_bytes(3))
        with patch.object(document, "DOCUMENT_MAX_CONVERTED_PAGES", 5), multi, convert_patch:
            document.t_parse.fn("doc1", "user1", None, "deck", 1,
                               storage_key="documents/user1/x.pptx")
            document._parse_pdf.assert_called_once()

    def test_malformed_converted_pdf_page_table_is_a_permanent_failure(self):
        """Page-table inspection is deterministic for fixed converted
        bytes; parser errors here must skip Prefect's retry backoff just as
        errors from the full PDF parse already do."""
        multi, convert_patch = self._patched_for_conversion(b"%PDF-corrupt")
        with multi, convert_patch, patch.object(
                document, "_pdf_page_count", side_effect=ValueError("bad xref")):
            with self.assertRaises(document.PermanentDocumentError) as ctx:
                document.t_parse.fn("doc1", "user1", None, "deck", 1,
                                   storage_key="documents/user1/x.pptx")
            self.assertIn("inspect converted PDF", str(ctx.exception))
            document._parse_pdf.assert_not_called()


class ViewerStorageKeyTests(unittest.TestCase):
    """Citation selection is a pure manifest-field choice. Ingestion has
    already recorded whether a browser-viewable derivative exists."""

    def test_persisted_view_key_is_selected_without_a_storage_probe(self):
        """The manifest records the derivative selected at ingest time;
        query-time citation rendering must be a pure field choice, not an
        S3/GCS HEAD request for every result."""
        with patch.object(detect, "storage", create=True) as storage_mock:
            result = detect.viewer_storage_key(
                "documents/user1/original.pptx",
                "docs/user1/doc1/deck/v2/converted.pdf")
            self.assertEqual(result, "docs/user1/doc1/deck/v2/converted.pdf")
            self.assertEqual(storage_mock.mock_calls, [])

    def test_original_pdf_key_is_the_fallback_without_a_derivative(self):
        result = detect.viewer_storage_key("documents/user1/x.pdf", None)
        self.assertEqual(result, "documents/user1/x.pdf")

    def test_url_pdf_with_no_storage_fields_stays_none(self):
        """A URL-registered PDF (no storage_key, never converted) has
        nothing to view via our own storage at all — must stay None so
        callers fall through to the external uri."""
        result = detect.viewer_storage_key(None, None)
        self.assertIsNone(result)

    def test_url_pptx_can_use_our_persisted_derivative_without_original_key(self):
        derivative = detect.converted_pdf_key("user1", "doc1", "deck")
        self.assertEqual(detect.viewer_storage_key(None, derivative), derivative)


class ExistingPptxViewKeyMigrationTests(unittest.TestCase):
    """Schema startup preserves citations for PPTX rows indexed before the
    manifest gained ``view_storage_key``."""

    def test_schema_init_backfills_an_existing_valid_converted_pdf(self):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [{
            "id": "doc1", "user_id": "user1", "kind": "deck",
        }]
        conn.execute.return_value.rowcount = 1
        pool_mock = MagicMock()
        pool_mock.connection.return_value.__enter__.return_value = conn
        derivative = detect.converted_pdf_key("user1", "doc1", "deck")

        with patch.object(db, "pool", return_value=pool_mock), \
             patch("src.storage.head", return_value={
                 "size": len(b"%PDF-valid"), "content_type": "application/pdf",
             }) as head_mock, \
             patch("src.storage.get_bytes", return_value=b"%PDF-valid"):
            db.init_schema()

        head_mock.assert_called_once_with(derivative)
        updates = [
            call for call in conn.execute.call_args_list
            if "UPDATE ms_videos" in call.args[0]
        ]
        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0].args[1], (derivative, "doc1"))


if __name__ == "__main__":
    unittest.main()
