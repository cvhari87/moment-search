#!/usr/bin/env python3
"""Benchmark + SLA gate for Assignment 3 — Moment Search at Scale.

    python benchmark/bench.py                 # accept-latency, ingest-vs-search, recall, throughput
    python benchmark/bench.py --resilience    # kill a worker mid-ingest, assert no loss
    python benchmark/bench.py --json out.json # also write machine-readable results

Exits non-zero if ANY target in sla.json is missed, so it doubles as your grading
gate and a CI check.

Self-isolation (every check below): each check creates its OWN sources under a
dedicated per-run sub-user id and tracks EXACTLY the ids it created — never reads
the full, unscoped /admin/sources listing. Three reasons this matters: (1) it must
never misattribute the real corpus's rows, or another check's rows, as its own
drops/successes; (2) the accept-latency check's 30 deliberately-broken probes are
SUPPOSED to end in 'failed' — a check that didn't scope to its own ids could
misread those as a resilience or throughput failure; (3) checks that register
sources under the SAME user compete in that user's own per-user FIFO admission
order (db.wfq_claim orders by created_at WITHIN a user; WFQ fairness is ACROSS
users, not within one) — measured live: 30 probes registered before a real
backfill, sharing one user, meant the probes were admitted first and starved the
backfill behind them. Separate sub-users close both gaps at once. recall@10 is the
one exception: it deliberately queries the REAL labeled corpus (RECALL_USER,
default "default"), since that's the corpus benchmark/queries.jsonl is grounded
against.

Nothing here silently discards a failure: accept-latency, search sampling,
backfill registration, backfill INGESTION (accepted documents that never reached
'indexed'), and recall each track their own error rate, and main() gates the
worst of them against sla.json's error_rate_max_pct — a run that's mostly failing
can no longer look clean just because its few successes were fast.

No gate here can pass without having actually tested its claim. Specifically: the
decoupling gate fails unless real ingest work was OBSERVED running during search
sampling (and its minimum can be raised from the CLI but never lowered); the
throughput gate fails unless EVERY accepted document reached 'indexed', so a fast
surviving subset can't carry a run where the rest died; the resilience gate fails
unless every requested document registered, unless the worker was killed with a
document specifically mid-'chunking'/'embedding' (the only states with a
committed checkpoint to resume from), and unless the worker's own logs prove that
checkpoint was reused rather than redone; and the accept-latency probes, whose
content is deliberately invalid, must all end specifically 'failed' — 'indexed'
would mean invalid content became searchable.

Those false-pass paths are regression-tested in benchmark/test_bench_gates.py
(stdlib unittest, no live stack needed):

    python3 -m unittest discover -s benchmark -p 'test_*.py'
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parent.parent
SLA = json.loads((ROOT / "benchmark" / "sla.json").read_text())
BASE = os.getenv("BASE_URL", "http://localhost:8100").rstrip("/")
ADMIN = os.getenv("ADMIN_TOKEN", "")
BENCH_USER = os.getenv("BENCH_USER", f"bench_{uuid.uuid4().hex[:8]}")
RECALL_USER = os.getenv("RECALL_USER", "default")  # the real labeled corpus lives here
PROBE_USER = f"{BENCH_USER}_probes"
BACKFILL_USER = f"{BENCH_USER}_backfill"
RESILIENCE_USER = f"{BENCH_USER}_resilience"

# Genuinely EXECUTING statuses — a task is actually running, contending for
# real resources. Distinct from 'pending'/'queued' (admitted but possibly not
# yet started) and from _TERMINAL below. Used both to prove the resilience
# check actually crashes live work (not an idle queue) and to prove the
# decoupling check's search sampling actually overlapped active ingest.
_ACTIVE_STATUSES = {"fetching", "sampling", "parsing", "chunking", "embedding"}
_TERMINAL = {"indexed", "failed", "skipped"}

# Same doc_id derivation as src/api/documents.py::_doc_id — replicated here (not
# imported: bench.py is an external HTTP-only client, never imports app code) so
# recall can compute the EXACT id the API would assign a given (user, uri) and
# check it directly, instead of trusting a static id that drifts across
# environments (see benchmark/queries.jsonl's own note on this).
def _doc_id(user_id: str, identity: str) -> str:
    return "doc_" + hashlib.sha256(f"{user_id}:{identity}".encode()).hexdigest()[:12]


PAPER_URI = "https://arxiv.org/pdf/2312.10997"
DECK_URI = "http://api:8000/corpus/one-index-for-every-source-deck.pdf"


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _req(method, path, body=None, token=None, headers=None, timeout=30):
    url = f"{BASE}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("content-type", "application/json")
    if token:
        req.add_header("authorization", f"Bearer {token}")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode(), (time.perf_counter() - t0) * 1000
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(), (time.perf_counter() - t0) * 1000
    except Exception as e:  # noqa: BLE001
        return 0, str(e), (time.perf_counter() - t0) * 1000


def _multipart(fields: dict, file_field: str, filename: str,
              file_bytes: bytes, content_type: str) -> tuple[bytes, str]:
    boundary = f"----benchpy{uuid.uuid4().hex}"
    parts = []
    for k, v in fields.items():
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode())
    parts.append(
        (f'--{boundary}\r\nContent-Disposition: form-data; name="{file_field}"; '
         f'filename="{filename}"\r\nContent-Type: {content_type}\r\n\r\n').encode()
        + file_bytes + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    return b"".join(parts), f"multipart/form-data; boundary={boundary}"


def _upload_document(kind: str, title: str, pdf_bytes: bytes, user_id: str, timeout=30):
    body, ctype = _multipart({"kind": kind, "title": title}, "file", f"{title}.pdf",
                             pdf_bytes, "application/pdf")
    req = urllib.request.Request(f"{BASE}/admin/documents/upload", data=body, method="POST")
    req.add_header("content-type", ctype)
    if ADMIN:
        req.add_header("authorization", f"Bearer {ADMIN}")
    req.add_header("x-user-id", user_id)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:  # noqa: BLE001
        return 0, str(e)


def p95(xs):
    return statistics.quantiles(xs, n=100)[94] if len(xs) >= 20 else (max(xs) if xs else 0.0)


# ── Synthetic PDF generation (stdlib only) ───────────────────────────────────
# Backfill/throughput/resilience checks need many DISTINCT, real,
# pymupdf-parseable documents. Reusing a real external URL (arXiv, etc.) N
# times just updates ONE row (doc_id is content-hash-of-uri based) and adds a
# real-network dependency to a benchmark run. Building minimal, byte-correct
# PDFs from scratch avoids both — this exercises the REAL parse/chunk/embed
# pipeline (not a mock), just against synthetic-but-valid content.

def _pdf_escape(s: str) -> str:
    return s.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")


def _wrap_lines(text: str, width: int = 90) -> list[str]:
    words = text.split()
    lines, buf, n = [], [], 0
    for w in words:
        buf.append(w)
        n += len(w) + 1
        if n >= width:
            lines.append(" ".join(buf))
            buf, n = [], 0
    if buf:
        lines.append(" ".join(buf))
    return lines or [""]


def _content_stream(text: str) -> bytes:
    lines = _wrap_lines(text)
    parts = ["BT", "/F1 11 Tf", "72 740 Td", "14 TL", f"({_pdf_escape(lines[0])}) Tj"]
    for line in lines[1:]:
        parts.append("T*")
        parts.append(f"({_pdf_escape(line)}) Tj")
    parts.append("ET")
    return "\n".join(parts).encode("latin-1")


def _make_pdf(pages: list[str]) -> bytes:
    """Minimal, byte-correct multi-page PDF (real xref table, not relying on
    a reader's repair-mode tolerance) — `pages[i]` becomes page i+1's
    extractable text. Verified against the app's real pymupdf parser."""
    n_pages = len(pages)
    font_obj = 3 + 2 * n_pages
    bodies: dict[int, bytes] = {
        1: b"<< /Type /Catalog /Pages 2 0 R >>",
    }
    kids = " ".join(f"{3 + 2 * i} 0 R" for i in range(n_pages))
    bodies[2] = f"<< /Type /Pages /Kids [{kids}] /Count {n_pages} >>".encode()
    for i, text in enumerate(pages):
        page_obj = 3 + 2 * i
        content_obj = 4 + 2 * i
        stream = _content_stream(text)
        bodies[page_obj] = (
            f"<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 {font_obj} 0 R >> >> "
            f"/MediaBox [0 0 612 792] /Contents {content_obj} 0 R >>").encode()
        bodies[content_obj] = f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream"
    bodies[font_obj] = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"

    n_objs = font_obj
    out = bytearray(b"%PDF-1.4\n")
    offsets = [0] * (n_objs + 1)
    for i in range(1, n_objs + 1):
        offsets[i] = len(out)
        out += f"{i} 0 obj\n".encode() + bodies[i] + b"\nendobj\n"
    xref_offset = len(out)
    out += f"xref\n0 {n_objs + 1}\n".encode()
    out += b"0000000000 65535 f \n"
    for i in range(1, n_objs + 1):
        out += f"{offsets[i]:010d} 00000 n \n".encode()
    out += (f"trailer\n<< /Size {n_objs + 1} /Root 1 0 R >>\n"
            f"startxref\n{xref_offset}\n%%EOF").encode()
    return bytes(out)


_LOREM = ("benchmark backfill synthetic content for throughput and decoupling "
          "measurement word count padding text filler sentence structure ")


def _synthetic_pdf(doc_index: int, n_pages: int = 3) -> bytes:
    # ~1680 chars/page at this padding -> ~2 chunks/page (DOCUMENT_CHUNK_CHARS
    # default 1000) -> ~6 chunks/doc at n_pages=3, verified live against the
    # real pipeline before this was wired in here.
    pages = [f"Document {doc_index} page {p}. " + _LOREM * 20 for p in range(n_pages)]
    return _make_pdf(pages)


# ── Self-isolated source tracking ────────────────────────────────────────────

def _sources_for(user_id: str) -> dict[str, dict]:
    st, body, _ = _req("GET", "/admin/sources", token=ADMIN, headers={"X-User-Id": user_id})
    if st != 200:
        return {}
    return {s["id"]: s for s in json.loads(body).get("sources", [])}


def _wait_terminal(user_id: str, ids: set[str], timeout: float, poll_s: float = 2.0) -> dict[str, dict]:
    deadline = time.time() + timeout
    rows = {}
    while time.time() < deadline:
        rows = _sources_for(user_id)
        if all(rows.get(i, {}).get("status") in _TERMINAL for i in ids):
            return rows
        time.sleep(poll_s)
    return rows


def _wait_for_active(user_id: str, ids: set[str], timeout: float, poll_s: float = 0.5,
                     statuses: set[str] | None = None) -> dict[str, str]:
    """Poll until at least one tracked id reaches a genuinely EXECUTING
    status — 'queued' alone only means admitted, the flow may not have
    started its first task yet. Returns {id: status} for every id in a
    matching status at the moment this returns; empty if none ever
    were within the timeout. Callers that need to prove they crashed or
    measured against LIVE work (not an idle queue) must treat an empty
    result as a hard failure of the check itself, not a soft warning — a
    guardrail review reproduced exactly this: killing a worker with nothing
    in flight, or sampling search "during" a backfill that had already fully
    drained, both let the respective gate pass without testing anything.

    `statuses` narrows what counts (default: any of _ACTIVE_STATUSES). The
    resilience check passes {'chunking','embedding'} specifically, because
    those are the ONLY statuses that guarantee a checkpoint was already
    committed — killing mid-'parsing' leaves nothing to prove resume
    against, and a later guardrail round proved that path could pass the
    gate while asserting nothing at all."""
    want = statuses or _ACTIVE_STATUSES
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = _sources_for(user_id)
        active = {i: rows[i]["status"] for i in ids if i in rows and rows[i]["status"] in want}
        if active:
            return active
        time.sleep(poll_s)
    return {}


def _cleanup(user_id: str, ids: set[str]) -> None:
    """Only deletes rows already confirmed terminal — deleting a row a flow
    run still owns (queued/parsing/etc.) races the worker and produces a
    real "no manifest row for X" crash when it next tries to write to that
    id (measured live). Anything still non-terminal is left alone and
    reported, not force-deleted; it'll finish on its own, and the next
    bench.py run gets a fresh BENCH_USER regardless, so nothing lingers
    forever in the way."""
    if not ids:
        return
    rows = _wait_terminal(user_id, ids, timeout=30, poll_s=3.0)
    done = {i for i in ids if rows.get(i, {}).get("status") in _TERMINAL}
    skipped = ids - done
    for i in done:
        _req("DELETE", f"/api/videos/{i}", token=ADMIN, headers={"X-User-Id": user_id})
    if skipped:
        print(f"[cleanup] left {len(skipped)} still-in-flight source(s) alone "
             f"(user {user_id!r}): {sorted(skipped)}")


def _backfill(n_docs: int, n_pages: int, user_id: str) -> tuple[set[str], int]:
    """Returns (registered ids, count of registration failures) — the
    failure count feeds the error-rate gate in main() instead of silently
    shrinking the effective cohort every downstream measurement works from."""
    ids = set()
    failed = 0
    for i in range(n_docs):
        pdf = _synthetic_pdf(i, n_pages)
        st, body = _upload_document("paper", f"bench backfill {i}", pdf, user_id)
        if st == 202:
            ids.add(json.loads(body)["id"])
        else:
            failed += 1
            print(f"[backfill] doc {i} failed to register: {st} {body[:200]}")
    return ids, failed


# ── SSE ──────────────────────────────────────────────────────────────────────

def _ask_stream_first_event(question: str, user_id: str, timeout=30,
                            top_k: int | None = None) -> tuple[float, list[dict]]:
    """Time-to-FIRST-SSE-event (the citations event, always sent first by
    /ask_stream) and its parsed citations — deliberately NOT full-stream-to-
    completion. Full-stream time is dominated by LLM answer synthesis (an
    external API call the ingest queue's own resources don't contend for)
    and would hide the actual thing the decoupling gate needs to prove: does
    RETRIEVAL — the latency-critical, queue-adjacent read path — stay fast
    while a real backfill runs. _ask_stream_full_latency() below reports the
    other number too, for context, but it isn't gated on.

    `top_k` is passed through to /ask_stream's own optional query param
    (added alongside this fix) — omitted, the app answers with its own
    default (config.TOP_K, 6); recall@10 passes 10 explicitly so it's
    actually testing what its name claims, not silently testing recall@6."""
    url = f"{BASE}/ask_stream?q=" + urllib.parse.quote(question)
    if top_k is not None:
        url += f"&top_k={top_k}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("x-user-id", user_id)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            buf = b""
            for line in r:
                buf += line
                if buf.endswith(b"\n\n"):
                    ms = (time.perf_counter() - t0) * 1000
                    text = buf.decode().strip()
                    citations = []
                    if text.startswith("data:"):
                        payload = json.loads(text[len("data:"):].strip())
                        citations = payload.get("citations", [])
                    return ms, citations
    except Exception:  # noqa: BLE001
        return float("inf"), []
    return float("inf"), []


def _ask_stream_full_latency(question: str, user_id: str, timeout=60) -> float:
    url = f"{BASE}/ask_stream?q=" + urllib.parse.quote(question)
    req = urllib.request.Request(url, method="GET")
    req.add_header("x-user-id", user_id)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            r.read()
            return (time.perf_counter() - t0) * 1000
    except Exception:  # noqa: BLE001
        return float("inf")


# ── 1. Accept latency (poison probes — Trap 3) ───────────────────────────────

def measure_accept_latency(n=30) -> tuple[float, set[str], float]:
    """POST /admin/documents should enqueue-and-return fast (no parsing
    in-request). Probes are deliberately unfetchable/non-PDF (Trap 3) and
    scoped to their OWN sub-user (PROBE_USER, not shared with the backfill)
    — sharing a user would put 30 probes ahead of the backfill in that
    user's own admission FIFO (WFQ fairness is ACROSS users, not within
    one), starving it. Measured live before this split existed.

    Returns (p95_ms, ids, error_rate) — error_rate is the fraction of
    registrations that did NOT return 202 (should be ~0; these probes are
    malformed URIs, not malformed REQUESTS, so registration itself should
    still succeed every time)."""
    lat = []
    ids = set()
    errors = 0
    for i in range(n):
        st, body, ms = _req("POST", "/admin/documents", token=ADMIN,
                            headers={"X-User-Id": PROBE_USER},
                            body={"uri": f"https://example.com/probe_{i}.pdf",
                                  "kind": "paper", "title": f"probe {i}"})
        if st == 202:
            lat.append(ms)
            ids.add(json.loads(body)["id"])
        else:
            errors += 1
    error_rate = errors / n if n else 0.0
    return (p95(lat) if lat else float("inf")), ids, error_rate


# ── 2 & 4. Decoupling ratio + throughput (one real backfill, measured together) ─

def measure_search_p95(n: int, user_id: str) -> tuple[float, float]:
    """Returns (p95_ms of SUCCESSFUL samples, error_rate of failed/timed-out
    samples). A guardrail review reproduced a case where one 10ms success
    plus 39 failures reported a misleadingly-clean 10ms p95 — that number
    alone is still a legitimate answer to "how fast are the successes," but
    error_rate is now returned alongside it and gated separately in main()
    (sla.json's error_rate_max_pct) so a mostly-failing run can't look clean
    just because its few successes happened to be fast."""
    q = "what does the survey say about hybrid retrieval"
    results = [_ask_stream_first_event(q, user_id) for _ in range(n)]
    lat = [ms for ms, _ in results if ms != float("inf")]
    error_rate = 1.0 - (len(lat) / n) if n else 0.0
    return (p95(lat) if lat else float("inf")), error_rate


def _poll_active(user_id: str, ids: set[str], stop_event: threading.Event, poll_s=0.5) -> list[bool]:
    """Background sampler for the decoupling check: one True/False per poll
    for whether ANY tracked backfill id was in a genuinely EXECUTING status
    at that instant (not 'pending'/'queued', which aren't consuming any
    resources search would contend with). Used to prove 'during' sampling
    actually overlapped real ingest work, not just that some rows hadn't
    reached a terminal state yet."""
    samples = []
    while not stop_event.is_set():
        rows = _sources_for(user_id)
        samples.append(any(rows.get(i, {}).get("status") in _ACTIVE_STATUSES for i in ids))
        time.sleep(poll_s)
    return samples


def measure_decoupling_and_throughput(n_docs=20, n_pages=3, n_samples=40, min_overlap_frac=0.5):
    """Trap 2, fixed three ways, not one:
      (a) 'during' must be sampled WHILE a real background ingest runs. The
          original stub called measure_search_p95() twice with nothing
          happening in between — idle == during by construction, so the
          20-point decoupling gate went green unearned.
      (b) the backfill must be sized so its own duration comfortably
          outlasts the full sampling window, or a small backfill can finish
          before sampling does and 'during' quietly degrades back toward
          idle. Registration returns fast (202s); the actual parse/chunk/
          embed work continues on the worker regardless of what this script
          does next, so 'during' sampling starts immediately after
          registration and genuinely overlaps real ingest activity for a
          measured backfill this size.
      (c) overlap is VERIFIED, not assumed or merely warned about: a
          background thread polls (every 0.5s) whether real ingest work
          (_ACTIVE_STATUSES — parsing/chunking/embedding, not just
          'pending'/'queued') is happening WHILE 'during' sampling runs.
          If fewer than `min_overlap_frac` of those polls found active
          work, the decoupling gate is forced to FAIL regardless of the
          numeric ratio — a guardrail review proved the old version could
          pass with zero real overlap (all-pending backfill, or a fully-
          drained one) since it only printed a warning, never failed.

    Returns (ratio, overlap_ok, throughput, ids, err_idle, err_during, err_backfill_reg).
    """
    idle, err_idle = measure_search_p95(n_samples, RECALL_USER)

    t_backfill_start = time.time()
    ids, backfill_reg_failed = _backfill(n_docs, n_pages, BACKFILL_USER)
    err_backfill_reg = (backfill_reg_failed / n_docs) if n_docs else 0.0

    stop_event = threading.Event()
    activity: list[bool] = []
    poll_thread = threading.Thread(
        target=lambda: activity.extend(_poll_active(BACKFILL_USER, ids, stop_event)))
    poll_thread.start()

    during, err_during = measure_search_p95(n_samples, RECALL_USER)

    stop_event.set()
    poll_thread.join()

    overlap_frac = (sum(activity) / len(activity)) if activity else 0.0
    overlap_ok = overlap_frac >= min_overlap_frac
    print(f"[decoupling] {sum(activity)}/{len(activity)} polls ({overlap_frac:.0%}) found "
         f"ACTIVE (parsing/chunking/embedding) ingest work during 'during' sampling — "
         f"{'OK' if overlap_ok else f'BELOW the {min_overlap_frac:.0%} minimum, gate forced to FAIL'}")

    rows_final = _wait_terminal(BACKFILL_USER, ids, timeout=max(120.0, 6.0 * len(ids)))
    t_backfill_end = time.time()

    indexed_ids = {i for i in ids if rows_final.get(i, {}).get("status") == "indexed"}
    indexed_chunks = sum(rows_final.get(i, {}).get("chunk_count") or 0 for i in indexed_ids)
    wall_s = t_backfill_end - t_backfill_start
    throughput = (indexed_chunks / wall_s) if wall_s > 0 else 0.0

    # Throughput is chunks/s of SUCCESSFUL ingestion, so it must be earned by
    # the WHOLE accepted cohort — not by whichever fast subset happened to
    # finish. Summing only indexed docs while ignoring failed/skipped/stuck
    # ones (the previous behavior) meant a run where most documents died
    # could still post a high chunks/s and pass: the survivors' chunks over
    # the survivors' shorter wall time. These are our own guaranteed-valid
    # synthetic PDFs, so any non-'indexed' outcome is a real ingestion
    # defect. It both forces the throughput gate to fail and feeds the
    # ingestion error rate, so it can never be absorbed silently.
    not_indexed = {i: rows_final.get(i, {}).get("status") for i in ids - indexed_ids}
    ingest_ok = not not_indexed
    err_backfill_ingest = (len(not_indexed) / len(ids)) if ids else 0.0
    if not_indexed:
        by_status: dict[str, int] = {}
        for s in not_indexed.values():
            by_status[s or "missing"] = by_status.get(s or "missing", 0) + 1
        print(f"[throughput] FAIL: {len(not_indexed)}/{len(ids)} accepted valid document(s) "
             f"never reached 'indexed' ({by_status}) — throughput measured over only the "
             f"successful subset would overstate real ingestion capacity, so the gate is "
             f"forced to FAIL and these count toward the ingestion error rate")

    ratio = (during / idle) if idle else float("inf")
    print(f"[decoupling] idle={idle:.1f}ms during={during:.1f}ms ratio={ratio:.2f} "
         f"(time-to-first-citations-event — the gate)")
    # Context only, never gated: full-stream (citations + LLM answer) latency
    # is dominated by an external API call the ingest queue doesn't contend
    # for, so it would hide the decoupling signal above if used as the gate
    # — reported here so a reader can see the two don't move together.
    full = _ask_stream_full_latency(
        "what does the survey say about hybrid retrieval", RECALL_USER)
    print(f"[decoupling] full-stream latency (context only, not gated): {full:.1f}ms")
    print(f"[throughput] {indexed_chunks} chunks / {wall_s:.1f}s = {throughput:.2f} chunks/s "
         f"({len(indexed_ids)}/{len(ids)} docs indexed, "
         f"{backfill_reg_failed} registration failures)")

    return (ratio, overlap_ok, throughput, ingest_ok, ids,
            err_idle, err_during, err_backfill_reg, err_backfill_ingest)


# ── 3. Recall@10 ──────────────────────────────────────────────────────────────

def _ensure_recall_corpus() -> None:
    """benchmark/queries.jsonl's own note: don't trust a static source_id
    across environments — verify the labeled paper+deck are actually
    indexed under RECALL_USER, registering them if missing, rather than
    assuming a fresh (or since-modified) environment already has them.
    Idempotent: re-registering an ALREADY-indexed row resets it to pending
    (db.upsert_pending's documented behavior), so this only POSTs when the
    expected id isn't already sitting there indexed."""
    rows = _sources_for(RECALL_USER)
    for uri, kind, title in ((PAPER_URI, "paper", "RAG Survey"),
                             (DECK_URI, "deck", "One Index for Every Source")):
        expected_id = _doc_id(RECALL_USER, uri)
        row = rows.get(expected_id)
        if row and row.get("status") == "indexed":
            continue
        print(f"[recall] {kind} not indexed under {RECALL_USER!r} "
              f"(expected {expected_id}) — registering")
        st, body, _ = _req("POST", "/admin/documents", token=ADMIN,
                           headers={"X-User-Id": RECALL_USER},
                           body={"uri": uri, "kind": kind, "title": title})
        if st != 202:
            print(f"[recall] WARNING: failed to register {kind}: {st} {body[:200]}")
            continue
        _wait_terminal(RECALL_USER, {expected_id}, timeout=180)


def _locator_matches(kind: str, got: dict, expected: dict) -> bool:
    if kind == "paper":
        return got.get("page") == expected.get("page")
    if kind == "deck":
        return got.get("slide") == expected.get("slide")
    if kind == "video":
        cs, ce = got.get("start_ms"), got.get("end_ms")
        es, ee = expected.get("start_ms"), expected.get("end_ms")
        if cs is None or ce is None or es is None or ee is None:
            return False
        return cs <= ee and ce >= es  # any overlap with the labeled window
    return False


def measure_recall(top_k: int = 10) -> tuple[float, float]:
    """recall@`top_k` — literally requests `top_k` results from /ask_stream
    (the app's own read-path default, config.TOP_K, is 6; the old version
    sliced whatever came back to [:10], which was never actually more than
    6 — testing recall@6 while calling itself recall@10)."""
    _ensure_recall_corpus()
    lines = (ROOT / "benchmark" / "queries.jsonl").read_text().splitlines()
    queries = [json.loads(l) for l in lines if l.strip()]

    expected_ids = {
        "paper": _doc_id(RECALL_USER, PAPER_URI),
        "deck": _doc_id(RECALL_USER, DECK_URI),
    }
    hits = 0
    errors = 0
    for q in queries:
        kind = q["kind"]
        expected_source_id = expected_ids.get(kind, q["source_id"])  # video ids are stable as-is
        ms, citations = _ask_stream_first_event(q["query"], RECALL_USER, top_k=top_k)
        if ms == float("inf"):
            errors += 1
        hit = any(c.get("kind") == kind and c.get("sourceId") == expected_source_id
                 and _locator_matches(kind, c.get("locator") or {}, q["locator"])
                 for c in citations[:top_k])
        hits += hit
        print(f"  [{'HIT ' if hit else 'MISS'}] {q['query'][:70]}")
    n = len(queries)
    recall = hits / n if n else 0.0
    error_rate = errors / n if n else 0.0
    return recall, error_rate


# ── --resilience ──────────────────────────────────────────────────────────────

def _verify_checkpoint_resume(targets: dict[str, str]) -> bool:
    """Greps the worker's own logs for the explicit "already committed —
    resuming from checkpoint" lines src/ingest/document.py's t_parse/t_chunk
    print — the only real evidence a recovered run skipped already-committed
    work instead of quietly redoing it.

    `targets` is {doc_id: status_at_kill_time}, and the assertion is
    status-SPECIFIC, because each status guarantees a different amount of
    committed work:
      - 'chunking'  → t_parse had committed parsed.json (and only that), so
                      require the parsed.json resume line. chunks.json did
                      NOT exist yet, so demanding its line would assert
                      something that was never true.
      - 'embedding' → t_parse AND t_chunk had both committed, so require
                      BOTH lines. Accepting just one here would let a run
                      that re-chunked from scratch still pass.
    An earlier version accepted EITHER line for either status, which a
    guardrail round showed would accept a partial redo as a clean resume."""
    result = subprocess.run(
        ["docker", "compose", "logs", "worker", "--since", "15m"],
        cwd=ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[resilience] FAIL: could not read worker logs to verify "
             f"checkpoint resume (exit {result.returncode})")
        return False
    logs = result.stdout
    ok = True
    for doc_id, status in sorted(targets.items()):
        required = [f"{doc_id}: parsed.json already committed"]
        if status == "embedding":
            required.append(f"{doc_id}: chunks.json already committed")
        missing = [line for line in required if line not in logs]
        if missing:
            print(f"[resilience] FAIL: {doc_id} (was {status!r} at kill time) is missing "
                 f"{len(missing)}/{len(required)} required checkpoint-resume log line(s): "
                 f"{missing} — work it had already committed was redone, not resumed")
            ok = False
        else:
            print(f"[resilience] checkpoint-resume confirmed for {doc_id} "
                 f"(was {status!r}, verified {len(required)} checkpoint line(s))")
    return ok


def run_resilience_check(n_docs=10, n_pages=2) -> tuple[bool, dict]:
    """Registers a real backfill, waits for GENUINELY ACTIVE work (not just
    "some time passed" — a guardrail review proved the old sleep(6) could
    pass with everything already indexed, or with the kill/restart commands
    silently failing), hard-kills the worker container, restarts it, and
    waits for the reconciler to recover every source — the automated
    version of the manual docker-kill-9 proof Block G did by hand. Uses the
    SAME RECONCILE_STALE_S the server is actually configured with (default
    300s) as the wait budget, not a shortened one — testing a faster
    threshold than production actually uses would prove a different, easier
    claim. Also verifies checkpoint-resume (not just eventual success) for
    any doc that had already committed one — see _verify_checkpoint_resume."""
    print(f"[resilience] backfilling {n_docs} synthetic documents under user {RESILIENCE_USER!r}...")
    ids, reg_failed = _backfill(n_docs, n_pages, RESILIENCE_USER)
    # Hard fail, NOT a warning: these are our own guaranteed-valid synthetic
    # PDFs, so a registration failure is a real defect. Worse, the old
    # warn-and-continue version then measured "all indexed" against only the
    # SHRUNKEN set that happened to register — a guardrail round reproduced
    # 9/10 failing to register, 1 doc indexed, and the gate still reporting
    # PASS. The cohort every downstream assertion uses must be the cohort
    # that was actually requested.
    if reg_failed or len(ids) != n_docs:
        print(f"[resilience] FAIL: only {len(ids)}/{n_docs} synthetic docs registered "
             f"({reg_failed} failure(s)) — refusing to measure crash recovery against a "
             f"silently shrunken cohort")
        _cleanup(RESILIENCE_USER, ids)
        return False, {}

    # Require a CHECKPOINT-BEARING target specifically, not merely "something
    # is running". Only 'chunking'/'embedding' guarantee committed work to
    # resume from; killing mid-'parsing' leaves nothing to prove, and the
    # previous version accepted that then auto-passed the resume assertion.
    print("[resilience] waiting for checkpoint-bearing in-flight work "
         "(chunking/embedding) before killing (up to 120s)...")
    active_before = _wait_for_active(RESILIENCE_USER, ids, timeout=120,
                                     statuses={"chunking", "embedding"})
    if not active_before:
        print("[resilience] FAIL: no document reached 'chunking' or 'embedding' before the "
             "timeout — killing now would test nothing that can be proven, since no "
             "checkpoint had been committed yet to resume from")
        _cleanup(RESILIENCE_USER, ids)
        return False, {}
    print(f"[resilience] active at kill time: {active_before}")

    print("[resilience] docker compose kill -s SIGKILL worker ...")
    kill = subprocess.run(["docker", "compose", "kill", "-s", "SIGKILL", "worker"], cwd=ROOT)
    if kill.returncode != 0:
        print(f"[resilience] FAIL: 'docker compose kill' exited {kill.returncode} — "
             f"can't validate crash recovery if the crash itself wasn't induced")
        return False, {}
    time.sleep(2)
    print("[resilience] docker compose up -d worker ...")
    up = subprocess.run(["docker", "compose", "up", "-d", "worker"], cwd=ROOT)
    if up.returncode != 0:
        print(f"[resilience] FAIL: 'docker compose up -d worker' exited {up.returncode} — "
             f"worker may not have restarted; can't validate recovery")
        return False, {}

    stale_s = float(os.getenv("RECONCILE_STALE_S", "300"))
    timeout = stale_s + 120  # sweep tick + real resume time, on top of the stale threshold
    print(f"[resilience] waiting up to {timeout:.0f}s for recovery "
         f"(reconciler stale threshold {stale_s:.0f}s + buffer)...")
    rows = _wait_terminal(RESILIENCE_USER, ids, timeout=timeout, poll_s=5.0)

    final = {i: rows.get(i, {}).get("status") for i in ids}
    indexed = [i for i, s in final.items() if s == "indexed"]
    failed = [i for i, s in final.items() if s == "failed"]
    stuck = [i for i in ids if final.get(i) not in _TERMINAL]
    print(f"[resilience] final: {len(indexed)} indexed, {len(failed)} failed, "
         f"{len(stuck)} stuck/non-terminal (of {len(ids)} total)")

    # Prove RESUME, not just eventual success. `active_before` is guaranteed
    # non-empty and guaranteed to contain only 'chunking'/'embedding' (the
    # wait above requires it), so there is no "nothing to assert" branch that
    # can auto-pass this anymore — the proof is mandatory, and each doc's
    # required log lines depend on exactly how far it had gotten.
    resumed_ok = _verify_checkpoint_resume(active_before)

    # These are our own guaranteed-valid synthetic PDFs, and registration was
    # already asserted complete above — so `ids` IS the full requested cohort
    # and anything short of ALL reaching 'indexed' means something was
    # actually lost or corrupted, not a legitimate content failure.
    no_loss = not stuck and len(indexed) == n_docs and resumed_ok
    _cleanup(RESILIENCE_USER, ids)
    return no_loss, final


# ── main ──────────────────────────────────────────────────────────────────────

MIN_OVERLAP_FLOOR = 0.5


def _overlap_frac(raw: str) -> float:
    """--min-overlap-frac may be raised (stricter) but never lowered below
    MIN_OVERLAP_FLOOR. Left unbounded, `--min-overlap-frac 0` made ZERO
    observed active-ingest polls satisfy the decoupling gate — i.e. a flag
    that switches off the very protection added to stop the gate passing
    without real overlap. A benchmark's own SLA-adjacent thresholds should
    not be loosenable from the command line, the same reason sla.json is
    left as-is rather than tuned until things go green."""
    try:
        val = float(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{raw!r} is not a number")
    if val < MIN_OVERLAP_FLOOR:
        raise argparse.ArgumentTypeError(
            f"{val} is below the hard floor of {MIN_OVERLAP_FLOOR} — this threshold can be "
            f"raised to make the decoupling check stricter, but never lowered: doing so "
            f"would let the gate pass with little or no real ingest overlap, which is "
            f"exactly what it exists to prevent")
    if val > 1.0:
        raise argparse.ArgumentTypeError(f"{val} is above 1.0 — it's a fraction of polls")
    return val


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--resilience", action="store_true")
    ap.add_argument("--json", dest="json_out", default="")
    ap.add_argument("--backfill-docs", type=int, default=20,
                    help="documents to backfill for decoupling/throughput (default 20)")
    ap.add_argument("--backfill-pages", type=int, default=3,
                    help="pages per backfill document (default 3, ~6 chunks/doc)")
    ap.add_argument("--search-samples", type=int, default=40,
                    help="/ask_stream samples per idle/during phase (default 40)")
    ap.add_argument("--resilience-docs", type=int, default=10,
                    help="documents to backfill for --resilience (default 10)")
    ap.add_argument("--min-overlap-frac", type=_overlap_frac, default=MIN_OVERLAP_FLOOR,
                    help=f"min fraction of 'during' polls that must find active ingest "
                         f"work, or the decoupling gate fails. May be RAISED to make the "
                         f"check stricter, never lowered below {MIN_OVERLAP_FLOOR} "
                         f"(default {MIN_OVERLAP_FLOOR})")
    args = ap.parse_args()

    results, failures = {}, []

    def gate(name, value, ok, target):
        results[name] = {"value": value, "target": target, "pass": bool(ok)}
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {value} (target {target})")
        if not ok:
            failures.append(name)

    if args.resilience:
        no_loss, detail = run_resilience_check(args.resilience_docs, args.backfill_pages)
        gate("no_loss_under_crash", no_loss, no_loss and SLA["no_loss_required"],
             "0 dropped, all indexed, checkpoint-resumed")
        results["no_loss_under_crash"]["detail"] = detail
        if args.json_out:
            pathlib.Path(args.json_out).write_text(json.dumps(results, indent=2))
            print(f"wrote {args.json_out}")
        return sys.exit(1 if failures else 0)

    all_ids: dict[str, tuple[str, set[str]]] = {}
    error_rates: dict[str, float] = {}

    # 1. accept latency
    a, probe_ids, error_rates["accept_latency"] = measure_accept_latency()
    all_ids["accept_latency_probes"] = (PROBE_USER, probe_ids)
    gate("accept_latency_p95_ms", round(a, 1), a <= SLA["accept_latency_p95_ms"],
         SLA["accept_latency_p95_ms"])

    # Trap 2b (guardrail's catch): the probes are registered but not yet
    # necessarily PROCESSED here — measuring "idle" right now would still
    # have them contending for the same global DISPATCH_MAX_INFLIGHT
    # capacity as anything else, contaminating the baseline. Separate
    # sub-users fixed cross-user starvation, not this — every accepted probe
    # must reach its correct, fast (since Block G) terminal 'failed' state
    # before ANYTHING downstream is measured. This is a hard block, not a
    # soft timeout-then-proceed: DISPATCH_MAX_INFLIGHT is GLOBAL (2 slots
    # shared by every user), so leftover in-flight probes don't just skew
    # "idle" — they can fully occupy both slots and starve the entire
    # backfill behind them too, which a live run caught directly (0/26
    # overlap polls found active work because the backfill was still
    # 'pending', starved behind still-processing probes this same bug would
    # have silently accepted with a shorter, soft-timeout wait).
    print("[accept_latency] waiting for ALL poison probes to reach 'failed' before "
         "measuring anything downstream...")
    probe_timeout = max(300.0, 10.0 * len(probe_ids))
    probe_rows = _wait_terminal(PROBE_USER, probe_ids, timeout=probe_timeout, poll_s=3.0)

    # Every probe must end specifically 'failed' — NOT merely terminal.
    # These are deliberately-invalid payloads; 'indexed' would mean invalid
    # content became searchable and 'skipped' would mean it was silently
    # waved through, and both are real defects this benchmark should catch
    # rather than accept as "drained". Accepting any terminal status here
    # (the previous behavior) turned this drain-check into a check that
    # could never fail for the reason that actually matters.
    not_failed = {i: probe_rows.get(i, {}).get("status") for i in probe_ids
                  if probe_rows.get(i, {}).get("status") != "failed"}
    if not_failed:
        still_running = {i: s for i, s in not_failed.items() if s not in _TERMINAL}
        wrong_terminal = {i: s for i, s in not_failed.items() if s in _TERMINAL}
        if still_running:
            print(f"[accept_latency] FAIL: {len(still_running)}/{len(probe_ids)} probe(s) still "
                 f"not terminal after {probe_timeout:.0f}s — refusing to measure idle/decoupling/"
                 f"throughput against a system that isn't actually idle yet")
        if wrong_terminal:
            print(f"[accept_latency] FAIL: {len(wrong_terminal)}/{len(probe_ids)} deliberately-"
                 f"INVALID probe(s) reached a terminal status other than 'failed' "
                 f"{sorted(wrong_terminal.items())} — invalid content must never end up "
                 f"'indexed' (searchable) or silently 'skipped'")
        gate("probes_all_failed_before_idle", False, False,
             "all poison probes specifically 'failed' before idle")
        _cleanup(PROBE_USER, probe_ids)
        if args.json_out:
            pathlib.Path(args.json_out).write_text(json.dumps(results, indent=2))
        sys.exit(1)

    # 2 & 4. search stays fast during a big ingest, and throughput
    (ratio, overlap_ok, throughput, ingest_ok, backfill_ids,
     error_rates["idle_search"], error_rates["during_search"],
     error_rates["backfill_registration"],
     error_rates["backfill_ingestion"]) = measure_decoupling_and_throughput(
        n_docs=args.backfill_docs, n_pages=args.backfill_pages,
        n_samples=args.search_samples, min_overlap_frac=args.min_overlap_frac)
    all_ids["backfill"] = (BACKFILL_USER, backfill_ids)
    ratio_pass = ratio <= SLA["search_p95_during_ingest_ratio_max"]
    gate("search_p95_during_ingest_ratio", round(ratio, 2),
         ratio_pass and overlap_ok, SLA["search_p95_during_ingest_ratio_max"])
    # ingest_ok: every accepted valid doc actually reached 'indexed'. A high
    # chunks/s earned by only the surviving subset is not a real pass.
    gate("ingest_throughput_chunks_per_s", round(throughput, 2),
         throughput >= SLA["ingest_throughput_min_chunks_per_s"] and ingest_ok,
         SLA["ingest_throughput_min_chunks_per_s"])

    # 3. recall@10 on labeled queries (real corpus, not the bench user) —
    # literally requests top_k=10 now, not whatever the app's own default
    # (6) happened to return.
    print("[recall] running labeled queries (top_k=10)...")
    recall, error_rates["recall"] = measure_recall(top_k=10)
    gate("recall_at_10", round(recall, 3), recall >= SLA["recall_at_10_min"],
         SLA["recall_at_10_min"])

    # 5. error rate — never silently absorbed into other numbers. A
    # guardrail review reproduced a run where 39/40 failed searches still
    # reported a clean p95 because failures were just dropped from the
    # percentile; this is the gate that actually catches that.
    worst_phase = max(error_rates, key=error_rates.get)
    max_error_pct = error_rates[worst_phase] * 100
    gate("error_rate_max_pct", round(max_error_pct, 2),
         max_error_pct <= SLA["error_rate_max_pct"], SLA["error_rate_max_pct"])
    if max_error_pct > 0:
        print(f"  (worst phase: {worst_phase} at {max_error_pct:.2f}%; "
             f"all phases: {json.dumps({k: round(v * 100, 2) for k, v in error_rates.items()})})")

    for label, (user_id, ids) in all_ids.items():
        _cleanup(user_id, ids)

    if args.json_out:
        pathlib.Path(args.json_out).write_text(json.dumps(results, indent=2))
        print(f"wrote {args.json_out}")

    print(f"\n{'ALL SLAs PASS' if not failures else 'SLA FAILURES: ' + ', '.join(failures)}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
