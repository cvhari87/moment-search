"""Central env-driven config — every knob in one place.

Same conventions as the digital-twin-akash service: module-level constants,
provider-neutral STORAGE_* credentials with AWS_* fallbacks, Prefect Cloud
read straight from PREFECT_API_URL / PREFECT_API_KEY by the SDK.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"  # local-provider storage root (dev only)


def _envbool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


# --- Database (Neon Postgres) — videos manifest, source of truth ------------
DATABASE_URL = os.getenv("DATABASE_URL", "")

# --- API auth ----------------------------------------------------------------
# Bearer token required on every mutating endpoint (presign, register, delete,
# retry). The tenant is the X-User-Id header (default "default") — swap this
# for real per-user auth (JWT/Clerk) later without touching the data model:
# every bucket key, Postgres row, and Qdrant point is already user_id-tagged.
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
DEFAULT_USER_ID = os.getenv("DEFAULT_USER_ID", "default")

# --- Object storage (videos + frame thumbnails) ------------------------------
# STORAGE_PROVIDER: local | aws | gcp | gcp_native | flyio
# aws/gcp/flyio share one boto3 S3 client (different endpoints); gcp_native
# uses Google's SDK + service-account JSON; local writes under ./data (dev).
STORAGE_PROVIDER = os.getenv("STORAGE_PROVIDER", "local").strip().lower()
STORAGE_BUCKET = (os.getenv("STORAGE_BUCKET", "")
                  or os.getenv("BUCKET_NAME", "")            # injected by `fly storage create`
                  or os.getenv("GCS_BUCKET_NAME", "")        # gcp_native conventions
                  or os.getenv("GOOGLE_CLOUD_BUCKET_NAME", ""))
STORAGE_ACCESS_KEY_ID = os.getenv("STORAGE_ACCESS_KEY_ID", "").strip() or os.getenv("AWS_ACCESS_KEY_ID", "").strip()
STORAGE_SECRET_ACCESS_KEY = os.getenv("STORAGE_SECRET_ACCESS_KEY", "").strip() or os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
AWS_REGION = os.getenv("STORAGE_REGION", "").strip() or os.getenv("AWS_REGION", "auto")
_PROVIDER_ENDPOINTS = {
    "aws": None,  # boto3 default
    "flyio": "https://fly.storage.tigris.dev",
    "gcp": "https://storage.googleapis.com",
}
STORAGE_ENDPOINT = os.getenv("AWS_ENDPOINT_URL_S3", "").strip() or _PROVIDER_ENDPOINTS.get(STORAGE_PROVIDER)


def gcs_service_account_info() -> dict:
    """Service-account JSON for STORAGE_PROVIDER=gcp_native, rebuilt from the
    GOOGLE_CLOUD_* env vars (the standard exploded-JSON convention)."""
    key = os.getenv("GOOGLE_CLOUD_PRIVATE_KEY", "").strip()
    # dotenv strips surrounding quotes locally, but `fly secrets import` keeps
    # them literally — strip defensively so the PEM is valid in both places.
    if len(key) >= 2 and key[0] == key[-1] and key[0] in "\"'":
        key = key[1:-1]
    return {
        "type": "service_account",
        "project_id": os.getenv("GOOGLE_CLOUD_PROJECT_ID", ""),
        "private_key_id": os.getenv("GOOGLE_CLOUD_PRIVATE_KEY_ID", ""),
        "private_key": key.replace("\\n", "\n"),  # dotenv keeps \n literal inside quotes
        "client_email": os.getenv("GOOGLE_CLOUD_CLIENT_EMAIL", ""),
        "client_id": os.getenv("GOOGLE_CLOUD_CLIENT_ID", ""),
        "auth_uri": os.getenv("GOOGLE_CLOUD_AUTH_URI", "https://accounts.google.com/o/oauth2/auth"),
        "token_uri": os.getenv("GOOGLE_CLOUD_TOKEN_URI", "https://oauth2.googleapis.com/token"),
        "auth_provider_x509_cert_url": os.getenv(
            "GOOGLE_CLOUD_AUTH_PROVIDER_X509_CERT_URL", "https://www.googleapis.com/oauth2/v1/certs"),
        "client_x509_cert_url": os.getenv("GOOGLE_CLOUD_CLIENT_X509_CERT_URL", ""),
        "universe_domain": os.getenv("GOOGLE_CLOUD_UNIVERSE_DOMAIN", "googleapis.com"),
    }


# Bucket key layout — every key is user-scoped (tenant isolation at the path level):
#   uploads/{user_id}/{video_id}.{ext}      raw uploaded video (presigned PUT target)
#   frames/{user_id}/{video_id}/NNNNNN.jpg  downscaled frame thumbnails (citations)
UPLOAD_KEY_PREFIX = "uploads/"
FRAME_KEY_PREFIX = "frames/"

# --- Presigned uploads (browser -> bucket, bypassing the API) -----------------
PRESIGN_EXPIRY_S = _int("PRESIGN_EXPIRY_S", 900)          # presigned PUT lifetime
PRESIGN_GET_EXPIRY_S = _int("PRESIGN_GET_EXPIRY_S", 3600)  # thumbnails / playback
MAX_UPLOAD_MB = _int("MAX_UPLOAD_MB", 2048)                # register rejects bigger objects
ALLOWED_UPLOAD_TYPES = ("video/",)                         # content-type must start with

# --- Video ingest lifecycle ---------------------------------------------------
# pending  = registered, waiting in our fair queue (not yet sent to Prefect)
# queued   = the dispatcher picked it and scheduled a Prefect run
# fetching = acquiring the source file; sampling = frames + dedup + thumbnails;
# embedding = CLIP + Qdrant upsert; skipped = duplicate (user_id, source_hash).
VIDEO_STATUSES = ("pending", "queued", "fetching", "sampling", "embedding",
                  "indexed", "skipped", "failed")
# In-flight = occupying execution capacity (scheduled or running).
INFLIGHT_STATUSES = ("queued", "fetching", "sampling", "parsing", "chunking", "embedding")

# --- Fair scheduling (WFQ) ----------------------------------------------------
# FIFO (default off): register enqueues to Prefect immediately -> Prefect runs
# them in submitted order, so one user with 50 videos blocks everyone behind
# them. Fair dispatch (WFQ, on): videos wait `pending` in Postgres and a
# dispatcher admits them round-robin ACROSS users, keeping only
# DISPATCH_MAX_INFLIGHT running at once — so the waiting line is fairly ordered
# in OUR DB, not FIFO inside Prefect. No user can starve the others.
ENABLE_FAIR_DISPATCH = _envbool("ENABLE_FAIR_DISPATCH", True)
# Max videos executing at once. Set to your total capacity:
# (worker machines) x WORKER_CONCURRENCY — anything above that would just pile
# up FIFO inside Prefect and defeat the fairness.
DISPATCH_MAX_INFLIGHT = _int("DISPATCH_MAX_INFLIGHT", _int("WORKER_CONCURRENCY", 2))
DISPATCH_INTERVAL_S = _float("DISPATCH_INTERVAL_S", 3.0)  # how often the dispatcher tops up

# --- Deployment isolation (Prefect Cloud is one shared workspace) -------------
# Local dev and the Fly.io deployment point at the SAME Prefect Cloud workspace
# (PREFECT_API_URL/KEY are copied verbatim into `fly secrets`) but have
# INCOMPATIBLE storage backends (local disk vs. Tigris/S3). flow.serve()'s
# runner has no concept of "environment" — any connected runner can pick up
# any scheduled run for a given deployment name, so a document backfilled
# locally could be executed by the Fly worker (or vice versa). Found live: a
# `parse-document` task burning its full retries=2 backoff (~2.5 minutes)
# before failing with a genuine botocore NoSuchKey — the traceback rooted in
# storage.py's `_s3().get_object()` even though THAT process's own
# STORAGE_PROVIDER is "local", because the Fly worker had won the race for a
# flow run whose bytes only exist on the dev machine's disk. This was the
# actual ceiling on ingest throughput, not local CPU/embedding capacity —
# scaling worker replicas or CLIP processes never touched it since the
# competing capacity was external. FLY_APP_NAME is set automatically on every
# Fly Machine and absent everywhere else, so it doubles as a free per-
# environment id: suffixing every deployment name with it means each
# environment's dispatcher only ever schedules runs its OWN workers can serve.
DEPLOYMENT_ENV = os.getenv("FLY_APP_NAME", "local").strip() or "local"

# --- Crash safety / reconciler (Block G) ---------------------------------------
# Prefect's @task(retries=N) only catches an in-process exception — a hard-killed
# worker (docker kill, OOM, host crash) leaves a row stuck in an in-flight status
# forever, since nothing inside the dead process can run to fix it. The
# reconciler (src/reconciler.py) sweeps for exactly that, on a timer.
#
# RECONCILE_STALE_S must sit comfortably above the longest in-task retry backoff
# any single stage can already go through on its own — t_parse's retries=2,
# retry_delay_seconds=[30, 120] (~150s worst case) and t_embed's retries=2,
# retry_delay_seconds=60 (~120s) — otherwise the sweep could "rescue" a row
# that's actually still alive, just mid-retry inside a live task, and admit a
# second concurrent run for it. (db.set_status's `generation` guard is what
# makes that race harmless even so — see db.py — since no fixed threshold is
# airtight against an arbitrarily slow but genuinely-alive task.)
RECONCILE_STALE_S = _float("RECONCILE_STALE_S", 300.0)
RECONCILE_INTERVAL_S = _float("RECONCILE_INTERVAL_S", 20.0)
# Total flow-run attempts (across resumes, not just in-task retries) before a
# repeatedly-crashing source is dead-lettered (marked 'failed') by the sweep
# instead of resurrected again — the poison-URI DLQ backstop for a source that
# crashes the worker process itself, not just one that cleanly raises (a clean
# raise already terminates in 'failed' within its own flow run — see
# src/ingest/document.py's _fetch_bytes / except Exception block).
MAX_INGEST_ATTEMPTS = _int("MAX_INGEST_ATTEMPTS", 5)

# --- Frame sampling (the biggest scaling lever) --------------------------------
# interval: one frame every FRAME_INTERVAL_SEC (widened to respect MAX_FRAMES).
# scene:    one frame per detected cut (ffmpeg scene filter).
FRAME_STRATEGY = os.getenv("FRAME_STRATEGY", "interval").strip().lower()
FRAME_INTERVAL_SEC = _float("FRAME_INTERVAL_SEC", 2.0)
SCENE_THRESHOLD = _float("SCENE_THRESHOLD", 0.4)
MAX_FRAMES = _int("MAX_FRAMES", 400)
THUMB_WIDTH = _int("THUMB_WIDTH", 480)   # frames are downscaled in the ffmpeg pass
THUMB_QUALITY = _int("THUMB_QUALITY", 3)  # ffmpeg -q:v (2 best .. 31 worst)

# Perceptual-hash dedup — drop visually-identical neighbours BEFORE embedding.
DEDUP_ENABLED = _envbool("DEDUP_ENABLED", True)
DEDUP_MAX_DISTANCE = _int("DEDUP_MAX_DISTANCE", 4)  # Hamming distance on 64-bit dHash

# --- CLIP embeddings ------------------------------------------------------------
# One model encodes frames and text queries into a shared space. Runs on CPU
# inside the worker today; EMBED_VERSION is stamped on every Qdrant point so a
# future re-embed (or an external GPU CLIP service) can replace stale vectors
# without guessing.
CLIP_MODEL = os.getenv("CLIP_MODEL", "clip-ViT-B-32").strip()
CLIP_BATCH = _int("CLIP_BATCH", 128)   # frames per embed call (inner batch is 32)
# Inference-service URL ("embedding is a URL"). Set -> api/worker send batches
# to the warm clip_service.py container instead of loading the model in-process
# (which costs each Prefect run subprocess a fresh ~15-30s torch load). Unset
# -> in-process embedding (simple mode, no extra service). Point it at a GPU
# machine later — nothing else changes.
CLIP_SERVICE_URL = os.getenv("CLIP_SERVICE_URL", "").strip().rstrip("/")
# Vector dimension override. 0 = auto: known CLIP models resolve from a table
# (so the API can create the collection at boot WITHOUT loading the model);
# unknown models load the model to measure. Set explicitly for custom models.
CLIP_DIM = _int("CLIP_DIM", 0)
EMBED_VERSION = os.getenv("EMBED_VERSION", f"{CLIP_MODEL}-v1")

# --- Multimodal: transcript (text) branch (Path 1) -----------------------------
# The visual branch is CLIP frames (above). This adds a SECOND branch: YouTube
# captions, chunked by time, embedded with a small semantic text model (bge via
# fastembed — CPU, free), in a separate Qdrant collection. At query time both
# branches run and fuse by RANK (RRF) — CLIP scores (~0.3) and text scores
# (~0.7) live on different scales, so raw-score comparison is meaningless.
# Uploaded files have no captions, so this is YouTube-only; a video with no
# captions just indexes visually (the branch is skipped, never fatal).
ENABLE_TRANSCRIPT = _envbool("ENABLE_TRANSCRIPT", True)
TEXT_COLLECTION = os.getenv("TEXT_COLLECTION", "moments_text")
# Transcript-branch embedding PROVIDER — env decides the model:
#   fastembed (default) -> bge via fastembed: CPU, free, NO API key (keeps search
#                          working keyless for a fresh cloner). Dim 384.
#   openai              -> OpenAI (or any OpenAI-compatible) embeddings API, e.g.
#                          text-embedding-3-small. Hosted, stronger retrieval,
#                          costs per call + needs a key. Reuses the LLM_* key /
#                          base_url by default (override with TEXT_EMBED_API_KEY /
#                          TEXT_EMBED_BASE_URL). Setting the provider alone flips
#                          the default model+dim to 3-small / 1536.
# The model & dim MUST match between indexing and querying, so switching provider
# means RE-SEEDING the transcript collection (its vector dim changes). The two
# branches fuse by RANK (RRF), so the text model is independent of CLIP.
TEXT_EMBED_PROVIDER = os.getenv("TEXT_EMBED_PROVIDER", "fastembed").strip().lower()
_TE_OPENAI = TEXT_EMBED_PROVIDER == "openai"
TEXT_EMBED_MODEL = os.getenv(
    "TEXT_EMBED_MODEL",
    "text-embedding-3-small" if _TE_OPENAI else "BAAI/bge-small-en-v1.5")
TEXT_EMBED_DIM = _int("TEXT_EMBED_DIM", 1536 if _TE_OPENAI else 384)
# openai provider: falls back to the LLM key/base_url in embeddings.py so ONE
# OpenAI key can power both the answer and the text embeddings.
TEXT_EMBED_API_KEY = os.getenv("TEXT_EMBED_API_KEY", "").strip()
TEXT_EMBED_BASE_URL = os.getenv("TEXT_EMBED_BASE_URL", "").strip()
TEXT_EMBED_VERSION = os.getenv("TEXT_EMBED_VERSION", f"{TEXT_EMBED_MODEL}-v1")
# fastembed's ONNX runtime defaults to using every visible core for intra-op
# parallelism WITHIN one process. Unset (0) lets fastembed pick its own
# default. Matters on whichever process actually holds the bge model
# in-process: embed_docs_local (bulk ingest) runs in the `clip` service when
# CLIP_SERVICE_URL is set, but embed_query_local (search) ALWAYS runs
# locally in the API now, regardless of CLIP_SERVICE_URL — see
# embeddings.embed_docs/embed_query. docker-compose.yml sets this on the
# `clip` service; the API gets fastembed's own default (it only ever runs
# one small query call at a time, not a bulk batch). An earlier attempt set
# this on the worker service instead, which turned out to be dead code —
# the worker never holds this model in-process at all (found in review).
TEXT_EMBED_THREADS = _int("TEXT_EMBED_THREADS", 0)
# embed_docs_local's own sub-batch size — a bulk ingest embed call chunks
# through _text_model() this many texts at a time, releasing _text_lock
# between sub-batches instead of holding it for the whole call. Cheap
# insurance for documents with many chunks, but NOT what fixed the
# decoupling-ratio SLA gate: this benchmark's synthetic documents produce
# ~9 chunks each, below this default, so sub-batching never triggers for
# them and made no measured difference on its own (found in review). What
# actually fixed the gate was embed_query running in a different PROCESS
# from embed_docs_local entirely (see embed_query's docstring) — sub-batching
# only bounds how long two calls in the SAME process wait on each other, it
# doesn't remove calls that never share a process in the first place.
TEXT_EMBED_BATCH = _int("TEXT_EMBED_BATCH", 32)
# Transcript chunking: group caption cues into ~CHUNK_SECONDS windows so a chunk
# is a coherent spoken passage with a real t_start/t_end, not one tiny cue.
TRANSCRIPT_CHUNK_SECONDS = _float("TRANSCRIPT_CHUNK_SECONDS", 20.0)
TRANSCRIPT_LANGS = [c.strip() for c in
                    os.getenv("TRANSCRIPT_LANGS", "en,en-US,en-GB").split(",") if c.strip()]

# --- Document ingest (papers, decks — Path 2) -----------------------------------
# Reuses TEXT_COLLECTION / TEXT_EMBED_* above: a paper/deck chunk is a text chunk
# like a transcript chunk, just with a page/slide locator instead of t_start/t_end.
# Chunking is page-aware, not time-aware: a chunk never spans two pages (the page
# number IS the locator), so pages are split independently into ~CHUNK_CHARS
# passages — same "coherent passage, not too small/large" sizing spirit as
# TRANSCRIPT_CHUNK_SECONDS, sized in characters instead of seconds since pages
# have no timeline. ~1000 chars ≈ the 256-token chunk size the RAG survey itself
# recommends (arXiv 2312.10997, p.8).
DOCUMENT_CHUNK_CHARS = _int("DOCUMENT_CHUNK_CHARS", 1000)
# A byte cap alone doesn't bound worst case: a 50MB PDF of mostly-text pages
# can still expand into tens of thousands of chunks, each an embedding call
# and a Qdrant point. Cap total chunks so one pathological document can't
# balloon a single embed_docs() call or dominate a worker indefinitely.
DOCUMENT_MAX_CHUNKS = _int("DOCUMENT_MAX_CHUNKS", 2000)
# Fetch cap for POST /admin/documents' uri (paper/deck PDFs) — bounds a single
# download's memory and time; a poison/oversized URI fails the fetch task
# cleanly (Prefect retries, then the flow fails) rather than hanging a worker.
DOCUMENT_FETCH_MAX_MB = _int("DOCUMENT_FETCH_MAX_MB", 50)
# SSRF hardening (src/ingest/document.py): every other private/loopback/
# link-local/reserved address is blocked outright. These specific host:port
# pairs are the ONLY exception — an explicit allowlist, not a private-IP
# range or bare-hostname exception (a bare hostname would let a document uri
# hit ANY port on that host, not just the one this app actually serves on —
# unnecessarily wide for what's actually needed). "api:8000" is this
# project's own docker-compose service (the deck is self-hosted there); add
# the exact Fly internal host:port here too once deployed (Block J).
DOCUMENT_FETCH_ALLOWED_INTERNAL_HOSTS = [
    h.strip().lower() for h in os.getenv("DOCUMENT_FETCH_ALLOWED_INTERNAL_HOSTS", "api:8000").split(",")
    if h.strip()
]
# Block M: a native .pptx upload/URI is converted to PDF server-side (headless
# LibreOffice) as the first step of t_parse, before the existing pymupdf
# extraction runs unchanged — see src/ingest/document.py's
# _convert_pptx_to_pdf. Generous but bounded: a large deck's conversion is
# genuinely slow (not instant, per Block M's own exit criteria), and a
# hung/crashed soffice process must not hold a worker slot indefinitely.
DOCUMENT_CONVERT_TIMEOUT_S = _float("DOCUMENT_CONVERT_TIMEOUT_S", 300.0)
# LibreOffice can expand a compact PPTX into a much larger PDF. Bound the
# derivative independently of the source fetch/upload cap so conversion can
# never allocate or checkpoint an arbitrarily large output.
DOCUMENT_MAX_CONVERTED_MB = _int("DOCUMENT_MAX_CONVERTED_MB", 100)
# A small, well-under-the-byte-cap .pptx can still have a huge SLIDE count
# (thousands of near-empty slides) — DOCUMENT_MAX_CHUNKS only catches that
# AFTER _parse_pdf has already run vision-captioning (a real LLM call) on
# every text-poor slide, so a pathological deck's cost is paid before the
# existing cap ever fires. Checked right after conversion, before parsing —
# a deck this large isn't a realistic ceiling to hit, it's an abuse/cost
# guard (found in review).
DOCUMENT_MAX_CONVERTED_PAGES = _int("DOCUMENT_MAX_CONVERTED_PAGES", 500)
# Deck-only: a slide whose extracted text is shorter than this is treated as
# text-poor (title slide, diagram, photo) and rendered + captioned by the
# vision LLM instead of embedding near-nothing. Papers never take this
# branch — a sparse paper page (a figure, a section break) is normal.
DECK_SLIDE_MIN_CHARS = _int("DECK_SLIDE_MIN_CHARS", 40)
# Render resolution for the text-poor-slide screenshot handed to the vision
# model — high enough to keep on-slide text legible, not print quality.
DECK_SLIDE_RENDER_DPI = _int("DECK_SLIDE_RENDER_DPI", 150)

# --- Fusion (multimodal retrieval) ---------------------------------------------
# RRF: rank-based fusion across branches (score-agnostic). rrf = 1/(K + rank).
RRF_K = _int("RRF_K", 60)
# Hits from either branch within this many seconds are the SAME moment.
FUSION_WINDOW_S = _float("FUSION_WINDOW_S", 15.0)
# A window with BOTH a frame and a trustworthy transcript hit earns summed
# two-branch RRF plus this multiplier. CROSS_MODAL_TEXT_MIN below (defined
# after TEXT_CONFIDENCE_THRESHOLD, which it defaults from) decides whether
# the pair is trustworthy; otherwise it competes as its strongest branch.
CROSS_MODAL_BOOST = _float("CROSS_MODAL_BOOST", 1.5)
# Per-branch candidates fetched before fusion.
BRANCH_TOP_K = _int("BRANCH_TOP_K", 20)

# --- YouTube download hardening ---------------------------------------------------
# YouTube increasingly answers yt-dlp's default web client with "Sign in to
# confirm you're not a bot". Mitigations, in order of reliability:
#   YT_COOKIES_FILE  path to a Netscape cookies.txt exported from a logged-in
#                    browser (yt-dlp wiki: "Exporting YouTube cookies").
#                    In docker-compose, drop it at ./data/cookies.txt and set
#                    YT_COOKIES_FILE=/app/data/cookies.txt
#   YT_PROXY_URL     route YouTube traffic through a (residential) proxy,
#                    e.g. http://user:pass@host:port
#   YT_RETRY_CLIENTS on a bot-check error, automatically retry once with these
#                    alternate YouTube player clients (comma-separated).
# Cookies make yt-dlp an authenticated client — the one fix that works from
# ANY IP (home OR datacenter). Two ways to supply them, so the same code works
# local and deployed:
#   YT_COOKIES_FILE  path to a mounted cookies.txt   (easy locally)
#   YT_COOKIES_B64   base64 of cookies.txt as a secret (Fly/cloud: no file mount
#                    needed — the worker writes it to a temp file at runtime)
YT_COOKIES_FILE = os.getenv("YT_COOKIES_FILE", "").strip()
YT_COOKIES_B64 = os.getenv("YT_COOKIES_B64", "").strip()
YT_PROXY_URL = os.getenv("YT_PROXY_URL", "").strip()
# Player clients. Default EMPTY = let yt-dlp pick (best, once a JS runtime is
# present — see below). Forcing tv/android used to help pre-JS-runtime, but now
# those clients hand back media URLs that 403 on download, so we only fall back
# to them if the default attempt fails outright.
YT_PLAYER_CLIENTS = [c.strip() for c in
                     os.getenv("YT_PLAYER_CLIENTS", "").split(",") if c.strip()]
YT_FALLBACK_CLIENTS = [c.strip() for c in
                       os.getenv("YT_FALLBACK_CLIENTS", "tv,android,ios").split(",") if c.strip()]
# yt-dlp 2025+ needs a JavaScript runtime to compute YouTube signatures, plus
# its EJS challenge-solver component — WITHOUT these every video fails with
# "This video is not available" / "Requested format is not available". The
# Dockerfile installs Node; these tell yt-dlp to use it + fetch the solver.
# (For bare-process dev, install node or deno yourself.)
YT_JS_RUNTIMES = [c.strip() for c in
                  os.getenv("YT_JS_RUNTIMES", "node").split(",") if c.strip()]
YT_REMOTE_COMPONENTS = [c.strip() for c in
                        os.getenv("YT_REMOTE_COMPONENTS", "ejs:github").split(",") if c.strip()]

# --- Sample corpus ------------------------------------------------------------
# On boot the worker auto-ingests the four "Deep Dive into LLMs" sample talks
# (src/samples.py) if they aren't indexed yet — a fresh clone is queryable on
# the / page without running anything by hand. Set false to skip.
SEED_SAMPLE_VIDEOS = _envbool("SEED_SAMPLE_VIDEOS", True)

# --- Work orchestration (Prefect Cloud) ----------------------------------------
# The SDK reads PREFECT_API_URL / PREFECT_API_KEY from the environment directly.
# WORKER_CONCURRENCY is read by worker.py; retries live on the flow's tasks.

# --- Qdrant ----------------------------------------------------------------------
# One shared multi-tenant collection: every point carries user_id (tenant payload
# index) and every search/upsert/delete is user_id-filtered.
QDRANT_URL = os.getenv("QDRANT_URL", "").strip()
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "").strip() or os.getenv("QDRANT_TOKEN", "").strip()
QDRANT_LOCAL_PATH = os.getenv("QDRANT_LOCAL_PATH", str(DATA / "qdrant"))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "moments")
# Low-RAM profile: original vectors on disk, int8-quantized copies pinned in
# RAM (~4x smaller), HNSW graph on disk; queries rescore against the originals.
# Frames balloon vector counts fast, so these default ON.
QDRANT_QUANTIZATION = _envbool("QDRANT_QUANTIZATION", True)
QDRANT_ON_DISK = _envbool("QDRANT_ON_DISK", True)
QDRANT_HNSW_ON_DISK = _envbool("QDRANT_HNSW_ON_DISK", True)

# --- Retrieval / faithfulness ------------------------------------------------------
TOP_K = _int("TOP_K", 6)                 # frames fed to the multimodal LLM (3-8)
# Gate 1: abstain WITHOUT calling the LLM if BOTH branches' best raw score is
# below their threshold. Fusion scores are RRF (tiny), so the gate uses each
# branch's own raw cosine. Calibrated 2026-07-29 via benchmark/
# calibrate_thresholds.py against benchmark/queries.jsonl (labeled positives)
# and benchmark/negative_queries.jsonl (irrelevant/nonsense queries), on this
# corpus: CLIP text->image noise floor runs ~0.23-0.33 REGARDLESS of query
# relevance (0.2 never separated anything — the gate's AND condition on
# visual was structurally unreachable, which is why nonsense queries were
# passing through); bge text-text noise floor runs ~0.53-0.70 for irrelevant
# queries vs. ~0.74-0.90 for real matches, which DOES separate cleanly. Both
# values matter together — see gate_citations() in src/rag/search.py: reject
# requires BOTH branches below threshold, so raising just one is not enough
# (see benchmark/calibrate_thresholds.py's AND-aware check). Re-run that
# script after growing either query set; these are not meant to be static.
CONFIDENCE_THRESHOLD = _float("CONFIDENCE_THRESHOLD", 0.28)              # visual (CLIP)
TEXT_CONFIDENCE_THRESHOLD = _float("TEXT_CONFIDENCE_THRESHOLD", 0.72)  # transcript (bge)

# Cross-modal eligibility: a frame+text window only earns the second branch's
# RRF contribution and CROSS_MODAL_BOOST if the TEXT hit's own raw score
# clears this bar. Text-only, deliberately — the
# calibration note above already establishes CLIP's 0.23-0.33 band carries
# no relevance signal AT ALL on this corpus, so gating on the frame's score
# too would strip the boost from good pairs and admit bad ones at roughly
# random, without targeting the actual failure. Diagnosed live
# (WHAT_I_DID.md, LEARNINGS.md "cross-modal boost" entries): an unrelated
# frame (0.23) paired with a merely-weak text hit (0.58, below
# TEXT_CONFIDENCE_THRESHOLD) got boosted 1.5x above a genuinely relevant
# text-only match (0.69, no nearby frame so no boost). Seeded from
# TEXT_CONFIDENCE_THRESHOLD (already calibrated, not a new guessed number)
# but kept as its own constant since it answers a different question — "is
# THIS window's text hit trustworthy" vs. "is there anything relevant
# anywhere" — and may need to diverge once there's ranking-specific data
# (see benchmark/bench.py's measure_ranking).
CROSS_MODAL_TEXT_MIN = _float("CROSS_MODAL_TEXT_MIN", TEXT_CONFIDENCE_THRESHOLD)

# --- Multimodal LLM (answer synthesis only — retrieval works without it) -----------
# LLM_PROVIDER: openai | nvidia | anthropic ("openai" also covers any
# OpenAI-compatible server via LLM_BASE_URL: Ollama, vLLM, OpenRouter, ...).
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai").strip().lower()
LLM_API_KEY = os.getenv("LLM_API_KEY", "").strip()
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "").strip()
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini").strip()
LLM_MAX_TOKENS = _int("LLM_MAX_TOKENS", 1024)
LLM_IMAGE_MAX_PX = _int("LLM_IMAGE_MAX_PX", 512)  # frames are downscaled again before the LLM


def llm_configured() -> bool:
    # Local OpenAI-compatible servers often need no key, so a base_url alone counts.
    return bool(LLM_API_KEY or LLM_BASE_URL)
