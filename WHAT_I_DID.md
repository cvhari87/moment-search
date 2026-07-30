# What I Did

This is a chronological record of work performed on Assignment 3. It records actions and
available evidence without turning incomplete checks into completed claims.

## 2026-07-28 — Section A: baseline setup

Status: **complete**

- Created the service configuration in the gitignored `.env` file, including disabling sample
  seeding for the baseline exercise.
- Started the unchanged application stack with Docker Compose.
- Confirmed the baseline application worked with an indexed video and completed the Section A
  review without evaluator failures.
- Kept `.env` out of Git as required. No application-code change or Section A commit was needed.
- Added the project-specific read-only guardrail reviewer at
  `.claude/agents/momentsearch-reviewer.md`.

### Issue discovered during startup

- Observed the worker crash during concurrent API and worker startup.
- Captured the PostgreSQL failure:
  `UniqueViolation: duplicate key value violates unique constraint pg_type_typname_nsp_index`
  for the `ms_videos` type.
- Confirmed that `src/db.py:init_schema()` executes the complete schema DDL without an advisory
  lock and can run concurrently in the API and worker processes.
- Did not modify the baseline implementation because Section A exists to establish unchanged
  behavior. The fix is assigned to Block C.

## 2026-07-28 — Section A2: crash experiment

Status: **experiment complete**

- Registered `yt_aircAruvnKk` for ingestion.
- Killed/restarted the worker during an active ingest.
- Inspected the Docker worker logs and the corresponding Prefect Cloud run records.
- Confirmed the interrupted Prefect run was eventually marked `Crashed` by automation.
- Confirmed Prefect did not automatically resume that run. A separate flow run completed the
  source ingestion on attempt 3.

### Recorded timeline

- `22:33:45 UTC` — interrupted flow run started.
- `22:33:49 UTC` — worker stopped during ingestion.
- `22:36:17 UTC` — worker started again.
- `22:37:17 UTC` — a new, distinct flow run started.
- `22:37:37 UTC` — the replacement run completed.
- `22:42:45 UTC` — Prefect automation marked the original run `Crashed`.

This was a behavior-discovery experiment. It was not the final resilience test and did not prove
automatic recovery, zero loss, or checkpoint reuse.

## 2026-07-28 — Section A2: Fly smoke-deploy attempt

Status: **incomplete evidence; revisit in Block J**

- Ran `fly launch`, which selected the temporary app name
  `momentsearch-rosy-grass-7193`.
- Found that the generated app name and `CLIP_SERVICE_URL` initially referred to different Fly
  app names.
- Corrected the internal CLIP hostname to use the generated app name before deployment work
  continued.
- Validated the generated Fly configuration with `flyctl config validate`.
- Subsequently removed the throwaway MomentSearch Fly app and restored the baseline Fly
  configuration.
- Did **not** personally observe and preserve evidence that the public Fly URL served the UI and
  answered a question. The smoke-deploy exit criterion therefore remains unverified.

## 2026-07-28 — Block B: lock the corpus, write `benchmark/queries.jsonl`

Status: **complete**

- **Paper locked:** arXiv `2312.10997` (the RAG survey). Downloaded the actual PDF and extracted
  text with `pdftotext` rather than assuming content — confirmed page 9 carries the "1) Mix/hybrid
  Retrieval" subsection that `eval.py`'s hardcoded probe query needs, plus grounded content on
  pages 2 (RAG paradigms) and 8 (chunking strategy) for additional labeled queries.
- **Video locked:** a real public YouTube talk on hybrid search (`yt_0cKtkaR883c`, "Pinecone's New
  *Hybrid* Search"), verified fetchable via `yt-dlp --dump-json` before registering, then
  registered and confirmed `indexed` through the existing pipeline. Transcript content pulled live
  from `/api/ask` and used to ground three video-based queries with real timestamps.
- **Deck:** two rounds of web search turned up no suitable public conference deck containing
  anything like "one index for every source." Rather than keep searching, authored an original
  deck about this project's own real architecture (WFQ + Prefect queue design, the shared-index
  decision, locators per kind, checkpointing/resilience) — genuinely true content, not filler
  written to match a query.
- Wrote `benchmark/queries.jsonl`: 14 labeled entries across all three kinds, including
  `eval.py`'s two hardcoded probe strings verbatim, each grounded in real extracted content.
- Fixed `docker-compose.yml`'s host port to `8100:8000` — a "Decisions locked" item from the plan
  that had been missed; `eval.py`/`bench.py` both default `--base-url` to `localhost:8100`.
- Guardrail review (fresh subagent, no context on how it was built): PASS on all four checks
  (correctness, the 7 named traps, honesty of the authored-deck decision, rubric-literal match
  against `eval.py`'s hardcoded strings). One follow-up noted: verification ran against
  `localhost:8000` before the port fix landed — addressed same commit.
- Commit: `7ddc94f`.

## 2026-07-28/29 — Block C: schema + async contract for multi-source ingestion

Status: **complete, after four rounds of independent review and fixes**

### Initial build (commit `2d5f55e`)

- `ms_videos` migrated with `kind` (default `'video'`), `uri`, `chunk_count` via idempotent
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` — existing rows and queries stay valid.
- Fixed the schema-init race identified in Section A: `init_schema()` now takes a
  transaction-scoped `pg_advisory_xact_lock` before running the DDL, so concurrent `api`+`worker`
  boot can no longer race Postgres's system catalogs. Verified live: rebuilt and restarted both
  containers together — no crash, versus the earlier reproducible `UniqueViolation`.
- New `POST /admin/documents` (validates `kind`/URI scheme, inserts `pending`, returns `202` —
  no PDF library imported anywhere in the path) and `GET /admin/sources` (unified video+document
  listing). `src/dispatcher.py` routes admitted rows to `jobs.enqueue_video` or
  `jobs.enqueue_document` by `kind`; `db.wfq_claim`/`count_inflight` needed no change.
- Guardrail review: PASS on correctness, traps, security, and rubric-literal checks. Verified live:
  401 without token, 202 in ~120ms with token, kind/scheme validation, unified sources listing,
  and a full video-path regression through the rebuilt dispatcher.

### Independent second-opinion review (Codex), round 1 — commit `342fc1a`

- **Fixed:** `GET /admin/sources` had no `require_auth` despite sitting under `/admin/` next to a
  route that requires it — an unauthenticated caller could enumerate a tenant's sources via
  `X-User-Id`. Added the dependency.
- **Fixed:** `_doc_id()` hashed only the URI, not the user — two different users registering the
  same paper would collide on one manifest row (`ON CONFLICT` never re-checks `user_id`), silently
  handing one tenant's document to another. Scoped the hash to `(user_id, uri)`.
- **Fixed (later partially reverted, see round 2):** the deck PDF lived under `data/`, which is
  fully gitignored — a fresh clone, CI, or the Fly image would have no way to get it. Moved it to
  a tracked `corpus/` directory with a `.gitignore` exception.
- **Improved:** recorded the real registered `doc_id` for the paper and deck in
  `benchmark/queries.jsonl`, alongside the existing (kind, locator) matching.

### Round 2 — commit `a982434`

- **Caught a real non-negotiable violation:** `ASSIGNMENT_AGENTS.md`'s non-negotiable #7 is
  explicit — "media/PDF artifacts are git-ignored." Committing the PDF in round 1 was the wrong
  fix. Reverted it. The actual fix: `corpus/generate_deck.py` (pure Python, `reportlab`, no
  Chrome/LibreOffice dependency) is the tracked source; the PDF itself regenerates at boot in
  `src/app.py`'s lifespan into the gitignored `data/corpus/` path — deterministic, so it exists
  wherever the process runs without ever being committed.
- **Self-caught while implementing the above (neither review round flagged this):** the deck's
  registered URI was `http://localhost:8100/...`, which is unreachable from the *worker*
  container's own network namespace (that's the worker's own loopback, not the api container's).
  Fixed to the docker-compose internal hostname `http://api:8000/...`, verified reachable from
  inside the worker container by direct HTTP request.
- **Fixed:** removed `POST /admin/documents`' FIFO-mode direct-Prefect-enqueue fallback —
  `ASSIGNMENT_AGENTS.md` non-negotiable #1 says ingestion is never triggered from the request path,
  full stop.
- **Fixed:** `upsert_pending`'s `ON CONFLICT` now sets `kind = EXCLUDED.kind` — previously
  re-registering the same URI under a different kind silently kept the old one.

### Round 3 — commit `3e5754c`

- **Fixed a real, 100%-reproducible bug introduced by round 2's fix:** `dispatcher.py`'s
  `start_in_background()` returned early (never started the thread) whenever
  `ENABLE_FAIR_DISPATCH=false`. That was harmless for videos (which still had a direct-enqueue
  fallback in that mode at the time) but documents no longer have one — so a document registered
  under that config would sit `pending` forever with nothing ever picking it up. Fixed: the
  dispatcher thread now always starts, unconditionally. Verified live in an isolated container
  with `ENABLE_FAIR_DISPATCH=false`.
- **Clarified, no code change (Block H doesn't exist yet to implement it in):** the review's
  suggested design for the deck's unstable `source_id` — have `bench.py` register/verify the
  paper and deck itself at run start and use the id the API actually returns, rather than trust a
  static id from `queries.jsonl` — is genuinely the correct mechanism, portable to any
  environment. Updated the note to describe this as the intended Block H design.

### Round 4 — commit `5d877d4`

- **Found the actual remaining race, and it was real, not theoretical:** under
  `ENABLE_FAIR_DISPATCH=false`, `POST /api/videos` (and the retry endpoint) still enqueued videos
  directly at registration time *without* updating the row's status away from `pending` —
  `enqueue_video()` only calls Prefect, it never touches Postgres. Once round 3 made the
  dispatcher always run, its next tick could legitimately re-claim that still-`pending` row and
  enqueue the same video a second time. I had earlier dismissed a version of this concern as a
  sub-millisecond, safe-by-idempotency race — that reasoning was wrong; the real window is
  enqueue-to-first-task-execution (genuine Prefect scheduling latency), which can exceed one 3s
  dispatcher tick.
- **Fixed at the root:** removed every direct-enqueue call site. The dispatcher is now the *only*
  path to Prefect for any source kind. `ENABLE_FAIR_DISPATCH` no longer picks "who enqueues" — it
  picks ordering: `db.wfq_claim(fair=True)` is the existing round-robin-across-users query,
  `wfq_claim(fair=False)` is a plain FIFO-by-creation-time query, both through the identical
  atomic claim.
- **Verified live, not just reasoned through:** ran a second worker instance in
  `ENABLE_FAIR_DISPATCH=false` alongside the real fair-mode one — two dispatcher threads with
  different ordering, actively competing for the same pending rows — then registered a video into
  that race. Confirmed via Prefect Cloud's flow-run history (by timestamp) that exactly one run
  was created.

### Separate small fix — commit `998a984`

- Two stale `localhost:8000` comments in `docker-compose.yml` (left over from before the port
  change) corrected to `8100`.

## 2026-07-29 — Block D: paper vertical slice with checkpointing

Status: **ingestion implementation complete** (parse/chunk/embed, checkpointing, resume-on-restart —
all live-verified below). **Page-citation exit criterion pending Block F** (`/ask_stream` doesn't
exist yet, so a query cannot return a clickable page citation through the API — the content is
correctly ingested and independently verified retrievable directly against Qdrant, but nothing
turns that into a proper citation yet). **Automatic-restart exit criterion pending Block G**
(checkpoints let a *manually restarted* run resume correctly, proven below by an actual kill/restart
test — but nothing yet detects a stuck row and restarts it automatically). Calling this block
unqualified "complete" would overstate what's actually been proven; see the round-2/round-3 review
sections below for the full reasoning on why these two are deferred rather than fixed here.

- New `src/ingest/document.py`: a three-stage Prefect flow (`ms-ingest-document`) mirroring
  `ingest_video`'s task/retry shape — `parse` (pymupdf, real per-page text, imported lazily so
  nothing PDF-related loads at API module scope), `chunk` (page-aware, greedy word-accumulation to
  `DOCUMENT_CHUNK_CHARS`, never spans two pages since the page number IS the locator), `embed`
  (upserts into the same `TEXT_COLLECTION` transcript chunks already live in, reusing the `video_id`
  payload field for `doc_id` so `_user_filter`/`search_text`/`delete_video` all work unchanged).
  Checkpointing is real, not cosmetic: `parse` and `chunk` each check `storage.exists()` for their
  own artifact first and skip straight to loading it if already committed.
- Deliberately does NOT delete existing Qdrant points before re-upserting (unlike video's
  `t_embed_index`) — deterministic point IDs make re-upserting safe, and deleting-first would open
  a real window where an already-indexed document goes unsearchable mid-re-run, which `eval.py`'s
  own `documents_async` check can hit directly (it re-submits the paper, resetting it to `pending`,
  then immediately queries with no wait).
- Added `DOCUMENT_CHUNK_CHARS` / `DOCUMENT_FETCH_MAX_MB` to `src/config.py`, `pymupdf` to
  `requirements.txt`.

### A real bug found and fixed mid-block, not just the planned work

Wiring `src/worker.py` to serve both `ms-ingest-video` and `ms-ingest-document` from one process
(via module-level `serve(video.to_deployment(...), document.to_deployment(...))` instead of the
single-flow `ingest_video.serve(...)`) broke **both** flows — video included, not just the new one
— with `ImportError: attempted relative import beyond top-level package` at flow-run time. Root
cause, confirmed by reading Prefect 3.8's installed source: the default `entrypoint_type=FILE_PATH`
loads each flow run's subprocess via `load_script_as_module()`, which executes the file as a
standalone script with no parent package context, so `from .. import db, storage` fails. Fixed by
passing `entrypoint_type=EntrypointType.MODULE_PATH` to both `to_deployment()` calls, which stores
a dotted entrypoint (`src.ingest.pipeline.ingest_video`) loaded via normal
`importlib.import_module` instead. Confirmed deployment names/routing (`ms-ingest-video/ingest`,
used verbatim in `src/jobs.py`) are unaffected — `entrypoint_type` only changes how the flow is
loaded, not its name.

### Verified live

- Video regression: re-registered `yt_LPZh9BOjkQs` after the entrypoint fix, reached `indexed`.
- Paper (arXiv 2312.10997): 120 chunks, `indexed`. Raw Qdrant `search_text` confirms real,
  correctly-paginated content (spot-checked pages 1, 5, 9, 11, 17, 20 all present with real text).
- Deck: 7 chunks, `indexed`. The exact `eval.py` hardcoded query ("the slide about one index for
  every source") ranks the deck's slide-4 chunk — containing the literal text "One shared vector
  index for every source" — as the #1 hit.
- Crash-resume (the block's actual exit criterion): the natural race window was too tight to hit
  reliably once the embedding model warmed up (embed dropped to well under a second), so a
  temporary `time.sleep()` debug delay was injected into `t_embed` via an env var, the worker
  killed mid-embed after confirming DB status was `embedding`, and on restart both `parse` and
  `chunk` logged "already committed — resuming from checkpoint" while only `embed` re-ran. Debug
  delay reverted before commit; `grep`'d the repo afterward to confirm no scaffolding was left.
- Guardrail review (fresh subagent): PASS on correctness, traps, non-negotiables, and independent
  live re-verification. One non-blocking note: deck chunks currently use payload key `page` (not
  `slide`) — expected, per-kind locator naming is Block E's job.
- Known, deliberate scope boundary: `src/rag/search.py`'s `_fuse()`/`retrieve()` were NOT touched.
  Document chunks surfacing in `search_text()` results get `t=0.0` (no `t_start`/`ms` in their
  payload) and collapse into one fake time-window per document — degraded citation quality, not a
  crash, and explicitly Block F's stated scope to fix, not silently broken by this block.

### Round 2 of independent review (Codex) — three more real bugs, two sequencing calls reaffirmed

Live-verified findings (not code reading alone — Codex re-ran the actual paper ingest and observed
two Prefect run IDs processing the same doc_id concurrently), three fixed:

- **Inflight accounting omitted document stages.** `INFLIGHT_STATUSES` (`src/config.py`) only
  listed video stage names (`queued, fetching, sampling, embedding`) — `parsing` and `chunking`
  weren't in it, so `db.count_inflight()` undercounted true concurrency while a document was being
  parsed/chunked, letting the dispatcher over-admit beyond `DISPATCH_MAX_INFLIGHT` during that
  window. Fixed by adding both to the tuple.
- **Document fetching permitted SSRF.** `_fetch_bytes()` accepted any http(s) URI and followed
  redirects with no destination check — including loopback, link-local, and cloud metadata
  addresses (169.254.169.254, the single most commonly exploited SSRF target — AWS/GCP/Azure/Fly
  all serve credentials there). Fixed in two layers: a cheap, no-DNS literal-IP reject at
  registration time (`src/api/documents.py`), and the authoritative fix at fetch time
  (`src/ingest/document.py`) — real DNS resolution, reject loopback/link-local/multicast/reserved,
  and a custom redirect handler that refuses to follow any redirect at all (closes the classic
  "public IP at validation time, redirects to internal on fetch" bypass). Deliberately does NOT
  block general RFC1918 private ranges — this project's own deck is legitimately self-hosted at a
  docker-compose-internal private address (`http://api:8000/...`), and a blanket private-range
  block would have broken that by design. Verified live both directions: `127.0.0.1`,
  `169.254.169.254`, and `localhost` (resolves to `::1`) all correctly rejected; the deck's own
  fetch from `api:8000` still succeeds.
- **Concurrent runs for the same document could race.** Re-registering an already in-flight source
  (`upsert_pending`'s `ON CONFLICT`) unconditionally reset status back to `pending`, so the
  dispatcher would fairly admit a *second* run while the first was still executing — observed live
  as two distinct Prefect run IDs both processing `doc_1bec551249fe`. Deterministic Qdrant point
  IDs made the double-embed harmless, but nothing stopped a slower, stale run's terminal
  `set_status` call from overwriting a newer run's `indexed` with its own outcome. Fixed at the
  root: `upsert_pending` now preserves status/error/progress when the current status is already
  in `INFLIGHT_STATUSES`, instead of always resetting to pending — so a re-registration of
  something already running is a no-op on status, not a second admission. The same unconditional
  reset existed in the video `retry` endpoint too (calls `set_status` directly, not
  `upsert_pending`) — fixed there too with the same guard. Verified live: re-registering /
  retrying a simulated in-flight video and document both now correctly report the preserved
  in-flight status instead of falsely claiming `pending`.

Two P2s also fixed while in the area: local checkpoint writes (`storage.put_bytes`) used a
non-atomic `Path.write_bytes()` — a kill mid-write could leave a truncated file that `exists()`
would then treat as a complete, valid checkpoint; fixed with a temp-file-plus-`os.replace()`
atomic-rename pattern. And a byte cap alone doesn't bound worst case for a mostly-text PDF, so
`DOCUMENT_MAX_CHUNKS` (default 2000) now caps total chunks during chunking.

**Two "P1" findings were reframed as sequencing calls already made deliberately, not new
defects — reaffirmed rather than changed:**

- *Paper citations aren't implemented / `/ask_stream` 404s.* True, and already documented above as
  the block's explicit scope boundary. Block D's own written exit criterion ("a query returns a
  correct, clickable page citation") is honestly in tension with Block F owning `_fuse()`/citation
  work — the resolution is that Block D proves the *ingested content* is correct and retrievable
  (verified directly against Qdrant, not through the not-yet-built citation layer), and Block F
  is where that content becomes a properly-shaped citation. Building a "minimal" citation path now
  would mean touching `search.py` logic Block F is specifically scoped to redo properly — not worth
  the throwaway work.
- *Checkpoints exist but automatic crash recovery doesn't.* Also already documented: a row stuck
  `embedding` after a kill stays stuck until something restarts a new run for it — automatically
  detecting and restarting a stale row is Block G's reconciler, not something Block D claims to
  do automatically. The crash-resume test manually triggered the restart, which is the correct way
  to verify the *checkpoint mechanism* Block D is actually responsible for.

### Round 3 of independent review — SSRF tightened further, narrow stale-write guard, checkpoint validation

Live-verified again (round 2's own review re-ran the paper ingest and confirmed it still works
after each fix). Two more real issues fixed, one explicitly scoped to Block G instead of fixed here:

- **SSRF fix was too broad.** Round 2 blocked loopback/link-local/metadata but deliberately
  allowed ALL of RFC1918 so the deck's own `http://api:8000/...` fetch would keep working. Fair
  challenge: that still lets an admin-token holder probe any other private-network service, not
  just the one address this app legitimately needs. Replaced the range-based exception with an
  explicit hostname allowlist (`DOCUMENT_FETCH_ALLOWED_INTERNAL_HOSTS`, defaults to `api`) — now
  *every* private/loopback/link-local/multicast/reserved address is blocked by default, and only
  that one specifically-trusted hostname bypasses the check. Also closed a DNS-rebinding gap in
  the previous fix: it validated a hostname's resolved IP, then let `urllib` re-resolve the SAME
  hostname again at actual connect time — a malicious DNS server could return a safe IP for
  validation and a different (internal) IP moments later for the real connection. Replaced
  `urllib.request` with custom `http.client.HTTPConnection`/`HTTPSConnection` subclasses that
  connect directly to the one IP already validated, while still presenting correct SNI/certificate
  validation against the real hostname for HTTPS. Verified live in both directions: `10.0.0.1`,
  `172.16.0.1`, `192.168.1.1`, `127.0.0.1`, `169.254.169.254` all now rejected at registration;
  the deck's own fetch from `api:8000` (private, allowlisted) and the real paper's HTTPS fetch
  from arxiv.org (public) both still succeed end-to-end through the new pinned-connection code,
  producing identical chunk counts to before (7 and 120).
- **Added a narrow stale-write guard, not full fencing.** `set_status()` now refuses to overwrite
  an already-`indexed` row with a `failed` write — the single most damaging outcome of two runs
  racing the same source (a slow/stale run's failure clobbering a newer run's success). This is
  deliberately NOT a full generation/lease/fencing mechanism (where every status write from a run
  would need to prove it still "owns" the current admission before being allowed to apply) — that
  class of fix is real, was correctly identified, and belongs in Block G: it's an admission-time
  concern spanning the dispatcher's claim, the flow-run parameters, and every status write in both
  ingest flows, not a one-function patch bolted onto Block D. Building it now, before Block G's
  broader reconciler design exists, risks throwaway/inconsistent infrastructure Block G would need
  to redo anyway. Verified live: a simulated stale `failed` write against an `indexed` row is
  correctly refused (row stays `indexed`, error stays unset).
- **Checkpoint JSON is now validated before being trusted**, not just assumed valid because the
  file exists. A corrupt/unreadable checkpoint (extremely unlikely now that writes are atomic, but
  not impossible — e.g. a hand-edited or foreign file at that key) is treated as "redo this stage"
  rather than letting `json.JSONDecodeError` propagate and potentially wedge every future retry.

Not implemented (P2, acknowledged): applying the chunk cap only after the entire PDF is parsed into
memory (the existing byte cap already bounds this reasonably — a full streaming/page-by-page limit
would be more invasive for marginal benefit at this scale); batching very large embed calls; a
committed automated regression-test suite (this whole build has been verified live/manually
throughout — every block, not just this one — adding a pytest suite now would be a scope change
to the verification approach itself, not a fix to Block D specifically).

### Round 4 of independent review — the stale-write guard had a real bypass, checkpoints weren't shape-checked

Both fixes verified live with an actual reproduction of the failure mode, not just re-reading the
code:

- **The stale-write guard from round 3 only blocked one hop.** It refused a direct
  `indexed -> failed` write, but a stale run's real lifecycle is
  `indexed -> parsing -> embedding -> failed` — by the time it writes `failed`, the row is already
  `embedding` (because the stale run's OWN `parsing` write clobbered `indexed` first), so the
  narrow check never saw the case it was meant to catch. Fixed by generalizing `set_status()`'s
  guard: once a row is `indexed`, EVERY write is refused unless the new status is `pending` (the
  one legitimate way anything un-terminals an indexed row — a deliberate re-registration through
  `upsert_pending`). `set_progress()` got the same guard for the same reason (a stale run's
  progress callbacks are lower severity but the same class of bug). Verified live by literally
  reproducing the exact bypass sequence — set a row to `indexed`, then call `set_status` with
  `parsing`, `embedding`, and `failed` in sequence, simulating a stale run's real lifecycle — and
  confirmed the row stayed `indexed` through all three, where the round-3 version would have let
  the final `failed` through.
- **Checkpoint validation only checked JSON syntax, not structure.** `_load_checkpoint()` rejected
  malformed JSON but accepted `[]`, `{"x": 1}`, `[1, 2]` — anything that parses. An empty chunks
  checkpoint would have let a source reach `indexed` with `chunk_count=0` and nothing actually
  upserted; a wrong-shaped parsed checkpoint would raise deep inside chunking on every retry,
  forever, since nothing regenerates a checkpoint that already "exists." Fixed with a shared
  structural validator (`_valid_page_list`): non-empty list of `{page: positive int, text: str}`,
  with an optional max-length check reused for the chunk cap. Added defense-in-depth in `t_embed`
  too: refuses to proceed with zero chunks or a vector/payload count mismatch, so even a bug
  upstream of checkpointing can't reach `indexed` with nothing indexed. Verified live with actual
  planted-corruption tests (not just code reading): an empty `[]` chunks checkpoint was rejected
  ("wrong shape or empty — redoing this stage") and the row still ended up correctly `indexed`
  with the real 120 chunks; a `{"x": 1}` parsed checkpoint was likewise rejected and correctly
  redone from a fresh fetch+parse.

Also from this round: tightened `DOCUMENT_FETCH_ALLOWED_INTERNAL_HOSTS` from a bare-hostname
allowlist to host:port pairs (`api:8000`, not just `api`) — a hostname-only allowlist would have
let a document uri hit *any* port on that host, wider than what's actually needed. Verified live:
`http://api:9999/...` (wrong port) correctly rejected, `http://api:8000/...` still works. Added the
missing `.env.example` documentation for all four `DOCUMENT_*` settings.

One reviewer claim was checked and found already resolved: `LEARNINGS.md`'s SSRF section was said
to still describe the obsolete "allow all private ranges" design — re-read it and confirmed it
already correctly describes the two-pass fix (the round-3 update had already landed).

### Round 5 — checkpoint validation checked shape, not content

One P2, fixed: `_valid_shape()` (round 4) confirmed a checkpoint was a non-empty list of
`{page: int, text: str}` but didn't check whether `text` actually had any real content —
`[{"page": 1, "text": ""}]` or whitespace-only text passed cleanly. A chunks checkpoint like that
would create searchable Qdrant points with empty citation text. Split the single validator into two
stage-specific ones, since the correct rule differs by stage: parsed pages only need at least ONE
page with real text (individual blank pages are normal PDF reality — a divider, an image-only
page — and must be allowed to stay), while EVERY chunk must have real text (a chunk's whole reason
for existing is to carry embeddable content, so an empty one signals a tampered/foreign file, not a
normal pipeline outcome — confirmed `_chunk_page()` never produces one in the ordinary path).
Verified live with the exact reproduction from the report: planted
`[{"page":1,"text":""},{"page":2,"text":"   "}]` as a chunks checkpoint, confirmed the log line
("wrong shape, empty, or blank — redoing this stage") and that the row ended up `indexed` with the
real 120 chunks, not the 2 blank ones.

## 2026-07-29 — UI fix: admin token was never sent by the browser

Status: **complete**

User hit `Error: Missing or invalid bearer token` clicking "Ingest" on `/get-started`. Investigated
before fixing: `ui/index.html`'s `fetch()` calls to every mutating endpoint (register, retry,
delete, presign) never included an `Authorization` header — confirmed via `git log -- ui/index.html`
that this file has never been touched on this branch (last change predates all of this work), so
it's a pre-existing upstream gap, not something Block C or D broke. It stayed invisible because
`require_auth()` is opt-in — skipped entirely when `ADMIN_TOKEN` is unset, which is how the
original app is normally run locally — and only surfaced once a real token was configured, which
this assignment requires.

Fixed: added an admin-token input field (shown only in "full"/`/get-started` mode, alongside the
existing add-video controls), backed by `sessionStorage` (persists across reloads in the same tab,
clears on tab close, never touches disk). A small `authHeaders()` helper attaches
`Authorization: Bearer <token>` to all four mutating calls (`POST /api/videos`, `POST
/api/videos/{id}/retry`, `DELETE /api/videos/{id}`, `POST /api/videos/presign`) — deliberately NOT
attached to the presigned PUT itself, which goes to object storage, a different auth domain
entirely. Rebuilt and restarted the API container; confirmed the served page includes the new
field and helper.

## 2026-07-29 — UI fix: documents polluting the video list

Status: **complete**

While manually testing the ingest flow in the browser, the paper and deck showed up in the
"Your videos" panel labeled "0f · upload" — confusing, since they're neither. Cause: `GET
/api/videos` called `db.list_videos()`, which is kind-agnostic by design (it's shared with `GET
/admin/sources`, which correctly wants every kind). Fixed by filtering to `kind == "video"` at the
`src/api/videos.py` endpoint layer specifically, not in `db.list_videos()` itself, so
`/admin/sources` (what bench.py/eval.py rely on) stays unaffected. Verified live: `/api/videos`
now returns only the 4 real videos, `/admin/sources` still returns all 6 rows. Full multi-kind UI
rendering (proper icons/badges per source kind) is still Block F's job — this only stops
documents from appearing somewhere they don't belong yet.

## 2026-07-29 — UI feature: document upload (no local-testing path existed before this)

Status: **complete**

User asked to test paper/deck ingestion locally and pointed out there was no way to do it through
the UI — only `POST /admin/documents` with a URL. Fair gap: videos have a full presign→upload
flow with a file picker; documents never got the equivalent, because `eval.py` only ever registers
by URL and never needed one. Added `POST /admin/documents/upload` (multipart file + kind + optional
title) — no presign step, since a PDF capped at `DOCUMENT_FETCH_MAX_MB` is cheap enough to pass
through the API process directly (presigning exists for videos because those are large enough that
routing gigabytes through this process would be wasteful, not because a bypass is required in
principle). *(This first cut wrote bytes to the same public `data/corpus/` path the deck lives at,
or to a presigned GET URL, and handed off to the URI-based registration path — both were replaced
by the durable, private `storage_key` design below the same day; see "document upload hardening.")*

Added a "Your documents" panel to the UI mirroring the video library's structure — file picker,
paper/deck selector, title field, status list with retry/delete. Retry and delete reuse the
existing `/api/videos/{id}/retry` and `DELETE /api/videos/{id}` endpoints for document rows too —
both are kind-blind (operate on the shared `ms_videos` table regardless of `kind`), so this isn't a
workaround, just not duplicating already-correct logic under a new path.

One dependency gap caught immediately by a container crash on rebuild: FastAPI's `File`/`Form`
support needs `python-multipart`, not installed. Added it to requirements.txt.

Verified live end-to-end: uploaded the real deck PDF via curl, watched it reach `indexed` with the
correct 7 chunks; confirmed auth is enforced (401 without a token), kind validation rejects
`kind=video`, and content validation rejects a non-PDF file even with a `.pdf` extension. User then
confirmed it works via the actual browser UI.

## 2026-07-29 — Document upload hardening

Status: **complete**

Independent review of the upload feature (previous entry) found four P1s, all confirmed live:
cloud uploads stored a presigned GET as the permanent `uri`, which expires in an hour and mints a
new URL (and doc_id) on every re-upload of identical bytes; local uploads landed in the public
`corpus/` prefix, servable to anyone with no auth; deleting a document left its PDF and
parse/chunk checkpoints behind; the UI never added indexed documents to the query-selection list.
Fixed by giving uploads a durable, private `storage_key` (`documents/{user}/{sha256}.pdf`, no HTTP
route) that `src/ingest/document.py` reads directly via `storage.get_bytes()` — no fetch, no
expiry, no public exposure, and re-uploading identical bytes resolves to the same `doc_id`
(content-hash identity). Delete now purges checkpoints via a new `doc_prefix()` helper. Migrated
the one legacy document row that predated this fix off the public `corpus/` path.

## 2026-07-29 — Block E: deck slice by parameterization

Status: **complete**

Papers and decks are one parameterized code path in `src/ingest/document.py`, not two modules:
`KIND_SPEC = {"paper": "page", "deck": "slide"}` is the one thing that actually differs — the name
of the locator field a chunk's Qdrant payload carries. Internally, parse/chunk still treat both as
"the PDF page index"; only `t_embed` renames the field at the payload boundary, closing the
deck-locator-key gap flagged in the previous entry's "Work to return to."

Added the vision-caption branch: a deck slide whose extracted text is under `DECK_SLIDE_MIN_CHARS`
(a title/diagram-only slide) is rendered via `page.get_pixmap(dpi=DECK_SLIDE_RENDER_DPI)` and
captioned by the vision LLM (`llm.caption_image()`, new — reuses `answer()`'s per-provider
client/downscale/base64 plumbing with a captioning system prompt instead of the video Q&A one),
resolved through the same tenant-first/server-fallback `resolve_llm()` search.py already uses for
query-time answers. Papers never take this branch — a sparse paper page is normal PDF reality, not
a gap to fill. Checkpoint keys were versioned by `kind` + `_PARSE_VERSION` (done during the
concurrent Block F session below, once it saw this branch land) so a deck's pre-caption checkpoint
can never be silently reused after the semantics changed.

Verified live, not just by self-report: re-ran the existing "One Index for Every Source" deck
end-to-end — `/ask_stream` for `eval.py`'s exact hardcoded probe returns `locator: {"slide": 4}`,
matching the graded expectation verbatim. Built a purpose-made synthetic 2-slide PDF (slide 1 =
real paragraph text, slide 2 = only shapes, no text) to isolate the caption branch specifically:
inspected the Qdrant payload directly and confirmed slide 1 kept its real text (branch correctly
did NOT fire) while slide 2's payload text was the vision model's actual description of what was
drawn ("a flag design... solid pink background... large navy blue circle..."). Cleaned up the test
document and file after.

## 2026-07-29 — Block F: cross-source retrieval + ask_stream

Status: **complete**

`_fuse()` previously bucketed every hit into time windows keyed on `t_start`/`ms`, which documents
don't have — every chunk from the same paper/deck defaulted to `t=0` and collapsed into a single
window, silently discarding all but one page/slide per document and having the UI try to open a
document citation as a `<video>` tag. Fixed by branching on payload kind: document hits now bucket
by exact `(video_id, page|slide)` instead of time proximity — one window per page/slide, no time
merging. Citations now carry `kind`/`sourceId`/`locator` (`{page}` | `{slide}` |
`{start_ms,end_ms}`) and `text` alongside every existing field, matching `eval.py`'s
paper_indexed/deck_indexed/grounded checks literally.

Added `GET /ask_stream` (SSE): emits a `citations` event first, then streams the synthesized
answer — `POST /api/ask` is untouched. Two follow-up fixes to that endpoint: (1) Confidence Gate 1
previously only decided whether to abstain from generating an answer, not whether to emit
citations at all — a query with nothing genuinely relevant indexed could return a full citation
list next to an "I couldn't find that" answer. Extracted `gate_citations()` as the single source of
truth, applied before both the SSE citations event and answer synthesis. (2) The SSE citations
event was text-filtered but `answer_from_citations()` still received the unfiltered list, so the
LLM's positional `[n]` refs could point at a citation the client never saw — fixed by filtering
once and threading that exact list through both steps.

Also recalibrated `CONFIDENCE_THRESHOLD`/`TEXT_CONFIDENCE_THRESHOLD` (0.2/0.35 → 0.28/0.72) using
new tooling (`benchmark/negative_queries.jsonl`, admin-only `GET /admin/debug/retrieve`,
`benchmark/calibrate_thresholds.py`) — the old visual threshold sat below this corpus's CLIP noise
floor, so the AND-gate's visual side could never reject a negative, making the text threshold
irrelevant too. **Caveat surfaced by the guardrail review below and independently verified**: every
positive in `queries.jsonl` is text/transcript-shaped, so this calibration never actually exercised
a genuine visual-only citation (no strong transcript match, relying solely on the CLIP frame
score). Built a throwaway synthetic silent video (a red frame with a solid blue rectangle, zero
audio/transcript) to close that gap directly: queried it with purely visual phrasing and got
`best_visual` 0.32–0.36 — comfortably clear of the new 0.28 threshold — while a genuinely
irrelevant negative query scored 0.25, safely below it. That's real evidence the recalibration
doesn't reject a clear visual-only match; it does not prove every possible subtle real-world visual
moment clears it, which remains an open, lower-severity residual risk (see below). Deleted the test
video after.

## 2026-07-29 — Guardrail review of the above three entries

A `momentsearch-reviewer` subagent reviewed the combined diff (upload hardening + Block E + Block
F) against the 7 traps, security, and the literal rubric — the review these three entries didn't
get individually before commit, since they landed from a concurrent session mid-flight rather than
the usual one-block-at-a-time cadence.

**P1, addressed**: the confidence recalibration's visual-only blind spot described above. Closed
with the synthetic-video test rather than by reverting the threshold, since real evidence of a
clear-margin pass is stronger than reverting to an equally-unvalidated older number.

**P2s, two fixed**:
- Vision-captured slide images were passed to `llm.caption_image()` as raw `pix.tobytes()`
  (PyMuPDF's default PNG output) while the payload was hardcoded `image/jpeg` — latent, not
  triggered under the shipped `DECK_SLIDE_RENDER_DPI=150` (renders exceed `LLM_IMAGE_MAX_PX` so
  `_downscale()` always re-encodes to real JPEG), but a smaller custom DPI would silently degrade
  or fail the caption call. Fixed by rendering via `pil_tobytes(format="JPEG")` instead, so the
  bytes are always real JPEG regardless of DPI.
- `POST /admin/documents/upload`'s docstring now explicitly states why its bounded, off-event-loop
  storage PUT is a deliberate narrow exception to "ingestion never does synchronous work in the
  request path" — the graded contract endpoint (`POST /admin/documents`, URI registration) still
  does zero I/O before returning 202.

**P2s, accepted as-is / deferred** (documented here so they aren't silently lost):
- The upload endpoint still does its (bounded, threadpooled) storage write in the request path
  rather than presigning — a real design tradeoff for a dev-convenience endpoint `eval.py` never
  calls, not worth a presign-flow rebuild under assignment time constraints.
- Checkpointing is per-document, not per-slide: a retry after a transient failure partway through
  a large deck recaptions every already-processed slide, not just the failed one. Pre-existing
  granularity (the paper path already re-parses wholesale on retry); the caption branch just makes
  it more expensive. Worth revisiting if a real deck turns out large enough for this to matter.
- `c2af9c7`'s legacy-document-row migration was confirmed only by absence (no orphaned public PDF
  under `data/corpus/`) — the underlying Postgres row itself wasn't independently inspected.

## Work to return to

- [x] **Block G:** stale-ingest reconciliation + poison-URI DLQ — done (commit `f492496`); see
  LEARNINGS.md.
- [x] **Block G/K:** `benchmark/bench.py --resilience` replaced with a real worker-kill + resume
  proof — done (commit `aa0fef3`).
- [x] **Block H:** register-and-capture recall check, decoupling overlap proof, throughput gate,
  resilience gate — all four built and hardened across two independent review rounds (commit
  `aa0fef3`); see LEARNINGS.md.
- [~] **Block I:** tune to pass — one gate now passes reliably, one still doesn't. Diagnosed and
  fixed six genuine bugs live rather than just cranking concurrency:
  1. `docker-compose.yml` hardcoded `WORKER_CONCURRENCY: 2` in the worker service's `environment:`
     block, silently overriding whatever `.env` said — fixed to read `${WORKER_CONCURRENCY:-2}`.
  2. Prefect's `serve()` runner only polls Cloud for newly-scheduled runs every
     `PREFECT_RUNNER_POLL_FREQUENCY` seconds (default 10) — measured via Prefect Cloud's own
     flow-run timestamps that this dwarfed the ~3s of real parse/chunk/embed work per small
     document. Fixed by passing `query_seconds=config.DISPATCH_INTERVAL_S` to `serve()`
     (`src/worker.py`) so the runner's cadence matches our own admission tick.
  3. With (2) fixed and concurrency raised, embed tasks started hitting
     `ResponseHandlingException(ConnectTimeout(...))` against Qdrant Cloud — each ingest flow run
     is an isolated subprocess (Prefect's own model) that builds a fresh `QdrantClient`, so N
     concurrent flow runs means N simultaneous first-connection TLS handshakes, occasionally
     timing out under load and falling into Prefect's 60s×2 retry backoff (~120s dead per
     occurrence). Fixed with a short local retry around the upsert (`src/rag/vector_store.py`'s
     `_upsert_with_retry`).
  4. Also found fastembed's ONNX runtime defaults to using every visible core per process — the
     first attempt bounded this via a new `TEXT_EMBED_THREADS` config set on the WORKER service.
     **A guardrail review caught that this was dead code**: with `CLIP_SERVICE_URL` set (the
     docker-compose default), `embed_docs`/`embed_query` route over HTTP to the single warm
     `clip` service (`src/rag/embeddings.py`'s `if config.CLIP_SERVICE_URL:` branch) — the worker
     never runs `_text_model()` itself. Worse, the review found the REAL bug one level up: a
     single shared `threading.Lock()` guarded all four embedding calls in that one `clip` process
     — CLIP images, CLIP text, BGE documents, BGE queries — so ingest's bulk document embeds and
     search's own single query embed serialized behind each other regardless of which model
     either used. First pass at a fix: split into `_clip_lock`/`_text_lock`
     (`src/rag/embeddings.py`), sub-batched `embed_docs_local` at `TEXT_EMBED_BATCH` (32) so a
     large document's embed call releases the BGE lock between sub-batches, and moved
     `TEXT_EMBED_THREADS` onto the `clip` service in `docker-compose.yml`, where the model
     actually runs. **Measured to not move the decoupling ratio at all** (three runs at
     1.92/1.98/1.38, no better than before) — a second review round found why: this benchmark's
     synthetic documents produce ~9 chunks each, below `TEXT_EMBED_BATCH=32`, so each document's
     embed call is still exactly ONE sub-batch. The sub-batching had nothing to interleave against
     for this workload; BGE ingestion and BGE search queries still fully serialized on
     `_text_lock`.
  5. Same review also found the dispatcher's `count_inflight()` + `wfq_claim()` were two separate,
     unlocked reads (`src/db.py`): each dispatcher (one per worker replica) computed
     `slots = cap - count_inflight()` from its own stale snapshot, so two dispatchers ticking
     around the same moment could each admit up to their own computed `slots`, collectively
     overshooting `DISPATCH_MAX_INFLIGHT` (the per-row atomic claim prevents double-claiming one
     row, but said nothing about the total claimed per tick). Fixed with `db.claim_pending()`,
     which wraps count + claim in one transaction under a Postgres advisory lock
     (`pg_advisory_xact_lock`), serializing the whole sequence across every dispatcher process.
  6. The actual fix for the decoupling ratio: stop routing search's query embed through the shared
     `clip` service at all. `embed_query()` (`src/rag/embeddings.py`) now ALWAYS runs the bge model
     locally, in the API's own process, even with `CLIP_SERVICE_URL` set — only `embed_docs` (bulk
     ingest) still routes to the shared service. A search query is one small, latency-critical
     call; giving it its own process and its own lock means it can never contend with ingest's
     bulk embeds in the first place, not just wait less long for the same lock. Added a matching
     warmup in `src/app.py`'s lifespan (mirrors `clip_service.py`'s own pattern) so the first search
     isn't slow. `/embed/query` on `clip_service.py` is now dead code from our own code's
     perspective — nothing calls it — but kept, explicitly marked deprecated, as a rolling-deploy
     safety net: Fly replaces machines per-process-group, not as one atomic cross-group cutover, so
     an old-image api machine could briefly still be calling this route while a new-image clip
     machine is already serving. A first pass removed it outright; a second review round flagged
     the deployment-transition risk, which is real enough (and the endpoint cheap enough to keep)
     that removal wasn't the right call.

  Added `tests/test_block_i_reliability.py` (17 stdlib-unittest tests, no live stack): the Qdrant
  retry's success/failure/exhaustion/non-retryable paths, the embed_docs/embed_query routing
  (including the corrected always-local rule for queries — pinning down exactly the regression
  fix 6 addresses), the lock-split + sub-batching behavior, and `db.claim_pending()`'s
  lock-then-count-then-claim ordering and arithmetic. Mutation-verified: reverting the lock split
  back to one shared lock, reverting `claim_pending`'s slot arithmetic to ignore the inflight
  count, and reverting `embed_query` to route remotely were each confirmed to make the
  corresponding test fail.

  Net result at the checked-in default (`WORKER_CONCURRENCY=5`, `DISPATCH_MAX_INFLIGHT=10`, 2
  worker replicas): **decoupling ratio now passes reliably** — 1.09, 1.03, 0.89 across three
  separate runs after fix 6 (target 1.3), a real and repeatable result, not a lucky sample.
  Throughput remains short — **4.06–7.41 chunks/s** across the same runs (target 8), closest at
  7.41 (same config, different run). Fixes 4-6 were real, independently verified correctness
  improvements, and fix 6 in particular directly and repeatably fixed the gate it targeted — a
  different outcome from fix 4 alone, which was correct but didn't move the number. The remaining
  throughput gap is general resource contention across many concurrent flow-run subprocesses (CPU,
  Postgres pool, network) sharing one Docker Desktop VM, not a single identifiable lock or race —
  sweeping cap 8/10/12 all landed in the 4-7.4 chunks/s range with no clear monotonic trend, which
  is itself the signal that further local tuning isn't informative. `sla.json` was NOT loosened.
  Worth re-measuring on the real Fly deployment (Block J), where the API and worker get genuinely
  separate machine resources instead of sharing one CPU pool.
- [x] **Block N:** `docker compose up -d --scale worker=2` + `DISPATCH_MAX_INFLIGHT` sized to
  `replicas x WORKER_CONCURRENCY` — done; both replicas' dispatcher logs confirmed identical
  `max in-flight` matching the formula, and both were observed claiming backfill documents across
  the Block I runs above.
- [x] **Block M:** native PPTX ingestion — done, verified live end-to-end, not just unit-tested.
  - `src/ingest/detect.py`: a new, deliberately lightweight module (stdlib + `src/storage.py`, no
    prefect) — `sniff_document_kind()` structurally validates PDF (`%PDF` magic bytes) or PPTX
    (zip magic bytes, PLUS a real `ppt/presentation.xml` part AND a `[Content_Types].xml` entry
    naming it a presentation — not just any zip, which would let a spoofed `.docx`/`.xlsx` through).
    It's its own module rather than living in `src/ingest/document.py` (which imports prefect at
    module scope) specifically so `src/api/documents.py` (upload validation) and `src/rag/search.py`
    (citation URLs, below) can both use it without dragging prefect into the API process.
  - `src/ingest/document.py`'s `t_parse`: after fetching bytes (upload or URL), sniffs PDF vs PPTX.
    A PPTX is converted to PDF via `soffice --headless --convert-to pdf` (`_convert_pptx_to_pdf`,
    isolated `-env:UserInstallation` profile per call — concurrent flow-run subprocesses,
    `WORKER_CONCURRENCY > 1`, would otherwise lock-contend on LibreOffice's shared default profile)
    BEFORE the existing pymupdf extraction runs — completely unchanged downstream: chunking, the
    slide locator, vision-captioning, checkpointing all reuse the identical code path a
    hand-exported PDF already went through. The converted PDF is checkpointed SEPARATELY
    (`converted.pdf`, committed before `parsed.json`), which buys a specific, bounded guarantee —
    stated precisely because an earlier version of this document overclaimed it (caught in review):
    a crash ANYWHERE PAST a completed conversion (during pymupdf extraction, during vision
    captioning of text-poor slides, during chunk/embed, or a Prefect-level retry of the whole
    task) does not pay LibreOffice's cost again. A crash DURING conversion itself necessarily
    reconverts — the checkpoint is written after `soffice` returns, so there is nothing to resume
    from, and resuming a half-written PDF would be worse than redoing it.
    Both halves verified live, not asserted:
    - *Crash past conversion:* deleted a real document's `parsed.json` (keeping `converted.pdf`),
      re-ran `t_parse` — printed "already committed — skipping LibreOffice conversion", finished in
      0.6s with no soffice subprocess spawned, byte-identical extracted text.
    - *Crash during conversion:* ran `t_parse` on a real deck in a thread and `SIGKILL`ed the
      `soffice` child process mid-flight. Result: `PermanentDocumentError` (fails safely, no silent
      success), **no `converted.pdf` and no `parsed.json` left behind** (no corrupt partial
      checkpoint that would poison every later retry), and an immediately following retry converted
      and parsed cleanly to all 3 pages with both checkpoints then present. That's the honest
      guarantee: fail-safe and idempotent-on-retry, not resume-mid-conversion.
  - **Found and fixed a gap the initial implementation missed**: a PPTX upload's `storage_key`
    correctly stays pointed at the ORIGINAL `.pptx` bytes (conversion happens at parse time, not
    upload time) — but `src/rag/search.py`'s `_doc_url` and `src/api/search.py`'s
    `/api/document/{id}` both serve citations straight from `storage_key`, which would hand a
    browser raw `.pptx` bytes labeled `application/pdf` (or presigned with no label at all) — not
    renderable in the citation iframe. Fixed with `detect.viewer_storage_key()`: swaps in the
    `converted.pdf` checkpoint for VIEWING whenever one exists, leaving `storage_key` itself
    untouched. Verified live: `/api/document/{id}` for a real uploaded `.pptx` deck returns
    `Content-Type: application/pdf`, and the file is a genuine 3-page PDF (`file` command
    confirms), not the original pptx.
  - `Dockerfile`: added `libreoffice-impress` (not the full `libreoffice` metapackage — pulls in
    just the Impress filter + `libreoffice-core`'s `soffice` binary, not Writer/Calc/Draw's own
    filters this feature never uses).
  - `ui/index.html`: the upload file picker was hardcoded `accept="application/pdf,.pdf"` — a
    `.pptx` wouldn't even appear in the browser's file dialog. Widened the accept list and updated
    the help text and empty-file error message.
  - Live verification, full loop: built a real 3-slide `.pptx` (python-pptx, in an isolated scratch
    venv — not a project dependency) with distinct per-slide sentinel text, uploaded it through the
    running API, confirmed it indexed (3 chunks, one per slide), asked a question whose answer only
    exists on slide 3, and got back citation `slide: 3` with the exact sentinel text, a correct
    `deeplink` (`/api/document/{id}#page=3`) pointing at the converted PDF, and a correct LLM
    answer quoting it — then confirmed a spoofed "renamed .txt as .pptx" upload is rejected 415.
  - Added `tests/test_block_m_pptx.py` (40 stdlib-unittest tests, no live stack, no real soffice —
    mocked `subprocess.run`): `sniff_document_kind`'s real PDF/PPTX fixtures plus the spoofing cases
    (docx-shaped zip, missing presentation part, missing `[Content_Types].xml`, corrupt zip),
    `_convert_pptx_to_pdf`'s success/failure/timeout/no-output-file paths and per-call profile
    isolation, `t_parse`'s convert-vs-skip checkpoint branches (via `t_parse.fn`, Prefect's escape
    hatch to the undecorated function — no engine/flow context needed), and `viewer_storage_key`.
    Mutation-verified (disabling the presentation-part check made the right test fail).

  **Second guardrail review round found four more real issues** in the above — all fixed, all
  re-verified live (not just re-tested):
  1. `sniff_document_kind`'s PPTX check was a byte-substring search
     (`b"presentationml.presentation" in content_types`) over `[Content_Types].xml`'s raw bytes,
     not real validation — a crafted archive containing that string ANYWHERE (a comment, an
     unrelated part's declaration) passed without actually mapping `/ppt/presentation.xml` to a
     presentation content type. The same unbounded `zf.read("[Content_Types].xml")` was also a zip-
     bomb vector: a small archive with one wildly-compressed entry at that name could exhaust memory
     decompressing it. Fixed: real XML parsing (`_content_types_names_presentation`, `ElementTree`,
     checks the actual `<Override PartName="/ppt/presentation.xml">` mapping) plus a zip-bomb guard
     (`_zip_bomb_guard` — rejects on the zip directory's own declared sizes, before decompressing
     anything) and a hard size cap on the one entry actually read.
  2. `viewer_storage_key` returned `None` immediately whenever `storage_key` was `None` — true for
     URL-registered documents, which have no `storage_key` at all. But a URL-registered `.pptx` gets
     converted and checkpointed by `t_parse` exactly the same way an uploaded one does; the early
     return meant its citation always fell through to the ORIGINAL external `.pptx` URL, never
     checking whether a converted PDF existed. Fixed by removing the early return — the function now
     always checks for the checkpoint first, `storage_key`'s nullness only matters as the fallback.
     Also had to loosen `/api/document/{id}`'s entry guard (`src/api/search.py`), which previously
     404'd immediately when `storage_key` was falsy — a URL-registered PPTX's converted PDF lives in
     OUR OWN storage regardless.
  3. The upload UI defaulted to kind="Paper", and nothing server-side corrected it — a normal PPTX
     upload (the common case, no kind selected) produced "page N" citations instead of "slide N".
     Fixed server-side (`_enforce_kind_for_sniffed_type` in `src/api/documents.py`): a sniffed pptx
     always registers as `kind="deck"` regardless of what was requested; a sniffed PDF keeps the
     user's real choice. Also auto-selects "Deck" in the UI dropdown on `.pptx` file selection so the
     visible choice doesn't contradict what the server enforces anyway.
  4. This document previously claimed "a worker killed mid-conversion doesn't pay LibreOffice's cost
     again" and cited a test that only proved the WEAKER claim (crash AFTER conversion completes).
     Corrected above with the real, narrower guarantee, and backed by an ACTUAL mid-conversion kill:
     ran `t_parse` in a thread and `SIGKILL`ed the `soffice` child process while it was actively
     converting. Confirmed `PermanentDocumentError` (fails safely), zero partial/corrupt checkpoint
     left behind, and a clean subsequent retry converts and parses correctly.
  - Follow-up also addressed: `_convert_pptx_to_pdf` now validates its own output is real PDF bytes
    (`%PDF` magic) before returning — a `soffice` exit-0-but-garbage-output can no longer get
    checkpointed as if valid — and `t_parse`'s checkpoint READ path revalidates the same way
    (mirrors `_load_checkpoint`'s existing revalidate-on-read principle for the JSON artifacts,
    which `converted.pdf` — raw bytes, not JSON — had no equivalent guard for).
  **Third guardrail review round (external) found two more P1s, both fixed**:
  1. `_content_types_names_presentation`'s content-type comparison was `startswith()` against the
     presentation type with its `.main+xml` suffix stripped — a crafted `[Content_Types].xml`
     declaring e.g. `...presentationml.presentation.evil` satisfied the prefix and was accepted as a
     real pptx. Fixed: exact equality against `_PRESENTATION_CONTENT_TYPE`.
  2. `_enforce_kind_for_sniffed_type` only runs on the upload path (`src/api/documents.py`), which
     has the bytes in hand at registration time; a URL registration (`POST /admin/documents`) can't
     sniff a `uri` until `t_parse` actually fetches it, so a document registered `kind="paper"` whose
     URL happened to serve a `.pptx` sailed through the conversion branch under the wrong kind —
     wrong "page N" locators, and `converted.pdf`/`parsed.json` checkpointed under `kind=paper`'s
     namespace instead of `kind=deck`'s. Fixed in `t_parse` itself: reject with
     `PermanentDocumentError` the moment the fetched bytes sniff as `pptx` but the row's `kind` isn't
     `deck`, before any checkpoint write.
  - Also corrected `Assignment3_Plan.md`'s Block M exit criterion, which literally read "killing the
    worker mid-conversion and restarting doesn't reconvert" — the ACTUAL, tested guarantee is the
    opposite of that for in-flight work: a *committed* `converted.pdf` is skipped on resume, but a
    crash mid-conversion (before that commit) safely reconverts from scratch rather than resuming a
    partial file. Reconversion cost is paid at most once per crash, not eliminated.
  - Follow-up P2s from the same round, also addressed: (a) `viewer_storage_key`'s
    `storage.exists(conv_key)` — previously called on every document citation regardless of kind,
    even though a converted PDF can structurally only ever exist for a PPTX-sourced deck — now skips
    the storage round-trip entirely for papers and plain-PDF decks (`storage_key` not ending in
    `.pptx`), which is the common case; (b) `t_parse`'s PPTX branch now rejects a converted deck over
    `DOCUMENT_MAX_CONVERTED_PAGES` (default 500) BEFORE `_parse_pdf` runs, so a deck with a
    pathological slide count can't burn vision-captioning LLM calls on every slide before
    `DOCUMENT_MAX_CHUNKS` would eventually have caught it post-parse.
  **Final Block M P2 hardening**:
  - Added `DOCUMENT_MAX_CONVERTED_MB` (default 100). New LibreOffice output is checked with
    `stat()` before `read_bytes()`, and an existing `converted.pdf` is checked with object-storage
    metadata before `get_bytes()`. The cap therefore protects worker memory, rather than detecting
    an oversized file only after it has already been loaded.
  - Wrapped converted-PDF page-table inspection as `PermanentDocumentError`. A corrupt derivative
    now skips Prefect's 30s/120s retries because the same fixed bytes cannot repair themselves.
  - Added `ms_videos.view_storage_key`. Ingestion records the validated converted PDF there;
    citation rendering now chooses `view_storage_key` or the original `storage_key` using only the
    manifest row. This removes the remaining S3/GCS `HEAD` request from every PPTX citation.
  - Documented the conversion timeout, converted-byte cap, and converted-page cap in `.env.example`,
    with focused regression coverage for each new failure boundary.
  - Final verification at that point: Block M 40/40, complete repository suite 80/80, benchmark
    suite 17/17; `git diff --check` and Python compilation also passed. Later guardrail fixes raised
    these counts; see the newer entries below.
- [ ] **Block J:** create or select the permanent Fly app and update both `fly.toml`'s `app` value
  and `CLIP_SERVICE_URL` to the same app name.
- [ ] **Block J:** verify the intended GitHub deployment branch after `fly launch`; it may rewrite
  the workflow.
- [ ] **Block J:** personally open the public URL, ask a question, and preserve the URL plus
  screenshot/log evidence.
- [ ] **Block J:** the deck's registered URI (`http://api:8000/...`) is docker-compose-internal —
  will need re-registering under Fly's internal or public hostname after redeploy, which will
  also change its `doc_id` (expected; see Block H's register-and-capture design above).
- [ ] Minor, deferred: per-slide caption checkpointing.

## 2026-07-29 — UI fix: no discoverable path to the upload flow from the sample page

Status: **complete**

User pointed out that `/get-started` (the upload/bring-your-own-videos UI) was reachable only via
small hero text below the search examples on the sample page (`/`) — easy to miss, especially since
that page otherwise reads as read-only. Added a persistent "Get started →" button to the header,
always visible without scrolling; hidden automatically on `/get-started` itself, using the same
`applyMode()` toggle the existing hero-text link already relies on, so both stay in sync without a
second source of truth for which mode is active. Rebuilt and restarted the `api` container;
confirmed the button renders in the sample page's header and links correctly. Commit `4dcf836`,
pushed to `feat/multi-source-ingestion`.

## 2026-07-29 — Bug: YouTube auto-caption filler tags corrupting cross-modal ranking

Status: **complete**

User reported that searching "trust" ranked a video moment first whose transcript was just
`[Music] [Music] [Music]` — visually and semantically unrelated to the query. Diagnosed live against
the actual branch scores rather than guessed: the visual (CLIP) branch found a frame at 20:22
scoring 0.2321; the text branch separately surfaced a `[Music]` filler chunk at ~20:14-20:25 scoring
0.5836 — individually a weak match, well below `TEXT_CONFIDENCE_THRESHOLD`. `_fuse()`'s
`CROSS_MODAL_BOOST` doesn't check whether the paired text hit is ITSELF a confident match, only that
a frame and a text hit exist within `FUSION_WINDOW_S` (15s) of each other on the same video — so an
unrelated frame and a meaningless `[Music]` caption artifact coincidentally "confirmed" each other
and outranked the one transcript passage that genuinely discusses trust (best text-branch rank,
0.6889, but with no nearby frame to pair with, so no boost).

Root cause: `src/ingest/transcript.py`'s `_parse_json3` indexed YouTube's non-speech auto-caption
event tags (`[Music]`, `[Applause]`, etc.) as if they were real spoken content — nothing filtered
them before chunking/embedding. Fixed: a cue that is ONLY bracket tags is dropped before it ever
reaches `chunk_cues`/embedding (`_NON_SPEECH_CUE_RE`); a bracket tag alongside real words is left
untouched. Added `tests/test_transcript_filter.py` (6 tests): music-only and mixed-bracket cues
dropped, real speech kept, a tag-plus-speech cue kept, and an end-to-end "dead air produces zero
chunks" case.

The fix only prevents this going forward — it doesn't retroactively clean already-indexed data.
Purged the 64 stale text-branch chunks for the affected sample video (`yt_l7al3WHQdvg`, "The Hard
Part of AI") directly from Qdrant and re-ran `t_transcript` under the fixed code, leaving its
already-correct CLIP frame index untouched (no need to re-download/re-sample the video) — 62 chunks
came back, zero `[Music]`-only among them. Verified live: `/api/ask` for "trust" now surfaces the
actual trust-related transcript passage and deck slides instead of the spurious music moment.

## 2026-07-29 — Bug: confidence gate too strict for short/keyword queries

Status: **complete**

User asked "leadership" against a real, indexed PDF titled "Leadership Run Amok" and got an
abstain, despite the document being genuinely about leadership. Diagnosed live: "leadership" scored
`best_text=0.6715` against real matching content — below `TEXT_CONFIDENCE_THRESHOLD` (0.72). Rather
than assume the number was just miscalibrated, ran this project's own
`benchmark/calibrate_thresholds.py` inputs against the live index: two GIBBERISH negative-test
queries ("asdkfj qwoeiru xzcvbn 12345 blorp", "zzxx flerm dorbat nnn 000 vvv qqq") scored 0.6967 and
0.6837 on the SAME branch — higher than the real "leadership" query. No single
`TEXT_CONFIDENCE_THRESHOLD` value can accept the real short query without also accepting nonsense —
a known bge behavior on short/degenerate text, not a tunable number.

Fix: a third, independent OR-pass signal alongside `best_visual`/`best_text` in
`src/rag/search.py`. `_lexical_hit()` confirms a citation set when ALL of the query's significant
words (stopword- and length-filtered) appear together, as real whole words, in a SINGLE candidate
the dense branch already retrieved — bounded to already-retrieved candidates, not a corpus-wide
keyword search. Threaded through `retrieve()` -> `gate_citations()` ->
`answer_from_citations()`/`ask()`, plus `/ask_stream` and `/admin/debug/retrieve`
(`src/api/search.py`); every existing caller that omits the new parameter defaults to `False` —
identical behavior to before this fallback existed.

**Caught a real bug in my own first version before shipping it.** An ANY-word-matches variant
false-passed 3 of the 10 calibrated negative queries — "how does photosynthesis work in plants",
"what's the weather like in Tokyo today", "how do I change a flat tire on my car" — each sharing
exactly one common word ("work", "car", etc.) with something unrelated in this project's own
eclectic corpus (AI/RAG papers, a leadership psychology article, engineering decks, video
transcripts). Tightened to require ALL significant words together in the SAME candidate (a real
lexical AND-match), then re-verified against the FULL calibration set: 10/10 negatives still
correctly abstain, 14/14 positives still pass, and "leadership"/"leadership run" now both return
grounded citations and a real synthesized answer live. Added `tests/test_lexical_gate.py` (17
tests) covering `_significant_words`, `_lexical_hit` (including the ANY-vs-ALL regression case
directly), and `gate_citations`'s three-way OR-pass.

## 2026-07-29 — Ranking quality: unqualified cross-modal boost, general case

Status: **complete**

User reported "trust" and "leadership" queries ranking a generic "LLM explained briefly" video and
a neural-network explainer clip above genuinely on-topic content (a "Trust in Action" deck, "The
Hard Part of AI"'s actual trust discussion, "Leadership Run Amok" pages) — screenshots showed
frame-only ("seen") citations from those two clips occupying half the top-6 slots for both queries,
plus an unrelated "Pinecone's New Hybrid Search" frame surfacing for "trust".

This is the SAME bug class as the `[Music]`-filler cross-modal-boost bug fixed earlier today, but
the general case that fix didn't close: `_fuse()`'s `CROSS_MODAL_BOOST` (`src/rag/search.py`) still
had no confidence check on the paired hit, it just multiplies a window's score by 1.5x whenever a
frame and a text hit land within `FUSION_WINDOW_S` of each other. The earlier fix only removed one
SOURCE of low-quality text hits (non-speech caption artifacts); it didn't change the fusion logic
itself, so any OTHER weak-but-real text hit paired with a generic frame reproduces the same failure
mode. And these two videos' frames are genuinely generic on this corpus — CLIP's visual similarity
sits in a narrow, non-discriminating band (~0.23-0.33) regardless of query relevance (see
`config.py`'s `CONFIDENCE_THRESHOLD` calibration comment), so their frames clear the visual top-20
candidate list for almost any query, and any weak vocabulary overlap in a nearby caption was enough
to trigger the boost.

Fix: qualify the entire two-branch score on the paired TEXT hit's own raw score, via a new
`CROSS_MODAL_TEXT_MIN` constant (`src/config.py`, defaults to the already-calibrated
`TEXT_CONFIDENCE_THRESHOLD`, kept as its own named constant since it answers a different question
and may need to diverge with more data). Deliberately did NOT also gate on the frame's own score —
the calibration comment already establishes CLIP's band carries no relevance signal at all, so
gating on it would strip the boost from good pairs and admit bad ones at roughly random, targeting
nothing. A below-threshold pair now competes as its strongest single branch; otherwise merely
adding its two RRF terms would still structurally outrank every document-only hit even without the
multiplier. Added `tests/test_fuse_boost.py` (7 tests) directly exercising `_fuse()`'s scoring and
cross-window ordering —
no test had covered that function before. Also removed dead `KNN_K` config (never referenced
outside its own definition; `search.py`'s module docstring falsely claimed retrieval "fetch[es]
KNN_K candidates" when it actually uses `BRANCH_TOP_K`) and fixed that docstring.

Before fixing, added a rank-sensitive eval metric that didn't exist: `benchmark/bench.py`'s
`measure_recall()` is a binary "is the gold citation present anywhere in the top 10" check — both
"trust" and "leadership" would have PASSED it even with the bug present, since the correct citation
was always in the result set, just outranked. Added `measure_ranking()` (MRR@6, the app's real
`TOP_K`, with a per-query-kind breakdown since the labeled set only has 3 video-kind queries out of
14) and wired it into `main()`'s gate sequence. Measured before and after on the existing 14 labeled
queries: `recall@10` and `MRR@6` were IDENTICAL pre/post fix (0.929 / 0.717) — none of those queries
happen to trigger this bug, so this fix is a pure precision improvement the existing labeled set
can't see, not something `bench.py` alone could have validated. Set `sla.json`'s new
`mrr_at_6_min: 0.60` from the measured 0.717 baseline (with margin), not a guessed number.

Verified live against the exact reported queries after rebuilding and restarting the `api`/`worker`
containers: "trust" now returns 3 citations, all text-branch, all genuinely on-topic (two "Trust in
Action" slides, "The Hard Part of AI"'s actual trust discussion) — the LLM-explainer clip,
neural-network clip, and Pinecone frame are gone entirely, not just reordered. "leadership" now
returns "Leadership Run Amok" pages 6 and 8 at ranks 1-2 (previously rank 2-3, sharing the page with
an irrelevant clip at rank 1), plus "The Hard Part of AI" at rank 3; both generic clips are gone.
Ran the full `benchmark/bench.py` suite afterward: all ranking-relevant gates pass
(`recall_at_10`, the new `mrr_at_6`, `error_rate_max_pct`, `search_p95_during_ingest_ratio`); the
only failure, `ingest_throughput_chunks_per_s` (4.36 vs target 8), is unrelated ingestion-pipeline
throughput on this machine, not a regression from this change.

A cross-encoder reranker (`README.md`'s own "later, under real load" roadmap item) was scoped as a
possible follow-up but deliberately NOT built — the boost fix alone resolved both reported cases
cleanly, and building a second-stage model on top of an already-fixed bug would have added latency
and a new failure mode without a demonstrated remaining gap. Left as an explicit future item if a
gap shows up on a broader/harder query set.

**Closing guardrail follow-up:** review caught that gating only the explicit multiplier was
insufficient: the unconditional sum of two RRF terms still made every weak frame+text pair score
above every single-branch document result. Below-threshold pairs now compete as their strongest
branch, with calibrated text confidence breaking equal-RRF ties. A deterministic cross-window test
locks down the exact weak-video-versus-strong-document ordering. The same review restored the Block
M invariant that cached parsed PPTX text cannot resume toward `indexed` when its viewer PDF violates
the configured cap; doing so would leave `view_storage_key` null and citations falling back to raw
PPTX bytes. `.env.example` now removes dead `KNN_K` and documents `CROSS_MODAL_TEXT_MIN`, while the
MRR metric and hard-gate control flow have dedicated benchmark tests. Final verification after the
fixes: repository 89/89, benchmark guardrails 19/19, compilation and diff checks clean; rebuilt-live
MRR@6 = 0.788 (paper 0.639, deck 1.0, video 0.733), and the existing PPTX citation still returned
`206 application/pdf` with `%PDF-1.7` bytes.

**Known follow-up (deferred, not started):** after the above fix, "leadership" still showed "Large
Language Models explained briefly" at rank 1, ahead of "Leadership Run Amok" pages 6/8 (screenshot
from live app). Its score (0.0167) is NOT boosted — it equals a plain rank-1 single-branch RRF value
(`1/(RRF_K+0)`), the same value the Leadership page-6 text hit has. Working hypothesis: this is a
**tie**, not a repeat of the cross-modal-boost bug. Pure RRF encodes only *rank within a branch*, not
match *magnitude* — a generic CLIP frame that happens to be the visual branch's rank-1 hit scores
identically to a genuinely relevant text hit that's the text branch's rank-1 hit, even though one is
confident-and-wrong and the other confident-and-right. Whatever's breaking the tie today favors the
frame window. Not yet root-caused against real window-level debug output (would need to log each
window's `modalities`/`rrf`/branch composition for this exact query to confirm vs. some other
mechanism). Candidate directions: magnitude-aware fusion (blend raw score into the RRF sum, not just
rank) or an explicit tie-break preferring text over frame-only windows. Deferred at user's request
("in the interest of time, maybe we tackle this later") — not investigated further this session.

## 2026-07-30 — Block K: final evidence

Status: **complete — one genuine FAIL surfaced and reported, not papered over**

- **Full regression:** repository unit tests (`tests/`, stdlib `unittest`) + benchmark self-tests
  (`benchmark/test_bench_gates.py`) = **108/108** (89 + 19). Run via
  `docker run --rm -v "$(pwd)":/app -w /app momentsearch-a3-api:latest python -m unittest
  discover ...` — the Dockerfile doesn't `COPY` `tests/`/`benchmark/` into the image, so these
  ran bind-mounted onto the built image rather than the image's own `/app`.
- **`eval.py`** against the local stack: 7/8 automated checks pass. `documents_async` FAILs only
  because its hardcoded probe (`arXiv 2312.10997`) is already registered in this long-lived dev
  database from Block B — re-POSTing it returns the row's *current* status (`queued`), not
  `pending`, failing the check's exact string match. Verified directly that async accept genuinely
  works: a never-before-seen URI returns `202 {"status":"pending",...}` in 125ms. `decoupled` is
  correctly deferred to `bench.py`, which independently PASSED it.
- **`bench.py`** (SLA suite, run with `ADMIN_TOKEN`/`BASE_URL` exported — it reads env, not CLI
  flags; a first attempt without exporting `ADMIN_TOKEN` produced cascading 401s that looked like a
  throughput regression and wasn't one): accept_latency_p95 143.9ms (✅ ≤300), decoupling ratio
  1.09× (✅ ≤1.3×, 63% of samples confirmed landing during genuinely active ingest work), recall@10
  0.929 (✅ ≥0.70), MRR@6 0.645 — paper 0.639 / deck 0.6 / video 0.733 (✅ ≥0.60). Throughput 0.95
  chunks/s (❌ vs 8) and this run's error_rate 25% (❌ vs 1%, 5/20 backfill docs stuck `queued`)
  are the same single-worker-replica ceiling documented since Block H/I — real, re-measured on this
  exact run, not assumed from history; Block N's `--scale worker=2` is the known fix, not applied
  here.
- **`bench.py --resilience` — a new, real regression, run twice to be sure:** the first run was
  launched immediately after the SLA suite above, while its leftover 5-document backlog was still
  draining on the same single worker — a contaminated experiment. After clearing that backlog and
  confirming the queue idle, a second, isolated run **still failed**: of 10 documents killed with 5
  genuinely `chunking`, only 3 resumed from checkpoint correctly; 2 had already-committed
  `parsed.json` work **redone instead of resumed**, and 2 ended `failed` with a raw boto3
  `NoSuchKey: ... GetObject ...` error — text that does not appear anywhere in this repo's source.
  Verified directly, for every failed document: `storage.get_bytes(storage_key)` called by hand
  against the live worker container **succeeded immediately** and returned the correct bytes — the
  underlying data was never actually lost. This points at the orchestration/retry layer (most
  likely Prefect Cloud's own post-kill task retry, not this app's checkpoint-resume code, which the
  3 successful resumes in the same run prove works) rather than confirmed data loss, but the
  rubric's literal bar ("finished stages not re-run") is not met by either run. This contradicts an
  earlier clean "10 indexed, 0 failed, 0 stuck" result recorded above under Block I, which used a
  temporarily-lowered `RECONCILE_STALE_S=8s`; this run used the production default (300s) — the
  leading suspect for a follow-up, not yet confirmed.
- **Live cross-source test**, sources the student didn't author: froze-registered
  [Umar Jamil's public RAG-notes slide deck](https://raw.githubusercontent.com/hkproj/retrieval-augmented-generation-notes/main/Slides.pdf)
  fresh this session (found via web search, not guessed), alongside the already-locked external
  arXiv RAG survey and the external Pinecone hybrid-search video. One query — "compare embeddings,
  vector databases, cosine similarity, and hybrid search techniques used in retrieval augmented
  generation" — returned citations of all three kinds (video timestamp, paper page, deck slide)
  with correct deeplinks to the original public sources, verified live on **both** localhost and
  the deployed Fly URL (`https://momentsearch-wispy-silence-981.fly.dev/`, confirmed 200 after a
  ~32s cold-start wake).
- **Secret / staged-file / log audit:** `git status` clean going in; `.env`/`.env.*` confirmed
  gitignored and untracked; grepped every tracked file and both `api`/`worker` container logs for
  the live values of `ADMIN_TOKEN`, `DATABASE_URL`, `LLM_API_KEY`, `PREFECT_API_KEY`,
  `QDRANT_API_KEY` — none found. `.env.example` contains only placeholders. Canary clean: no
  `ROBOT_WAS_HERE.md`, no 🦥-prefixed commits in the last 100.
- **Documented "How I ran it"** as a new `README.md` section (exact commands for the regression
  suite, `eval.py`/`bench.py` invocation including the env-vs-CLI-flag gotcha, and the secret
  audit) and wrote `PRODUCT_EVAL.md` at the repo root with the full self-assessment above. The
  plan's `/fde-momentsearch-scaled-eval` reference turned out to be an actual project skill
  (`.claude/skills/fde-momentsearch-scaled-eval/`), used directly for the report structure.
- **Not done in this block, by design:** no attempt to fix the resilience regression or the
  throughput gap — Block K is evidence-gathering, not a fix block. Both are recorded as the top
  follow-up items in `PRODUCT_EVAL.md` rather than tuned away or silently retried until green.

## 2026-07-30 — cross-environment dispatcher collision (real bug, fixed; distinct from the resilience regression)

Status: **fixed and deployed — but confirmed NOT the same bug as Block K's `--resilience` FAIL**

- **Trigger:** external review (a second reader's analysis of `PRODUCT_EVAL.md`) proposed that
  Block K's `NoSuchKey` failures were an environment-affinity bug: local dev and the Fly deployment
  share one Prefect Cloud workspace and one Neon manifest, but have incompatible storage backends
  (`local` disk vs. Tigris/S3). Confirmed live: `flow.serve()`'s runner has no concept of
  "environment," so either environment's worker can win the claim for any pending row, including
  one whose bytes only exist on the other machine's disk.
- **Root cause, two layers deep.** The first fix (suffix every deployment name with
  `FLY_APP_NAME`, defaulting to `"local"`) closed HALF the problem: it stopped a NEW registration
  from being scheduled under a name the wrong environment's worker was polling. It did not stop the
  dispatcher's own claim query from admitting an old row for execution — proven by a second
  `bench.py` run that still produced `NoSuchKey`, traced to flow runs scheduled under the
  now-orphaned unsuffixed `ingest` deployment name. The full fix (user-approved, "full fix now"
  over a narrower patch): a `storage_env` column on `ms_videos`, stamped with `DEPLOYMENT_ENV` at
  registration time for any row backed by this app's own storage (`storage_key` set), left `NULL`
  for anything fetched fresh over HTTPS at ingest time (YouTube URLs, external paper/deck URIs) —
  those have no environment affinity at all. `db.claim_pending`'s admission query now filters
  `storage_env IS NULL OR storage_env = %(env)s`, so a dispatcher can never admit a row whose bytes
  it can't reach, in either direction.
- **A second, unrelated bug surfaced during the Fly rollout:** after deploying the fix, Fly's
  worker silently crash-looped (`except Exception: print + sleep 15s + retry`, invisible in
  `fly logs` due to Python's default block-buffered stdout with no `PYTHONUNBUFFERED` set) on a
  `403 Forbidden: "You have reached the maximum number of deployments for your workspace...
  Current limit: 5"` — a previously-undocumented Prefect Cloud free-tier cap. The two now-orphaned
  unsuffixed `ingest` deployments (one per flow) were still registered and eating two of the five
  slots. Deleted them via `client.delete_deployment()`; Fly's worker recovered on its next retry
  cycle.
- **Verified:** re-ran `bench.py` after both fixes — zero `NoSuchKey` in `docker compose logs
  worker` across the run. Fly's worker log confirmed it registered under the new
  `ingest-momentsearch-wispy-silence-981` deployment name and is polling for scheduled runs.
- **Explicitly NOT what this fixes — checked carefully before claiming otherwise:** re-reading
  Block K's own `--resilience` writeup (`PRODUCT_EVAL.md` §1) shows its isolated run used
  `STORAGE_PROVIDER=local` for every container involved — a single-environment SIGKILL-and-restart
  experiment, with no Fly worker and no cross-environment claim possible. That run's two `NoSuchKey`
  failures and two redone-instead-of-resumed stages are therefore a DIFFERENT bug from the one just
  fixed here, despite an identical-looking error string. The eval's own leading suspect (a race
  between this app's staleness reconciler and Prefect Cloud's native post-kill task retry, gated by
  `RECONCILE_STALE_S`) remains open and is still the top item in `PRODUCT_EVAL.md`'s "Top fixes"
  list. Caught this by checking the actual `STORAGE_PROVIDER` value of the failing run before
  reporting the collision fix as a resilience fix — worth flagging because the first read of the
  eval table treated "same error text" as "same bug," which would have been wrong.
- **Not done:** did not re-run `bench.py --resilience` specifically after this fix (only the
  general SLA suite, which doesn't run the SIGKILL experiment) — that gate's actual pass/fail state
  post-fix is still unconfirmed, and the reconciler/Prefect-retry race behind it is untouched.
