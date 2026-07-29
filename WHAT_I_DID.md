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

## Work to return to

- [ ] **Block E:** deck slice by parameterization (slide locator + vision-caption branch for
  text-empty slides). Also the point to fix the deck payload's locator key from `page` to `slide`.
- [ ] **Block G:** implement stale-ingest reconciliation; Prefect crash detection is not job
  redelivery (confirmed directly in the Section A2 crash experiment).
- [ ] **Block G/K:** replace the `benchmark/bench.py --resilience` stub and prove zero dropped
  sources, eventual completion, and no re-running of committed stages.
- [ ] **Block H:** implement the register-and-capture pattern for `bench.py`'s recall check (see
  round 3 above) rather than relying on the static `source_id` values recorded in
  `queries.jsonl`.
- [ ] **Block J:** create or select the permanent Fly app and update both `fly.toml`'s `app` value
  and `CLIP_SERVICE_URL` to the same app name.
- [ ] **Block J:** verify the intended GitHub deployment branch after `fly launch`; it may rewrite
  the workflow.
- [ ] **Block J:** personally open the public URL, ask a question, and preserve the URL plus
  screenshot/log evidence.
- [ ] **Block J:** the deck's registered URI (`http://api:8000/...`) is docker-compose-internal —
  will need re-registering under Fly's internal or public hostname after redeploy, which will
  also change its `doc_id` (expected; see Block H's register-and-capture design above).
