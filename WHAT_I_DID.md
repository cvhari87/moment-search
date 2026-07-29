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

## Work to return to

- [ ] **Block D:** the actual paper ingestion pipeline (`src/ingest/document.py`) doesn't exist
  yet. The paper (`doc_1bec551249fe`) and deck (`doc_2e9855253277`) are pre-registered and sitting
  `pending`; the dispatcher currently claims them, gets `prefect.exceptions.ObjectNotFound` from
  `jobs.enqueue_document` (the `ms-ingest-document/ingest` deployment isn't registered by any
  worker yet), and correctly resets them to `pending` to retry next tick. Expected, not a bug —
  resolves the moment Block D's flow is registered.
- [ ] **Block E:** deck slice by parameterization (slide locator + vision-caption branch for
  text-empty slides).
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
