# Learnings

This document records technical and process lessons from building Assignment 3. The activity log
lives separately in `WHAT_I_DID.md`.

## The system in plain English

We are building a search engine for a mixed digital library. The library contains videos,
research papers, and slide decks. A user should be able to ask one question and receive an answer
that points back to the exact video moment, paper page, or deck slide where the evidence came
from.

The original application already knows how to process and search videos. Our job is to add papers
and decks without breaking that working video path. The finished system will reuse the same front
door, waiting line, workers, storage, search system, and user interface for all three source
types.

### A library analogy

Imagine a large library with a front desk and a team working behind it:

| Technical component | Library equivalent | What it does |
|---|---|---|
| FastAPI API | Front desk | Accepts new sources and questions, checks permission, and responds quickly |
| Neon Postgres | Master ledger | Records what the library owns, who owns it, and where processing has reached |
| WFQ dispatcher | Fair line manager | Decides which waiting job may enter next without letting one user dominate |
| Prefect Cloud | Job supervisor | Assigns admitted jobs, tracks runs and retries, and shows an operations dashboard |
| Worker | Processing team | Downloads, parses, chunks, captions, embeds, and indexes sources |
| Object storage | Warehouse | Keeps uploads and durable intermediate artifacts outside disposable workers |
| Qdrant Cloud | Meaning-based card catalog | Finds relevant content by meaning instead of requiring exact keyword matches |
| LLM | Research assistant | Writes an answer using retrieved evidence and attaches citations |
| UI and SSE | Reader's desk | Shows the answer and citations as they arrive |

No single component is expected to do every job. The master ledger is excellent at recording
facts and state, but it is not the meaning-based catalog. The catalog is excellent at similarity
search, but it is not the authority on whether a source is still processing or who owns it.

### What happens when an administrator adds a paper

This is the target flow after Blocks C and D are implemented:

1. The administrator sends the paper URL to `POST /admin/documents` with a Bearer token.
2. The API checks the token, source kind, and basic URL rules.
3. The API adds a `pending` row to Postgres and returns HTTP `202 Accepted`. A `202` means "we
   accepted the job," not "the paper is already searchable."
4. The fair dispatcher sees the pending paper when worker capacity is available. If another user
   has been waiting longer in the fair rotation, their source gets a turn first.
5. The dispatcher asks Prefect to create a document-ingestion run.
6. A worker fetches the PDF with time, size, network, and file-validation limits.
7. The worker extracts each real page, creates page-aware chunks, and writes durable checkpoint
   files to object storage. A chunk is a small passage suitable for search.
8. The embedding model turns every chunk into a vector: a list of numbers representing semantic
   meaning.
9. The worker writes those vectors and their real page locators to Qdrant.
10. Only after Qdrant confirms the write does Postgres change the source status to `indexed`.

The deck flow is almost identical. Its locator is called `slide` instead of `page`, and a slide
with little extractable text may be rendered as an image and captioned before it is embedded.

### What happens when a user asks a question

This is the target read path after Block F:

1. The user sends a normal-language question to `/ask_stream`.
2. The system converts the question into search vectors compatible with the stored indexes.
3. Qdrant searches video transcript chunks, paper chunks, deck chunks, and the auxiliary visual
   video branch, always applying the user's ownership filter.
4. The retrieval layer combines and ranks those candidates. This is called **fusion**.
5. Each winning result already carries a real locator: timestamp, page, or slide.
6. The LLM receives the question plus only the retrieved evidence and prepares an answer.
7. The API streams citations and answer text to the browser using SSE.
8. The UI renders clickable citations that return the user to the original evidence.

If retrieval finds nothing, the system must return no citations rather than inventing a plausible
page or timestamp. The LLM is the writer, not the source of truth for locators.

### What exists now and what we are still building

At the end of Section A2, the video ingestion and video search paths work. The infrastructure
boxes—API, Postgres, dispatcher, Prefect, workers, storage, Qdrant, CLIP service, and UI—already
exist. The remaining blocks extend those boxes:

| Block | Addition |
|---|---|
| C | Shared source schema, safe migrations, `/admin/documents`, and `/admin/sources` |
| D | Paper parsing, page-aware chunks, checkpoints, and indexing |
| E | Deck support through the same flow, including optional visual slide captioning |
| F | Cross-source retrieval, SSE answers, citations, and UI rendering |
| G | Stale-work recovery and bounded failure for poison inputs |
| H–K | Real benchmarks, tuning, deployment, regression checks, and preserved evidence |

The architecture diagram is therefore a picture of the destination. It is not a claim that every
box and arrow is already implemented.

### A short glossary

| Term | Plain-English meaning |
|---|---|
| Source | One registered video, paper, or deck |
| Manifest | The Postgres list of sources and their processing state |
| Ingestion | Turning a raw source into searchable material |
| Chunk | A small searchable passage tied to a timestamp, page, or slide |
| Locator | The place where evidence came from: `start_ms`, `page`, or `slide` |
| Embedding | A numeric representation that places similar meanings near one another |
| Vector database | A database designed to find nearby embeddings efficiently |
| Asynchronous | Work continues after the initial API response instead of blocking it |
| Worker | A separate process that performs slow background work |
| Dispatcher | Code that decides which waiting source is admitted next |
| Orchestrator | A system that starts and observes multi-stage jobs and task retries |
| Checkpoint | A durable saved stage output that lets replacement workers resume |
| Idempotent | Safe to repeat without creating duplicates or corrupting state |
| SSE | A simple HTTP stream that sends incremental events from server to browser |
| Tenant filter | A rule ensuring one user's search cannot return another user's data |

## Stack map and why each piece exists

The stack follows one organizing rule: **keep the latency-critical search path separate from the
slow, bursty ingestion path**. Each service owns one kind of state or work, and the boundaries are
deliberate. Registration should remain cheap even when ingesting a long video, a large paper, or a
200-slide deck takes minutes.

| Component | What it owns | Why it is here | What it does not own |
|---|---|---|---|
| Neon Postgres | Source manifest, ownership, lifecycle, fair-admission state | Transactional coordination and managed serverless Postgres | Vectors, large files, or workflow execution |
| Qdrant Cloud | Searchable CLIP and semantic-text vectors plus filterable payloads | Vector-native nearest-neighbor retrieval and payload filtering | Authoritative source lifecycle or raw documents |
| Object storage | Raw uploads, frames, transcripts, parsed documents, and checkpoints | Durable, inexpensive storage for large bytes and intermediate artifacts | Relational coordination or semantic search |
| Postgres WFQ dispatcher | Fair ordering and admission limits | Stops one tenant or backfill monopolizing worker capacity | Executing ingestion stages |
| Prefect Cloud | Admitted flow runs, task/flow states, configured retries, run history | Observable orchestration of multi-step background jobs | Fairness or guaranteed hard-crash recovery |
| Worker processes | Fetching, parsing, sampling, chunking, embedding, and indexing | Keeps expensive ingestion off the public API process | Serving user-facing search traffic |
| CLIP/text embedding services | Conversion of images, passages, and queries into compatible vectors | Makes visual and semantic similarity searchable | Writing final answers |
| LLM provider | Grounded answer synthesis and, where implemented, visual captioning/enrichment | Converts retrieved evidence into a useful response | Choosing or inventing citation locators |
| FastAPI application | Admin/search contracts, auth checks, UI, and SSE | Thin public boundary over the state and retrieval layers | Heavy document or video processing |
| Docker Compose | Reproducible local process topology | Starts the application processes consistently | Hosting managed cloud state locally |
| Fly.io | Production process groups built from one image | Independent runtime/scaling for API, worker, and CLIP service | Replacing Neon, Qdrant, Prefect, or object storage |

### Storage and state

**Neon Postgres** holds the source manifest: one row per video, paper, or deck, scoped by
`user_id`. The row records metadata and progress through source-specific stages such as
`pending`, `queued`, `fetching` or `parsing`, `chunking`, `embedding`, and a terminal state such as
`indexed` or `failed`. Postgres transactions also support the atomic WFQ claim that prevents two
dispatchers from admitting the same source.

Neon is a suitable managed implementation because this is relatively light, transactional
coordination traffic and Neon can suspend idle compute. That avoids operating a continuously
running database server for the assignment. The design depends on ordinary Postgres semantics,
however; Neon is an operational choice rather than an application-level dependency.

**Qdrant Cloud** stores the vectors used for retrieval. The architecture is one logical search
surface backed by two physical collections because the vector spaces are incompatible:

- the visual collection stores CLIP embeddings for video frames;
- the shared text collection stores semantic text embeddings for video transcript chunks and is
  the collection papers and decks join;
- every point carries filterable payload such as `user_id`, source/video ID, modality, source
  `kind`, and a real locator (`start_ms`, `page`, or `slide` as applicable).

Qdrant provides HNSW search, payload indexes, tenant filtering, on-disk vectors/graphs,
quantization, and rescoring controls. Those vector-specific controls are the reason to keep a
specialized vector database rather than placing embeddings in the manifest database. `pgvector`
would be a valid simpler alternative, and its filtering behavior depends on index/query design;
the choice should be justified by measured retrieval and operational needs rather than a blanket
claim that every Qdrant query is faster.

The current application performs hybrid **multimodal** retrieval: a CLIP text-to-image branch and
a dense semantic-text branch are searched independently and fused using reciprocal rank fusion
(RRF). BM25/sparse retrieval and HyDE are not currently implemented in this checkout. If they are
added, they become additional retrieval branches rather than facts we should claim in advance.

**Object storage** keeps bytes that do not belong in either database. Depending on environment,
the abstraction can use local disk, an S3-compatible provider, Google Cloud Storage, or Fly
Tigris. It holds uploaded videos and extracted frame thumbnails today and is the natural home for
raw documents, parsed page/slide output, transcripts, and durable `parsed`/`chunks` checkpoints.
Keeping those artifacts outside workers is what allows a replacement worker to resume from a
committed boundary.

### Orchestration and background execution

The queue is a collaboration between **Postgres, the dispatcher, Prefect, and workers**:

1. The API validates a registration and inserts a `pending` manifest row.
2. With fair dispatch enabled, the Postgres WFQ dispatcher admits pending rows across users only
   when an inflight slot is available. With it disabled, the API enqueues immediately.
3. `run_deployment(timeout=0)` creates a Prefect flow run without waiting for ingestion to finish.
4. A serving worker accepts the run and executes the flow's configured tasks.
5. Only after a successful Qdrant upsert may the application mark the source `indexed`.

**Prefect Cloud** provides flow/task state, dependencies, configured in-process retries, and an
operations dashboard. It does not own fair ordering, and the observed hard-crash behavior means
we do not assume it automatically redelivers every killed run or skips every completed task. The
assignment's no-loss property comes from application-owned stale-row recovery, durable
checkpoints, and idempotent Qdrant writes; Prefect makes those runs observable and coordinates
their normal execution.

### Intelligence and retrieval

There are three distinct model responsibilities:

- **CLIP embeddings** place video frames and text queries in the same 512-dimensional visual
  space. Locally and on Fly, a separate CLIP service keeps one expensive model warm so API and
  worker processes do not repeatedly load Torch/model weights.
- **Semantic text embeddings** represent transcript/document chunks and text queries. They are
  selected with `TEXT_EMBED_PROVIDER`: FastEmbed/BGE is the default CPU/keyless option, while an
  OpenAI or OpenAI-compatible embeddings endpoint is optional. Indexing and query configuration
  must use the same model and vector dimension; changing providers requires reindexing.
- **The answer LLM** synthesizes a response only after retrieval. `LLM_PROVIDER` currently supports
  OpenAI-compatible endpoints, NVIDIA NIM, and Anthropic. An OpenAI-compatible base URL also lets
  a tenant use services such as vLLM, Ollama, OpenRouter, or another compatible host. There is no
  native Gemini provider branch in this checkout. A future document flow may reuse the multimodal
  answer interface to caption text-poor slides, but that should be described as implemented only
  after the document flow exists.

The LLM is not the locator authority. Page, slide, and timestamp values originate in indexed
payloads, retrieval selects them, and answer generation may only cite that retrieved set. The
search layer also applies confidence gates and strips references to citation numbers that were
not supplied.

### Application and deployment

**FastAPI** serves the public UI, existing JSON search API, administrative registration routes,
and the assignment's SSE `/ask_stream` compatibility endpoint. Bearer authentication protects
mutating/admin operations, while `user_id` scopes Postgres rows, object keys, and Qdrant filters.
The API should validate and record work, not import PDF/ML-heavy dependencies or perform parsing
inside the request.

**Docker Compose** starts more than just API and worker: the local topology contains a warm CLIP
service, a one-shot sample-seeding gate, the API, and the Prefect-serving worker. These containers
share configuration but durable production state remains in Neon, Qdrant Cloud, Prefect Cloud,
and the configured object store. The host maps port `8100` to the API container's internal port
`8000`.

**Fly.io** deploys the same Docker image with different commands. The current configuration has
separate `api`, `worker`, and `clip` process groups plus a release-time seed command. This lets the
API use small, auto-stopping machines, scales workers with ingestion load, and keeps the CLIP model
warm independently. One image simplifies releases without forcing all processes to share the same
runtime or scaling policy.

### The organizing principle, tested rather than assumed

The fast path reads Postgres metadata and Qdrant vectors, optionally calls the answer LLM, and
streams a response. The slow path downloads media, parses files, creates chunks/frames, calls
models, and writes indexes. Queueing and separate processes prevent the slow path from directly
blocking the API, but they do not automatically eliminate shared CPU, network, provider quotas, or
database bottlenecks. `benchmark/bench.py` must therefore measure search latency while a real
backfill is still active. The architecture creates the isolation boundary; the benchmark proves
that the chosen capacities preserve it.

## Why this architecture

The final system is an extension of the existing video application, not a replacement for it.
The API, manifest, dispatcher, Prefect orchestration, workers, object storage, Qdrant retrieval,
and UI remain in place. Papers and decks join those same paths so the existing video behavior is
preserved while retrieval becomes cross-source.

### Neon provides the database function we need

"Serverless" and "full backend" describe different things:

- **Serverless** describes how infrastructure is operated and billed. Compute can scale with
  demand and suspend while idle without us administering a database server.
- **Full backend** describes the breadth of bundled product features, such as authentication,
  file storage, realtime subscriptions, generated APIs, and edge functions.

Neon is primarily managed serverless Postgres. Supabase is a broader backend platform built
around Postgres. We chose Neon because this application already owns its FastAPI API, uses a
simple Bearer-auth contract, and has a separate object-storage abstraction. We need reliable
transactional Postgres for the source manifest, not another bundled application backend.

In everyday terms, choosing Supabase here would be like renting an entire furnished office suite
when we already have desks, meeting rooms, and reception and only need a records room. Supabase is
not worse; its additional services are simply not the missing part of this particular system.
Neon gives us the Postgres records room without asking us to redesign the rest of the building.

This keeps the relational layer focused on work it is good at:

- one durable row per registered source;
- source ownership and metadata;
- lifecycle transitions such as `pending`, `parsing`, `embedding`, and `indexed`;
- fair, atomic admission of pending work; and
- recovery queries for stale inflight work.

The tradeoff is that authentication, files, orchestration, and vector search remain separate
services. That is acceptable here because those boundaries already exist in the provided
application and each service has a specific responsibility.

### Postgres is the manifest; Qdrant is the retrieval index

Neon answers structured questions such as "which sources exist?", "who owns this source?", and
"what stage is it in?" Qdrant answers similarity questions such as "which indexed chunks best
match this query?"

Keeping these responsibilities separate gives us:

- transactional lifecycle updates in Postgres;
- vector search and payload filtering in Qdrant;
- small manifest rows instead of storing large embeddings in operational tables; and
- independent scaling of ingestion metadata and retrieval traffic.

Qdrant was also already part of the working video system. Reusing it protects the existing video
path and avoids an unnecessary migration to `pgvector` while adding document ingestion.

The simple mental model is: Postgres remembers **facts about the job**; Qdrant remembers **the
meaning of the content**. A source can exist in Postgres before it has any searchable vectors in
Qdrant, which is why the lifecycle status matters.

### One logical hybrid index uses two physical collections

The product exposes one cross-source retrieval experience, but the current embedding branches
have incompatible vector spaces and dimensions:

- the visual collection stores CLIP embeddings for video frames;
- the shared text collection stores video transcript chunks and will also store paper and deck
  chunks.

All three source kinds therefore meet in the shared text collection, while the video-only visual
collection remains an auxiliary branch. Retrieval searches the applicable branches and fuses the
results before answer generation. "One index" in the product architecture means one unified,
filtered retrieval surface—not that incompatible vectors are forced into one unnamed Qdrant
vector configuration.

An analogy is keeping two card catalogs that use different numbering systems—one for images and
one for text—behind a single librarian. The visitor asks one question; the librarian consults
both catalogs and combines the results. The visitor experiences one search even though the
specialized catalogs remain separate internally.

### Ingestion is asynchronous because its workload is different from search

Search is read-only and latency-sensitive. Ingestion is bursty and performs slow operations such
as downloading, PDF parsing, frame sampling, captioning, chunking, embedding, and vector upserts.
Running those operations inside `POST /admin/documents` would make API latency depend on document
size and could starve searches during a backfill.

The API therefore validates the request, inserts a `pending` manifest row, and returns `202`.
Workers perform the expensive stages separately. This makes the API fast and allows worker
capacity to scale without scaling the public web process.

This resembles a restaurant host accepting an order and handing it to the kitchen. The host
should not leave the front desk to cook a large meal while every other guest waits. HTTP `202`
is the claim ticket showing that the kitchen accepted the work.

### The queue has two layers with different jobs

Postgres and Prefect are not interchangeable queue implementations:

- The Postgres WFQ dispatcher owns admission control and fairness. Pending rows remain in the
  manifest until capacity is available, and atomic claims prevent two dispatchers from owning the
  same admission.
- Prefect orchestrates admitted runs, task retries, execution, and the operational dashboard.
  It is not treated as a durable broker with an assumed visibility timeout or automatic
  redelivery guarantee.

This split prevents one user or large backfill from monopolizing Prefect's run queue while still
retaining Prefect's useful flow and task observability.

The dispatcher is the person controlling entry to a fair line. Prefect is the supervisor inside
the workshop. Asking Prefect alone to provide fairness would admit every job into its waiting room
in arrival order, allowing one user who submits fifty documents to stand ahead of everyone else.

### Prefect turns a multi-step job into an observable workflow

**Prefect is a workflow orchestration tool**: it manages the *when* and *how* of running
multi-step jobs, especially jobs with dependencies, retries, and failure handling. It is the layer
that turns "run this sequence of steps reliably, even when something goes wrong" into a workflow
we can observe and control instead of hand-rolling all of that coordination.

In MomentSearch, Prefect contributes the following:

- **Receiving admitted work.** `POST /api/videos` and `POST /admin/documents` first register a
  `pending` source in Postgres and return `202` without doing ingestion. With fair dispatch
  enabled, the Postgres dispatcher later admits the source and calls `run_deployment(timeout=0)`
  to create a Prefect flow run. With fair dispatch disabled, the API creates that run immediately.
  Either way, parsing and embedding stay outside the request path.
- **Holding and assigning flow runs.** Prefect holds an admitted run until a serving worker has
  capacity to execute it. This is one queueing layer, but not the whole queue architecture:
  Postgres owns the fair waiting line and admission limits; Prefect owns execution after admission.
- **Running the pipeline as discrete tasks.** Ingestion is represented as a flow containing stages
  such as fetch, parse or sample, chunk, enrich, embed, and index. Prefect records task and flow
  states and understands their execution order.
- **Retrying configured task failures.** Tasks explicitly decorated with retry settings can retry
  an in-process exception, such as a transient download or embedding failure. This is narrower
  than saying Prefect always resumes every failed pipeline: only configured tasks receive those
  retries, and a hard-killed worker is a different failure mode.
- **Providing observability.** The Prefect dashboard exposes flow runs, current and terminal
  states, task failures, and retry history. That makes a backfill inspectable instead of leaving us
  to infer progress from worker logs.

Prefect is therefore much more than "cron with extra steps." Cron starts a command on a schedule
and largely forgets about it. Workflow orchestrators such as Prefect, Airflow, Dagster, and
Temporal model dependencies and execution state. Their exact durability and retry semantics
differ, so the product must still verify what its chosen configuration actually guarantees.

For this project, **resumability and no data loss are not delegated to Prefect alone**. The crash
experiment showed that a hard-killed worker can leave an admitted source stranded even after
Prefect eventually marks the old run `Crashed`. MomentSearch therefore owns recovery through:

- durable parsed/chunk checkpoints in object storage;
- deterministic Qdrant point IDs so repeated upserts are safe;
- Qdrant-upsert-before-Postgres-`indexed` ordering; and
- a staleness reconciler that returns abandoned inflight rows to `pending` for fair readmission.

This distinction matters for `benchmark/bench.py --resilience`: Prefect tracks and exposes the
run, while application-owned checkpoints and stale-work recovery provide the tested no-loss
behavior and prevent finished expensive stages from being repeated.

Prefect also does not store the product's source data or vectors. Neon Postgres remains the
authority for source ownership and lifecycle state, object storage holds durable intermediate
artifacts, and Qdrant holds searchable vectors. Prefect is the supervisor that coordinates the
work which produces that state without making the latency-critical search path wait for it.

### One manifest table lets every source inherit scheduling behavior

Videos, papers, and decks use the same manifest table with a `kind` discriminator. This avoids a
parallel document scheduling system: fairness, inflight limits, status listing, retries, and the
future staleness sweep can operate across all source kinds consistently.

Separate tables would make each shared concern require a union or duplicate implementation and
would increase the chance that documents and videos behave differently under load or failure.

Think of the `kind` column as the item-type field in one delivery ledger. We do not need separate
ledgers for boxes, envelopes, and tubes when they all use the same drivers, capacity rules, and
delivery statuses.

### Papers and decks share one parameterized ingestion path

A PDF paper and a PDF slide deck have the same broad stages: fetch, parse pages, create
locator-aware chunks, embed, and upsert. Their main semantic difference is the locator name
(`page` versus `slide`) and the deck fallback that captions visually meaningful, text-poor
slides.

One parameterized flow keeps checkpointing, retry behavior, security limits, and terminal status
handling consistent. It also makes fixes apply to both kinds instead of allowing two nearly
identical pipelines to drift.

This is the software version of one recipe with a parameter that says whether to label each sheet
as a page or a slide. Two copied recipes would gradually acquire different fixes and mistakes.

### Object storage makes checkpoints durable across disposable workers

Workers are intentionally replaceable. Parsed pages and chunks must therefore be committed to
object storage before their stage is considered complete. A replacement worker can detect those
artifacts and resume from the next stage rather than repeating expensive or externally visible
work.

Postgres records lifecycle state; object storage records durable stage outputs; Qdrant records
searchable vectors. Setting `indexed` only after a successful Qdrant upsert keeps those three
forms of state in a safe order.

A checkpoint is like saving a game after completing a level. If the console loses power during
the next level, the player resumes from the saved boundary instead of starting the entire game
again. The save must live outside the worker because the worker itself may disappear.

### Recovery belongs to the application

The crash experiment showed that restarting the worker and Prefect eventually labeling a run
`Crashed` do not guarantee redelivery. The application must detect stale inflight manifest rows,
return them to `pending`, and safely run them again using durable checkpoints and deterministic
Qdrant point IDs.

This makes the resilience claim depend on code we can test rather than undocumented or
workspace-specific orchestration behavior.

Restarting a worker is like reopening a factory after a power cut. The building may be open, but
an order abandoned halfway through production will remain abandoned unless someone finds it and
puts it back into the line. The staleness reconciler is that person.

### Separate Fly process groups match separate scaling pressures

The API, ingestion worker, and CLIP embedding service use the same image but run as separate Fly
process groups:

- API capacity scales with request traffic;
- worker capacity scales with ingestion throughput; and
- the CLIP service keeps one expensive model warm and can move to larger CPU or GPU capacity
  independently.

Separating them prevents model memory and ingestion CPU work from competing directly with the
public API. It also means internal service discovery, including `CLIP_SERVICE_URL`, must always
use the actual Fly app and process-group names.

Using one container image still keeps deployment simple: all process groups ship the same code
package. Fly starts that package with a different command and machine size for each responsibility.
This gives independent scaling without maintaining three unrelated build pipelines.

## Concurrent database initialization needs coordination

`CREATE TABLE IF NOT EXISTS` is idempotent after a table exists, but it does not make two
simultaneous first-time table creations race-free. PostgreSQL can still encounter conflicts in
its system catalogs, as demonstrated by the duplicate `pg_type_typname_nsp_index` entry for
`ms_videos`.

Application processes should not independently execute migration DDL without coordination. For
this project, `init_schema()` should acquire a transaction-scoped PostgreSQL advisory lock before
executing the schema and migrations. The lock and DDL must run in the same transaction so a crash
automatically releases the lock.

Container restart policies can conceal this class of bug: a later startup may succeed once the
first process finishes creating the table. A successful restart is not evidence that startup is
race-safe.

In plain English, two builders were told to create the same named structure at the same time.
`IF NOT EXISTS` helps when the second builder arrives after the first has finished; it does not
fully protect the moment when both builders begin together. The advisory lock is a single key to
the construction area: one process performs the database setup while the others wait, then the
key is automatically returned when the transaction finishes or crashes.

## Process restart is not job recovery

Docker restarting the worker restores the polling process, but it does not repair the durable
business state of an ingest that was killed mid-stage. A row left in `queued`, `fetching`,
`sampling`, or `embedding` remains counted as inflight and is not selected by a dispatcher that
only claims `pending` rows.

Prefect eventually classified the interrupted run as `Crashed`, but it did not automatically
resume or redeliver it. The replacement run began before the original was marked crashed and had
a different run ID. Therefore the planned staleness reconciler is the primary recovery mechanism,
not merely a backstop around Prefect.

The process and the job are separate things. Restarting the worker restores a person who can do
work; it does not automatically tell that person which half-finished order was left on the floor.
Postgres must retain enough state for the reconciler to rediscover that order.

## A crash experiment and a resilience proof are different

The Section A2 experiment answered a discovery question: what does the current orchestration
stack do after a worker disappears? Either outcome was acceptable at this stage.

The final resilience requirement is stronger. It must demonstrate that:

- no accepted source is dropped;
- stale work is detected and redelivered;
- the source eventually reaches a terminal successful state;
- committed stages are not repeated; and
- redelivery does not duplicate indexed data.

The current `benchmark/bench.py --resilience` path is still a stub, so the experiment must not be
reported as the final resilience gate passing.

The difference is similar to dropping one package to see what the courier does versus running a
formal delivery audit that proves every package arrived, no package was duplicated, and completed
journey segments were not repeated. A useful experiment can reveal behavior without satisfying
the stronger proof.

## Fly private DNS is tied to the actual app name

For a Fly process group, the internal hostname follows:

`<process-group>.process.<app-name>.internal`

Changing or auto-generating the Fly app name without changing `CLIP_SERVICE_URL` points API and
worker processes at a nonexistent private-DNS name. The `app` value and internal service URLs
must be reviewed as one configuration unit.

`fly launch` can also rewrite existing configuration and deployment workflow files. Always review
the complete Git diff after running it, including process groups, VM sizing, restart policies,
release commands, and the GitHub branch that triggers deployment.

Private DNS is Fly's internal phone book. The process group is the department name and the app
name is the company name. If the company is renamed but the phone-book entry still uses the old
company, calls to the CLIP department cannot be routed even though that department is healthy.

## Evidence must survive a throwaway environment

A successful-looking command is not durable proof of a deployed product. Before destroying a
smoke-test app, preserve:

- the public URL;
- a screenshot of the UI;
- a successful question and answer;
- relevant process health or logs; and
- the app/configuration name used for the test.

If nobody personally observed the behavior and no evidence remains, record the criterion as
unverified and repeat it later rather than upgrading it to a pass.

This is the engineering equivalent of keeping a receipt. "I ran the command" records an action;
a captured URL, response, screenshot, and timestamp demonstrate the outcome. A temporary system
can be deleted, but the proof should survive it.

## Handle configuration values without re-parsing `.env`

Ad hoc shell parsing of `.env` is fragile for quoting and special characters such as `&`.
Prefer the configuration parser already used by the runtime, such as reading a value from a
running Docker Compose service. Secret values printed this way still need to be kept out of shell
transcripts, screenshots, logs, and committed files.

An `.env` file looks like simple text, but quotes, spaces, ampersands, and other characters have
special meaning to different shells and parsers. Reimplementing its parser with `grep`, `cut`, or
`source` can silently change a value. Letting Docker Compose parse the file keeps configuration
behavior consistent, although displaying a secret is still a disclosure risk.

## "Never in the request path" means never calling the scheduler, not just never parsing

The first pass at `POST /admin/documents` satisfied the *literal* text of "no PDF library imported
in the request path" while still violating the *intent* of "ingestion is asynchronous, always":
under `ENABLE_FAIR_DISPATCH=false` it called `jobs.enqueue_document()` — a real network round-trip
to Prefect Cloud — directly from the request handler. `timeout=0` only says "don't wait for the
flow to finish," not "don't wait for Prefect to acknowledge the scheduling call." A cold or
degraded Prefect connection could still blow the `documents_async` latency gate.

The fix was to make the dispatcher the *only* path to Prefect for every source kind, with no
exceptions carved out for any configuration flag. A non-negotiable stated as a boundary ("ingestion
never triggers from the request path") should be read as a boundary around every network call the
request path makes, not just the specific operation the negotiable happened to name as an example.

This is the difference between a bouncer checking IDs at the door and a bouncer who also pats
everyone down for weapons. "No admission without ID" that only checks IDs still lets weapons in if
that was never the literal rule someone wrote down — the actual intent was "nothing dangerous gets
in," and the specific check named was just the most obvious instance of it.

## Removing a fallback can create a new failure mode if you don't trace what depended on it

Deleting the direct-enqueue fallback (previous lesson) was correct, but it surfaced two bugs in
sequence, not one:

1. The dispatcher thread had a hidden dependency on `ENABLE_FAIR_DISPATCH` being true to even
   *start*. Once documents no longer had a fallback, that flag being false meant "nothing ever
   picks this up" — a silent, permanent dead end, worse than the slow-request risk it replaced.
2. After fixing (1) by always starting the dispatcher, the *video* path's own still-surviving
   direct-enqueue fallback (kept for FIFO-mode teaching purposes) started racing the now-always-on
   dispatcher for the same row, because that fallback never updated the row's status away from
   `pending` before or after its own direct Prefect call.

Neither bug was visible from reading either changed file in isolation. Each was only visible by
asking "what code elsewhere in the system currently depends on this flag/thread/status meaning
what it used to mean?" — a question worth asking explicitly any time a fix touches something
another code path silently relies on, not just the path being fixed.

Pulling one thread on a sweater and only checking the hole you made, not what else was holding
together because of that thread, is how a small repair turns into two more repairs.

## A URL is not portable just because it is well-formed

The deck's registration URI (`http://localhost:8100/corpus/...`, later `http://api:8000/...`)
looked like an ordinary, valid HTTP URL at every step — Pydantic validated it, the scheme regex
accepted it, `curl` from the host machine fetched it fine. None of that caught that it was the
*wrong host* for the actual reader (the worker container, in its own network namespace, where
`localhost` means the worker itself, not the api container). The bug was only visible by asking
specifically "who is going to fetch this, and from where," and then testing that fetch from inside
that exact context — not from the host machine, which has a different view of `localhost` entirely.

The same shape of problem recurs across environments: a URL that resolves correctly in
docker-compose (`api:8000`, compose's internal DNS) will not resolve on Fly (which needs
`<process>.process.<app>.internal` or a public hostname), and a URL that resolves locally
(`localhost:8100`) will not resolve inside any container at all. "It fetched successfully when I
tested it" only proves the URL works from wherever *you* tested it, not from wherever the system
will actually use it at runtime.

## A committed fixture is still committed — read the non-negotiables literally before rationalizing an exception

Reasoning "this PDF is authored project content, not user-uploaded media, so the blanket gitignore
rule wasn't really meant for it" felt sound in the moment, and produced a working, reproducible
fix. It was still wrong, because the assignment's own non-negotiables document said the plainer,
more literal thing: "media/PDF artifacts are git-ignored," with no carve-out for authorship.

The lesson isn't "don't use engineering judgment" — it's that engineering judgment should be
applied to *how* to satisfy an explicit rule (regenerate from a tracked source at boot, rather
than commit the binary), not to *whether* the rule applies to your specific case. A rule stated as
a non-negotiable, in a document literally titled "Non-negotiables," is not the place to look for
an implicit exception — if the fixture genuinely needs to survive a fresh clone, the constraint is
"find a way that doesn't commit the binary," not "decide the binary doesn't count as the kind of
artifact the rule meant."

## Each round of independent review found something the previous round's fix didn't cover

Four consecutive rounds of second-opinion review on the same two blocks (B and C) each surfaced at
least one real, previously-unnoticed issue — including issues introduced *by* the previous round's
fix (the dispatcher-never-starts bug came directly from fixing the Prefect-in-request-path bug; the
video double-enqueue race came directly from fixing the dispatcher-never-starts bug). This wasn't
review noise or diminishing returns — each finding was concrete, reproducible, and would have
manifested under real conditions (a specific config flag, a specific network topology, a specific
timing window), not edge cases invented to pad a report.

The practical implication: after a fix, the right question is not "does this specific finding go
away" but "what does this fix change about the system's other behavior, and has that been tested
under the condition that would expose it" — for the video-enqueue race, that meant actually running
two competing dispatcher instances against real Prefect Cloud and checking the run count, not
reasoning about probabilities from a desk. Reasoning ("this race window is sub-millisecond, so it's
safe") was tried first and was wrong; only live testing under the actual adversarial condition
(two dispatchers, same pending row, real network latency) gave a trustworthy answer.

## A working single-flow pattern doesn't automatically generalize to multiple flows

`ingest_video.serve(name="ingest", limit=limit)` had worked, unmodified, since Section A. Adding a
second flow by switching to the module-level `serve(video.to_deployment(...),
document.to_deployment(...))` seemed like the obvious, minimal change — same underlying Prefect
primitives, just two of them instead of one. It broke *both* flows, including the one that had
never been touched, with a relative-import error at flow-run time.

The two call shapes are not equivalent even though the docs describe `.serve()` as a convenience
wrapper around the same machinery: `to_deployment()`'s default `entrypoint_type` serializes the
flow's location as a bare file path, and whatever code path actually loads that entrypoint at
flow-run time treats a file path differently from however `.serve()`'s own internal bookkeeping
had been resolving it — enough to change whether the flow's module is loaded as a real package
member (relative imports work) or executed as a standalone script (they don't). The fix
(`entrypoint_type=EntrypointType.MODULE_PATH`) was one keyword argument, but finding it required
reading the installed library's actual source rather than assuming "this is basically the same
call, just called twice."

The general lesson: when a framework offers two API surfaces that produce superficially similar
results (one flow served vs. many, one deployment vs. several), don't assume the multi-item form
is just a loop over the single-item form's internals. Verify the specific mechanism — here, by
reading `import_object()`'s actual branching logic in the installed `prefect` package — rather than
extrapolating from "it's the same library, it should work the same way."

## An error message can be real and still point at the wrong layer

The browser UI's "Missing or invalid bearer token" error is the literal, correct response from the
server's auth check — but the natural next question ("why is auth rejecting me?") leads toward the
token's *value* (wrong token? expired? typo?) when the actual defect was that the UI never sent a
token *at all*. The fix wasn't about the token being wrong; it was that an entire code path
(`Authorization` headers on mutating `fetch()` calls) had simply never been written in the first
place.

Confirming this took reading the actual client code (`ui/index.html`'s `fetch()` calls) rather than
reasoning from the error message alone, and checking `git log` on that file to establish it predated
every change made this session — the fix needed to add a missing capability (a token input +
header-attachment helper), not debug an existing one. An error string names the check that failed;
it doesn't always name the reason the check had nothing to check against.

## Security hardening: scope the exception to the specific need, not the general category it falls under

The document fetcher's SSRF fix went through two passes, and the first pass's own reasoning is
worth keeping precisely because it was a real, live-tested improvement that still wasn't tight
enough. Pass one: block loopback/link-local/metadata but allow all of RFC1918, because the deck is
deliberately self-hosted at a docker-compose-internal address (`http://api:8000/...`) which lives
in that private space — a blanket "block all private ranges" would have broken that legitimate,
intended fetch. True, and verified both directions (dangerous addresses rejected, the deck's own
fetch still worked). But the fix generalized the exception to the wrong *category*: "this app needs
ONE specific private host, therefore allow the entire private-address space" is a much bigger
exception than the actual need. An admin-token holder could use that same permissiveness to probe
any *other* service on the private network — internal databases, dashboards, other containers —
none of which this app has any legitimate reason to reach.

Pass two fixed the actual mismatch: block every private/loopback/link-local/reserved address by
default, and allowlist the *specific hostname* (`api`) the app actually needs — not the address
range it happens to fall in. The lesson generalizes past SSRF: when a legitimate need requires
punching a hole in a security boundary, scope the hole to the exact thing that needs to get
through (one hostname), not the general category that thing belongs to (an entire private /8
block) — the category is almost always bigger than the need, and the gap between them is exactly
the residual exposure. Re-verifying after tightening still meant testing both directions again:
confirm the now-broader block list actually rejects what it should (10.x, 172.16.x, 192.168.x, not
just loopback/metadata), and confirm the one allowlisted case still works end-to-end.

A related, easy-to-miss gap in the SAME fix: validating a hostname's resolved IP and then handing
the ORIGINAL HOSTNAME (not the validated IP) to the HTTP client for the actual connection leaves a
DNS-rebinding window — the client re-resolves the hostname at connect time, which could return a
different address than what was just validated. Closing it required pinning the actual TCP
connection to the specific IP already validated (while still presenting the correct hostname for
TLS SNI/certificate checks) — validating a value and then not actually using that same validated
value for the operation it was validated for is a subtle way for a check to become decorative.

## A "reset to pending on re-registration" default silently assumes registration only ever happens once per lifecycle

`upsert_pending`'s original behavior — always reset status to `pending` on conflict — was correct
for the common case (registering something new, or retrying something that already finished) and
wrong for a case nobody had designed for: re-registering something that was *currently running*.
Nothing about the function signature or its callers made that distinction visible; the bug only
showed up as two Prefect run IDs processing the same document simultaneously in a live log, not as
a code-review-visible defect.

The fix (preserve status when already in-flight, reset only from terminal-ish states) is a
one-line change in principle, but finding it required noticing that "re-registration" is not one
event with one correct response — it's at least three different situations (new source, retry a
finished/failed one, accidental duplicate of a running one) that had been handled with a single
unconditional code path. The same gap existed in a second, unrelated call site (the video retry
endpoint, which sets status directly rather than through `upsert_pending`) — a reminder that when a
race is rooted in "an operation doesn't check current state before overwriting it," every call site
that performs that same kind of overwrite needs the same audit, not just the one where the race was
first observed.

## A narrow mitigation and the full architectural fix are different deliverables — say which one you're doing

Closing the re-registration path stopped the *observed* trigger of the duplicate-run race, but a
deeper version of the same problem remains open: a dispatcher retry after a timed-out enqueue, a
queued-but-delayed run starting late, or a future staleness sweep resetting a row while the
original run is still alive — none of which go through `upsert_pending` at all, so that fix doesn't
touch them. The complete answer is a generation/lease token stamped at admission time and checked
by every status write before it's allowed to apply — real, understood, and NOT built here.

The choice made instead: a narrow, one-line guard (a `failed` write can't overwrite an already
`indexed` row) that closes the single most damaging *consequence* of the race — a late failure
clobbering a real success — without closing the race itself. That's a legitimate, honest thing to
ship, but only if it's labeled as what it is. The risk in a moment like this isn't picking the
smaller fix; it's letting the smaller fix quietly stand in for the bigger one in memory or
documentation. Writing "narrow stale-write guard, not a full fencing mechanism" directly in the
function's own docstring — not just in a commit message or a chat reply — is what keeps that
distinction from evaporating the next time someone (human or agent) reads the code without this
conversation's context and reasonably assumes a guard against overwriting means the race is solved.

## Guarding the last transition isn't the same as guarding the state

The first version of the stale-write guard blocked exactly the transition that had actually been
observed racing: `indexed -> failed`. It felt complete because it was tested against the real bug.
It wasn't complete, because a stale run doesn't necessarily fail *immediately* — its real lifecycle
walks through every intermediate status (`parsing`, `embedding`) before it ever reaches `failed`,
and the FIRST of those intermediate writes already clobbers `indexed`. By the time the stale run's
own `failed` write happens, the guard's condition (`current status is indexed`) is no longer true —
the stale run itself made it false a few statements earlier. A guard written against "the last thing
that goes wrong" instead of "the state that must be protected" has a hole exactly where the failure
mode has more than one step.

The fix was to invert the framing: instead of listing the specific bad transition to block
(`indexed -> failed`), protect the state itself (once `indexed`, nothing but a deliberate reset
applies) — every write attempt is refused, not just the one shaped like the observed bug. Verifying
this required literally replaying the multi-step sequence (`indexed`, then `parsing`, then
`embedding`, then `failed`, as separate calls) rather than testing the single transition that had
been fixed — because a fix that's tested only against the exact reproduction that motivated it will
reliably pass that one test while leaving the general case, which produced it, still open.
