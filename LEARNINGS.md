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

### The queue prepares sources; it does not answer searches

There are two very different paths through the application:

```text
INGESTION (slow, queued)                 SEARCH (fast, never queued)

register source                          ask question
      |                                        |
      v                                        v
Postgres: pending                        embed the question
      |                                        |
      v                                        v
fair dispatcher                          search Qdrant immediately
      |                                        |
      v                                        v
Prefect flow -> worker                   fuse and rank evidence
      |                                        |
      v                                        v
parse/embed/index                        cited answer
```

The queue exists because preparing a source can take seconds or minutes. A video may need to be
downloaded, sampled, transcribed, and embedded. A paper or deck may need to be downloaded, parsed,
chunked, visually captioned, and embedded. Doing that inside an API request would make the browser
wait, consume API capacity, and make failures difficult to resume. Registration therefore returns
`202 Accepted` quickly and leaves the expensive work to Prefect-served workers.

Search is different. It only searches content that has already reached `indexed`. The question is
embedded, Qdrant is queried, the results are fused, and the cited answer is returned immediately.
Putting searches behind the ingestion queue would let a large upload backlog delay normal users,
violating the requirement that search stay responsive during backfills.

### How each source becomes searchable

All source types use the same admission machinery, but their workers produce different evidence:

| Source | Worker processing | Searchable representation | Locator |
|---|---|---|---|
| Video | Download, sample representative frames, obtain captions when available, chunk and embed | Frames in the visual collection; transcript chunks in the shared text collection | Timestamp |
| PDF paper | Validate and parse the PDF page-by-page, create page-aware chunks, embed text | Chunks in the shared text collection | Page |
| PDF slide deck | Extract each slide's text; visually caption text-poor slides; embed the resulting chunks | Chunks/captions in the shared text collection | Slide |

At query time, the visual video branch and the shared text branch are searched independently and
then fused. A single answer can therefore cite a video at `05:32`, a paper on page 5, and a deck on
slide 4. Every search applies the user's tenant filter before results are returned.

Native `.ppt`/`.pptx` parsing is not required by the current plan. The reliable presentation path
is to export the presentation to PDF first; native PowerPoint support remains optional. Calling a
PDF a `deck` changes its locator semantics from page to slide and enables the text-poor-slide
captioning path.

The four queue participants have separate responsibilities:

- **Postgres** records `pending` work and owns lifecycle state.
- **The dispatcher** chooses which source enters next and applies either fair round-robin or FIFO
  ordering. It is always the only component that submits ingestion runs to Prefect.
- **Prefect** coordinates and exposes the admitted multi-stage run.
- **The worker** performs the actual download, parsing, chunking, embedding, and indexing.

This separation is why videos, papers, and decks can share one queue without making interactive
search wait behind them.

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
2. The Postgres dispatcher admits pending rows only when an inflight slot is available. With fair
   dispatch enabled it rotates across users; with fair dispatch disabled it uses FIFO ordering.
   In both modes, the dispatcher remains the only path that submits a run to Prefect.
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

Block H replaced the original `benchmark/bench.py --resilience` stub with a real worker-kill
workflow. Independent review then found an equally important second lesson: replacing a stub with
executable code does not automatically make the result trustworthy. The first implementation
could report success even when no tracked source was active and both Docker commands failed,
because it checked only whether the documents were `indexed` at the end. The resilience gate must
therefore prove its preconditions and intervention as well as its final state: work was genuinely
in flight, the worker was actually killed, it was successfully restarted, abandoned work was
readmitted, and an already-committed stage was reused rather than repeated.

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

## A benchmark is production code for claims

Block H made the benchmark executable, but the guardrail review showed that measurement code needs
the same adversarial testing as application code. A benchmark is not just a stopwatch wrapped
around the product. It is a small program that selects a cohort, establishes preconditions,
performs an intervention, observes results, handles failures, and decides pass or fail. A defect in
any of those steps can make a broken system look healthy.

The most useful test for a benchmark is often not the happy path. It is to deliberately replace
its dependencies with bad outcomes and ask whether it still turns green. For example, the first
Block H resilience implementation was tested with documents that were already indexed and mocked
Docker commands that both returned failure. It still returned `no_loss=True`. That reproduction
was valuable because it proved the problem was in the benchmark's decision logic, independent of
Docker, Prefect, Postgres, or timing noise.

In plain English: a smoke alarm is not trustworthy merely because it makes a noise when its test
button is pressed. We also need to know it does not announce “all clear” when its sensor is
disconnected. Benchmarks need tests for false passes, not only demonstrations that they can pass.

## A decoupling benchmark must prove that the two workloads actually overlapped

The purpose of the decoupling ratio is to answer a specific question: does search remain fast
*while ingestion is actively consuming resources*? Measuring search before a backfill and again
after the backfill has already drained does not answer that question, even if the script labels the
second number “during.”

The first Block H implementation improved on the original idle-versus-idle stub by launching a real
backfill. However, if that backfill finished before all search samples were collected, the script
only printed a warning and still allowed the ratio to pass. It also treated `pending` rows as
evidence of overlap. A pending row is merely waiting in line; it does not prove a worker is parsing,
chunking, embedding, or writing to Qdrant.

A trustworthy measurement makes overlap a validity condition, not an informational message. It
should observe active worker-owned statuses throughout a meaningful portion of the sampling window
and invalidate the result if that condition is not met. The workload should be sized dynamically
from a pilot measurement or extended until it comfortably outlasts the search sample. Otherwise a
fast-draining workload can make the system appear isolated simply because there was little or no
contention to observe.

Layman analogy: measuring traffic before roadworks begin and after the crew has packed up cannot
tell us whether the road stayed usable during construction. Seeing unopened work orders in the
office does not prove construction was happening either; we need to observe crews actively working
while traffic is measured.

### A required benchmark threshold must not be lowerable from the command line

The hardened Block H benchmark defined sufficient overlap as at least half of its observations
finding real ingest work active. That is what `min_overlap_frac=0.5` means: if the benchmark polls
the system 20 times while measuring search, at least 10 polls must see a document actively being
parsed, chunked, or embedded. This does not say that half the documents must be running; it says
that ingestion must genuinely overlap at least half of the measurement window.

The first version exposed that value through `--min-overlap-frac`. This looked like a useful tuning
option, but it also allowed a command such as `--min-overlap-frac 0`. With a required fraction of
zero, even 0 active observations out of 20 satisfies the mathematical comparison (`0 >= 0`). The
script could then report a passing decoupling result without observing ingestion and search running
together at all — recreating the exact false pass the overlap check was introduced to prevent.

This is different from choosing a larger benchmark workload or raising the threshold for a stricter
test. A user may safely make a gate harder, but a required grading invariant must have a fixed floor
that cannot be weakened by ordinary runtime configuration. The safe designs are therefore either:

- remove the command-line option and keep the required `0.5` value inside the benchmark; or
- retain the option for stricter experiments, but reject every value below `0.5` (and values above
  `1.0`) during argument validation.

In plain English: if an exam requires 50% to pass, a command-line switch must not let the person
taking it redefine passing as 0%. Configuration is useful for changing the size of the experiment;
it must not be a back door for changing what success means.

## Isolation means shared capacity is quiet, not merely that IDs are separate

Giving every benchmark phase its own user and tracking only the IDs that phase created is good
cohort isolation. It prevents a resilience check from mistaking another user's failed document for
one of its own. But separate identifiers do not create separate CPUs, worker slots, database
connections, or dispatcher capacity.

This mattered for the 30 deliberately broken `example.com/probe_N.pdf` acceptance probes. Block H
gave them a separate benchmark user, which fixed per-user FIFO starvation, but it immediately began
the “idle” search baseline without waiting for those probes to reach `failed`. The probes could
still occupy the globally shared ingest capacity. That makes the supposedly idle baseline busy and
can also steal capacity from the throughput backfill that follows.

Each benchmark phase must therefore leave the shared system in a known state before the next phase
starts. For the poison probes, that means waiting for the exact accepted ID set and requiring every
one to reach the expected terminal state, `failed`, before measuring idle search. Namespace
isolation answers “whose rows are these?”; quiescence answers “what else is using the machine?” We
need both.

## Failed measurements belong in the denominator

Latency calculated only from successful requests can look excellent while the service is mostly
broken. The first Block H implementation discarded non-202 registrations and removed failed or
timed-out search requests before calculating p95. In the extreme case reproduced during review,
one successful 10 ms search plus 39 failed searches became a reported 10 ms p95. The fast number
was mathematically correct for the one retained sample and operationally meaningless for the test
that was attempted.

The same rule applies to throughput. If 20 documents are requested but only five register, counting
the five successful documents can produce an attractive chunks-per-second number while silently
shrinking the workload. A benchmark must retain the attempted cohort, require the expected sample
count, report missing/failed outcomes, and gate the error percentage against
`error_rate_max_pct`. Throughput should not pass unless all guaranteed-valid benchmark documents
reach the expected successful terminal state.

This is a general observability lesson: removing errors from a dataset does not remove failure from
the system. It only removes evidence of failure from the report.

## A resilience result needs four kinds of evidence

A final `indexed` status is necessary but insufficient proof of crash recovery. A rigorous
resilience check establishes four separate facts:

1. **Precondition:** at least one tracked source was actively executing when the experiment began.
2. **Intervention:** the intended worker process was successfully killed, and its replacement was
   successfully started.
3. **Outcome:** every accepted, valid source eventually reached `indexed`; none was failed, stuck,
   duplicated, or dropped.
4. **Resume behavior:** logs or checkpoint state show that a stage committed before the crash was
   reused instead of rerun.

Checking only the fourth-stage destination (“all rows are indexed now”) cannot distinguish recovery
from a no-op experiment where everything completed before the kill, the kill command failed, or a
new run repeated all work from the beginning. The benchmark should fail closed when it cannot prove
one of these facts. In other words, “nothing was lost” is meaningful only after proving that
something was genuinely interrupted.

## Metric names are contracts: recall@10 requires ten observable results

The first recall implementation examined `citations[:10]`, but `/ask_stream` returned the
application's configured default of six citations (`TOP_K=6`). Taking the first ten items from a
six-item response does not turn it into recall@10. In this case the mistake was conservative—it
could create a false failure, not inflate recall—but the reported metric still did not match its
name or the SLA.

The requested retrieval depth must be explicit at the API boundary used by the benchmark. If the
metric is recall@10, the system must expose ten ranked candidates for that measurement, and the
benchmark must score those ten. A label is not a transformation; calling a six-result measurement
“@10” does not change what was observed.

## "Scale worker=2" is a config number; the bottleneck it's supposed to fix might not be where the plan assumed

Block N's plan text guessed the throughput gap (1.39 vs 8 chunks/s, a 5.8x gap) "likely wants
`--scale worker=2`" — at most a 2x lever. Scaling to 2 replicas alone got 3.19 chunks/s: real, but
nowhere near enough, and a guess about the mechanism would have stopped there satisfied with
"it improved." Measuring instead (Prefect Cloud exposes `created`/`start_time`/`end_time` on every
flow run via its own client) showed each ingest flow run spent 3-12s sitting between "created" and
"started" — several times longer than the ~3s of actual parse/chunk/embed work for a small
document. That gap was Prefect's `serve()` runner polling Cloud for new work only every
`PREFECT_RUNNER_POLL_FREQUENCY` seconds (default 10), completely independent of replica count or
`WORKER_CONCURRENCY`. No amount of scaling fixes a bottleneck that isn't the one you scaled.

The fix was a one-line `query_seconds=` kwarg to `serve()`. The lesson isn't "always check Prefect's
poll interval" — it's that a plausible-sounding mechanism ("just needs another worker") is still a
guess until something that actually timestamps the pipeline confirms which stage the wall-clock
time is going to. The orchestrator you're already paying for usually already has that instrumentation
built in; reach for it before adding new logging or reasoning from wall-clock deltas.

## Fixing the bottleneck you found can surface the next one — and it can be worse

Fixing the Prefect poll-interval gap directly caused a NEW failure: raising concurrency to actually
use the now-fast pickup pushed several ingest tasks to open Qdrant connections at once, and some
hit `ResponseHandlingException(ConnectTimeout(...))` — a TLS handshake timeout. Each ingest flow
run is an isolated subprocess (by Prefect's own design, for crash isolation — see Block G), so each
one builds a brand-new `QdrantClient` with no connection to reuse; N concurrent flow runs is N
simultaneous cold handshakes, not N requests on a warm pool. Left alone, that exception fell into
Prefect's OWN task-level retry (`retries=2, retry_delay_seconds=60`), turning one transient timeout
into ~120s of dead time — which is worse than the original poll-interval gap it replaced, per flow
run affected.

Two counts of resilience are not the same thing. Prefect's task retry exists to survive a truly
dead dependency, at a cost (60s×2 here) sized for that case. A transient connect timeout under a
burst of concurrent cold handshakes is a different, cheaper-to-recover failure mode, and paying the
expensive backstop's cost for it every time is itself a performance bug. The fix was a short local
retry (a few seconds, exponential) around the specific upsert call, narrowly scoped to the
exception type that actually indicates "couldn't connect," with Prefect's retry left in place
underneath as the genuine backstop for a Qdrant that's actually down. The general shape: when a
fix surfaces a new failure, ask whether an existing broad safety net (a retry, a timeout, a circuit
breaker) is now firing on a narrower, cheaper case than the one it was sized for, before assuming
the new failure needs new infrastructure.

## "18 cores available" doesn't mean 18 cores of usable concurrency if each process assumes it owns all of them

Even after both fixes above, pushing concurrency further to close the remaining throughput gap
made the DECOUPLING gate — not throughput — get worse, non-monotonically (the same config measured
ratio 1.55, then 3.85, then 2.47 across repeated runs; the noise itself was a clue that something
was thrashing rather than cleanly bottlenecked). The API's own single query-embedding call
(fastembed/ONNX, the transcript branch's `embed_query`) shares that code path with ingest's
`embed_docs` call — same model, same library — and ONNX Runtime's default is to grab every visible
core for intra-op parallelism WITHIN one process. This machine reports 18 cores to Docker, which
looks like plenty of headroom; it is not, once N concurrent ingest subprocesses (Prefect isolates
each flow run into its own process — the same property that caused the Qdrant issue above) each try
to claim all 18 for themselves. That's oversubscription/thrashing, not a lack of cores, and it
directly slowed the one embedding call search's own latency depends on.

The fix (a `TEXT_EMBED_THREADS` cap, applied to the worker service only, left uncapped for the API)
is asymmetric on purpose: the API only ever runs one such call at a time and wants fastembed's own
larger default for low single-call latency; the worker runs many at once and wants each to leave
room for the others. A single knob shared between a low-concurrency, latency-sensitive caller and a
high-concurrency, throughput-sensitive caller will always be wrong for at least one of them — split
it per-caller instead of picking a compromise value that serves neither well (an earlier attempt at
one shared `threads=2` for both made the API's own call slower and the ratio measurably worse, not
better).

## When a benchmark result is genuinely noisy, more tuning passes stop being informative

After the three fixes above, sweeping `DISPATCH_MAX_INFLIGHT`/`WORKER_CONCURRENCY` across
cap 6/8/9/10 on this one laptop produced a real, reproducible tension (throughput needs enough
concurrency that it measurably slows search — Block I's SLA gates are pulling in genuinely opposite
directions on shared CPU) but ALSO real noise (identical configs producing different decoupling
ratios run to run). At that point, another sweep is not more evidence about the system; it's a
sample of the host machine's momentary load, which this benchmark script cannot control for. The
honest move — the same one Block H already established for the original 1.39 chunks/s and 25%
overlap failures — is to record a representative, reproducible number, document the real trade-off
and its likely cause, and flag it for re-measurement in an environment that doesn't collapse the
API and every worker replica onto one shared CPU pool (Block J's actual Fly deployment, where they
can get separate machines), rather than keep spending time chasing a moving target locally.

## "The knob you changed" and "the code path that actually runs" can be two different things

The `TEXT_EMBED_THREADS` fix above was written, tested for syntax, and deployed with real conviction
— and it was inert. It was set as an environment variable on the `worker` service's container, on
the theory that ingest's flow-run subprocesses were the ones calling `embeddings.embed_docs_local`
(the function that reads it). A guardrail review checked one assumption behind that theory:
`embed_docs()` — the function ingest actually calls — is a small dispatcher (`src/rag/embeddings.py`)
that branches on `CLIP_SERVICE_URL`. With it set (the docker-compose default, and it IS set here),
`embed_docs()` sends an HTTP request to the `clip` service and returns; `embed_docs_local` never
runs in the worker process at all. The env var was real, the code that reads it was real, the two
just never met at runtime.

The review didn't stop at "this specific setting is misplaced" — it asked the next question: if
`embed_docs_local` runs in the `clip` service, not the worker, what ELSE about that process matters
that the original diagnosis missed? The answer was bigger than the misplaced env var: a single
`threading.Lock()` in `embeddings.py` guarded FOUR different embedding calls — CLIP image embeds,
CLIP text embeds, BGE document embeds, BGE query embeds — all funneling through the one warm `clip`
process regardless of which caller (api search, or worker ingest) needed them. Ingest's bulk
document-embedding calls and search's own single query embed were serializing behind each other on
a lock that had nothing to do with either being CPU-bound — a strictly bigger effect than the
thread-oversubscription theory the original fix targeted, and the actual best explanation for why
the decoupling ratio kept degrading under concurrency.

The generalizable point: verifying that a fix's CODE is correct (it compiles, the logic is sound,
the config is read somewhere) is not the same as verifying it sits on the path that actually
executes for the scenario being fixed. When a fix targets "component X is slow because of Y,"
confirm which process/branch/service Y actually runs in before wiring the mitigation to a
plausible-looking but untested location — especially in an architecture with more than one place
the same function could execute (local in-process vs. a remote service behind an env-var switch,
here; a similar shape exists anywhere a codebase has both a fast path and a fallback path for the
same operation).

## Fixing a real, confirmed bug doesn't guarantee the metric it explains moves

Splitting the CLIP/BGE locks and sub-batching `embed_docs_local` (releasing the BGE lock between
sub-batches so a large document's embed call can't monopolize it) are both correct, independently
verified changes — verified by a mutation test (reverting the lock split back to one shared lock
makes the new `test_clip_and_text_locks_are_distinct_objects` fail), not just "it compiles and looks
right." Fixing the dispatcher's count-and-claim race (`db.claim_pending`, a Postgres advisory lock
serializing the whole sequence across every replica) is likewise a real correctness fix — the old
code could genuinely admit more sources than `DISPATCH_MAX_INFLIGHT` when two dispatchers ticked at
the same moment, undermining the exact capacity sizing Block N/I depend on.

None of that guaranteed the decoupling ratio would improve, and on this laptop it didn't — three
post-fix runs at the same config (`WORKER_CONCURRENCY=4`, cap=8) measured ratios of 1.92, 1.98, and
(at a lower cap=6) 1.38, no better than before the fixes and still short of the 1.3 target. A fix
being correct, confirmed by a reviewer, AND covered by a test that would catch its regression is
still not the same claim as "this fix closes the gap in the metric that motivated it." The honest
report distinguishes those two claims rather than assuming the second follows from the first: ship
the correctness fix on its own merits (it prevents a real bug — a lock unnecessarily serializing
unrelated work, an admission cap that could be silently exceeded), and separately, keep measuring
whether the ORIGINAL symptom (the failing SLA gate) actually moved. Here it mostly didn't, which
means the remaining gap is general resource contention across many concurrent processes sharing one
laptop's CPU/network/Postgres-connection-pool — not one single lock or race — and that's the
honest thing to write down, not "fixed, should be better now."

## ...but "didn't move the metric" is a reason to look harder, not a reason to stop

The section above was written after the lock-split/sub-batching fix, and it was the honest state at
that point. A second guardrail review pushed one question further: the benchmark's synthetic
documents produce roughly nine chunks each. `TEXT_EMBED_BATCH` was set to 32. Sub-batching a
9-chunk call at a batch size of 32 sub-batches into exactly one batch — the code was correct and
the test for it passed, but it had zero opportunity to do the thing it was written to do (release
the lock mid-call so a waiting query embed could interleave) for this specific workload. The lock
split was still worth keeping (CLIP and BGE are unrelated models; there was no reason for one to
block the other), but it was solving a smaller problem than the one causing the failing gate.

The actual fix followed directly from the same diagnosis, taken one step further: if BGE ingestion
and BGE search queries share a lock in the SAME process, no batch size fixes that — they're
fully serialized either way, just with a shorter or longer wait. The fix was to stop putting them
in the same process at all. `embed_query()` now always runs the bge model locally in the API,
never through the shared `clip` service, even when `CLIP_SERVICE_URL` is set — search's one small
call and ingestion's bulk calls now have separate model instances and separate locks by
construction, not by tuning a shared one. Measured result: the decoupling ratio passed on 3 of the
next 4 runs (1.09, 1.03, 0.89 against a 1.3 target; the fourth failed only the overlap-validity
floor, with a ratio of 1.14 that would itself have passed) — a repeatable result across different
concurrency settings, not a lucky sample.

The generalizable point: "this correct fix didn't move the metric" is itself informative — it
usually means the mental model of WHERE the bottleneck lives is still one level too shallow, not
that the bottleneck is unfixable. The lock split fixed "CLIP and BGE shouldn't share a lock." The
real bottleneck was "two DIFFERENT WORKLOADS (bulk throughput-oriented ingestion vs. single
latency-critical search) shouldn't share a lock, regardless of which model either uses" — a
question about workload separation, not model separation. Sub-batching was an attempt to make
contention shorter; the actual fix was to make contention impossible. When a targeted fix doesn't
move the number it was meant to move, the next move is to ask what's one level ABOVE the fix just
made — not to conclude the metric is stuck and move on.

## Local source code, the Docker image, and the browser are three different versions of the application

Editing a file in the repository does not automatically change the application already running at
`localhost:8100`. Docker Compose builds a snapshot of the repository into an image and starts a
container from that snapshot. This project's Compose file mounts `./data` into the containers, but
does **not** mount `./src` or `./ui`. That is intentional: the containers behave more like the
eventual deployment, but it also means a source edit remains invisible until the affected services
are rebuilt and recreated.

In plain English, imagine editing the recipe after a cake has already been baked. The recipe on the
counter is newer, but the cake still reflects the old recipe. Refreshing the browser only asks for
another slice of the existing cake; it does not rebake it. A hard refresh therefore cannot load a
Python or HTML change that never entered the running Docker image. After an implementation change,
rebuild the stack (while preserving the required two-worker shape) with:

```bash
docker compose up -d --build --scale worker=2
```

When the UI appears stale, compare the running container with the checkout instead of assuming a
browser-cache problem. Useful evidence includes the container creation/image time, whether a newly
added file exists under `/app`, and checksums or identifying strings from the host and container
copies. In the Block M check, the running API still contained the old “Only PDF uploads are
accepted” branch and did not contain the new `src/ingest/detect.py`, proving that the container was
stale rather than the browser.

There is a second, independent UI distinction. `/` is the curated, read-only sample experience. It
deliberately hides the upload controls and the entire documents panel. `/get-started` switches the
same HTML page into full mode and reveals those controls. Being on the sample route can therefore
look like a missing feature even when the feature is already present in the running image.

Finally, backend capability and visible UI support are separate deliverables. Teaching the API and
worker to validate, store, convert, and ingest a PPTX does not change an HTML file picker that still
accepts only `application/pdf,.pdf`, nor does it change client-side wording such as “Choose a PDF
first.” A complete vertical feature must be checked through every layer:

1. The browser lets the user select the new file type and describes it accurately.
2. The API validates and registers the upload.
3. The queue admits it and a worker processes it.
4. Durable checkpoints prevent repeated work after a restart.
5. Search returns the result with the correct page or slide citation.

Rebuilding solves “new code is not running.” Updating and testing every layer solves “the new
capability is not exposed end to end.” They are different problems and should be verified
separately.

## The right storage key for re-processing isn't automatically the right key for viewing

Block M's `t_parse` deliberately leaves an uploaded `.pptx`'s `storage_key` pointing at the
ORIGINAL bytes — converting to PDF at parse time rather than upload time means the upload path
stays a dumb, fast, unopinionated byte store, and a re-parse (say, after `_PARSE_VERSION` bumps)
always has the true source material to work from, not a lossy derivative. That's the right call for
INGESTION. It's the wrong call for VIEWING: `src/rag/search.py`'s `_doc_url` and
`src/api/search.py`'s `/api/document/{id}` both serve citations straight from `storage_key`, so a
PPTX-sourced deck's citation would hand the browser raw `.pptx` bytes — mislabeled
`application/pdf` in the local-dev route, unlabeled in the presigned-URL route — either way, not
something an `<iframe>` can render. Nothing in that code path was WRONG for a PDF-sourced document;
it simply predated a second document kind whose "the thing we store" and "the thing a citation
should open" are no longer the same file.

This wasn't caught by the unit tests (they mocked storage and never asked "what does a browser
actually receive"), and it wasn't something the plan's own exit criteria ("slide-numbered citations
indistinguishable from a hand-exported PDF") would fail on paper — search and indexing worked
perfectly; only clicking through to VIEW a PPTX-sourced citation would have been broken. It surfaced
because live end-to-end verification included actually fetching `/api/document/{id}` and checking
what `file` reported the bytes were, not just checking that `/api/ask` returned the right slide
number. The general shape: whenever a feature introduces a new "the stored artifact and the
served/rendered artifact can differ" case, grep for every OTHER reader of that same stored key, not
just the one reader the feature's own code path touches — a field named `storage_key` looks like a
single, uniform contract until one caller needs the original and another needs a derivative of it.

The fix (`src/ingest/detect.py`'s `viewer_storage_key`) needed to be reachable from BOTH viewing
routes without importing `src/ingest/document.py` — which imports `prefect` at module scope, a
dependency this codebase deliberately keeps out of the API process (see the CLIP-service work:
"processes in remote mode never drag in torch," same principle). That's why the checkpoint-key
naming (`PARSE_VERSION`, `converted_pdf_key`) lives in the same tiny, dependency-light module as the
magic-byte sniffing, rather than back in `document.py` where the writer of that checkpoint lives —
the reader and the writer need to agree on the key FORMAT without the reader inheriting the writer's
entire (heavy) module.

### Do expensive existence checks when producing an artifact, not every time it is viewed

The first citation fix asked object storage, “does the converted PDF exist?” each time a PPTX
citation was rendered. In local development that is a cheap disk check. In production it is a
network `HEAD` request to S3/GCS, repeated for every cited document in every answer. The worker
already knows the exact moment the conversion has succeeded, so the better design is to record the
converted PDF's key in the Postgres manifest (`view_storage_key`) once. Search then makes a simple
choice from data it already fetched: use the browser-friendly derivative when present, otherwise
use the original. In lay terms: write “the viewable copy is in this drawer” on the catalogue card
when filing it, instead of walking to the storeroom to check the drawer whenever someone searches.

### An input-size limit does not automatically limit generated output

A PPTX under the 50 MB upload cap can expand into a much larger PDF during LibreOffice conversion.
Checking `len(pdf)` after `read_bytes()` is too late: the worker has already allocated memory for
the entire file. The safe boundary is metadata first—filesystem `stat()` for fresh output and
object-storage `HEAD` for a cached derivative—then read only if it is within
`DOCUMENT_MAX_CONVERTED_MB`. Slide count is a separate dimension, so
`DOCUMENT_MAX_CONVERTED_PAGES` remains necessary even with the byte cap.

Malformed converted PDFs are also permanent for a fixed input and converter version. Page-count
inspection now translates parser errors into `PermanentDocumentError`, avoiding retries that wait
30 and 120 seconds only to inspect the same corrupt bytes again.

## A second review round on the same feature found four MORE real issues — not noise, a different depth of scrutiny

The first guardrail review on Block M passed cleanly. A second round, asked to look harder, found:
a security-relevant validation check that was a substring search dressed up as structural
validation (and a zip-bomb vector in the same function); a helper whose early-return guard silently
assumed the ONE caller it was written for was the ONLY caller, breaking a second real code path
(URL-registered PPTX) it hadn't been tested against; a UI default that would silently produce the
wrong citation semantics for the common case (no kind selected) unless the server independently
enforced the invariant; and a piece of THIS SESSION'S OWN documentation overclaiming what a test
had actually proven (a "mid-conversion crash" test that had only exercised "crash after conversion
completes").

None of these were found by re-running the existing test suite — it still passed, because the
tests exercised what the code was WRITTEN to do, not what a determined adversary or an unconsidered
second caller could do to it. Two generalizable patterns:

1. **A structural check needs to validate structure, not the presence of a recognizable string.**
   `b"presentationml.presentation" in content_types` LOOKS like it's checking the same thing real
   XML parsing would — and passes every legitimate file — but a substring search can be satisfied by
   bytes that were never meant to assert what the check is trusting them to assert. The tell was in
   the code itself: an `in` check over raw bytes is always weaker than parsing the format and
   checking a specific field, and the gap between them is exactly where a crafted, malicious input
   lives. If a check is verifying trust (an upload could be malicious), prefer "parse it and check
   the specific thing" over "does this byte sequence appear somewhere."
2. **A helper's early-return guard encodes an assumption about ITS CALLER, not about the world** —
   `if storage_key is None: return None` was correct for the ONE call site that existed when it was
   written (uploads), and silently wrong the moment a second call site (URL registration) had a
   legitimately different reason for `storage_key` to be `None`. The function's docstring even said
   "shared by search.py's presigned path and api/search.py's local-dev fallback" — plural callers,
   stated explicitly — but the guard itself was written for the single case that had actually been
   tested. When a helper is DESIGNED to be shared, an early return that special-cases "the input
   looks like case A" needs to be checked against every OTHER case the function claims to support,
   not just the one that motivated writing it.

On the documentation-overclaim specifically: writing "verified live, not just asserted" and then
describing a test that verifies a WEAKER claim than the one being made is a failure mode worth
naming on its own — it reads as more rigorous than it was, to a future reader (including a future
instance of me) who trusts the "verified live" framing without re-deriving what was actually run.
The fix wasn't just correcting the sentence; it was going back and running the STRONGER test (an
actual `SIGKILL` of the `soffice` process mid-conversion) to find out what the real guarantee is,
which turned out to be narrower and differently-shaped than either the plan's original wording or
this document's first draft claimed — fail-safe and idempotent-on-retry, not resume-mid-conversion.
That distinction only surfaces if you're willing to test the claim as literally stated, not the
adjacent, easier-to-test claim that resembles it.

## Closing a substring-search hole doesn't close every hole in the same family — check for the equality check being weakened, too

The second review round (above) fixed `sniff_document_kind`'s PPTX check from a byte-substring
search to real XML parsing against `_PRESENTATION_CONTENT_TYPE`. A THIRD, independent review found
the replacement still wasn't exact: the comparison stripped `.main+xml` off the constant and used
`startswith()` against what remained, so a crafted `[Content_Types].xml` declaring
`...presentationml.presentation.evil` still passed — a different, narrower substring-style hole
inside what looked like the fully-fixed version. Same underlying flaw, one layer down: "the value
starts with what I expect" is still weaker than "the value IS what I expect," the same gap between
"the string appears somewhere" and "the field maps correctly" that the SECOND round's fix was
written to close. The generalizable check: after fixing a validation function from loose-match to
structural parsing, re-read the comparison operator itself — `startswith`/`in`/`re.match` (unanchored)
are all still weaker than `==`/`fullmatch`, and a fix that upgrades WHAT gets compared without also
upgrading HOW it's compared can leave the same class of bug one level deeper, invisible to a test
suite that only re-runs the cases the previous round already found.

## A confidence threshold calibrated on questions doesn't automatically cover keywords

A user's single-word query ("leadership") against a genuinely on-topic, correctly-indexed PDF was
gated to an abstain — `TEXT_CONFIDENCE_THRESHOLD` (0.72) is a real, working number, calibrated by
`benchmark/calibrate_thresholds.py` against this project's own labeled positive/negative query set.
But every labeled positive in that set is a full natural-language question ("what does the survey
say about hybrid retrieval"); nothing in the calibration set exercises a bare keyword. Checked live
rather than just lowering the number: two GIBBERISH negative queries scored HIGHER on bge cosine
similarity than the real "leadership" query — short, low-information text (real or nonsense) scores
in an overlapping range on dense embeddings, so no single threshold value can separate a genuine
short query from noise on that signal alone. The number wasn't wrong; the SIGNAL was insufficient
for a query shape the calibration set never covered. The fix that actually generalizes needs a
second, independent signal for the case dense similarity structurally can't resolve (here, an exact
lexical AND-match against already-retrieved candidates) — not a lower number, which just moves
which false positives get through instead of removing them. Lesson: a confidence gate's calibration
is only as good as the query SHAPES in its labeled set, not just their topics — a calibration set
built entirely from one query style (full questions) tells you nothing about how the same threshold
behaves on a structurally different style (keywords) until you actually test that shape.

Building the fix surfaced a second version of the same discipline: an ANY-word-matches first draft
of the lexical fallback fixed the reported case but silently RE-broke 3 of the 10 already-calibrated
negative queries (coincidental single-word overlaps with unrelated corpus content) — caught only
because the fix was re-verified against the SAME full calibration set the original threshold was
checked against, not just the one query it was built to fix. A gate-loosening fix is exactly as
capable of reintroducing false positives as a gate-tightening fix is of reintroducing false
negatives — both need the full before/after comparison, not a spot-check of the motivating case.

## A fusion boost that rewards two signals "agreeing" needs both signals to be independently trustworthy, not just present

Cross-modal fusion (`_fuse()`'s `CROSS_MODAL_BOOST`) ranks a video moment higher when a frame hit
and a transcript hit land within the same time window — two independent signals pointing at the
same instant being stronger evidence than either alone. That reasoning only holds if both hits are
actually independent EVIDENCE, not just independently PRESENT. YouTube's auto-captions emit literal
`[Music]`/`[Applause]` tags during non-speech stretches; nothing filtered those before they were
chunked and embedded, so a near-meaningless filler chunk could land in the same time window as an
unrelated frame purely by coincidence, and the boost treated their coincidental proximity as if it
were corroboration. The bug wasn't in the fusion math — rewarding cross-modal agreement is the
right idea — it was upstream, in what was allowed to count as a "signal" in the first place. The
generalizable point: a fusion/ensemble step that combines multiple weak signals into a stronger one
is only as sound as its weakest input's SEMANTIC validity, not just its presence in the candidate
list — filtering degenerate/non-content inputs at the SOURCE (here, transcript ingestion) is a more
robust fix than trying to make the combination logic defensive against every way an individual
input could be meaningless.

**Follow-up, same day: source-filtering only closes the instance, not the class.** The above fix
removed `[Music]`/`[Applause]` filler from the text branch, which closed THAT specific bug — but
`_fuse()`'s boost logic itself still had no confidence check, so any OTHER weak-but-real text hit
(not filler, just genuinely tangential to the query) paired with a generic frame reproduced the
identical failure mode. It surfaced again the same day on different queries ("trust", "leadership")
against different videos. Source-side filtering is the right fix for a specific, enumerable class of
garbage input (bracket tags have a clean, unambiguous signature); it is NOT a substitute for making
the combination logic itself defensive when the failure mode is general (any weak signal, not a
specific known-bad pattern) — eventually the combination logic needs its own confidence floor
regardless of how clean the inputs are upstream. Second fix: gate the boost directly on the paired
hit's own raw score (`CROSS_MODAL_TEXT_MIN` in `src/config.py`), not on filtering what can become an
input in the first place. The confidence check must qualify the whole combination, not only its
explicit multiplier: summing two RRF terms already guarantees a paired window outranks any
single-branch document result. Below the floor, retain only the strongest branch score. Also
notable: only ONE of the two signals needed a confidence floor here,
not both — CLIP's visual similarity on this corpus doesn't discriminate by relevance at all (a
~0.23-0.33 band regardless of query), so gating on the frame's score too would have added noise, not
signal. When adding a confidence check to a multi-signal fusion step, check whether each signal is
actually discriminative on your data before assuming symmetric treatment is correct.

## The resilience and error-rate bug, explained without jargon

This section tells the same story as the two technical entries that follow it, but assumes no
technical background. It uses the library analogy from the top of this document — front desk,
master ledger, fair line manager, job supervisor, processing team, warehouse, card catalog — so if
any of those terms are unfamiliar, that section explains them first.

### The setup nobody had thought all the way through

This library isn't one building. It's two: a small local branch (the laptop this was developed on)
and a permanent public branch (the copy running on Fly.io that anyone can visit at its website).
That's normal and intentional — most real systems run a "development" copy and a "production" copy
side by side.

But these two branches were built to **share things** on purpose. They share the same master
ledger (one database, recording every item the library owns and how far along its processing is).
They share the same job supervisor (one service that hands out work and tracks who's doing what).
That sharing is a real feature: if you register a new paper or video at the local branch, someone
visiting the public branch's website can immediately search for it too, because both branches are
reading from the same ledger and the same catalog.

What the two branches do **not** share is their warehouse. The local branch keeps its raw uploaded
files on the laptop's own hard drive. The public branch keeps its raw uploaded files in a completely
separate cloud storage locker, because a laptop's hard drive isn't something the public internet can
reach. Two branches, two separate warehouses, physically nothing in common — but one shared ledger
and one shared job supervisor sitting on top of both of them.

Nobody had written down a rule for what should happen when a job shows up on the shared to-do list:
"whichever branch's processing team is free first gets to grab it." That sounds reasonable until you
notice the missing question — free to grab it, sure, but is that team's warehouse the one that
actually has the item's physical materials in it?

### The test that exposed it

To prove the system is trustworthy, one of the checks is: what happens if a member of the
processing team collapses mid-task — the process running their work gets forcibly killed, as if
someone tripped over a power cord? A well-built system should notice, hand the abandoned job to
someone else, and pick it back up roughly where it left off, not lose it and not restart it wastefully
from page one.

Running that test kept failing. Some jobs came back finished, but a few came back with an error that,
translated out of its technical wording, says roughly: **"we went to our warehouse to get this item's
materials, and they weren't there."** That is a scary-sounding error — it's the kind of message you'd
expect to see if a file had genuinely been deleted or corrupted.

Except the materials hadn't been deleted. Someone checked, by hand, right after a failure: walked
into the warehouse that had supposedly lost the file, and asked for it directly. It was there, intact,
handed over immediately. So the error message was, in a real sense, *lying by implication* — it
sounded like "your file is gone" when what had actually happened was "the wrong team looked in the
wrong warehouse."

### Finding out which team actually did the work

Here is where the two shared systems mentioned earlier — the shared job supervisor — became useful
for solving the mystery, not just for running the library. Every job the job supervisor hands out
gets a paper trail: a full record of exactly which step of the work ran, in order, right up until
the moment it failed. That record is kept centrally, so it can be pulled up regardless of which
branch's team actually did the work.

Pulling up the paper trail for one of the failed jobs answered the question directly, instead of by
guesswork: the very last thing recorded, right before the failure, was a step that — by design — only
ever runs when a processing team is about to reach into the *cloud* warehouse, never the *local* one.
That's the smoking gun. It proves, in black and white, that the public branch's processing team had
picked up a job for a file that only ever existed in the local branch's warehouse. Of course they
came back empty-handed. They were never going to find it — they were looking in the wrong building
entirely.

(An earlier attempt to explain this same failure had checked something adjacent — "were all the
processing teams we personally started for this experiment pointed at the local warehouse?" — and
concluded yes, so this couldn't be the shared-warehouse problem. That check was too narrow: it only
counted the teams deliberately started *for the experiment*. It didn't account for the fact that the
public branch's own processing team is *always* running in the background, day and night, whether or
not anyone is thinking about it during a local test. The paper trail settled it more directly than
that earlier reasoning could.)

### Why this also explains the "25% of jobs failed" problem

A separate-looking number from the same round of testing — one in four newly-registered documents
either erroring out or getting permanently stuck — turned out to be the exact same story wearing a
different hat. Those documents weren't victims of a parsing bug or a broken chunk of processing
logic. They were simply being grabbed by whichever branch's team happened to be free first, and when
that happened to be the wrong branch, the job could never have been completed no matter how well the
rest of the code worked — it wasn't a matter of trying harder, the required materials were physically
unreachable from where that team was standing. Once the underlying mistake was fixed, this number
dropped to zero on the very next real test, without anyone touching the actual document-processing
code at all — further proof it had never been a processing bug in the first place.

### The fix, in plain terms

Two changes were needed, not one, because there were two separate moments where the wrong team could
end up grabbing a job:

1. **When a brand-new job is first handed out**, each branch's job supervisor now only offers work to
   its own processing team, by name — the local branch's supervisor only calls the local team, the
   public branch's supervisor only calls the public team.
2. **When an abandoned job gets reset and put back on the to-do list** (the recovery step that runs
   after a team member "collapses" mid-task), the to-do list itself now records which warehouse each
   item's materials actually live in, and every team — local or public — was taught to skip over any
   item on the list that isn't tagged for their own warehouse, even if they'd otherwise be free to
   take it.

The first fix alone was tried first and looked sufficient, but wasn't — it only closed off the first
moment. The second moment (an abandoned job being reset and re-offered to whichever team happens to
ask next) was still wide open, and testing found that out rather than assuming the first fix had
covered everything.

### How we know it's actually fixed, not just declared fixed

The same "collapse a team member mid-task" experiment was run again afterward, against the corrected
system, and this time all ten test documents came back fully processed, with the paper trail
explicitly confirming that the interrupted ones picked up from where they'd left off rather than
starting over. The one-in-four failure rate from before measured at zero on that same re-run. Neither
result was assumed from reading the fix's description — both were re-created and watched happen,
because a bug this subtle (a scary-looking "file is lost" error that was really a "wrong team showed
up" error) is exactly the kind of thing that's easy to mark "fixed" on paper without it actually being
fixed everywhere the mistake could still happen.

### The general lesson, for anyone building something similar

Whenever two systems are deliberately set up to share one waiting list or one ledger, but do **not**
share everything sitting behind that ledger, "who's allowed to see a job" and "who's allowed to *do*
a job" are two different questions — and only the first one gets answered for free just by sharing
the list. The second one has to be designed on purpose, checked at every point a job can be handed
out (not just the obvious first one), and re-tested by actually reproducing the failure, not just by
reasoning about whether the fix should have worked.

## A shared queue across environments needs environment affinity, not just a shared schema

Local dev and the Fly deployment were designed to share state on purpose — one Neon manifest, one
Prefect Cloud workspace — so that registering a source in either place and searching from either
place works against the same index. That design goal quietly implied a second one that was never
stated: a dispatcher in either environment must never be ALLOWED to claim a row whose bytes it
can't reach. Nothing enforced that. `flow.serve()`'s runner has no concept of "which environment am
I," so the Fly worker and the local worker were both eligible to win the claim for literally any
pending row, including one uploaded to local disk that Fly's Tigris-backed storage provider could
never see. The failure mode (`NoSuchKey` from a process whose OWN `STORAGE_PROVIDER` was `local`)
pointed at `storage.py`'s S3 branch, which made it look like a storage-layer bug; the actual defect
was upstream, in the admission layer that let the wrong environment's worker pick up the row at
all.

The fix had to close TWO separate admission points, not one — a lesson from finding the first patch
insufficient, not from planning it correctly the first time. Suffixing deployment names with
`FLY_APP_NAME` (`ingest-local` vs. `ingest-<fly-app>`) stopped a NEWLY dispatched run from being
picked up cross-environment, but did nothing about the dispatcher's own claim query, which was still
happily admitting old rows under the orphaned unsuffixed name for whichever environment's worker
polled first. Only adding a `storage_env` column — checked at the SAME point every other admission
invariant is checked, `db.claim_pending`'s query — closed the actual race, because claiming and
scheduling are two different moments a wrong-environment assignment can happen at, and a fix aimed
at only one of them leaves the other exploitable. The general shape: when two independently-scaling
deployments share one coordination layer (a queue, a manifest, a workspace) but do NOT share every
resource behind it, every place that layer hands out work — not just the most visible one — needs
to know which resources the recipient can actually reach.

A second-order finding from the same rollout: fixing the collision meant naming deployments
per-environment, which silently orphaned the old shared-name deployments — invisible until Prefect
Cloud's free-tier hard cap (5 deployments per workspace, previously undocumented anywhere in this
project) rejected the new registration with a `403` that a bare `except Exception: sleep 15;
retry` loop swallowed into infinite, silent retries. Combined with Python's default block-buffered
stdout (no `PYTHONUNBUFFERED` in the Dockerfile) making `fly logs` look empty rather than erroring,
the worker looked "up but doing nothing" for longer than it should have. A fix that changes how many
distinct named resources a system registers is worth checking against any hard cap the platform
places on that resource count — the cap doesn't announce itself until you cross it, and by then the
failure mode it produces may not resemble the cap at all.

## The same error string from two different runs is not evidence of the same bug

A collision-mechanism fix (above) and Block K's own recorded `--resilience` FAIL both produced the
identical error text: a raw boto3 `NoSuchKey ... GetObject ...` traceback that appears nowhere in
this repo's source. It would have been easy — and wrong — to report the collision fix as having
resolved the resilience regression, since the symptom matched exactly. Checking one field before
making that claim closed the question: Block K's isolated `--resilience` run had
`STORAGE_PROVIDER=local` for EVERY container involved, including the one that failed. There was no
second environment in that experiment at all, so the cross-environment claim race the fix targets
could not have been the mechanism — the failure has to come from something else (the eval's own
leading suspect: a race between the app's staleness reconciler and Prefect Cloud's own post-kill
task retry, gated by `RECONCILE_STALE_S`).

The generalizable check: an error string names WHERE something went wrong (here, an S3 GetObject
call), not WHY the code reached that state. Two runs producing the same exception type from the
same library call are only evidence of the same root cause if the PRECONDITIONS that could produce
that exception are actually the same between them — here, "is more than one environment's worker in
play at all" is exactly the precondition that differed, and checking it directly (one grep for the
run's `STORAGE_PROVIDER` value) was cheaper and more conclusive than reasoning from the matching
traceback. Claiming a fix resolved a regression it merely resembles is a worse outcome than not
fixing the regression yet, because it closes the investigation on a still-open bug.

**Correction, found later — the elimination check above was itself wrong, for the reason the entry
warns about.** "`STORAGE_PROVIDER=local` for every container involved" only checked the containers
this test explicitly spun up (the local docker-compose stack). It did not — could not, from that
vantage point — see that the Fly deployment was independently alive against the SAME shared Prefect
Cloud workspace and Postgres manifest, and therefore was also "involved" whether or not this
experiment intended it to be. Pulling the actual Prefect Cloud run for the failing doc_id
(`doc_9e69d7212490`) settled it directly: the traceback shows `storage.py`'s S3 branch
(`_s3().get_object(...)`) executing, which is only reachable when the executing process's OWN
`STORAGE_PROVIDER` is not `"local"` — meaning a Fly worker, not a local one, ran this task. Block K's
`--resilience` FAIL and the collision fix WERE the same bug; see the next entry for the confirmed
root cause and re-verification.

The lesson this adds, not replaces: "check the precondition directly instead of trusting the
matching traceback" is correct, but a precondition check is only as good as its own scope — "every
container involved" quietly meant "every container I set up for this test," which is a narrower
claim than it reads as when two environments share a live coordination layer. When a system is
DESIGNED to have two independent deployments sharing one queue/database (as this one is), a
same-environment assumption needs to be verified against the shared resource itself (here: which
process actually ran the task, per its own traceback) — not against the guest list of what one
side deliberately started.

## The actual root cause of the resilience and error-rate failures: a cross-environment queue collision, not a checkpoint bug

Both numbers `PRODUCT_EVAL.md` originally reported as failing — `no_loss_under_crash: False` and a
25% error rate on a 20-document backfill — traced to the same single mechanism, confirmed via the
live Prefect Cloud run logs rather than inferred: local dev and the Fly deployment share one Prefect
Cloud workspace and one Neon Postgres manifest (by design, so a source registered in either place is
searchable from both), but have incompatible storage backends — local disk vs. Fly's Tigris/S3
bucket. Until commit `a2623de`, nothing enforced that a dispatcher could only claim rows whose bytes
its OWN environment could reach: `db.claim_pending()`'s admission query and Prefect's deployment
names were both environment-agnostic, so a stale row (reset to `pending` by the reconciler after a
local worker crash) was just as claimable by the Fly worker as by the local one. When Fly won that
race, its worker tried to fetch a locally-uploaded document's bytes from its own Tigris bucket,
which had never received them, and raised a genuine `botocore.errorfactory.NoSuchKey` — not because
any bytes were lost (a manual `storage.get_bytes()` call against the local container, after the
fact, returned the file immediately), but because the wrong environment's worker was executing the
task at all. The same mechanism explains the error rate: documents that "failed" or sat stuck
weren't hitting a real ingest defect, they were being claimed and orphaned by an environment that
could never have completed them.

The fix closed both admission points this needed (deployment names suffixed by
`config.DEPLOYMENT_ENV` so a dispatcher's own workers execute what it schedules, plus a `storage_env`
column filtered into the claim query so a dispatcher can never claim a row pinned to another
environment's storage) — and was re-verified live in a later session, not just trusted from the
commit message: a fresh `--resilience` run came back 10/10 indexed, 0 failed, checkpoint-resume
explicitly confirmed from the worker logs, and the same backfill's error rate measured 0.0%, down
from 25%. The generalizable lesson, distinct from the entry above: when a bug's proposed root cause
and its proposed fix are described in a commit message, "verify the fix landed" and "verify the fix
was pushed to every environment the bug could occur in" are separate checks — a local-only
re-verification would have looked identical whether or not the same collision could still happen on
a stale Fly deployment that hadn't picked up the fix yet.

## An eval metric can be blind to exactly the regression it should catch

`benchmark/bench.py`'s `measure_recall()` (recall@10: is the gold citation present ANYWHERE in the
top 10) passed cleanly on both "trust" and "leadership" the whole time the cross-modal-boost bug was
live — the correct citation was always in the result set, just buried below irrelevant ones. A
presence/recall metric structurally cannot detect a ranking-quality regression where nothing is
missing, only misordered; it will stay green through the exact bug a user experiences as "the search
results are wrong." Rank-sensitive metrics (MRR, nDCG) and presence metrics (recall@k) answer
different questions and neither substitutes for the other — a retrieval eval suite needs both, and a
"the app now behaves better" claim needs to be checked against the rank-sensitive one specifically,
not inferred from an unchanged recall number. Corollary: after adding the rank-sensitive metric here
and re-measuring, MRR@6 was IDENTICAL before and after the fix on the existing 14-query labeled
set — none of those queries happened to trigger this particular bug, meaning even a rank-sensitive
metric only catches what its labeled query set actually exercises. A benchmark's blind spots are
defined by its query set's coverage, not just by which metric it computes; a fix validated only
against `bench.py` numbers, without also manually replaying the actual reported queries, would have
shipped without any automated signal that it worked.
