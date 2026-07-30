# Product Evaluation — Moment Search at Scale

- **Student:** Hari Chidambaram
- **Date:** 2026-07-30
- **Video demo:** https://www.loom.com/share/d9d10e4ca7c5448eb9ab2a7b6db619f4
- **App target:** http://localhost:8100 (local Docker Compose) and https://momentsearch-wispy-silence-981.fly.dev (deployed; both point at the same Neon Postgres manifest + Qdrant Cloud index)
- **LLM / embedding provider:** LLM_PROVIDER=openai, LLM_MODEL=gpt-4o-mini; text embeddings via fastembed (bge, local ONNX); visual embeddings via CLIP_MODEL=clip-ViT-B-32
- **Queue:** Prefect Cloud (managed), 2 worker replicas (`docker compose up -d --scale worker=2`), matching `.env`'s `DISPATCH_MAX_INFLIGHT=10` (`2 × WORKER_CONCURRENCY=5`)

## Verdict

The product ingests papers and decks alongside video into one shared index and answers
cross-source questions with grounded, correctly-typed citations (video timestamp, paper page,
deck slide) that deep-link to the original source — verified live against three sources the
student did not author, on both localhost and the public Fly deployment, in a single query.
An earlier pass this same day found a genuine crash-recovery bug: `--resilience` failed twice,
with the underlying cause traced (via the actual Prefect Cloud traceback) to local dev and the
Fly deployment sharing one Prefect Cloud workspace and one Postgres manifest but having
incompatible storage backends — either environment's dispatcher could claim a pending row and
either environment's worker could execute it, so a Fly worker sometimes tried to fetch bytes that
only ever existed on the local dev machine's disk, producing a real `NoSuchKey`. That bug is now
**fixed** (commit `a2623de`) and **independently re-verified in this session**: a fresh
`--resilience` run came back 10/10 indexed, 0 failed, 0 stuck, with checkpoint-resume explicitly
confirmed from the worker logs. The same fix also erased the run's error rate — every run measured
in this session came back at **0.0%**, down from an original 25%.

At 2 worker replicas (the capacity `.env` is already configured for), ingest throughput measured
**7.33 chunks/s — 92% of the ≥8 target**, up from 0.95–1.64 chunks/s at a single replica across
prior sessions, and the queue-decoupling gate now cleanly **passes** (1.15×–1.22×, well under the
1.3× ceiling, with enough in-flight work this run for `bench.py`'s own sampling-confidence check to
trust the measurement). The one number still short of its bar is throughput itself. It was tested
directly: bumping the shared `clip` embedding service from 3 to 5 worker processes (in case
embedding concurrency, not scheduling, was the ceiling) produced no measurable change — 7.33 vs.
7.38 chunks/s, noise-level — and was reverted. That rules out embedding compute as the bottleneck;
the remaining candidates are `DISPATCH_MAX_INFLIGHT`'s exact admission cap combined with Prefect's
own fixed per-run scheduling latency, and each flow run opening its own fresh `QdrantClient`/TLS
handshake rather than sharing a pooled connection (both documented in `.env`'s own comments as
prior, related findings) — untested further this session, see "Top fixes."

**Rubric result (from `eval/REPORT.md`):** 8 pass / 9 checks (`decoupled` is deferred to
`bench.py`, which independently PASSED it at 2 replicas — see table)

## 1. Performance & scale (from `benchmark/bench.py`, 2 worker replicas)

| Metric | Result | SLA | Pass? |
|---|---|---|---|
| `/admin/documents` accept p95 | 148.7 ms | ≤ 300 ms | ✅ |
| Search p95 during ingest ÷ idle | 1.15× (59% of polls caught active work, above the 50% confidence floor) | ≤ 1.3× | ✅ |
| Cross-source recall@10 | 0.929 | ≥ 0.70 | ✅ |
| MRR@6 (rank-sensitive) | 0.788 (paper 0.639 · deck 1.0 · video 0.733) | ≥ 0.60 | ✅ |
| Error rate (this run) | 0.0% (20/20 backfill docs indexed, 0 registration failures) | ≤ 1.0% | ✅ |
| No-loss under worker crash (`--resilience`) | **True** — 10/10 indexed, 0 failed, 0 stuck, checkpoint-resume confirmed | required | ✅ |
| Ingest throughput | 7.33 chunks/s (up from 0.95–1.64 chunks/s at 1 replica) | ≥ 8 | ❌ |

Throughput is the one metric still short, and it's close: 92% of target at the capacity `.env` is
already configured for. A direct experiment (5 `clip` workers instead of 3) ruled out embedding
compute as the ceiling — see Verdict and "Top fixes" for what's left to try.

### Resilience — the same test that failed twice earlier today, now fixed and re-verified

Two runs earlier this session both failed `--resilience`: of 10 documents killed mid-ingest, some
lost their committed checkpoints (re-parsed instead of resuming) and two ended `failed` with a raw
`botocore.errorfactory.NoSuchKey: ... GetObject ... The specified key does not exist.` — an error
that traced, via the actual Prefect Cloud flow-run logs, to `storage.py`'s **S3 branch**
(`_s3().get_object(...)`) executing even though the worker that scheduled the run had
`STORAGE_PROVIDER=local`. Root cause: local dev and the Fly deployment share one Prefect Cloud
workspace and one Neon Postgres manifest, and until commit `a2623de`, neither Prefect deployment
names nor the dispatcher's row-claim query were scoped by environment — so a stale, reconciler-reset
row could be claimed and executed by *either* environment's worker, and a Fly worker fetching a
locally-uploaded document's bytes from its own Tigris/S3 bucket genuinely found nothing there
(the bytes were never lost — they just never left the local disk they were written to).

The fix (already on this branch, not a proposal): Prefect deployment names suffixed by
`config.DEPLOYMENT_ENV` so a dispatcher's own workers always execute what it schedules, plus a new
`storage_env` column filtered into `claim_pending()`'s admission query so a dispatcher can never
claim a row whose bytes live in another environment's storage. Re-run in this session, fresh
against current `HEAD`:

```
[resilience] active at kill time: {'doc_565b752f802f': 'embedding'}
[resilience] final: 10 indexed, 0 failed, 0 stuck/non-terminal (of 10 total)
[resilience] checkpoint-resume confirmed for doc_565b752f802f (was 'embedding', verified 2 checkpoint line(s))
[PASS] no_loss_under_crash: True (target 0 dropped, all indexed, checkpoint-resumed)
```

## 2. Live cross-source test

- **Sources queried (not authored by student, all previously registered and re-verified live this
  session):** video [`Pinecone's New Hybrid Search`](https://www.youtube.com/watch?v=0cKtkaR883c)
  (`yt_0cKtkaR883c`) · paper [arXiv 2312.10997, the RAG survey](https://arxiv.org/pdf/2312.10997)
  (`doc_1bec551249fe`) · deck [Umar Jamil's public RAG slides](https://raw.githubusercontent.com/hkproj/retrieval-augmented-generation-notes/main/Slides.pdf)
  (`doc_79f8d5671e44`)
- **All reached `indexed`?** Yes — confirmed live via `GET /admin/sources` immediately before querying.
- **Async accept?** Yes: `eval.py`'s live probe POST to `/admin/documents` returned `202` with a
  `"pending"` body in 284ms.
- **One query, multiple kinds?** `"compare embeddings, vector databases, cosine similarity, and
  hybrid search techniques used in retrieval augmented generation"` → citations of all three kinds
  (video, paper, deck) in one `/ask_stream` response, confirmed on **both** localhost and the
  deployed Fly URL (`top_k=15`; the corpus has grown substantially since earlier work — several
  more of the student's own videos/decks/papers are now indexed alongside these three — so the
  default `top_k` no longer reliably surfaces the deck citation for this exact query; raising
  `top_k` does. Noted honestly rather than silently using a wider default.)
- **Locators deep-link correctly?** Video → `https://www.youtube.com/watch?v=0cKtkaR883c&t=92`
  (jumps to 01:32); paper → `https://arxiv.org/pdf/2312.10997#page=1`; deck →
  `https://raw.githubusercontent.com/hkproj/retrieval-augmented-generation-notes/main/Slides.pdf#page=25`.
  All three point straight at the original public source.
- **Grounding:** `eval.py`'s `grounded` check: 4/4 citations carry real text + a locator.
- **Decoupling:** measured by `bench.py` above — 1.15×–1.22× (under the 1.3× ceiling) during a live
  20-document backfill, gate PASSED at 2 replicas.
- **Screenshots:** not captured in this text-only session; the Loom demo linked above is the
  recorded evidence of the UI itself.

### Sample citations (one per kind)

| Kind | Locator | Snippet | Correct? |
|---|---|---|---|
| video | 01:32 (`t=92`) | "the new hybrid search that Pine Cone has come up with that merges both vector search and a more traditional search into one single index..." | ✅ |
| paper | p.1 | "the evolution of retrieval augmentation techniques, assess the strengths and weaknesses of various approaches..." | ✅ |
| deck | slide 25 | "Umar Jamil – https://github.com/hkproj/retrieval-augmented-generation-notes ... Base LLM Vector DB + Embeddings RAG + = Fine-tuned LLM + RAG" | ✅ |

## 3. Dimension scorecard

| Dimension | Pass / Partial / Fail | Evidence |
|---|---|---|
| Multi-format ingestion (paper + deck) | ✅ Pass | Video, paper, and deck all live in `/admin/sources` as `indexed`; verified this session |
| Correct locators (page / slide / timestamp) | ✅ Pass | Sample citations table above; deeplinks resolve to the exact page/slide/timestamp on the original public source |
| One shared index | ✅ Pass | `GET /admin/sources` lists video+paper+deck together; one Qdrant collection, one `/ask_stream` answering across all three |
| Cross-source recall vs SLA | ✅ Pass | recall@10 0.929 / MRR@6 0.788, both above `benchmark/sla.json`'s targets |
| Grounded answers (no invented locators) | ✅ Pass | `eval.py`'s `grounded` check: 4/4 citations carry real text + locator |
| Queue decoupling (search fast during ingest) | ✅ Pass | 1.15×–1.22× idle p95 during a live 20-doc backfill (target ≤1.3×), confirmed at 2 replicas with sufficient sampling confidence |
| Resilience (no loss on crash) | ✅ Pass | Failed twice earlier today; root-caused to a cross-environment queue collision (Fly worker vs. local-only bytes); fixed in commit `a2623de`; re-verified clean in this session (10/10 indexed, checkpoint-resume confirmed) |
| Deploy (Fly.io, cross-source) | ✅ Pass | `https://momentsearch-wispy-silence-981.fly.dev/` returns 200 (after a ~32s cold-start wake) and answered the same cross-source query with all three kinds |

## 4. Integrity check

- **Canary (course policy MS-3.14):** clean — no `ROBOT_WAS_HERE.md`, no 🦥-prefixed commits in the
  last 100 (verified independently this session, not just via `eval.py`'s own check).

## 5. Top fixes before shipping

1. **Ingest throughput — the last real gap, and it's close.** At 2 replicas (the capacity `.env`
   is already sized for) throughput measured 7.33 chunks/s, 92% of the ≥8 target. A direct test
   ruled out `clip` embedding concurrency as the ceiling (5 workers vs. 3 made no measurable
   difference). Two untested candidates remain, both flagged in `.env`'s own history: (a)
   `DISPATCH_MAX_INFLIGHT`'s exact admission cap combined with Prefect's fixed ~3-12s per-run
   scheduling latency — each flow run pays that overhead regardless of how little real work it
   does; (b) each flow run opens its **own** fresh `QdrantClient`/TLS handshake (`src/worker.py`'s
   own docstring) instead of sharing a pooled connection — `.env` documents this already having
   caused `ConnectTimeout` failures once at higher concurrency, so it's a real ceiling, not just a
   theory. A pooled/shared Qdrant client is the deeper architectural fix; tuning
   `DISPATCH_INTERVAL_S` is the cheap thing to try first.
2. **`top_k` default vs. corpus growth.** The default `top_k` no longer reliably surfaces all
   three kinds for the demo's cross-source query now that the corpus has grown well past the
   original locked Block B set — cosmetic to this specific demo query, not a retrieval-quality
   regression (recall@10/MRR@6 are both still well above SLA), but worth knowing before recording
   a fresh demo video against the current, larger corpus.
