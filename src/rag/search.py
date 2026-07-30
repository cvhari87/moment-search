"""Read path: question -> retrieve -> gate -> cited answer (or honest abstain).

Retrieval is milliseconds; the multimodal LLM call is seconds and dominates
cost. So the shape is a confidence funnel: fetch KNN_K candidates, collapse
temporal near-duplicates, trim to TOP_K, and — Gate 1 — if even the best
score is below CONFIDENCE_THRESHOLD, abstain WITHOUT calling the LLM. That
one free check kills most hallucination risk. Generated answers get their
[n] citations validated; invented references are stripped.
"""
from __future__ import annotations

import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .. import config, db, llm, storage
from ..ingest import detect
from ..config import (BRANCH_TOP_K, CONFIDENCE_THRESHOLD, CROSS_MODAL_BOOST,
                      DOCUMENT_FETCH_ALLOWED_INTERNAL_HOSTS, FUSION_WINDOW_S,
                      RRF_K, TEXT_CONFIDENCE_THRESHOLD, TOP_K)
from . import vector_store
from .embeddings import embed_query, embed_text

ABSTAIN = ("I couldn't find that in your videos — nothing indexed looks "
           "related to the question (neither what's on screen nor what's said).")


def _seconds(ms: int) -> str:
    s = ms // 1000
    return f"{s // 60:02d}:{s % 60:02d}"


_STOPWORDS = frozenset("""
the a an and or but if then than so is are was were be been being this
that these those there here it its what which who whom whose when where
why how do does did doing have has had having will would shall should
can could may might must not no nor of in on at to from for with about
into through during before after above below between out over under
again further once you your yours i me my we our us they them their
he she his her him
""".split())
_WORD_RE = re.compile(r"[a-zA-Z]{3,}")


def _significant_words(text: str) -> set[str]:
    """Lowercased words worth treating as a real keyword signal — drops
    common function/question words (which would match almost any chunk) and
    anything under 3 letters. Feeds _lexical_hit's confirmation check."""
    return {w for w in _WORD_RE.findall(text.lower()) if w not in _STOPWORDS}


def _lexical_hit(question_words: set[str], candidates: list[dict]) -> bool:
    """True if ALL of the query's significant words appear as real, whole
    words (not substrings inside longer words — both sides are tokenized
    the same way) together in the SAME already-retrieved text-branch
    candidate.

    A confirmation signal independent of raw cosine similarity: dense (bge)
    embeddings score short/keyword queries measurably lower than full
    natural-language questions, even against genuinely relevant prose —
    confirmed live against this project's own calibration set
    (benchmark/calibrate_thresholds.py): a literal "leadership" query
    scored BELOW two gibberish negative-test queries on cosine similarity
    alone, meaning no TEXT_CONFIDENCE_THRESHOLD value can both accept it and
    reject nonsense.

    Requiring EVERY significant word (not just one) in a single candidate is
    deliberate, not the obvious first cut: an earlier ANY-word version
    false-passed 3 of 10 calibrated negatives — "how does photosynthesis
    work in plants", "what's the weather like in Tokyo today", "how do I
    change a flat tire on my car" — each sharing exactly one common word
    ("work", "weather"-adjacent terms, "car") with SOMETHING in this
    project's own eclectic corpus (AI/RAG papers, a leadership psychology
    article, engineering decks, video transcripts), verified live against
    benchmark/negative_queries.jsonl (found in review). ALL-words-together
    is a real, bounded lexical AND-match — the query's actual content has
    to genuinely be in the candidate, not just one coincidental word."""
    if not question_words:
        return False
    for h in candidates:
        text = (h.get("text") or "").lower()
        if not text:
            continue
        if question_words <= set(_WORD_RE.findall(text)):
            return True
    return False


def _locator(h: dict) -> tuple[str, float | int]:
    """Bucketing key for one hit. Document chunks carry a page/slide number
    (set in src/ingest/document.py's t_embed, KIND_SPEC) instead of a
    timestamp — that number IS their citation target, so two different pages
    must never merge into one window the way two video frames a few seconds
    apart genuinely can (they're "the same moment"; page 1 and page 2 are
    not). Without this, every document chunk fell through to
    `h.get("ms", 0)` (no ms key on a document payload) = t=0.0 for ALL of
    them, so every page of a document collapsed into one window keyed on
    video_id + t=0 and only the single best-rrf chunk survived."""
    if "page" in h:
        return ("page", h["page"])
    if "slide" in h:
        return ("slide", h["slide"])
    return ("time", float(h.get("t_start", h.get("ms", 0) / 1000.0)))


def _fuse(visual_hits: list[dict], text_hits: list[dict]) -> list[dict]:
    """Reciprocal-Rank-Fusion of the two branches into moment windows.

    Raw scores are incomparable (CLIP ~0.3 vs bge ~0.7), so we rank each branch
    on its own and score by rank: rrf = 1/(RRF_K + rank). Video hits bucket by
    proximity (within FUSION_WINDOW_S seconds of each other, same video) into
    one 'moment'; document hits bucket by EXACT page/slide match instead (see
    _locator) since there's no timeline to be "close" on. Windows sum their
    rrf, and boost when BOTH modalities agree — two independent signals
    pointing at the same instant is the strongest evidence (documents only
    ever populate the text slot; there is no CLIP frame branch for a PDF).
    """
    def ranked(hits, modality):
        out = []
        for rank, h in enumerate(hits):
            out.append({**h, "modality": modality, "rrf": 1.0 / (RRF_K + rank),
                       "loc": _locator(h)})
        return out

    windows: list[dict] = []
    # Hits arrive best-first (rrf desc), so the first hit landing in a window for
    # a given modality is that modality's best hit there.
    for h in sorted(ranked(visual_hits, "frame") + ranked(text_hits, "text"),
                    key=lambda x: x["rrf"], reverse=True):
        loc_kind, loc_val = h["loc"]
        if loc_kind == "time":
            w = next((w for w in windows if w["video_id"] == h["video_id"]
                      and w["loc"][0] == "time"
                      and abs(w["loc"][1] - loc_val) <= FUSION_WINDOW_S), None)
        else:  # page/slide — exact match only, never proximity-merged
            w = next((w for w in windows if w["video_id"] == h["video_id"]
                      and w["loc"] == h["loc"]), None)
        if w is None:
            w = {"video_id": h["video_id"], "loc": h["loc"], "rrf": 0.0,
                 "modalities": set(), "frame": None, "text": None}
            windows.append(w)
        w["modalities"].add(h["modality"])
        slot = "frame" if h["modality"] == "frame" else "text"
        # Keep only the BEST hit per modality. Summing every hit would let a
        # burst of near-identical frames clustered in one 15s window inflate its
        # score past a genuine frame+transcript match — the bug that ranked a
        # silent frame-burst above the moment that actually answered.
        if w[slot] is None:
            w[slot] = h
    for w in windows:
        # Score = best frame + best transcript hit; ×boost when BOTH modalities
        # agree at this instant (two independent signals = strongest evidence).
        w["rrf"] = (w["frame"]["rrf"] if w["frame"] else 0.0) + \
                   (w["text"]["rrf"] if w["text"] else 0.0)
        if {"frame", "text"} <= w["modalities"]:
            w["rrf"] *= CROSS_MODAL_BOOST
    windows.sort(key=lambda w: w["rrf"], reverse=True)
    return windows


def _deeplink(video: dict | None, video_id: str, ms: int) -> str:
    secs = ms // 1000
    if video and video.get("source") == "youtube" and video.get("url"):
        sep = "&" if "?" in video["url"] else "?"
        return f"{video['url']}{sep}t={secs}"
    return f"/api/video/{video_id}#t={secs}"


def _doc_deeplink(video: dict | None, user_id: str, doc_id: str,
                  page: int | None) -> str:
    url = _doc_url(video, user_id, doc_id) or f"/api/document/{doc_id}"
    # #page=N is a client-side PDF-viewer fragment, harmless to append to a
    # presigned query-string URL (fragments never reach the server).
    return f"{url}#page={page}" if page else url


def _thumb_url(user_id: str, video_id: str, idx: int) -> str:
    """Browser-facing thumbnail URL. Presigned GET straight to the bucket when
    the provider supports it (an <img> tag can't send auth headers); the API
    serves the bytes itself only in local-dev mode."""
    if storage.presign_capable():
        return storage.presign_get(storage.frame_key(user_id, video_id, idx))
    return f"/api/frame/{video_id}/{idx:06d}.jpg?u={user_id}"


def _media_url(video: dict | None, user_id: str, video_id: str) -> str | None:
    """Playback URL for uploaded videos (YouTube plays via its own URL)."""
    if not video or video.get("source") != "upload" or not video.get("storage_key"):
        return None
    if storage.presign_capable():
        return storage.presign_get(video["storage_key"])
    return f"/api/video/{video_id}?u={user_id}"


def _doc_url(video: dict | None, user_id: str, doc_id: str) -> str | None:
    """Browser-facing PDF URL for a document citation. Prefers our own bytes
    (storage_key — private, content-addressed, never expires) over `uri`;
    falls back to `uri` only for URL-registered documents, which never get a
    storage_key (src/ingest/document.py reads those straight off the source
    URL instead of a local copy — see docstring there).

    A PPTX-sourced deck's storage_key points at the ORIGINAL .pptx bytes —
    Block M converts to PDF at parse time, not at upload time, so the
    upload itself is untouched. viewer_storage_key swaps in the converted
    PDF checkpoint for VIEWING when one exists, since a browser can't
    render raw .pptx bytes inline the way it can a PDF."""
    if not video:
        return None
    effective_key = detect.viewer_storage_key(
        video.get("storage_key"), video.get("view_storage_key"))
    if effective_key:
        if storage.presign_capable():
            return storage.presign_get(effective_key)
        return f"/api/document/{doc_id}?u={user_id}"
    uri = video.get("uri")
    if uri:
        parsed = urllib.parse.urlparse(uri)
        netloc = parsed.netloc.lower()
        # Only rewrite when the URI's host:port is OUR OWN allowlisted
        # internal fetch target (src/config.py's DOCUMENT_FETCH_ALLOWED_
        # INTERNAL_HOSTS — the same list src/ingest/document.py trusts for
        # SSRF purposes; single source of truth for "this host is us").
        # Matching on path alone (e.g. any URL containing "/corpus/") would
        # rewrite an attacker- or third-party-controlled external URL like
        # https://example.com/corpus/other.pdf into our OWN /corpus/other.pdf
        # — serving one user's citation as a completely different file.
        if netloc in DOCUMENT_FETCH_ALLOWED_INTERNAL_HOSTS:
            # `uri` here is the ingestion-time fetch target, e.g.
            # http://api:8000/corpus/deck.pdf — that hostname only resolves
            # inside the docker-compose network, never in the user's browser.
            # The path alone (leading "/") is host-relative: it resolves
            # against whatever origin actually served this citation, which
            # is the same process that owns the /corpus/{name} route.
            return parsed.path
    return uri


def _locator_label(loc: tuple[str, float | int] | None) -> str:
    """Human-readable citation target: '00:12' for a video moment, 'page 3' /
    'slide 3' for a document chunk — used both for the UI's timestamp pill
    and (via _build_moments) the label the LLM sees, so a document moment
    never renders as a blank/None timestamp."""
    if loc is None:
        return ""
    kind, val = loc
    return _seconds(int(val * 1000)) if kind == "time" else f"{kind} {val}"


def retrieve(question: str, user_id: str, *, top_k: int | None = None,
             video_id: str | None = None,
             video_ids: list[str] | None = None) -> dict[str, Any]:
    """Multimodal retrieve: query BOTH branches (CLIP frames + transcript text),
    fuse by RRF into time windows, and return numbered moment-citations.

    Returns {citations, best_visual, best_text, lexical_hit} — best_visual/
    best_text feed the confidence gate (RRF scores are too small to
    threshold on); lexical_hit is a third, independent OR-pass signal for
    that same gate (see _lexical_hit). video_ids scopes the search to
    chosen videos (UI select/unselect)."""
    k = top_k or TOP_K

    # Visual branch — CLIP text→image.
    vhits = vector_store.search(embed_text(question), user_id, top_k=BRANCH_TOP_K,
                                video_id=video_id, video_ids=video_ids)
    best_visual = vhits[0]["score"] if vhits else 0.0

    # Text branch — bge query→transcript-chunk (only if transcript is enabled).
    thits: list[dict] = []
    best_text = 0.0
    lexical_hit = False
    if config.ENABLE_TRANSCRIPT:
        thits = vector_store.search_text(embed_query(question), user_id,
                                         top_k=BRANCH_TOP_K, video_id=video_id,
                                         video_ids=video_ids)
        best_text = thits[0]["score"] if thits else 0.0
        lexical_hit = _lexical_hit(_significant_words(question), thits)

    windows = _fuse(vhits, thits)[:k]
    videos = db.videos_by_ids(sorted({w["video_id"] for w in windows}))
    citations = []
    for i, w in enumerate(windows, 1):
        vid = w["video_id"]
        meta = videos.get(vid)
        fr, tx = w["frame"], w["text"]
        loc_kind, loc_val = w["loc"]
        is_doc = loc_kind in ("page", "slide")
        if is_doc:
            # A page/slide number, not a timeline position — there is no
            # frame, no seekable ms, and the citation resolves to a PDF page
            # rather than a video timestamp.
            ms = idx = None
        else:
            # Anchor on the frame's exact timestamp when there is one (precise
            # visual seek); otherwise the transcript chunk's start.
            ms = int(fr["ms"]) if fr else int(loc_val * 1000)
            idx = int(fr["idx"]) if fr else None
        # Evaluator/assignment contract (Assignment3_Plan.md Block F): every
        # citation carries kind/sourceId/locator/text alongside the existing
        # video-shaped fields below, which stay untouched so the current UI
        # keeps working unmodified.
        if loc_kind == "page":
            locator: dict[str, Any] = {"page": loc_val}
        elif loc_kind == "slide":
            locator = {"slide": loc_val}
        elif tx and "t_end" in tx:
            # A real span when the transcript chunk has one; start==end (a
            # single instant) when all we have is a frame timestamp.
            locator = {"start_ms": int(tx.get("t_start", loc_val) * 1000),
                      "end_ms": int(tx["t_end"] * 1000)}
        else:
            locator = {"start_ms": ms, "end_ms": ms}
        citations.append({
            "n": i,
            "video_id": vid,
            "sourceId": vid,
            "title": (meta or {}).get("title") or vid,
            "url": (meta or {}).get("url"),
            "source": (meta or {}).get("source"),
            "kind": (meta or {}).get("kind", "video"),
            "locator": locator,
            "page": loc_val if loc_kind == "page" else None,
            "slide": loc_val if loc_kind == "slide" else None,
            "ms": ms,
            "timestamp": _locator_label(w["loc"]),
            "idx": idx,
            "thumbnail": _thumb_url(user_id, vid, idx) if idx is not None else None,
            "media_url": None if is_doc else _media_url(meta, user_id, vid),
            "doc_url": _doc_url(meta, user_id, vid) if is_doc else None,
            "deeplink": (_doc_deeplink(meta, user_id, vid, loc_val) if is_doc
                        else _deeplink(meta, vid, ms)),
            "score": round(w["rrf"], 4),
            "text": (tx or {}).get("text"),
            "transcript": (tx or {}).get("text"),
            "modalities": sorted(w["modalities"]),
        })
    return {"citations": citations, "best_visual": best_visual, "best_text": best_text,
            "lexical_hit": lexical_hit}


def _fallback_answer(citations: list[dict[str, Any]]) -> str:
    """No-LLM summary: rank the visually-closest moments. Honest about being
    similarity, not synthesis."""
    top = citations[0]
    where = f"{top['title']} at {top['timestamp']}" if top.get("title") else top["timestamp"]
    others = ", ".join(f"{c['timestamp']} [{c['n']}]" for c in citations[1:4])
    msg = f"Closest visual match: {where} [{top['n']}] (similarity {top['score']})."
    if others:
        msg += f" Other relevant moments: {others}."
    return msg


_CITE_RE = re.compile(r"\[(\d+(?:\s*,\s*\d+)*)\]")


def _validate_citations(answer: str, n_frames: int) -> str:
    """Strip invented [n] references the model has no frame for."""
    def fix(m: re.Match) -> str:
        nums = [int(x) for x in re.split(r"\s*,\s*", m.group(1))]
        valid = [str(x) for x in nums if 1 <= x <= n_frames]
        return f"[{', '.join(valid)}]" if valid else ""
    return _CITE_RE.sub(fix, answer)


def _build_moments(user_id: str, citations: list[dict[str, Any]]) -> list[dict]:
    """Turn citations into what the LLM sees: each moment carries its frame
    image (if any) and/or its transcript excerpt (if any), numbered to match."""
    def frame_bytes(c):
        if c.get("idx") is None:
            return None
        try:
            return storage.get_bytes(storage.frame_key(user_id, c["video_id"], c["idx"]))
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=6) as ex:
        images = list(ex.map(frame_bytes, citations))
    return [{"image": img, "transcript": c.get("transcript"),
             "timestamp": c["timestamp"]} for img, c in zip(images, citations)]


def resolve_llm(user_id: str) -> tuple[llm.LLMConfig | None, str]:
    """Which model answers for this tenant: their own hosted endpoint
    (ms_user_llms — e.g. a vLLM server) first, the server-wide LLM_* env
    config as fallback. Returns (config, source) with source in
    {"user", "server", "none"}."""
    row = db.get_user_llm(user_id)
    if row and row.get("model"):
        return llm.from_row(row), "user"
    cfg = llm.env_config()
    return (cfg, "server") if cfg else (None, "none")


def gate_citations(citations: list[dict[str, Any]], best_visual: float,
                   best_text: float, lexical_hit: bool = False) -> list[dict[str, Any]]:
    """Confidence Gate 1, applied to the CITATIONS themselves, not just
    whether to bother calling the LLM. A query with nothing relevant indexed
    (raw per-branch bests both below threshold, AND no literal keyword
    confirmation) must not surface citations at all — "I couldn't find that"
    next to five confident-looking source cards is a worse contract
    violation than an honest empty result, both to a person reading the UI
    and to the evaluator's "empty retrieval -> empty citations" requirement.
    The single source of truth for this decision: answer_from_citations and
    /ask_stream (src/api/search.py) both call this instead of each
    re-deriving the same threshold check.

    `lexical_hit` (see retrieve()/_lexical_hit) is a third OR-pass alongside
    best_visual/best_text — a short, keyword-style query ("leadership")
    that clears neither cosine threshold can still pass here if the query's
    own words genuinely appear in an already-retrieved candidate, rather
    than abstaining just because dense embeddings underscore short queries
    (found in review, confirmed against benchmark/calibrate_thresholds.py's
    own negative set: a real "leadership" query scored BELOW two gibberish
    negatives on cosine similarity alone, so no threshold value could have
    fixed this)."""
    if not citations:
        return []
    if (CONFIDENCE_THRESHOLD and best_visual < CONFIDENCE_THRESHOLD
            and best_text < TEXT_CONFIDENCE_THRESHOLD and not lexical_hit):
        return []
    return citations


def answer_from_citations(question: str, user_id: str, citations: list[dict[str, Any]],
                          best_visual: float, best_text: float,
                          lexical_hit: bool = False) -> dict[str, Any]:
    """The slow half of the read path (LLM call), split out from retrieve()
    (the fast half) so a caller can act on citations before this runs —
    /ask_stream (src/api/search.py) emits the citations SSE event right after
    retrieve() returns, then calls this, so a client sees grounded citations
    well before the LLM's answer is ready instead of waiting on both together
    the way POST /api/ask (which just calls ask() below) necessarily does."""
    gated = gate_citations(citations, best_visual, best_text, lexical_hit)
    result: dict[str, Any] = {"question": question, "citations": gated}

    if not citations:
        result.update(answer="No relevant moments were found. Try ingesting a video first.",
                      llm_used=False, abstained=True)
        return result
    if not gated:
        # Retrieval found SOMETHING, but nothing confident enough to ground
        # an answer in — abstain, and (per gate_citations above) show no
        # citations either, rather than an answer that contradicts its own
        # source cards.
        result.update(answer=ABSTAIN, llm_used=False, abstained=True)
        return result

    cfg, source = resolve_llm(user_id)
    if cfg is None:
        # No generative model — summarize the best matches instead of inventing.
        result.update(answer=_fallback_answer(gated), llm_used=False,
                      note=("Retrieval-only results. Connect your own model "
                            "(vLLM/Ollama/API) in settings, or set LLM_API_KEY "
                            "on the server, for a synthesized, grounded answer."))
        return result

    moments = _build_moments(user_id, gated)
    result["answer"] = _validate_citations(llm.answer(question, moments, cfg),
                                           len(gated))
    result["llm_used"] = True
    result["llm_source"] = source          # "user" = their own hosted model
    result["llm_model"] = cfg.model
    return result


def ask(question: str, user_id: str, *, top_k: int | None = None,
        video_id: str | None = None,
        video_ids: list[str] | None = None) -> dict[str, Any]:
    r = retrieve(question, user_id, top_k=top_k, video_id=video_id, video_ids=video_ids)
    return answer_from_citations(question, user_id, r["citations"],
                                 r["best_visual"], r["best_text"], r["lexical_hit"])
