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
from ..config import (BRANCH_TOP_K, CONFIDENCE_THRESHOLD, CROSS_MODAL_BOOST,
                      FUSION_WINDOW_S, RRF_K, TEXT_CONFIDENCE_THRESHOLD, TOP_K)
from . import vector_store
from .embeddings import embed_query, embed_text

ABSTAIN = ("I couldn't find that in your videos — nothing indexed looks "
           "related to the question (neither what's on screen nor what's said).")


def _seconds(ms: int) -> str:
    s = ms // 1000
    return f"{s // 60:02d}:{s % 60:02d}"


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
    URL instead of a local copy — see docstring there)."""
    if not video:
        return None
    if video.get("storage_key"):
        if storage.presign_capable():
            return storage.presign_get(video["storage_key"])
        return f"/api/document/{doc_id}?u={user_id}"
    uri = video.get("uri")
    if uri:
        path = urllib.parse.urlparse(uri).path
        if path.startswith("/corpus/"):
            # `uri` here is the ingestion-time fetch target, e.g.
            # http://api:8000/corpus/deck.pdf — that hostname only resolves
            # inside the docker-compose network, never in the user's browser.
            # The path alone (leading "/") is host-relative: it resolves
            # against whatever origin actually served this citation, which
            # is the same process that owns the /corpus/{name} route.
            return path
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

    Returns {citations, best_visual, best_text} — the two raw bests feed the
    confidence gate (RRF scores are too small to threshold on). video_ids scopes
    the search to chosen videos (UI select/unselect)."""
    k = top_k or TOP_K

    # Visual branch — CLIP text→image.
    vhits = vector_store.search(embed_text(question), user_id, top_k=BRANCH_TOP_K,
                                video_id=video_id, video_ids=video_ids)
    best_visual = vhits[0]["score"] if vhits else 0.0

    # Text branch — bge query→transcript-chunk (only if transcript is enabled).
    thits: list[dict] = []
    best_text = 0.0
    if config.ENABLE_TRANSCRIPT:
        thits = vector_store.search_text(embed_query(question), user_id,
                                         top_k=BRANCH_TOP_K, video_id=video_id,
                                         video_ids=video_ids)
        best_text = thits[0]["score"] if thits else 0.0

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
    return {"citations": citations, "best_visual": best_visual, "best_text": best_text}


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


def answer_from_citations(question: str, user_id: str, citations: list[dict[str, Any]],
                          best_visual: float, best_text: float) -> dict[str, Any]:
    """The slow half of the read path (LLM call), split out from retrieve()
    (the fast half) so a caller can act on citations before this runs —
    /ask_stream (src/api/search.py) emits the citations SSE event right after
    retrieve() returns, then calls this, so a client sees grounded citations
    well before the LLM's answer is ready instead of waiting on both together
    the way POST /api/ask (which just calls ask() below) necessarily does."""
    result: dict[str, Any] = {"question": question, "citations": citations}

    if not citations:
        result.update(answer="No relevant moments were found. Try ingesting a video first.",
                      llm_used=False, abstained=True)
        return result

    # Gate 1 — confidence on the RAW per-branch bests (not the RRF score).
    # Abstain only if NEITHER what's on screen nor what's said looks relevant.
    visual_ok = best_visual >= CONFIDENCE_THRESHOLD
    text_ok = best_text >= TEXT_CONFIDENCE_THRESHOLD
    if CONFIDENCE_THRESHOLD and not visual_ok and not text_ok:
        result.update(answer=ABSTAIN, llm_used=False, abstained=True)
        return result

    cfg, source = resolve_llm(user_id)
    if cfg is None:
        # No generative model — summarize the best matches instead of inventing.
        result.update(answer=_fallback_answer(citations), llm_used=False,
                      note=("Retrieval-only results. Connect your own model "
                            "(vLLM/Ollama/API) in settings, or set LLM_API_KEY "
                            "on the server, for a synthesized, grounded answer."))
        return result

    moments = _build_moments(user_id, citations)
    result["answer"] = _validate_citations(llm.answer(question, moments, cfg),
                                           len(citations))
    result["llm_used"] = True
    result["llm_source"] = source          # "user" = their own hosted model
    result["llm_model"] = cfg.model
    return result


def ask(question: str, user_id: str, *, top_k: int | None = None,
        video_id: str | None = None,
        video_ids: list[str] | None = None) -> dict[str, Any]:
    r = retrieve(question, user_id, top_k=top_k, video_id=video_id, video_ids=video_ids)
    return answer_from_citations(question, user_id, r["citations"],
                                 r["best_visual"], r["best_text"])
