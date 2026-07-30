"""_fuse()'s CROSS_MODAL_BOOST eligibility (src/rag/search.py).

A window earns two-branch RRF scoring and ×CROSS_MODAL_BOOST only when a
frame AND a text hit land in the same window AND the text hit's own raw score
clears CROSS_MODAL_TEXT_MIN. Presence alone used to be enough, which is the diagnosed bug (WHAT_I_DID.md
/ LEARNINGS.md "cross-modal boost" entries, dated 2026-07-29): CLIP's
visual similarity on this corpus sits in a narrow, non-discriminating band
(~0.23-0.33) regardless of query relevance, so a generic frame from almost
any video clears the visual top-K for almost any query. Any real-word text
hit that merely weakly overlaps the query, paired with such a frame, got
boosted 1.5x with no check that the pairing was actually trustworthy.

Below-threshold pairs compete as their strongest branch, preventing an
unconditional RRF sum from structurally outranking single-branch document
results. Tests cover both the scoring decision and that cross-window ordering.

    python3 -m unittest discover -s tests -p 'test_*.py'
"""
from __future__ import annotations

import unittest

from src.rag.search import CROSS_MODAL_BOOST, CROSS_MODAL_TEXT_MIN, RRF_K, _fuse


def _frame(video_id="v1", score=0.23, ms=5000, idx=1):
    return {"video_id": video_id, "score": score, "ms": ms, "idx": idx}


def _text(video_id="v1", score=0.58, t_start=5.0, t_end=8.0, text="some caption"):
    return {"video_id": video_id, "score": score, "t_start": t_start,
            "t_end": t_end, "text": text}


class CrossModalBoostGateTests(unittest.TestCase):
    def test_weak_pair_does_not_outrank_a_strong_text_only_document(self):
        """A below-threshold text hit does not make a generic frame into
        independent corroboration. The window must compete as its strongest
        single branch, rather than receiving an unconditional two-branch RRF
        sum that structurally beats every document-only result."""
        weak = CROSS_MODAL_TEXT_MIN - 0.01
        document = {"video_id": "good-doc", "score": 0.9, "page": 3,
                    "text": "the exact answer"}

        windows = _fuse(
            [_frame(video_id="bad-video", score=0.23)],
            [document, _text(video_id="bad-video", score=weak)],
        )

        self.assertEqual(windows[0]["video_id"], "good-doc")

    def test_below_text_threshold_uses_only_the_strongest_branch(self):
        """The exact shape of the diagnosed bug: a generic frame (weak
        visual score) paired with a text hit that's real but below
        CROSS_MODAL_TEXT_MIN must NOT get the agreement bonus."""
        weak_text_score = CROSS_MODAL_TEXT_MIN - 0.01
        windows = _fuse([_frame(score=0.23)], [_text(score=weak_text_score)])
        self.assertEqual(len(windows), 1)
        w = windows[0]
        self.assertEqual({"frame", "text"}, w["modalities"])
        single_branch = 1.0 / (RRF_K + 0)
        self.assertAlmostEqual(w["rrf"], single_branch)

    def test_at_or_above_text_threshold_is_boosted(self):
        strong_text_score = CROSS_MODAL_TEXT_MIN
        windows = _fuse([_frame(score=0.23)], [_text(score=strong_text_score)])
        self.assertEqual(len(windows), 1)
        w = windows[0]
        unboosted = 1.0 / (RRF_K + 0) + 1.0 / (RRF_K + 0)
        self.assertAlmostEqual(w["rrf"], unboosted * CROSS_MODAL_BOOST)

    def test_frame_confidence_does_not_gate_the_boost(self):
        """CLIP's noise floor doesn't discriminate on this corpus (see
        config.py's CONFIDENCE_THRESHOLD comment) — a LOW frame score must
        not itself block the boost when the text hit is confident; only the
        text side is checked."""
        windows = _fuse([_frame(score=0.01)], [_text(score=CROSS_MODAL_TEXT_MIN + 0.1)])
        w = windows[0]
        unboosted = 1.0 / (RRF_K + 0) + 1.0 / (RRF_K + 0)
        self.assertAlmostEqual(w["rrf"], unboosted * CROSS_MODAL_BOOST)

    def test_frame_only_window_is_never_boosted(self):
        windows = _fuse([_frame()], [])
        w = windows[0]
        self.assertEqual({"frame"}, w["modalities"])
        self.assertAlmostEqual(w["rrf"], 1.0 / (RRF_K + 0))

    def test_text_only_window_is_never_boosted(self):
        windows = _fuse([], [_text(score=CROSS_MODAL_TEXT_MIN + 0.1)])
        w = windows[0]
        self.assertEqual({"text"}, w["modalities"])
        self.assertAlmostEqual(w["rrf"], 1.0 / (RRF_K + 0))

    def test_document_hits_unaffected_boost_still_requires_text_min(self):
        """Documents only ever populate the text slot (no CLIP frame branch
        for a PDF), so a doc-only window is identical to the text-only case
        above — included for explicitness since _locator() special-cases
        page/slide bucketing."""
        doc_hit = {"video_id": "doc1", "score": 0.9, "page": 3, "text": "page text"}
        windows = _fuse([], [doc_hit])
        w = windows[0]
        self.assertEqual({"text"}, w["modalities"])
        self.assertAlmostEqual(w["rrf"], 1.0 / (RRF_K + 0))


if __name__ == "__main__":
    unittest.main()
