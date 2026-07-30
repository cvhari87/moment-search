"""Gate 1's lexical-confirmation fallback (src/rag/search.py) — a third,
independent OR-pass alongside best_visual/best_text. Short, keyword-style
queries ("leadership") score measurably lower on bge cosine similarity than
full natural-language questions, even against genuinely relevant prose —
confirmed live: "leadership" scored BELOW two gibberish negative-test
queries from benchmark/negative_queries.jsonl, meaning no single
TEXT_CONFIDENCE_THRESHOLD value can accept the real query without also
accepting nonsense. _lexical_hit rescues that case by checking whether the
query's own significant words literally appear in a candidate the dense
branch ALREADY retrieved — bounded confirmation, not a corpus-wide keyword
search (found in review).

    python3 -m unittest discover -s tests -p 'test_*.py'
"""
from __future__ import annotations

import unittest

from src.rag.search import _lexical_hit, _significant_words, gate_citations


class SignificantWordsTests(unittest.TestCase):
    def test_drops_stopwords_and_short_words(self):
        self.assertEqual(_significant_words("what is the leadership style"),
                         {"leadership", "style"})

    def test_lowercases(self):
        self.assertEqual(_significant_words("LEADERSHIP Run Amok"),
                         {"leadership", "run", "amok"})

    def test_empty_or_all_stopwords_yields_empty_set(self):
        self.assertEqual(_significant_words("what is the"), set())
        self.assertEqual(_significant_words(""), set())


class LexicalHitTests(unittest.TestCase):
    def test_true_when_the_significant_word_appears_verbatim(self):
        words = _significant_words("leadership")
        candidates = [{"text": "Leadership Run Amok: building trust in teams"}]
        self.assertTrue(_lexical_hit(words, candidates))

    def test_false_when_no_candidate_contains_any_significant_word(self):
        words = _significant_words("leadership")
        candidates = [{"text": "an unrelated passage about sourdough bread"}]
        self.assertFalse(_lexical_hit(words, candidates))

    def test_false_for_empty_question_words(self):
        candidates = [{"text": "leadership is the topic here"}]
        self.assertFalse(_lexical_hit(set(), candidates))

    def test_whole_word_match_not_substring(self):
        """"run" must not match inside "running" — both sides are
        tokenized the same way, so this is a real word-boundary check, not
        a naive substring search."""
        words = _significant_words("running")
        candidates = [{"text": "he went for a run this morning"}]
        self.assertFalse(_lexical_hit(words, candidates))

    def test_handles_missing_or_empty_text_field(self):
        words = _significant_words("leadership")
        candidates = [{}, {"text": None}, {"text": ""}]
        self.assertFalse(_lexical_hit(words, candidates))

    def test_checks_every_candidate_not_just_the_first(self):
        words = _significant_words("leadership")
        candidates = [{"text": "irrelevant filler"}, {"text": "leadership matters"}]
        self.assertTrue(_lexical_hit(words, candidates))

    def test_requires_ALL_significant_words_not_just_one(self):
        """The actual regression this guards against (found in review): an
        earlier ANY-word version false-passed real negative queries like
        "how does photosynthesis work in plants" purely because "work"
        coincidentally appears in unrelated indexed content. A candidate
        containing only ONE of two significant query words must not pass."""
        words = _significant_words("photosynthesis plants")
        candidates = [{"text": "the team's work on this project was excellent"}]
        self.assertFalse(_lexical_hit(words, candidates))

    def test_passes_when_all_significant_words_appear_in_one_candidate(self):
        words = _significant_words("leadership run")
        candidates = [{"text": "leadership style affected an end run of decisions"}]
        self.assertTrue(_lexical_hit(words, candidates))

    def test_words_spread_across_different_candidates_does_not_pass(self):
        """Each word alone appearing in a DIFFERENT candidate is exactly
        the coincidental-overlap case the ALL-in-one-candidate requirement
        exists to reject."""
        words = _significant_words("leadership run")
        candidates = [{"text": "leadership matters here"}, {"text": "he went for a run"}]
        self.assertFalse(_lexical_hit(words, candidates))


class GateCitationsLexicalFallbackTests(unittest.TestCase):
    _CITATIONS = [{"n": 1}]

    def test_low_scores_no_lexical_hit_still_abstains(self):
        self.assertEqual(gate_citations(self._CITATIONS, 0.1, 0.5, lexical_hit=False), [])

    def test_low_scores_with_lexical_hit_passes(self):
        self.assertEqual(gate_citations(self._CITATIONS, 0.1, 0.5, lexical_hit=True),
                         self._CITATIONS)

    def test_lexical_hit_defaults_to_false_when_omitted(self):
        """Every existing caller that doesn't pass lexical_hit must see
        IDENTICAL behavior to before this fallback existed."""
        self.assertEqual(gate_citations(self._CITATIONS, 0.1, 0.5), [])

    def test_empty_citations_stays_empty_even_with_lexical_hit(self):
        """lexical_hit can't manufacture citations that don't exist —
        it only rescues a NONEMPTY citation list from being gated away."""
        self.assertEqual(gate_citations([], 0.1, 0.5, lexical_hit=True), [])

    def test_a_normally_passing_score_is_unaffected_by_lexical_hit_false(self):
        self.assertEqual(gate_citations(self._CITATIONS, 0.9, 0.1, lexical_hit=False),
                         self._CITATIONS)


if __name__ == "__main__":
    unittest.main()
