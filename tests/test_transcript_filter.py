"""src/ingest/transcript.py's non-speech cue filter — YouTube auto-captions
emit literal "[Music]"/"[Applause]"/etc. event tags during dead air, which
_parse_json3 must drop before they ever reach chunk_cues/embedding (found in
review: a "[Music]" chunk landing near an unrelated frame in time got
cross-modal-boosted above a genuinely relevant transcript passage — see
src/rag/search.py's _fuse and CROSS_MODAL_BOOST).

    python3 -m unittest discover -s tests -p 'test_*.py'
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.ingest.transcript import _parse_json3, chunk_cues


def _json3(events: list[dict]) -> Path:
    tmp = tempfile.NamedTemporaryFile(suffix=".json3", delete=False)
    tmp.write(json.dumps({"events": events}).encode())
    tmp.close()
    return Path(tmp.name)


def _event(text: str, t_start_ms: int, dur_ms: int = 1000) -> dict:
    return {"tStartMs": t_start_ms, "dDurationMs": dur_ms,
            "segs": [{"utf8": text}]}


class ParseJson3NonSpeechFilterTests(unittest.TestCase):
    def test_music_only_cue_is_dropped(self):
        path = _json3([_event("[Music]", 0)])
        self.assertEqual(_parse_json3(path), [])

    def test_repeated_bracket_tags_are_dropped(self):
        path = _json3([_event("[Music] [Music] [Music]", 0)])
        self.assertEqual(_parse_json3(path), [])

    def test_mixed_bracket_tags_are_dropped(self):
        path = _json3([_event("[Music] [Applause]", 0)])
        self.assertEqual(_parse_json3(path), [])

    def test_real_speech_is_kept(self):
        path = _json3([_event("trust takes years to build", 0)])
        cues = _parse_json3(path)
        self.assertEqual(len(cues), 1)
        self.assertEqual(cues[0]["text"], "trust takes years to build")

    def test_bracket_tag_mixed_with_real_speech_is_kept(self):
        """Only a cue that IS a bracket tag (and nothing else) is dropped —
        a tag alongside real words is genuine content and must survive."""
        path = _json3([_event("[Music] but here's the thing", 0)])
        cues = _parse_json3(path)
        self.assertEqual(len(cues), 1)
        self.assertEqual(cues[0]["text"], "[Music] but here's the thing")

    def test_filtered_music_cue_never_reaches_chunk_cues(self):
        """End-to-end: a dead-air stretch of nothing but [Music] events
        produces zero chunks, not a chunk whose entire text is filler."""
        path = _json3([
            _event("[Music]", 0, 5000),
            _event("[Music]", 5000, 5000),
            _event("[Music]", 10000, 5000),
        ])
        cues = _parse_json3(path)
        self.assertEqual(chunk_cues(cues, chunk_seconds=10), [])


if __name__ == "__main__":
    unittest.main()
