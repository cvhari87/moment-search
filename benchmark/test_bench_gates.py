#!/usr/bin/env python3
"""Regression tests for bench.py's own FALSE-PASS paths.

    python3 -m unittest discover -s benchmark -p 'test_*.py' -v

stdlib `unittest` only, matching bench.py's own no-new-dependency constraint
(the repo has no pytest and no test suite to slot into).

Every test here reproduces a specific way the benchmark was once able to
report PASS while proving nothing, each found by an independent guardrail
review of bench.py. They assert the CURRENT code fails those scenarios. The
point is not coverage of bench.py generally — it is that a benchmark used as
grading evidence must not be able to go green by accident, and these are the
exact ways it previously could. No network, no docker, no live stack: every
external boundary (HTTP polls, subprocess, docker logs) is mocked, so the
control flow under test is the real thing while the environment is not.
"""
from __future__ import annotations

import io
import pathlib
import sys
import unittest
from contextlib import redirect_stdout
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import bench  # noqa: E402


def _rows(**id_to_status: str) -> dict[str, dict]:
    """/admin/sources-shaped rows: {id: {"status": ..., "chunk_count": ...}}."""
    return {i: {"id": i, "status": s, "chunk_count": 10} for i, s in id_to_status.items()}


class ResilienceFalsePassTests(unittest.TestCase):
    """The guardrail's reproduction: 10 documents requested, 9 fail to
    register, the 1 survivor is killed mid-'parsing' (no checkpoint exists),
    checkpoint proof is therefore skipped and auto-passes, and 1/1 indexed
    reports no_loss=PASS. Every link in that chain is now broken."""

    def test_fails_when_not_every_requested_document_registers(self):
        with mock.patch.object(bench, "_backfill", return_value=({"d1"}, 9)), \
             mock.patch.object(bench, "_cleanup"), \
             mock.patch.object(bench, "_wait_for_active") as waited, \
             mock.patch.object(bench, "subprocess") as sp, \
             redirect_stdout(io.StringIO()) as out:
            no_loss, _ = bench.run_resilience_check(n_docs=10, n_pages=2)

        self.assertFalse(no_loss, "a 1-of-10 registration shortfall must not pass")
        self.assertIn("only 1/10", out.getvalue())
        waited.assert_not_called()
        sp.run.assert_not_called()  # must abort BEFORE killing anything

    def test_fails_when_nothing_reaches_a_checkpoint_bearing_status(self):
        """Killing mid-'parsing' proves nothing: no checkpoint has been
        committed yet, so there is no resume to verify. This used to be
        accepted, and then auto-passed the resume assertion outright."""
        ids = {f"d{i}" for i in range(3)}
        with mock.patch.object(bench, "_backfill", return_value=(ids, 0)), \
             mock.patch.object(bench, "_cleanup"), \
             mock.patch.object(bench, "_wait_for_active", return_value={}) as waited, \
             mock.patch.object(bench, "subprocess") as sp, \
             redirect_stdout(io.StringIO()) as out:
            no_loss, _ = bench.run_resilience_check(n_docs=3, n_pages=2)

        self.assertFalse(no_loss, "'parsing' alone guarantees no committed checkpoint")
        self.assertIn("no document reached 'chunking' or 'embedding'", out.getvalue())
        sp.run.assert_not_called()
        # and it must have been ASKING for chunking/embedding, not "anything running"
        self.assertEqual(waited.call_args.kwargs["statuses"], {"chunking", "embedding"})

    def test_wait_for_active_ignores_statuses_with_no_committed_checkpoint(self):
        """Directly: 'parsing'/'fetching' must not satisfy a request for a
        checkpoint-bearing status, but 'chunking' must."""
        with mock.patch.object(bench, "_sources_for",
                               return_value=_rows(d0="parsing", d1="fetching")), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(
                bench._wait_for_active("u", {"d0", "d1"}, timeout=0.2, poll_s=0.05,
                                       statuses={"chunking", "embedding"}),
                {})

        with mock.patch.object(bench, "_sources_for",
                               return_value=_rows(d0="parsing", d1="chunking")), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(
                bench._wait_for_active("u", {"d0", "d1"}, timeout=0.2, poll_s=0.05,
                                       statuses={"chunking", "embedding"}),
                {"d1": "chunking"})

    def test_fails_when_recovery_leaves_a_document_unindexed(self):
        ids = {"d0", "d1"}
        with mock.patch.object(bench, "_backfill", return_value=(ids, 0)), \
             mock.patch.object(bench, "_cleanup"), \
             mock.patch.object(bench, "_wait_for_active", return_value={"d0": "chunking"}), \
             mock.patch.object(bench, "_wait_terminal",
                               return_value=_rows(d0="indexed", d1="failed")), \
             mock.patch.object(bench, "_verify_checkpoint_resume", return_value=True), \
             mock.patch.object(bench.subprocess, "run",
                               return_value=mock.Mock(returncode=0)), \
             mock.patch.object(bench.time, "sleep"), \
             redirect_stdout(io.StringIO()):
            no_loss, _ = bench.run_resilience_check(n_docs=2, n_pages=2)

        self.assertFalse(no_loss, "a lost document is a lost document")

    def test_fails_when_checkpoint_resume_is_not_proven(self):
        ids = {"d0"}
        with mock.patch.object(bench, "_backfill", return_value=(ids, 0)), \
             mock.patch.object(bench, "_cleanup"), \
             mock.patch.object(bench, "_wait_for_active", return_value={"d0": "chunking"}), \
             mock.patch.object(bench, "_wait_terminal", return_value=_rows(d0="indexed")), \
             mock.patch.object(bench, "_verify_checkpoint_resume", return_value=False), \
             mock.patch.object(bench.subprocess, "run",
                               return_value=mock.Mock(returncode=0)), \
             mock.patch.object(bench.time, "sleep"), \
             redirect_stdout(io.StringIO()):
            no_loss, _ = bench.run_resilience_check(n_docs=1, n_pages=2)

        self.assertFalse(no_loss, "all-indexed is not proof of RESUME")


class CheckpointProofTests(unittest.TestCase):
    """The resume proof is status-specific: how far a document had gotten
    determines what it must be able to prove. Accepting either line for
    either status let a partial redo read as a clean resume."""

    @staticmethod
    def _logs(text: str):
        return mock.patch.object(bench.subprocess, "run",
                                 return_value=mock.Mock(returncode=0, stdout=text))

    def test_chunking_requires_the_parsed_checkpoint_line(self):
        with self._logs(""), redirect_stdout(io.StringIO()):
            self.assertFalse(bench._verify_checkpoint_resume({"d0": "chunking"}))

        with self._logs("[parse] d0: parsed.json already committed\n"), \
             redirect_stdout(io.StringIO()):
            self.assertTrue(bench._verify_checkpoint_resume({"d0": "chunking"}))

    def test_embedding_requires_BOTH_checkpoint_lines(self):
        """A doc that was 'embedding' had committed parse AND chunk work.
        Proving only the parse half means it silently re-chunked."""
        with self._logs("[parse] d0: parsed.json already committed\n"), \
             redirect_stdout(io.StringIO()) as out:
            self.assertFalse(bench._verify_checkpoint_resume({"d0": "embedding"}))
        self.assertIn("chunks.json already committed", out.getvalue())

        both = ("[parse] d0: parsed.json already committed\n"
                "[chunk] d0: chunks.json already committed\n")
        with self._logs(both), redirect_stdout(io.StringIO()):
            self.assertTrue(bench._verify_checkpoint_resume({"d0": "embedding"}))

    def test_unreadable_logs_are_a_failure_not_a_pass(self):
        with mock.patch.object(bench.subprocess, "run",
                               return_value=mock.Mock(returncode=1, stdout="")), \
             redirect_stdout(io.StringIO()):
            self.assertFalse(bench._verify_checkpoint_resume({"d0": "chunking"}))


class ThroughputTests(unittest.TestCase):
    """Throughput must be earned by the whole accepted cohort. Summing only
    the indexed subset let a run where most documents died post a high
    chunks/s: the survivors' chunks over the survivors' shorter wall time."""

    def _measure(self, final_rows: dict[str, dict], ids: set[str]):
        with mock.patch.object(bench, "measure_search_p95", return_value=(100.0, 0.0)), \
             mock.patch.object(bench, "_backfill", return_value=(ids, 0)), \
             mock.patch.object(bench, "_poll_active", return_value=[True] * 10), \
             mock.patch.object(bench, "_wait_terminal", return_value=final_rows), \
             mock.patch.object(bench, "_ask_stream_full_latency", return_value=3000.0), \
             redirect_stdout(io.StringIO()) as out:
            result = bench.measure_decoupling_and_throughput(
                n_docs=len(ids), n_pages=3, n_samples=2)
        return result, out.getvalue()

    def test_partial_ingestion_fails_the_gate_and_feeds_the_error_rate(self):
        ids = {"d0", "d1", "d2", "d3"}
        rows = _rows(d0="indexed", d1="failed", d2="skipped", d3="pending")
        (ratio, overlap_ok, throughput, ingest_ok, _,
         _, _, err_reg, err_ingest), printed = self._measure(rows, ids)

        self.assertFalse(ingest_ok, "3 of 4 documents did not index — cannot pass")
        self.assertAlmostEqual(err_ingest, 0.75)
        self.assertIn("never reached 'indexed'", printed)

    def test_fully_successful_ingestion_still_passes(self):
        ids = {"d0", "d1"}
        (ratio, overlap_ok, throughput, ingest_ok, _,
         _, _, _, err_ingest), _ = self._measure(_rows(d0="indexed", d1="indexed"), ids)

        self.assertTrue(ingest_ok)
        self.assertEqual(err_ingest, 0.0)
        self.assertGreater(throughput, 0.0)

    def test_zero_observed_overlap_fails_regardless_of_a_good_ratio(self):
        ids = {"d0"}
        with mock.patch.object(bench, "measure_search_p95", return_value=(100.0, 0.0)), \
             mock.patch.object(bench, "_backfill", return_value=(ids, 0)), \
             mock.patch.object(bench, "_poll_active", return_value=[False] * 10), \
             mock.patch.object(bench, "_wait_terminal", return_value=_rows(d0="indexed")), \
             mock.patch.object(bench, "_ask_stream_full_latency", return_value=3000.0), \
             redirect_stdout(io.StringIO()):
            result = bench.measure_decoupling_and_throughput(n_docs=1, n_pages=3, n_samples=2)

        ratio, overlap_ok = result[0], result[1]
        self.assertEqual(ratio, 1.0, "idle == during, a perfect-looking ratio")
        self.assertFalse(overlap_ok, "...but nothing was actually ingesting")


class OverlapFloorTests(unittest.TestCase):
    """The overlap requirement must not be switchable off from the CLI."""

    def test_below_floor_is_rejected(self):
        for bad in ("0", "0.0", "0.49", "-1"):
            with self.subTest(value=bad):
                with self.assertRaises(Exception):
                    bench._overlap_frac(bad)

    def test_floor_itself_and_stricter_values_are_accepted(self):
        self.assertEqual(bench._overlap_frac("0.5"), 0.5)
        self.assertEqual(bench._overlap_frac("0.9"), 0.9)
        self.assertEqual(bench._overlap_frac("1"), 1.0)

    def test_nonsense_values_are_rejected(self):
        for bad in ("abc", "", "1.5"):
            with self.subTest(value=bad):
                with self.assertRaises(Exception):
                    bench._overlap_frac(bad)


class PoisonProbeDrainTests(unittest.TestCase):
    """The 30 accept-latency probes carry deliberately-invalid content. They
    must all end specifically 'failed'. Accepting any terminal status meant
    an invalid document becoming SEARCHABLE ('indexed') — a real defect —
    would have been accepted as a clean drain."""

    def _run_main_with_probe_status(self, status: str):
        argv = ["bench.py"]
        with mock.patch.object(sys, "argv", argv), \
             mock.patch.object(bench, "measure_accept_latency",
                               return_value=(100.0, {"p0"}, 0.0)), \
             mock.patch.object(bench, "_wait_terminal",
                               return_value={"p0": {"status": status}}), \
             mock.patch.object(bench, "_cleanup"), \
             mock.patch.object(bench, "measure_decoupling_and_throughput") as decoupling, \
             mock.patch.object(bench, "measure_recall") as recall, \
             redirect_stdout(io.StringIO()) as out:
            with self.assertRaises(SystemExit) as ctx:
                bench.main()
        return ctx.exception.code, out.getvalue(), decoupling, recall

    def test_probe_that_became_searchable_is_a_hard_failure(self):
        code, printed, decoupling, recall = self._run_main_with_probe_status("indexed")

        self.assertEqual(code, 1)
        self.assertIn("other than 'failed'", printed)
        decoupling.assert_not_called()
        recall.assert_not_called()

    def test_silently_skipped_probe_is_a_hard_failure(self):
        code, printed, decoupling, _ = self._run_main_with_probe_status("skipped")

        self.assertEqual(code, 1)
        self.assertIn("other than 'failed'", printed)
        decoupling.assert_not_called()

    def test_probe_still_running_is_a_hard_failure(self):
        code, printed, decoupling, _ = self._run_main_with_probe_status("parsing")

        self.assertEqual(code, 1)
        self.assertIn("not terminal", printed)
        decoupling.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
