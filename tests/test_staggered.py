"""Unit tests for the staggered-arrival helpers (no endpoint needed)."""
from __future__ import annotations

import hashlib
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import bench  # noqa: E402
import receipt  # noqa: E402


def stream_row(started: float, offsets: list[float]) -> dict:
    return {
        "started_monotonic_seconds": started,
        "first_output_monotonic_seconds": started + offsets[0],
        "finished_monotonic_seconds": started + offsets[-1] + 0.01,
        "event_seconds": offsets,
    }


class StallAnalysisTest(unittest.TestCase):
    def test_spanning_and_window_gaps(self) -> None:
        # events every 0.1 s, then a 2 s stall between 1.0 and 3.0
        offsets = [round(0.1 * i, 3) for i in range(11)] + [3.0, 3.1, 3.2]
        row = stream_row(100.0, offsets)
        stall = bench.stall_analysis(row, window_start=101.5, window_end=103.05)
        self.assertEqual(stall["events"], len(offsets))
        self.assertAlmostEqual(stall["spanning_gap_seconds"], 2.0, places=3)
        self.assertAlmostEqual(stall["max_gap_seconds"], 2.0, places=3)
        self.assertAlmostEqual(stall["arrival_window_max_gap_seconds"], 2.0, places=3)
        self.assertEqual(stall["arrival_window_gaps"], 1)
        self.assertAlmostEqual(stall["median_gap_seconds"], 0.1, places=3)

    def test_arrival_after_stream_finished(self) -> None:
        row = stream_row(0.0, [0.1, 0.2, 0.3])
        stall = bench.stall_analysis(row, window_start=5.0, window_end=6.0)
        self.assertIsNone(stall["spanning_gap_seconds"])
        self.assertIsNone(stall["arrival_window_max_gap_seconds"])
        self.assertEqual(stall["arrival_window_gaps"], 0)

    def test_single_event_stream(self) -> None:
        row = stream_row(0.0, [0.5])
        stall = bench.stall_analysis(row, 0.2, 0.9)
        self.assertEqual(stall["p95_gap_seconds"], 0.0)
        self.assertIsNone(stall["spanning_gap_seconds"])


class SummariseValidRoundsTest(unittest.TestCase):
    def test_excludes_invalid_rounds(self) -> None:
        rounds = [
            {"overlap_valid": True, "x": 1.0},
            {"overlap_valid": False, "x": 100.0},
            {"overlap_valid": True, "x": 3.0},
        ]
        summary = bench.summarise_valid_rounds(rounds, ("x", "missing"))
        self.assertEqual(summary["valid_rounds"], 2)
        self.assertEqual(summary["total_rounds"], 3)
        self.assertEqual(summary["x"]["median"], 2.0)
        self.assertIsNone(summary["missing"])


class ReceiptValidationTest(unittest.TestCase):
    def test_staggered_section_is_checked(self) -> None:
        errors: list[str] = []
        settings = {"staggered": [2], "staggered_runs": 1, "staggered_depth": 8}
        stream = {
            "prompt_tokens": 8, "completion_tokens": 4, "ttft_seconds": 0.1,
            "decode_seconds": 0.1, "decode_tokens_per_second": 30.0, "wall_seconds": 0.3,
            "event_seconds": [0.1, 0.2], "output": "ok", "output_characters": 2,
            "output_sha256": hashlib.sha256(b"ok").hexdigest(),
        }
        result = {"staggered": {"2": {
            "decode_first": {"rounds": [{"overlap_valid": True, "solo": stream,
                                          "newcomer": stream, "incumbents": [stream]}],
                             "valid_rounds": 1, "total_rounds": 1},
            "prefill_first": {"rounds": [{"overlap_valid": False, "solo": stream,
                                           "incumbent": stream, "newcomers": []}],
                              "valid_rounds": 0, "total_rounds": 1},
        }}}
        receipt.check_staggered(errors, settings, result, require_v11=False)
        self.assertEqual(errors, [])
        result["staggered"]["2"]["decode_first"]["valid_rounds"] = 0
        receipt.check_staggered(errors, settings, result, require_v11=False)
        self.assertTrue(any("valid/total" in error for error in errors))

    def test_absent_section_is_ignored(self) -> None:
        errors: list[str] = []
        receipt.check_staggered(errors, {"staggered": []}, {}, require_v11=True)
        receipt.check_staggered(errors, {}, {}, require_v11=True)
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
