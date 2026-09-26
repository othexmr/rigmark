import json
import unittest
from unittest.mock import patch

import bench
import receipt


class DecodePrecisionTest(unittest.TestCase):
    def stream_row(self, duration):
        events = [
            {"choices": [{"delta": {"content": "first "}}]},
            {"choices": [{"delta": {"content": "last"}, "finish_reason": "stop"}]},
            {"choices": [], "usage": {"prompt_tokens": 32768, "completion_tokens": 8}},
        ]

        class Response:
            def __enter__(self):
                return iter([
                    ("data: " + json.dumps(event) + "\n").encode()
                    for event in events
                ] + [b"data: [DONE]\n"])

            def __exit__(self, *args):
                pass

        with patch("bench.urllib.request.urlopen", return_value=Response()), patch(
            "bench.time.monotonic", side_effect=[0.0, 1.0, 1.0 + duration, 2.0]
        ):
            row = bench.Client("http://localhost:8000", "", 10).stream(
                "/v1/chat/completions", {"stream": True}
            )
        return json.loads(json.dumps(row))

    def errors(self, row):
        errors = []
        receipt.check_stream_row(errors, row, "stream", require_v11=True)
        return errors

    def test_new_streams_preserve_timing_through_json_round_trip(self):
        for duration in (0.000064375, 0.000075333, 0.00000025):
            with self.subTest(duration=duration):
                row = self.stream_row(duration)
                self.assertEqual(row["decode_seconds"], (1.0 + duration) - 1.0)
                self.assertEqual([], self.errors(row))

    def test_legacy_microsecond_rounding_accepts_observed_bursts(self):
        for window, rate in ((0.000064, 108737.872), (0.000075, 92920.699)):
            with self.subTest(window=window):
                row = self.stream_row(7 / rate)
                row.update(decode_seconds=window, decode_tokens_per_second=rate)
                self.assertEqual([], self.errors(row))

    def test_rate_outside_legacy_rounding_interval_is_rejected(self):
        row = self.stream_row(0.000064375)
        row["decode_seconds"] = 0.000064
        for rate in (7 / 0.0000646, 7 / 0.0000634, 999999):
            with self.subTest(rate=rate):
                row["decode_tokens_per_second"] = round(rate, 3)
                self.assertTrue(any("does not match tokens/time" in e for e in self.errors(row)))

    def test_full_precision_duration_does_not_get_legacy_allowance(self):
        row = self.stream_row(0.000064375)
        row["decode_tokens_per_second"] = round(7 / 0.0000647, 3)
        self.assertTrue(any("does not match tokens/time" in e for e in self.errors(row)))

    def test_unmeasurable_legacy_window_is_still_rejected(self):
        row = self.stream_row(0.00000025)
        row["decode_seconds"] = 0.0
        self.assertTrue(any("not measurable" in e for e in self.errors(row)))

    def test_single_sse_event_is_still_rejected(self):
        row = self.stream_row(0.000064375)
        row["measured_sse_events"] = 1
        self.assertTrue(any("unmeasurable SSE window" in e for e in self.errors(row)))


if __name__ == "__main__":
    unittest.main()
