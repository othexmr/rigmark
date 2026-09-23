"""Open-loop arrival traces and the replay rate sweep (CPU and loopback only)."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

import replay as R
import replay_compare as C
import replay_prepare as P
import replay_sweep as S


class Arrivals(unittest.TestCase):
    def test_seeded_and_rate_scaled(self):
        a = P.open_loop_starts(1.0, 20, 7)
        self.assertEqual(a, P.open_loop_starts(1.0, 20, 7))
        b = P.open_loop_starts(2.0, 20, 7)
        self.assertGreater(len(b), len(a))
        for x, y in zip(a, b):                      # same unit sequence, clock compressed by the rate ratio
            self.assertAlmostEqual(y, x / 2, places=5)
        self.assertNotEqual(a, P.open_loop_starts(1.0, 20, 8))

    def test_bounds(self):
        for rate, duration in ((0, 10), (-1, 10), (float('nan'), 10), (1, 0), (1, 4000)):
            with self.assertRaises(ValueError):
                P.open_loop_starts(rate, duration, 1)
        with self.assertRaisesRegex(ValueError, '128 arrivals'):
            P.open_loop_starts(100, 60, 1)

    def test_prepared_trace_places_long_reviews_and_records_process(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / 'code.txt'; f.write_text('def add(a, b): return a + b')
            g = Path(d) / 'doc.txt'; g.write_text('A short, real document.')
            t = P.prepare_open_loop(f, g, [f, g], 2.0, 10, 3, 3, 'run-isolated')
            self.assertEqual([s['category'] == 'long-review' for s in t['sessions']][:6],
                             [False, False, True, False, False, True])
            self.assertEqual(t['provenance']['arrivals'], {'process': 'poisson', 'rate_per_s': 2.0, 'duration_s': 10,
                                                           'seed': 3, 'long_every': 3})
            self.assertTrue(all(len(s['turns']) == 1 for s in t['sessions']))
            R.validate(t)


class Summary(unittest.TestCase):
    def point(self, rate, attainment, valid=True, completion=1.0):
        return {'rate': rate, 'status': 'COMPLETED', 'score': {'slo_fraction_all_planned': attainment,
                'client_schedule_valid': valid, 'completion_fraction': completion, 'planned_requests': 10}}

    def test_capacity_is_highest_rate_with_all_lower_rates_meeting_target(self):
        s = S.summarise([self.point(.1, 1.), self.point(.2, .95), self.point(.4, .6)], .9)
        self.assertEqual(s['max_rate_meeting_target_per_s'], .2)
        self.assertTrue(s['monotone'])

    def test_non_monotone_reported_not_smoothed(self):
        s = S.summarise([self.point(.1, 1.), self.point(.2, .5), self.point(.4, .95)], .9)
        self.assertEqual(s['max_rate_meeting_target_per_s'], .1)
        self.assertFalse(s['monotone'])

    def test_invalid_schedule_and_failures_never_meet_target(self):
        s = S.summarise([self.point(.1, 1., valid=False), {'rate': .2, 'status': 'FAILED_NO_RETRY', 'score': None}], .9)
        self.assertIsNone(s['max_rate_meeting_target_per_s'])


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers['Content-Length']))
        self.send_response(200); self.send_header('Content-Type', 'text/event-stream'); self.end_headers()
        event = {'choices': [{'index': 0, 'delta': {'content': 'ok'}, 'finish_reason': 'stop'}],
                 'usage': {'prompt_tokens': 5, 'completion_tokens': 1}}
        self.wfile.write(f'data: {json.dumps(event)}\n\ndata: [DONE]\n\n'.encode()); self.wfile.flush()


class EndToEnd(unittest.TestCase):
    def test_two_rate_sweep_with_loopback_server(self):
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        try:
            with tempfile.TemporaryDirectory() as d:
                root = Path(d)
                f = root / 'code.txt'; f.write_text('def add(a, b): return a + b')
                g = root / 'doc.txt'; g.write_text('A short, real document.')
                R.save(root / 'identity.json', {'model': 'mock'})
                argv = [sys.executable, '-B', str(Path(R.__file__).with_name('rigmark')), 'replay-sweep',
                        '--code', str(f), '--document', str(g), '--context', str(f), '--rates', '4,8', '--duration', '1',
                        '--seed', '2', '--output', str(root / 'sweep'), '--base-url', f'http://127.0.0.1:{server.server_port}',
                        '--model', 'mock', '--identity', str(root / 'identity.json'), '--slo-visible', '2',
                        '--slo-gap', '2', '--slo-total', '5', '--max-dispatch-lag', '1']
                p = subprocess.run(argv, capture_output=True, text=True, timeout=30)
                self.assertEqual(p.returncode, 0, p.stderr)
                summary = json.loads((root / 'sweep' / 'sweep.json').read_text())
                self.assertEqual([r['rate_per_s'] for r in summary['rates']], [4.0, 8.0])
                self.assertEqual(summary['max_rate_meeting_target_per_s'], 8.0)
                # Each rate directory is an ordinary, re-verifiable replay receipt.
                result = C.compare(root / 'sweep' / 'rate-4', root / 'sweep' / 'rate-4')
                self.assertEqual(result['overall']['completion_fraction']['candidate'], 1)
        finally:
            server.shutdown(); server.server_close(); thread.join()


if __name__ == '__main__':
    unittest.main(verbosity=2)
