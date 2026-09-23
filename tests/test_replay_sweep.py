"""Open-loop arrival traces and the replay rate sweep (CPU and loopback only)."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from contextlib import redirect_stdout
import io

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
        return {'rate': rate, 'status': 'COMPLETED', 'measurement_valid': True, 'score': {'slo_fraction_all_planned': attainment,
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

    def test_failure_or_missing_validity_cannot_claim_capacity(self):
        for field, value in (('measurement_valid', False), ('measurement_valid', None),
                             ('status', 'FAILED_NO_RETRY')):
            p = self.point(1, 1)
            p[field] = value
            summary = S.summarise([p], .9)
            self.assertIsNone(summary['max_rate_meeting_target_per_s'])
            self.assertFalse(summary['rates'][0]['target_met'])


class FailureReceipts(unittest.TestCase):
    def run_sweep(self, failure=None, completion=1.0):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source = root / 'code.txt'; source.write_text('def add(a, b): return a + b')
            identity = root / 'identity.json'; identity.write_text('{}')
            output = root / 'sweep'
            argv = ['replay-sweep', '--code', str(source), '--document', str(source), '--context', str(source),
                    '--rates', '4,8', '--duration', '4', '--seed', '2', '--output', str(output),
                    '--base-url', 'http://127.0.0.1:9', '--model', 'mock', '--identity', str(identity),
                    '--slo-visible', '2', '--slo-gap', '2', '--slo-total', '5', '--target', '.5']
            def execute(trace, client, run, run_id, extra):
                if failure == 'exception':
                    raise OSError('simulated write failure')
                if failure == 'identity':
                    identity.write_text('{"changed":true}')
                return [dict(id=f'{session["id"]}:0', status='completed' if i < len(trace['sessions']) * completion else 'error',
                             category=session['category'], started_s=0, finished_s=1,
                             dispatch_lag_s=1 if failure == 'schedule' else 0,
                             user_visible_ttft_s=.1, client_e2e_s=1)
                        for i, session in enumerate(trace['sessions'])]
            stdout = io.StringIO()
            with patch.object(sys, 'argv', argv), patch.object(R, 'execute', side_effect=execute) as executed, redirect_stdout(stdout):
                code = S.main()
            summary = json.loads((output / 'sweep.json').read_text())
            self.assertEqual(json.loads(stdout.getvalue()), summary)
            return code, summary, json.loads((output / 'rate-4/terminal.json').read_text()), executed.call_count

    def test_identity_change_excludes_rate_and_stops_with_failure(self):
        code, summary, terminal, calls = self.run_sweep('identity')
        self.assertEqual(code, 2)
        self.assertFalse(terminal['identity_file_unchanged'])
        self.assertEqual(summary['rates'][0]['slo_attainment'], 1)
        self.assertIsNone(summary['max_rate_meeting_target_per_s'])
        self.assertEqual(calls, 1)

    def test_invalid_schedule_stops_with_failure(self):
        code, summary, terminal, calls = self.run_sweep('schedule')
        self.assertEqual(code, 2)
        self.assertFalse(terminal['measurement_valid'])
        self.assertIsNone(summary['max_rate_meeting_target_per_s'])
        self.assertEqual(calls, 1)

    def test_exception_retains_failure_and_returns_nonzero(self):
        code, summary, terminal, calls = self.run_sweep('exception')
        self.assertEqual(code, 2)
        self.assertEqual(terminal['status'], 'FAILED_NO_RETRY')
        self.assertEqual(summary['stopped_after_rate_per_s'], 4)
        self.assertIsNone(summary['max_rate_meeting_target_per_s'])
        self.assertEqual(calls, 1)

    def test_ordinary_partial_workload_failure_can_meet_declared_target(self):
        code, summary, terminal, calls = self.run_sweep(completion=.5)
        self.assertEqual(code, 0)
        self.assertTrue(terminal['measurement_valid'])
        self.assertEqual(terminal['status'], 'COMPLETED_WITH_FAILURES_NO_RETRY')
        self.assertEqual(summary['max_rate_meeting_target_per_s'], 8)
        self.assertEqual(calls, 2)

    def test_overload_stop_is_not_an_infrastructure_failure(self):
        code, summary, terminal, calls = self.run_sweep(completion=0)
        self.assertEqual(code, 0)
        self.assertTrue(terminal['measurement_valid'])
        self.assertEqual(summary['stopped_after_rate_per_s'], 4)
        self.assertIsNone(summary['max_rate_meeting_target_per_s'])
        self.assertEqual(calls, 1)


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
                # Same seed and inputs, separate campaigns: traces match but system prefixes and requests differ.
                argv[argv.index('--output') + 1] = str(root / 'repeat')
                repeated = subprocess.run(argv, capture_output=True, text=True, timeout=30)
                self.assertEqual(repeated.returncode, 0, repeated.stderr)
                first, second = root / 'sweep/rate-4', root / 'repeat/rate-4'
                manifests = [json.loads((p / 'manifest.json').read_text()) for p in (first, second)]
                self.assertEqual(manifests[0]['trace_sha256'], manifests[1]['trace_sha256'])
                self.assertNotEqual(manifests[0]['run_id'], manifests[1]['run_id'])
                requests = [json.loads((p / 'request-0001.json').read_text()) for p in (first, second)]
                self.assertNotEqual(requests[0]['request_sha256'], requests[1]['request_sha256'])
                C.load_run(second)

        finally:
            server.shutdown(); server.server_close(); thread.join()


if __name__ == '__main__':
    unittest.main(verbosity=2)
