"""Replay SLO basis: visible answer text (the original definition) or any output including reasoning."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import replay as R
import replay_compare as C


def frame(content='', reasoning='', finish=None, usage=None):
    delta = {'content': content} if content else {}
    if reasoning:
        delta['reasoning_content'] = reasoning
    obj = {'choices': [{'index': 0, 'delta': delta, 'finish_reason': finish}]}
    if usage:
        obj['usage'] = usage
    return json.dumps(obj)


class Fields(unittest.TestCase):
    def test_output_gap_spans_reasoning_and_answer(self):
        s = R.StreamState()
        for text, reasoning, t in (('', 'th', .1), ('', 'ink', .4), ('ok', '', 1.4), (' done', '', 1.5)):
            s.observe(frame(text, reasoning), t)
        s.observe(frame(finish='stop', usage={'prompt_tokens': 3, 'completion_tokens': 4}), 1.6)
        r = s.result()
        self.assertEqual((r['first_output_s'], r['first_visible_s']), (.1, 1.4))
        self.assertAlmostEqual(r['longest_output_delivery_gap_s'], 1.0)       # last reasoning delta -> first answer
        self.assertAlmostEqual(r['longest_visible_delivery_gap_s'], .1)

    def row(self, visible_ttft, output_ttft, visible_gap, output_gap):
        return {'id': 'a:0', 'status': 'completed', 'started_s': 0, 'finished_s': 30, 'client_e2e_s': 30,
                'user_visible_ttft_s': visible_ttft, 'user_output_ttft_s': output_ttft,
                'longest_visible_delivery_gap_s': visible_gap, 'longest_output_delivery_gap_s': output_gap,
                'usage_valid': False}

    def test_basis_selects_the_timing_fields(self):
        rows = [self.row(25., .5, .2, 1.)]                   # 25 s of reasoning before the answer
        slo = {'visible': 5, 'gap': 2, 'total': 60}
        self.assertEqual(R.score(rows, slo)['slo_good_requests'], 0)
        self.assertEqual(R.score(rows, dict(slo, basis='visible'))['slo_good_requests'], 0)
        self.assertEqual(R.score(rows, dict(slo, basis='output'))['slo_good_requests'], 1)
        # a reasoning stall breaks the output-basis gap bound, not the visible one
        self.assertEqual(R.score([self.row(1., .5, .2, 3.)], dict(slo, basis='output'))['slo_good_requests'], 0)
        with self.assertRaisesRegex(ValueError, 'SLO basis'):
            R.score(rows, dict(slo, basis='events'))


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers['Content-Length']))
        self.send_response(200); self.send_header('Content-Type', 'text/event-stream'); self.end_headers()
        self.wfile.write(f'data: {frame(reasoning="thinking")}\n\n'.encode()); self.wfile.flush()
        time.sleep(.3)
        self.wfile.write(f'data: {frame("answer", finish="stop")}\n\n'.encode())
        self.wfile.write(f'data: {json.dumps({"choices": [], "usage": {"prompt_tokens": 4, "completion_tokens": 2}})}\n\n'
                         'data: [DONE]\n\n'.encode()); self.wfile.flush()


class Cli(unittest.TestCase):
    def setUp(self):
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True); self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()

    def cli(self, *argv):
        return subprocess.run([sys.executable, '-B', str(Path(R.__file__).with_name('rigmark')), *map(str, argv)],
                              capture_output=True, text=True, timeout=30)

    def test_replay_records_basis_and_comparator_refuses_mixed_bases(self):
        trace = {'protocol': R.PROTOCOL, 'system': 'S', 'cache_policy': 'run-isolated', 'provenance': {'kind': 'test'},
                 'sessions': [{'id': 'a', 'category': 'code', 'start_s': 0, 'turns': [{'user': 'q', 'max_tokens': 16}]}]}
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); R.save(root / 'trace.json', trace); R.save(root / 'identity.json', {'model': 'mock'})
            common = ['replay', '--trace', root / 'trace.json', '--base-url', self.url, '--model', 'mock',
                      '--identity', root / 'identity.json', '--max-dispatch-lag', '1', '--slo-visible', '.2',
                      '--slo-gap', '1', '--slo-total', '5', '--run']
            p = self.cli(*common[:-7], '--slo-basis', 'output', '--output', root / 'x', '--run-id', 'x', '--run')
            self.assertEqual(p.returncode, 2); self.assertIn('needs the three SLO thresholds', p.stderr)
            for arm, basis in (('control', 'output'), ('candidate', 'output'), ('visible', 'visible')):
                p = self.cli(*common, '--slo-basis', basis, '--output', root / arm, '--run-id', arm)
                self.assertEqual(p.returncode, 0, p.stderr)
            output = json.loads((root / 'control/manifest.json').read_text())['slo']
            self.assertEqual(output, {'visible': .2, 'gap': 1, 'total': 5, 'basis': 'output'})
            self.assertNotIn('basis', json.loads((root / 'visible/manifest.json').read_text())['slo'])
            # The answer arrives ~0.3 s after the reasoning starts: inside the output-basis SLO, outside the visible one.
            self.assertEqual(json.loads((root / 'control/score.json').read_text())['slo_good_requests'], 1)
            self.assertEqual(json.loads((root / 'visible/score.json').read_text())['slo_good_requests'], 0)
            result = C.compare(root / 'control', root / 'candidate')
            self.assertEqual(result['overall']['completion_fraction']['candidate'], 1)
            with self.assertRaisesRegex(ValueError, 'incompatible slo'):
                C.compare(root / 'control', root / 'visible')

    def test_sweep_basis_and_cpu_receipt(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            f = root / 'code.txt'; f.write_text('def add(a, b): return a + b')
            R.save(root / 'identity.json', {'model': 'mock'})
            p = self.cli('replay-sweep', '--code', f, '--document', f, '--context', f, '--rates', '4', '--duration', '1',
                         '--seed', '2', '--output', root / 'sweep', '--base-url', self.url, '--model', 'mock',
                         '--identity', root / 'identity.json', '--slo-visible', '.2', '--slo-gap', '1',
                         '--slo-total', '5', '--slo-basis', 'output', '--max-dispatch-lag', '1')
            self.assertEqual(p.returncode, 0, p.stderr)
            summary = json.loads((root / 'sweep/sweep.json').read_text())
            self.assertEqual(summary['rates'][0]['slo_attainment'], 1.0)
            self.assertIsNotNone(summary['rates'][0]['output_ttft_median_s'])
            self.assertEqual(json.loads((root / 'sweep/rate-4/manifest.json').read_text())['slo']['basis'], 'output')
            terminal = json.loads((root / 'sweep/rate-4/terminal.json').read_text())
            self.assertGreater(terminal['client_cpu_us_per_sse_event'], 0)
            C.load_run(root / 'sweep/rate-4')


if __name__ == '__main__':
    unittest.main(verbosity=2)
