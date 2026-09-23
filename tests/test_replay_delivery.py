"""Replay: opt-in exact completion-token delivery (usage | ids) and client overhead receipts."""
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


def chunk(content='', finish=None, usage=None, ids=None, choices=True):
    obj = {'choices': [{'index': 0, 'delta': {'content': content}, 'finish_reason': finish}] if choices else []}
    if usage is not None:
        obj['usage'] = usage
    if ids is not None and choices:
        obj['choices'][0]['token_ids'] = ids
    return json.dumps(obj)


def usage(completion, prompt=9):
    return {'prompt_tokens': prompt, 'completion_tokens': completion}


class DeliveryStates(unittest.TestCase):
    def test_usage_mode_counts_multi_token_chunks_exactly(self):
        s = R.StreamState('usage')
        s.observe(chunk('', usage=usage(0)), .10)            # role chunk
        s.observe(chunk('a', usage=usage(1)), .20)
        s.observe(chunk('bcd', usage=usage(4)), .30)         # three accepted speculative tokens in one event
        s.observe(chunk('', finish='stop', usage=usage(4)), .31)
        s.observe(chunk(usage=usage(4), choices=False), .32)  # usage-only frame, as bench.py skips it
        s.observe('[DONE]', .33)
        r = s.result()
        self.assertEqual(r['token_delivery']['status'], 'EXACT_COMPLETION_TOKEN_COUNTS')
        self.assertEqual(r['token_delivery']['event_token_counts'], [0, 1, 3, 0])
        self.assertTrue(r['token_timeline_exact'])
        self.assertEqual(r['delivery_token_accounting'], 'usage')

    def test_usage_mode_missing_counts_is_unavailable_not_estimated(self):
        s = R.StreamState('usage')
        s.observe(chunk('a', usage=usage(1)), .1)
        s.observe(chunk('bc'), .2)                           # text without continuous usage
        s.observe(chunk(usage=usage(3), choices=False), .3)
        r = s.result()
        self.assertEqual(r['token_delivery']['status'], 'UNAVAILABLE')
        self.assertIsNone(r['token_delivery']['event_token_counts'])
        self.assertFalse(r['token_timeline_exact'])

    def test_ids_mode_exact_and_mismatch(self):
        s = R.StreamState('ids')
        s.observe(chunk('ab', ids=[5, 6]), .1)
        s.observe(chunk('c', finish='stop', ids=[7]), .2)
        s.observe(chunk(usage=usage(3), choices=False), .3)
        self.assertTrue(s.result()['token_timeline_exact'])
        s.usage['completion_tokens'] = 4
        self.assertFalse(s.result()['token_timeline_exact'])

    def test_off_mode_records_no_delivery(self):
        s = R.StreamState()
        s.observe(chunk('abc', usage=usage(3)), .1)
        r = s.result()
        self.assertIsNone(r['token_delivery'])
        self.assertEqual(r['delivery_token_accounting'], 'off')

    def test_unknown_mode_rejected(self):
        with self.assertRaises(ValueError):
            R.StreamState('events')
        with self.assertRaises(ValueError):
            R.Client('http://127.0.0.1:9', 'm', delivery='events')


class Handler(BaseHTTPRequestHandler):
    bodies = []

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        Handler.bodies.append(body)
        continuous = body.get('stream_options', {}).get('continuous_usage_stats')
        ids = body.get('return_token_ids')
        frames = [('ab', 2, [1, 2]), ('cde', 5, [3, 4, 5])]
        self.send_response(200); self.send_header('Content-Type', 'text/event-stream'); self.end_headers()
        for text, total, token_ids in frames:
            data = chunk(text, usage=usage(total) if continuous else None, ids=token_ids if ids else None)
            self.wfile.write(f'data: {data}\n\n'.encode()); self.wfile.flush()
        final = chunk('', finish='stop', usage=usage(5) if continuous else None, ids=[] if ids else None)
        self.wfile.write(f'data: {final}\n\n'.encode())
        self.wfile.write(f'data: {chunk(usage=usage(5), choices=False)}\n\ndata: [DONE]\n\n'.encode()); self.wfile.flush()


class Wire(unittest.TestCase):
    def setUp(self):
        Handler.bodies = []
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True); self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()

    def test_request_options_per_mode(self):
        for mode in ('off', 'usage', 'ids'):
            r = R.Client(self.url, 'm', 2, delivery=mode).stream([{'role': 'user', 'content': 'x'}], 8, {})
            self.assertIsNone(r['error'])
            body = Handler.bodies[-1]
            self.assertEqual(body['stream_options'].get('continuous_usage_stats'), True if mode == 'usage' else None)
            self.assertEqual(body.get('return_token_ids'), True if mode == 'ids' else None)
            self.assertEqual(r['token_timeline_exact'], mode != 'off')
            if mode != 'off':
                self.assertEqual(sum(r['token_delivery']['event_token_counts']), 5)
            self.assertGreater(r['sse_bytes'], 0)

    def test_cli_usage_mode_end_to_end_and_mode_gate(self):
        trace = {'protocol': R.PROTOCOL, 'system': 'S', 'cache_policy': 'run-isolated', 'provenance': {'kind': 'test'},
                 'sessions': [{'id': 'a', 'category': 'code', 'start_s': 0,
                               'turns': [{'user': 'q', 'max_tokens': 16}, {'user': 'f', 'max_tokens': 16, 'think_s': .01}]}]}
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); R.save(root / 'trace.json', trace); R.save(root / 'identity.json', {'model': 'mock'})
            for arm, mode in (('control', 'usage'), ('candidate', 'usage'), ('other', 'off')):
                argv = [sys.executable, '-B', str(Path(R.__file__).with_name('rigmark')), 'replay', '--trace', str(root / 'trace.json'),
                        '--output', str(root / arm), '--base-url', self.url, '--model', 'mock', '--identity', str(root / 'identity.json'),
                        '--run-id', arm, '--max-dispatch-lag', '1', '--delivery-tokens', mode, '--run']
                p = subprocess.run(argv, capture_output=True, text=True, timeout=20)
                self.assertEqual(p.returncode, 0, p.stderr)
            manifest = json.loads((root / 'control/manifest.json').read_text())
            self.assertEqual(manifest['delivery_token_accounting'], 'usage')
            score = json.loads((root / 'control/score.json').read_text())
            self.assertEqual(score['token_delivery_exact_requests'], 2)
            self.assertGreater(score['client_sse_events'], 0)
            terminal = json.loads((root / 'control/terminal.json').read_text())
            self.assertGreaterEqual(terminal['client_cpu_seconds'], 0)
            result = C.compare(root / 'control', root / 'candidate')
            self.assertEqual(result['overall']['completion_fraction']['candidate'], 1)
            with self.assertRaisesRegex(ValueError, 'incompatible delivery_token_accounting'):
                C.compare(root / 'control', root / 'other')
            request = next((root / 'candidate').glob('request-*.json'))
            bad = json.loads(request.read_text()); bad['token_delivery']['event_token_counts'][0] += 1
            R.save(request, bad)
            with self.assertRaisesRegex(ValueError, 'reconcile'):
                C.compare(root / 'control', root / 'candidate')


class Interference(unittest.TestCase):
    def test_counts_come_from_reconciled_delivery(self):
        inc = {'id': 'inc', 'started_s': 0., 'finished_s': 10., 'first_output_s': .1, 'token_timeline_exact': True,
               'events': [{'seconds': t, 'visible_characters': 1, 'delta_token_count': None} for t in (.1, 3., 5.)],
               'token_delivery': {'status': 'EXACT_COMPLETION_TOKEN_COUNTS', 'event_seconds': [.1, 3., 5.],
                                  'event_token_counts': [1, 3, 4], 'total_tokens': 8}}
        arrival = {'id': 'new', 'started_s': 2., 'finished_s': 9., 'first_output_s': 4., 'events': []}
        evidence = R.interference([inc, arrival])[1]['incumbents'][0]
        self.assertEqual(evidence['generated_tokens_delivered_during_wait'], 7)


if __name__ == '__main__':
    unittest.main(verbosity=2)
