"""Quality sanity set: declared content checks, their score and the comparator's re-verification."""
import json
from pathlib import Path
import tempfile
import unittest

import replay as R
import replay_compare as C


class QualitySet(unittest.TestCase):
    def test_example_trace_is_valid_with_one_answer_check_per_task(self):
        trace = R.validate(json.loads((Path(R.__file__).parent / 'examples/replay/quality-16.json').read_text()))
        self.assertEqual(len(trace['sessions']), 16)
        for s in trace['sessions']:
            (turn,) = s['turns']
            self.assertEqual(len(turn['contains_all']), 1)
            self.assertTrue(turn['contains_all'][0].startswith('ANSWER: '))

    def test_pass_fraction_counts_every_planned_request_with_checks(self):
        rows = [{'id': 'a:0', 'status': 'completed', 'started_s': 0, 'finished_s': 1, 'declared_content_checks': 1,
                 'declared_content_checks_pass': True, 'usage_valid': False},
                {'id': 'b:0', 'status': 'invalid_answer', 'started_s': 0, 'finished_s': 1, 'declared_content_checks': 1,
                 'declared_content_checks_pass': False, 'usage_valid': False},
                {'id': 'b:1', 'status': 'blocked_by_previous_turn', 'declared_content_checks': 1},
                {'id': 'c:0', 'status': 'completed', 'started_s': 0, 'finished_s': 1, 'declared_content_checks': 0,
                 'declared_content_checks_pass': True, 'usage_valid': False}]
        s = R.score(rows)
        self.assertEqual(s['content_checks_declared_requests'], 3)
        self.assertAlmostEqual(s['content_checks_pass_fraction'], 1 / 3)
        self.assertIsNone(R.score(rows[3:])['content_checks_pass_fraction'])


class FakeClient:
    def stream(self, messages, cap, extra):
        import time
        started = time.monotonic()
        state = R.StreamState()
        answer = 'Working... ANSWER: 851' if '37 multiplied' in messages[-1]['content'] else 'ANSWER: wrong'
        state.observe(json.dumps({'choices': [{'index': 0, 'delta': {'content': answer}, 'finish_reason': 'stop'}],
                                  'usage': {'prompt_tokens': 5, 'completion_tokens': 4}}), .001)
        state.observe('[DONE]', .002)
        time.sleep(.005)                     # the stamped event times must lie inside the request window
        row = state.result(); row.update(started=started, finished=time.monotonic(), error=None)
        return row


class Receipts(unittest.TestCase):
    def test_comparator_rejects_edited_check_count(self):
        trace = json.loads((Path(R.__file__).parent / 'examples/replay/quality-16.json').read_text())
        trace['sessions'] = trace['sessions'][:2]
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / 'run'; out.mkdir()
            rows = R.execute(trace, FakeClient(), out, 'q')
            self.assertEqual([r['status'] for r in rows], ['completed', 'invalid_answer'])
            summary = R.summarise_run(rows, None, 1)
            self.assertEqual(summary['content_checks_pass_fraction'], .5)
            raw = (json.dumps(trace) + '\n').encode()
            (out / 'trace.json').write_bytes(raw); (out / 'identity.json').write_bytes(b'{}')
            R.save(out / 'manifest.json', {'receipt_version': 1, 'protocol': R.PROTOCOL, 'trace_sha256': R.digest(raw),
                                           'trace': trace, 'runner_sha256': 'x', 'identity': {}, 'identity_sha256': R.digest(b'{}'),
                                           'extra_body': {}, 'timeout': 1, 'slo': None, 'max_dispatch_lag_s': 1})
            R.save(out / 'score.json', summary)
            R.save(out / 'terminal.json', {'status': 'COMPLETED_WITH_FAILURES_NO_RETRY', 'identity_file_unchanged': True,
                                           'client_schedule_valid': True, 'completed': 1, 'planned': 2})
            C.load_run(out)                      # consistent receipts verify
            request = sorted(out.glob('request-*.json'))[0]
            bad = json.loads(request.read_text()); bad['declared_content_checks'] = 0
            R.save(request, bad)
            with self.assertRaisesRegex(ValueError, 'declared check count'):
                C.load_run(out)


if __name__ == '__main__':
    unittest.main(verbosity=2)
