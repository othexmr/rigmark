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
            self.assertEqual(R.declared_check_count(turn), 1)
            self.assertIsInstance(turn['exact_answer'], str)

    def test_expected_answers_rederived(self):
        """Every expected answer is recomputed here; an edited task or answer has to keep these in step."""
        def fib(n):
            a, b = 0, 1
            for _ in range(n):
                a, b = b, a + b
            return a
        derived = {'q01': 37 * 23, 'q02': sum(i * i for i in range(1, 11)),
                   'q03': ' '.join(reversed('north east south west'.split())), 'q04': int('2A', 16),
                   'q05': __import__('calendar').monthrange(2024, 2)[1], 'q06': (13 * 60 + 15) - (9 * 60 + 40),
                   'q07': 12345 % 97, 'q08': 'strawberry'.count('r'), 'q09': 20 + 30, 'q10': fib(10),
                   'q11': int('101101', 2), 'q12': len(set('mississippi')), 'q13': int(2.5 * 3600), 'q14': fib(12),
                   'q15': len('the quick brown fox jumps over the lazy dog'.split()), 'q16': sum(map(int, '98765'))}
        trace = json.loads((Path(R.__file__).parent / 'examples/replay/quality-16.json').read_text())
        self.assertEqual({s['id']: s['turns'][0]['exact_answer'] for s in trace['sessions']},
                         {k: str(v) for k, v in derived.items()})

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

    def test_exact_final_answer_rejects_substrings_duplicates_and_conflicts(self):
        for output in ('ANSWER: 8510', 'ANSWER: 851 followed by text',
                       'ANSWER: 851\nANSWER: 851', 'ANSWER: 999\nANSWER: 851',
                       'ANSWER: 851\nActually 999', '', 'Working... ANSWER: 851'):
            with self.subTest(output=output):
                status, checks = R.request_outcome(
                    {'output': output, 'done': True, 'finish_reason': 'stop'}, {'exact_answer': '851'})
                self.assertEqual(status, 'invalid_answer')
                self.assertFalse(checks)
        self.assertEqual(R.request_outcome(
            {'output': 'Working...\nANSWER: 851\n', 'done': True, 'finish_reason': 'stop'},
            {'exact_answer': '851'}), ('completed', True))
        # Generic contains_all remains a substring contract.
        self.assertEqual(R.request_outcome(
            {'output': 'ANSWER: 8510', 'done': True, 'finish_reason': 'stop'},
            {'contains_all': ['ANSWER: 851']}), ('completed', True))

    def test_exact_answer_schema(self):
        for value in (None, 851, '', ' 851', '851\n999', 'ANSWER: 851'):
            trace = json.loads((Path(R.__file__).parent / 'examples/replay/quality-16.json').read_text())
            trace['sessions'][0]['turns'][0]['exact_answer'] = value
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'exact_answer'):
                R.validate(trace)

    def test_correct_text_in_failed_or_truncated_requests_is_a_quality_miss(self):
        rows = []
        for fields in ({'error': 'timeout', 'done': False, 'finish_reason': None},
                       {'done': True, 'finish_reason': 'length'},
                       {'done': False, 'finish_reason': 'stop'}):
            row = dict(fields, output='ANSWER: 851', started_s=0, finished_s=1,
                       declared_content_checks=1)
            row['status'], row['declared_content_checks_pass'] = R.request_outcome(row, {'exact_answer': '851'})
            rows.append(row)
        self.assertEqual(R.score(rows)['content_checks_pass_fraction'], 0)
        self.assertEqual(R.score(rows)['completion_fraction'], 0)


class FakeClient:
    def stream(self, messages, cap, extra):
        import time
        started = time.monotonic()
        state = R.StreamState()
        answer = 'Working...\nANSWER: 851' if '37 multiplied' in messages[-1]['content'] else 'ANSWER: wrong'
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
