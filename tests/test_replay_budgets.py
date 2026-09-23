"""Replay traces: declared output budgets (max_tokens) per turn kind, recorded in the trace."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import replay as R
import replay_prepare as P


def inputs(d):
    f = Path(d) / 'code.txt'; f.write_text('def add(a, b): return a + b')
    g = Path(d) / 'doc.txt'; g.write_text('A short, real document.')
    return f, g


class Budgets(unittest.TestCase):
    def test_defaults_are_the_original_budgets(self):
        self.assertEqual(P.budgets(), {'initial': 1024, 'long': 1536, 'followup': 768})
        with tempfile.TemporaryDirectory() as d:
            f, g = inputs(d)
            t = P.prepare(f, g, [f, g], 6, 'sessions', 1, 'run-isolated')
            self.assertEqual([[x['max_tokens'] for x in s['turns']] for s in t['sessions']],
                             [[1024, 768, 768]] * 5 + [[1536, 768, 768]])
            self.assertEqual(t['provenance']['output_budgets'], P.budgets())

    def test_declared_budgets_reach_every_turn_kind(self):
        b = P.budgets(4096, 8192, 2048)
        with tempfile.TemporaryDirectory() as d:
            f, g = inputs(d)
            t = P.prepare(f, g, [f, g], 3, 'sessions', 1, 'run-isolated', b)
            self.assertEqual([[x['max_tokens'] for x in s['turns']] for s in t['sessions']],
                             [[4096, 2048, 2048], [4096, 2048, 2048], [8192, 2048, 2048]])
            self.assertEqual(t['provenance']['output_budgets'], b)
            o = P.prepare_open_loop(f, g, [f, g], 2.0, 10, 3, 3, 'run-isolated', b)
            self.assertEqual({s['category'] == 'long-review': s['turns'][0]['max_tokens'] for s in o['sessions']},
                             {False: 4096, True: 8192})
            self.assertEqual(o['provenance']['output_budgets'], {'initial': 4096, 'long': 8192})
            R.validate(t); R.validate(o)

    def test_out_of_range_budgets_rejected(self):
        for value in (15, 16385, 1024.0, True, '1024'):
            with self.assertRaises(ValueError):
                P.budgets(value)
        self.assertEqual(P.budgets(16, 16384)['long'], 16384)


class Cli(unittest.TestCase):
    def run_cli(self, *argv):
        return subprocess.run([sys.executable, '-B', str(Path(R.__file__).with_name('rigmark')), *argv],
                              capture_output=True, text=True, timeout=30)

    def test_prepare_replay_flags(self):
        with tempfile.TemporaryDirectory() as d:
            f, g = inputs(d); out = Path(d) / 'trace.json'
            p = self.run_cli('prepare-replay', '--code', f, '--document', g, '--context', f, '--users', '2',
                             '--direction', 'sessions', '--max-tokens', '3000', '--long-max-tokens', '5000',
                             '--followup-max-tokens', '1500', '--output', out)
            self.assertEqual(p.returncode, 0, p.stderr)
            t = json.loads(out.read_text())
            self.assertEqual([[x['max_tokens'] for x in s['turns']] for s in t['sessions']],
                             [[3000, 1500, 1500], [5000, 1500, 1500]])
            p = self.run_cli('prepare-replay', '--code', f, '--document', g, '--context', f, '--max-tokens', '8',
                             '--output', Path(d) / 'refused.json')
            self.assertEqual(p.returncode, 2)
            self.assertIn('[16, 16384]', p.stderr)
            self.assertFalse((Path(d) / 'refused.json').exists())

    def test_sweep_refuses_followup_budget(self):
        with tempfile.TemporaryDirectory() as d:
            f, g = inputs(d); R.save(Path(d) / 'identity.json', {'model': 'mock'})
            p = self.run_cli('replay-sweep', '--code', f, '--document', g, '--context', f, '--rates', '1',
                             '--output', Path(d) / 'sweep', '--base-url', 'http://127.0.0.1:9', '--model', 'mock',
                             '--identity', Path(d) / 'identity.json', '--slo-visible', '1', '--slo-gap', '1',
                             '--slo-total', '1', '--followup-max-tokens', '512')
            self.assertEqual(p.returncode, 2)
            self.assertIn('single-turn', p.stderr)
            self.assertFalse((Path(d) / 'sweep').exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
