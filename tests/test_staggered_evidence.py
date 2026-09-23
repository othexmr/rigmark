"""CPU counterexamples for scheduler-interference claims; no endpoint."""
import copy
import hashlib
import unittest
import threading
import time
from unittest.mock import patch

import bench
import compare
import receipt
import staggered_metrics as metrics


def stream(start, offsets, finish=None):
    finish = start + offsets[-1] + .01 if finish is None else finish
    row = dict(started_monotonic_seconds=float(start), first_output_monotonic_seconds=start+offsets[0],
        finished_monotonic_seconds=finish, event_seconds=offsets, measured_sse_events=len(offsets),
        prompt_tokens=8, completion_tokens=4, ttft_seconds=offsets[0],
        decode_seconds=offsets[-1]-offsets[0], wall_seconds=finish-start,
        time_to_last_output_seconds=offsets[-1], output='ok', output_characters=2,
        output_sha256=hashlib.sha256(b'ok').hexdigest())
    row['decode_tokens_per_second'] = round(3/row['decode_seconds'], 3)
    return row


def fixture():
    solo = stream(0., [.1, .2])
    incumbent = stream(1., [.1, .2, 3.2])
    arrival = stream(2., [1., 1.2])
    df = dict(round=1, solo=solo, newcomer=arrival, incumbents=[incumbent])
    df.update(metrics.derived(df, 2, 'decode_first')[0])
    stall = incumbent['stall'] = bench.stall_analysis(incumbent, 2., 3.)
    df.update(incumbent_max_arrival_window_gap_seconds=stall['arrival_window_max_gap_seconds'],
              incumbent_max_p95_gap_seconds=stall['p95_gap_seconds'])
    metrics.add_evidence(df, 2, 'decode_first')
    long = stream(10., [3., 3.2]); short = stream(11., [.2, .3])
    pf = dict(round=1, solo=solo, incumbent=long, newcomers=[short], long_solo_ttft_seconds=.1)
    pf.update(metrics.derived(pf, 2, 'prefill_first')[0]); metrics.add_evidence(pf,2,'prefill_first')
    settings = dict(staggered=[2], staggered_runs=1, staggered_depth=8, staggered_metrics_version=2)
    result = {'staggered': {'2': {
        'decode_first': bench.summarise_valid_rounds([df], bench.DECODE_FIRST_KEYS),
        'prefill_first': bench.summarise_valid_rounds([pf], bench.PREFILL_FIRST_KEYS)}}}
    return settings, result


class Evidence(unittest.TestCase):
    def test_crossing_gap_is_not_lost_and_legacy_field_unchanged(self):
        row=stream(0., [.1, .2, 5.2])
        result=bench.stall_analysis(row,1.,2.)
        self.assertIsNone(result['arrival_window_max_gap_seconds'])
        self.assertEqual(result['max_intersecting_gap_seconds'],5.)
        self.assertEqual(result['max_clipped_gap_seconds'],1.)

    def test_terminal_silence_is_censored_not_an_inter_event_gap(self):
        row=stream(0., [.1,.2],finish=5.)
        result=bench.stall_analysis(row,1.,3.)
        self.assertIsNone(result['max_intersecting_gap_seconds'])
        self.assertEqual(result['terminal_silence_clipped_seconds'],2.)

    def test_open_connection_is_not_a_still_delivering_incumbent(self):
        settings, result=fixture(); df=result['staggered']['2']['decode_first']['rounds'][0]
        df['incumbents']=[stream(1., [.1,.2],finish=5.)]
        expected,evidence=metrics.derived(df,2,'decode_first')
        self.assertTrue(expected['overlap_valid'])
        self.assertEqual(evidence['open_incumbent_streams_at_arrival'],1)
        self.assertEqual(evidence['incumbents_with_output_after_arrival'],0)
        self.assertFalse(evidence['all_incumbents_output_live'])

    def test_correct_receipt_and_mutations(self):
        settings, pristine=fixture()
        errors=[];receipt.check_staggered(errors,settings,pristine,False)
        self.assertEqual(errors,[])
        def df(r):return r['staggered']['2']['decode_first']
        mutations=(
            lambda r:df(r)['rounds'][0].update(overlap_valid=False),
            lambda r:df(r)['rounds'][0].update(newcomer_ttft_seconds=99.),
            lambda r:df(r)['rounds'][0]['evidence'].update(open_incumbent_streams_at_arrival=True),
            lambda r:df(r)['rounds'][0]['incumbents'][0]['stall'].update(max_intersecting_gap_seconds=99.),
            lambda r:df(r)['rounds'][0]['newcomer'].update(event_seconds=[1.2,1.]),
            lambda r:df(r)['rounds'][0]['newcomer'].update(event_seconds=[float('nan'),1.2]),
            lambda r:df(r)['newcomer_ttft_seconds'].update(median=99.),
            lambda r:df(r).update(valid_rounds=True),
            lambda r:df(r)['rounds'][0].update(round=2),
            lambda r:r['staggered']['2']['prefill_first']['rounds'][0].update(long_solo_ttft_seconds=99.),
        )
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                result=copy.deepcopy(pristine);mutate(result);errors=[]
                receipt.check_staggered(errors,settings,result,False)
                self.assertTrue(errors)

    def test_prefill_first_short_starts_before_long_is_invalid_but_readable(self):
        short_done = threading.Event()
        long_started = threading.Event()
        class Client:
            calls = 0
            timeout = 2
            def stream(self, path, payload, **kwargs):
                if path == '/v1/completions':
                    time.sleep(.02)  # Simulate worker startup before the request clock.
                    long_started.set()
                    kwargs['on_request_start'](time.monotonic())
                    if not short_done.wait(2):
                        raise RuntimeError('short request did not run')
                    return stream(10., [3., 3.2])
                self.calls += 1
                if self.calls == 1:
                    return stream(0., [.1, .2])
                if not long_started.is_set():
                    raise RuntimeError('short arrived before long request started')
                short_done.set()
                return stream(9., [.1, .2])
        settings, result = fixture()
        settings.update(staggered_workload='code', staggered_delay_seconds=0,
                        staggered_arrival_tokens=64, staggered_incumbent_tokens=64)
        prompts = {'system': 'test', 'workloads': {'code': 'test'}, 'prefill_unit': 'test'}
        with patch.object(bench, 'exact_token_ids', return_value=list(range(8))):
            row = bench.staggered_prefill_first_round(Client(), 'model', prompts, 2, 1,
                                                       settings, 0, {}, 'test', .1)
        self.assertFalse(row['overlap_valid'])
        self.assertEqual(row['newcomers_started_during_prefill'], 0)
        result['staggered']['2']['prefill_first'] = bench.summarise_valid_rounds([row], bench.PREFILL_FIRST_KEYS)
        errors = []; receipt.check_staggered(errors, settings, result, False)
        self.assertEqual(errors, [])

    def test_staggered_context_reserves_actual_generation(self):
        bench.validate_prefill_depths([32768],33792,1024)
        with self.assertRaises(ValueError):bench.validate_prefill_depths([32768],33000,1024)
        with self.assertRaises(ValueError):bench.validate_prefill_depths([-1],33000,1024)

    def test_comparison_id_not_sampling_seed_changes_prefix_nonce(self):
        self.assertEqual(bench.nonce('matched','arrival',2,1),bench.nonce('matched','arrival',2,1))
        self.assertNotEqual(bench.nonce('offset1','arrival',2,1),bench.nonce('offset2','arrival',2,1))

    def test_new_and_old_metric_versions_cannot_mix(self):
        settings,result=fixture(); left={'settings':settings};right=copy.deepcopy(left)
        right['settings'].pop('staggered_metrics_version')
        self.assertIn('settings.staggered_metrics_version',compare.comparable(left,right))

    def test_previous_round_survives_later_request_failure(self):
        settings,result=fixture();df=result['staggered']['2']['decode_first']['rounds'][0]
        output={};writes=[]
        with patch.object(bench,'staggered_decode_first_round',return_value=df), \
             patch.object(bench,'staggered_prefill_first_round',side_effect=RuntimeError('failed')), \
             patch('builtins.print'):
            with self.assertRaisesRegex(RuntimeError,'failed'):
                bench.run_staggered(None,'model',{},settings,0,{},'run',output,
                                    lambda:writes.append(copy.deepcopy(output)))
        self.assertEqual(len(writes),1)
        self.assertEqual(output['2']['decode_first']['rounds'],[df])
        self.assertEqual(output['2']['prefill_first']['rounds'],[])


if __name__=='__main__':unittest.main()
