#!/usr/bin/env python3
"""Open-loop arrival-rate sweep over application replay: SLO goodput per rate. No server lifecycle operations.

For each rate, in ascending order:
- prepare one seeded Poisson trace (replay_prepare.prepare_open_loop). The same seed gives the same prompts in the
  same order at every rate; only the arrival clock is compressed;
- run it with the unchanged replay client and its own receipts;
- read the score.

The summary reports:
- per rate: completion, SLO attainment over all planned requests, goodput, and visible TTFT;
- the highest rate at which attainment reaches the target with every lower rate also reaching it (the monotone
  capacity estimate). A non-monotone curve is reported, not smoothed.

No retries. The sweep stops after a rate whose completion fraction falls below --stop-below. SLO thresholds are
required, because goodput without a declared target is undefined.
"""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import time

import replay as R
import replay_prepare as P


def summarise(points, target):
    """points: [{'rate': r, 'score': score.json or None, 'status': ...}] in ascending rate order."""
    table, capacity, monotone, broken = [], None, True, False
    for p in points:
        s = p.get('score') or {}
        attainment = s.get('slo_fraction_all_planned')
        met = attainment is not None and attainment >= target and s.get('client_schedule_valid') is True
        row = {'rate_per_s': p['rate'], 'status': p['status'], 'planned': s.get('planned_requests'),
               'completion_fraction': s.get('completion_fraction'), 'slo_attainment': attainment,
               'slo_goodput_per_s': s.get('slo_goodput_per_s'),
               'visible_ttft_median_s': (s.get('visible_ttft_s') or {}).get('median'),
               'visible_ttft_p95_s': (s.get('visible_ttft_s') or {}).get('p95'),
               'e2e_median_s': (s.get('e2e_s') or {}).get('median'),
               'client_schedule_valid': s.get('client_schedule_valid'), 'target_met': met}
        table.append(row)
        if met and not broken:
            capacity = p['rate']
        elif met and broken:
            monotone = False
        else:
            broken = True
    return {'target_attainment': target, 'rates': table,
            'max_rate_meeting_target_per_s': capacity, 'monotone': monotone,
            'semantics': 'Attainment counts every planned request (failures included) against the declared SLO; '
                         'capacity is the highest rate with every lower rate also meeting the target.'}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--code', type=Path, required=True)
    ap.add_argument('--document', type=Path, required=True)
    ap.add_argument('--context', type=Path, action='append', required=True)
    ap.add_argument('--rates', required=True, help='comma-separated requests per second, e.g. 0.1,0.2,0.4')
    ap.add_argument('--duration', type=float, default=120)
    ap.add_argument('--seed', type=int, default=1)
    ap.add_argument('--long-every', type=int, default=6)
    ap.add_argument('--cache-policy', choices=('natural', 'run-isolated'), default='run-isolated')
    ap.add_argument('--output', type=Path, required=True)
    ap.add_argument('--base-url', required=True)
    ap.add_argument('--model', required=True)
    ap.add_argument('--identity', type=Path, required=True)
    ap.add_argument('--timeout', type=float, default=600)
    ap.add_argument('--extra-body', default='{}')
    ap.add_argument('--delivery-tokens', choices=R.DELIVERY_MODES, default='off')
    ap.add_argument('--slo-visible', type=float, required=True)
    ap.add_argument('--slo-gap', type=float, required=True)
    ap.add_argument('--slo-total', type=float, required=True)
    ap.add_argument('--target', type=float, default=0.9, help='SLO attainment target over all planned requests')
    ap.add_argument('--stop-below', type=float, default=0.5, help='stop after a rate whose completion fraction is lower')
    ap.add_argument('--max-dispatch-lag', type=float, default=0.05)
    P.add_budget_arguments(ap)
    a = ap.parse_args()
    rates = [float(x) for x in a.rates.split(',') if x.strip()]
    if not rates or rates != sorted(rates) or len(set(rates)) != len(rates) or any(not (math.isfinite(r) and r > 0) for r in rates):
        ap.error('rates must be distinct, positive and ascending')
    if not 0 < a.target <= 1 or not 0 <= a.stop_below <= 1:
        ap.error('target in (0, 1], stop-below in [0, 1]')
    if a.followup_max_tokens is not None:
        ap.error('open-loop arrivals are single-turn; --followup-max-tokens does not apply')
    try:
        output_budgets = P.budgets(a.max_tokens, a.long_max_tokens)
    except ValueError as error:
        ap.error(str(error))
    extra = json.loads(a.extra_body)
    if not isinstance(extra, dict) or set(extra) - {'chat_template_kwargs'}:
        ap.error('only chat_template_kwargs allowed')
    slo = {'visible': a.slo_visible, 'gap': a.slo_gap, 'total': a.slo_total}
    for value in slo.values():
        R.number(value, 'SLO')
    a.output.mkdir(parents=True, exist_ok=False)
    identity_raw = a.identity.read_bytes(); identity = json.loads(identity_raw)
    client = R.Client(a.base_url, a.model, a.timeout, os.getenv('OPENAI_API_KEY', ''), a.delivery_tokens)
    points = []
    for rate in rates:
        label = f'rate-{rate:g}'
        run = a.output / label
        trace = P.prepare_open_loop(a.code, a.document, a.context, rate, a.duration, a.seed, a.long_every, a.cache_policy,
                                    output_budgets)
        raw = (json.dumps(trace, indent=2, allow_nan=False) + '\n').encode()
        run.mkdir()
        (run / 'trace.json').write_bytes(raw); (run / 'identity.json').write_bytes(identity_raw)
        R.save(run / 'manifest.json', {'receipt_version': 1, 'protocol': R.PROTOCOL, 'trace_sha256': R.digest(raw),
                                       'trace': trace, 'runner_sha256': R.digest(Path(R.__file__).read_bytes()),
                                       'identity': identity, 'identity_sha256': R.digest(identity_raw), 'model': a.model,
                                       'run_id': f'sweep-{a.seed}-{label}', 'extra_body': extra, 'timeout': a.timeout,
                                       'max_dispatch_lag_s': a.max_dispatch_lag,
                                       'delivery_token_accounting': a.delivery_tokens,
                                       'cache_claim': 'No cache flush. Run-isolated prefix is not proof of coldness.',
                                       'slo': slo, 'sweep': {'rate_per_s': rate, 'rates': rates, 'target': a.target}})
        cpu = time.process_time()
        try:
            rows = R.execute(trace, client, run, f'sweep-{a.seed}-{label}', extra)
            score = R.summarise_run(rows, slo, a.max_dispatch_lag)
            R.save(run / 'score.json', score)
            unchanged = a.identity.read_bytes() == identity_raw
            passed = unchanged and score['client_schedule_valid'] and all(r['status'] == 'completed' for r in rows)
            R.save(run / 'terminal.json', {'status': 'COMPLETED' if passed else 'COMPLETED_WITH_FAILURES_NO_RETRY',
                                           'identity_file_unchanged': unchanged,
                                           'client_schedule_valid': score['client_schedule_valid'],
                                           'identity_scope': 'supplied receipt only; owner must verify live runtime',
                                           'completed': sum(r['status'] == 'completed' for r in rows), 'planned': len(rows),
                                           'client_cpu_seconds': round(time.process_time() - cpu, 6)})
            points.append({'rate': rate, 'score': score, 'status': 'COMPLETED' if passed else 'COMPLETED_WITH_FAILURES_NO_RETRY'})
        except Exception as error:
            R.save(run / 'terminal.json', {'status': 'FAILED_NO_RETRY', 'type': type(error).__name__, 'message': str(error)})
            points.append({'rate': rate, 'score': None, 'status': 'FAILED_NO_RETRY'})
        summary = summarise(points, a.target)
        R.save(a.output / 'sweep.json', summary)
        last = points[-1]['score'] or {}
        if points[-1]['score'] is None or (last.get('completion_fraction') or 0) < a.stop_below:
            summary['stopped_after_rate_per_s'] = rate
            R.save(a.output / 'sweep.json', summary)
            break
    print(json.dumps(summarise(points, a.target)))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
