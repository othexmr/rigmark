#!/usr/bin/env python3
"""Compare two application replays without merging them into RigMark v1 cells."""
import argparse
import json
import math
from pathlib import Path

from replay import PROTOCOL, digest, validate, summarise_run, request_outcome


def validate_request_timing(row, due):
    """Check persisted scalar timing against the trace clock and raw events."""
    def finite(value):
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError('non-finite request timing')
        return value

    def equal(key, expected):
        actual = row.get(key)
        if expected is None:
            if actual is not None:
                raise ValueError('request timing differs: ' + key)
        elif abs(finite(actual) - expected) > 1e-6:
            raise ValueError('request timing differs: ' + key)

    start, finish = finite(row['started_s']), finite(row['finished_s'])
    if start < 0 or finish < start:
        raise ValueError('invalid request clock order')
    equal('due_s', due)
    lag = max(0, start - due)
    equal('dispatch_lag_s', lag)
    equal('client_e2e_s', finish - due)
    events = row.get('events', [])
    if not isinstance(events, list):
        raise ValueError('invalid event timeline')
    previous = 0
    for event in events:
        time = finite(event['seconds'])
        if time < previous or time > finish - start + 1e-6:
            raise ValueError('invalid event clock order/bounds')
        previous = time
        for key in ('visible_characters', 'reasoning_characters'):
            if type(event.get(key)) is not int or event[key] < 0:
                raise ValueError('invalid event character count')
        count = event.get('delta_token_count')
        if count is not None and (type(count) is not int or count < 0):
            raise ValueError('invalid event token count')
    visible = [e['seconds'] for e in events if e['visible_characters']]
    reasoning = [e['seconds'] for e in events if e['reasoning_characters']]
    output = [e['seconds'] for e in events if e['visible_characters'] or e['reasoning_characters']]
    equal('first_visible_s', visible[0] if visible else None)
    equal('first_reasoning_s', reasoning[0] if reasoning else None)
    equal('first_output_s', output[0] if output else None)
    equal('user_visible_ttft_s', lag + visible[0] if visible else None)
    equal('longest_visible_delivery_gap_s', max((b-a for a,b in zip(visible,visible[1:])), default=None))
    if 'output' in row and sum(e['visible_characters'] for e in events) != len(row['output']):
        raise ValueError('event output length differs')
    usage = row.get('usage', {})
    valid = all(type(usage.get(k)) is int and usage[k] >= 0 for k in ('prompt_tokens','completion_tokens'))
    exact = bool(valid and events and all(e.get('delta_token_count') is not None for e in events)
                 and sum(e['delta_token_count'] for e in events) == usage['completion_tokens'])
    if row.get('usage_valid', False) is not valid or row.get('token_timeline_exact', False) is not exact:
        raise ValueError('usage/timeline validity differs')


def load_run(path):
    manifest = json.loads((path/'manifest.json').read_text())
    required = ('receipt_version', 'protocol', 'trace_sha256', 'runner_sha256', 'trace',
                'extra_body', 'timeout', 'slo', 'max_dispatch_lag_s', 'identity_sha256', 'identity')
    if any(k not in manifest for k in required):
        raise ValueError('incomplete replay manifest')
    if manifest['protocol'] != PROTOCOL or manifest['receipt_version'] != 1:
        raise ValueError('unsupported replay protocol/receipt version')
    raw = (path/'trace.json').read_bytes()
    trace = validate(json.loads(raw))
    if digest(raw) != manifest['trace_sha256'] or trace != manifest['trace']:
        raise ValueError('trace hash/content differs')
    raw = (path/'identity.json').read_bytes()
    if digest(raw) != manifest['identity_sha256'] or json.loads(raw) != manifest['identity']:
        raise ValueError('identity snapshot differs')
    terminal = json.loads((path/'terminal.json').read_text())
    if terminal.get('status') not in ('COMPLETED', 'COMPLETED_WITH_FAILURES_NO_RETRY'):
        raise ValueError('incomplete replay execution')
    if terminal.get('identity_file_unchanged') is not True:
        raise ValueError('identity file changed')
    rows = [json.loads(p.read_text()) for p in sorted(path.glob('request-*.json'))]
    expected = {(s['id'], i): (s['category'], t['max_tokens'])
                for s in trace['sessions'] for i, t in enumerate(s['turns'])}
    if len(rows) != len(expected):
        raise ValueError('missing/extra request receipts')
    seen = set()
    for r in rows:
        key = (r['session'], r['turn'])
        if key in seen or key not in expected or (r['category'], r['max_tokens']) != expected[key]:
            raise ValueError('duplicate or mismatched request receipt')
        seen.add(key)
        if r['id'] != f"{key[0]}:{key[1]}":
            raise ValueError('request identity differs')
    by_request = {(r['session'], r['turn']): r for r in rows}
    for session in trace['sessions']:
        failed = False
        for i, turn in enumerate(session['turns']):
            row = by_request[(session['id'], i)]
            if failed:
                if row['status'] != 'blocked_by_previous_turn' or 'started_s' in row:
                    raise ValueError('dependent request status differs')
            else:
                expected_status, checks = request_outcome(row, turn)
                if row['status'] != expected_status or row.get('declared_content_checks_pass') is not checks:
                    raise ValueError('request status differs from raw output')
                if 'output' in row and digest(row['output'].encode()) != row.get('output_sha256'):
                    raise ValueError('request output hash differs')
                due = (session['start_s'] if i == 0 else
                       by_request[(session['id'], i-1)]['finished_s'] + turn.get('think_s', 0))
                validate_request_timing(row, due)
                failed = expected_status != 'completed'
    rows.sort(key=lambda r: (r['session'], r['turn']))
    score = summarise_run(rows, manifest['slo'], manifest['max_dispatch_lag_s'])
    if score != json.loads((path/'score.json').read_text()):
        raise ValueError('score differs from raw request receipts')
    if terminal.get('planned') != len(rows) or terminal.get('completed') != sum(r['status']=='completed' for r in rows):
        raise ValueError('terminal counts differ')
    expected_status = ('COMPLETED' if score['client_schedule_valid'] and
                       all(r['status'] == 'completed' for r in rows)
                       else 'COMPLETED_WITH_FAILURES_NO_RETRY')
    if (terminal['status'] != expected_status or
            terminal.get('client_schedule_valid') is not score['client_schedule_valid']):
        raise ValueError('terminal status/validity differs from request receipts')
    if not score['client_schedule_valid']:
        raise ValueError('client failed intended arrival schedule')
    return manifest, score


def compare(control, candidate):
    (left, a), (right, b) = [load_run(p) for p in (control, candidate)]
    manifests, scores = [left, right], [a, b]
    for key in ('protocol','receipt_version','trace_sha256','runner_sha256','extra_body','timeout','slo','max_dispatch_lag_s'):
        if manifests[0][key] != manifests[1][key]:
            raise ValueError('incompatible '+key)
    def cell(a,b):
        return {'control':a,'candidate':b,'delta':b-a if a is not None and b is not None else None}
    def table(a,b):
        result={k:cell(a.get(k),b.get(k)) for k in ('completion_fraction','common_epoch_makespan_s',
                  'completed_requests_per_s','slo_goodput_per_s','slo_fraction_all_planned','reported_completion_tokens')}
        for k in ('visible_ttft_s','e2e_s','longest_visible_gap_s','dispatch_lag_s'):
            result[k]={q:cell(a[k].get(q),b[k].get(q)) for q in ('n','median','max','p95','p99')}
        result['status_counts']={'control':a['status_counts'],'candidate':b['status_counts']}
        return result
    return {'protocol':manifests[0]['protocol'],'trace_sha256':manifests[0]['trace_sha256'],
            'control':str(control),'candidate':str(candidate),'overall':table(*scores),
            'by_category':{k:table(scores[0]['by_category'][k],scores[1]['by_category'][k]) for k in scores[0]['by_category']},
            'scope':'One replay comparison, no significance or promotion claim. Actual outputs and follow-up context can differ. Review request receipts and achieved overlap.'}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('control',type=Path);p.add_argument('candidate',type=Path)
    a=p.parse_args();print(json.dumps(compare(a.control,a.candidate),indent=2,allow_nan=False))
if __name__=='__main__':main()
