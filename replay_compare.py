#!/usr/bin/env python3
"""Compare two application replays without merging them into RigMark v1 cells."""
import argparse
import json
from pathlib import Path

from replay import PROTOCOL, digest, validate, summarise_run, request_outcome


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
