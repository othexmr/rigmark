#!/usr/bin/env python3
"""Application workload companion to RigMark. No server lifecycle operations."""
from __future__ import annotations
import argparse
import codecs
import hashlib
import http.client
import io
import json
import math
import os
from pathlib import Path
import statistics
import threading
import time
from urllib.parse import urlsplit

from token_timeline import TokenTimeline

PROTOCOL = 'rigmark-application-replay.1'
# Opt-in exact completion-token delivery, shared with bench.py's staggered suite (docs/token-delivery.md).
DELIVERY_MODES = ('off', 'usage', 'ids')


def digest(value):
    return hashlib.sha256(value).hexdigest()


def save(path, value):
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')
    tmp.replace(path)


def number(value, name, maximum=3600):
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= maximum:
        raise ValueError(f'invalid {name}')
    return value


def validate(trace):
    if trace.get('protocol') != PROTOCOL:
        raise ValueError('wrong trace protocol')
    if trace.get('cache_policy') not in ('natural', 'run-isolated'):
        raise ValueError('declare cache policy')
    if not isinstance(trace.get('system'), str) or not trace['system']:
        raise ValueError('system text required')
    if not isinstance(trace.get('provenance'), dict) or not trace['provenance'].get('kind'):
        raise ValueError('declare workload provenance; examples are not production traces')
    sessions = trace.get('sessions')
    if not isinstance(sessions, list) or not 1 <= len(sessions) <= 128:
        raise ValueError('require 1..128 sessions')
    ids = set(); total = 0
    for s in sessions:
        sid = s.get('id')
        if not isinstance(sid, str) or not sid or sid in ids:
            raise ValueError('unique nonempty session IDs required')
        ids.add(sid)
        if not isinstance(s.get('category'), str) or not s['category']:
            raise ValueError('category required')
        number(s.get('start_s'), 'start_s')
        if not isinstance(s.get('turns'), list) or not 1 <= len(s['turns']) <= 32:
            raise ValueError('require 1..32 turns per session')
        total += len(s['turns'])
        for i, turn in enumerate(s['turns']):
            if not isinstance(turn.get('user'), str) or not turn['user'].strip():
                raise ValueError('user text required, no synthetic token padding')
            number(turn.get('think_s', 0), 'think_s', 300)
            if i == 0 and turn.get('think_s', 0) != 0:
                raise ValueError('first turn uses start_s, not think_s')
            if type(turn.get('max_tokens')) is not int or not 1 <= turn['max_tokens'] <= 16384:
                raise ValueError('invalid output cap')
            checks = turn.get('contains_all', [])
            if not isinstance(checks, list) or not all(isinstance(v, str) and v for v in checks):
                raise ValueError('invalid contains_all checks')
            if 'exact_answer' in turn:
                answer = turn['exact_answer']
                if (not isinstance(answer, str) or not answer.strip() or answer != answer.strip()
                        or len(answer.splitlines()) != 1 or 'ANSWER:' in answer):
                    raise ValueError('invalid exact_answer check')
    if total > 2048:
        raise ValueError('trace too large for this bounded client')
    return trace


class SSE:
    """Incremental UTF-8 SSE framing; chunks and events are never called tokens."""
    def __init__(self):
        self.decoder = codecs.getincrementaldecoder('utf-8')('strict')
        self.pending = ''; self.data = []; self.bytes = 0

    def feed(self, raw, final=False):
        self.bytes += len(raw)
        if self.bytes > 32 * 1024 * 1024:
            raise ValueError('response exceeds 32 MiB client bound')
        self.pending += self.decoder.decode(raw, final=final)
        result = []
        while '\n' in self.pending:
            line, self.pending = self.pending.split('\n', 1)
            line = line.removesuffix('\r')
            if line == '':
                if self.data:
                    result.append('\n'.join(self.data)); self.data = []
            elif line.startswith('data:'):
                self.data.append(line[5:].removeprefix(' '))
        if final and (self.pending or self.data):
            raise ValueError('unterminated SSE frame')
        return result


class StreamState:
    def __init__(self, delivery='off'):
        if delivery not in DELIVERY_MODES:
            raise ValueError('unsupported delivery token mode')
        self.events = []; self.parts = []; self.usage = {}; self.finish = None
        self.done = False; self.total_events = 0; self.delivery = delivery
        self.timeline = None if delivery == 'off' else TokenTimeline(delivery)

    def observe(self, raw, elapsed):
        if raw == '[DONE]':
            self.done = True; return
        event = json.loads(raw)
        if not isinstance(event, dict) or event.get('error'):
            raise ValueError('invalid/error SSE event')
        self.total_events += 1
        if self.total_events > 100000:
            raise ValueError('SSE event bound exceeded')
        if isinstance(event.get('usage'), dict):
            self.usage = event['usage']
        choices = event.get('choices') or []
        if not choices:
            return
        if len(choices) != 1 or choices[0].get('index', 0) != 0:
            raise ValueError('only one index-zero choice supported')
        c = choices[0]; delta = c.get('delta') or {}
        content = delta.get('content') or c.get('text') or ''
        reasoning = delta.get('reasoning_content') or delta.get('reasoning') or ''
        if not isinstance(content, str) or not isinstance(reasoning, str):
            raise ValueError('non-text delta')
        if c.get('finish_reason') is not None:
            self.finish = c['finish_reason']
        if self.timeline is not None:
            # Same observation rule as bench.py: every choice-bearing chunk, text or not.
            self.timeline.observe(c, event.get('usage'), elapsed, bool(content or reasoning))
        ids = c.get('token_ids')
        count = len(ids) if isinstance(ids, list) and all(type(i) is int and i >= 0 for i in ids) else None
        if content or reasoning or count:
            self.events.append({'seconds': elapsed, 'visible_characters': len(content),
                                'reasoning_characters': len(reasoning), 'delta_token_count': count})
        self.parts.append(content)

    def result(self):
        visible = [e['seconds'] for e in self.events if e['visible_characters']]
        text = [e['seconds'] for e in self.events if e['visible_characters'] or e['reasoning_characters']]
        output = ''.join(self.parts)
        completion = self.usage.get('completion_tokens')
        prompt = self.usage.get('prompt_tokens')
        usage_valid = all(type(v) is int and v >= 0 for v in (completion, prompt))
        exact = usage_valid and bool(self.events) and all(e['delta_token_count'] is not None for e in self.events)
        exact = bool(exact and sum(e['delta_token_count'] for e in self.events) == completion)
        delivery = None
        if self.timeline is not None:
            delivery = self.timeline.finish(completion if usage_valid else None)
            exact = delivery['status'] == 'EXACT_COMPLETION_TOKEN_COUNTS'
        return {'events': self.events, 'first_output_s': next((e['seconds'] for e in self.events
                 if e['visible_characters'] or e['reasoning_characters']), None),
                'first_visible_s': visible[0] if visible else None,
                'first_reasoning_s': next((e['seconds'] for e in self.events if e['reasoning_characters']), None),
                'longest_visible_delivery_gap_s': max((b-a for a,b in zip(visible,visible[1:])), default=None),
                'longest_output_delivery_gap_s': max((b-a for a,b in zip(text,text[1:])), default=None),
                'output': output, 'output_sha256': digest(output.encode()),
                'usage': self.usage, 'usage_valid': usage_valid,
                'token_timeline_exact': exact, 'token_delivery': delivery,
                'delivery_token_accounting': self.delivery, 'finish_reason': self.finish,
                'done': self.done, 'total_sse_events': self.total_events}


class DeadlineReader(io.RawIOBase):
    """Bound each underlying receive, including reads inside header parsing."""
    def __init__(self, raw, sock, deadline):
        super().__init__()
        self.raw, self.sock, self.deadline = raw, sock, deadline

    def readable(self):
        return True

    def readinto(self, buffer):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('total request deadline')
        self.sock.settimeout(remaining)
        return self.raw.readinto(buffer)

    def close(self):
        try:
            self.raw.close()
        finally:
            super().close()


class DeadlineResponse(http.client.HTTPResponse):
    def __init__(self, sock, *args, deadline, **kwargs):
        super().__init__(sock, *args, **kwargs)
        # HTTPResponse has not read yet. Preserve the socket-file ownership while
        # placing the deadline below buffering, where slow partial lines recur.
        raw = self.fp.detach()
        self.fp = io.BufferedReader(DeadlineReader(raw, sock, deadline))


class Client:
    def __init__(self, base_url, model, timeout=180, api_key='', delivery='off'):
        u = urlsplit(base_url.rstrip('/'))
        if u.scheme not in ('http', 'https') or not u.hostname or u.username or u.password or u.query or u.fragment:
            raise ValueError('absolute HTTP(S) base URL, no embedded credentials/query/fragment')
        if delivery not in DELIVERY_MODES:
            raise ValueError('unsupported delivery token mode')
        self.url = u; self.model = model; self.timeout = timeout; self.key = api_key; self.delivery = delivery

    def stream(self, messages, max_tokens, extra_body):
        u = self.url
        cls = http.client.HTTPSConnection if u.scheme == 'https' else http.client.HTTPConnection
        conn = cls(u.hostname, u.port, timeout=self.timeout)
        prefix = u.path.rstrip('/').removesuffix('/v1')
        payload = {'model': self.model, 'messages': messages, 'temperature': 0,
                   'max_tokens': max_tokens, 'stream': True, 'stream_options': {'include_usage': True}, **extra_body}
        if self.delivery == 'usage':
            payload['stream_options'] = {'include_usage': True, 'continuous_usage_stats': True}
        elif self.delivery == 'ids':
            payload['return_token_ids'] = True
        encoded = json.dumps(payload).encode()
        headers = {'Content-Type': 'application/json', 'Accept': 'text/event-stream'}
        if self.key: headers['Authorization'] = 'Bearer ' + self.key
        started = time.monotonic(); state = StreamState(self.delivery); parser = SSE(); failure = None
        response = None
        conn.response_class = lambda *args, **kwargs: DeadlineResponse(
            *args, deadline=started+self.timeout, **kwargs)
        try:
            conn.connect()
            def budget():
                remaining = self.timeout - (time.monotonic() - started)
                if remaining <= 0: raise TimeoutError('total request deadline')
                if conn.sock is not None: conn.sock.settimeout(remaining)
            budget()
            conn.request('POST', prefix + '/v1/chat/completions', encoded, headers)
            budget()
            response = conn.getresponse()
            if response.status != 200:
                raise RuntimeError(f'HTTP {response.status}')
            if 'text/event-stream' not in response.getheader('Content-Type', ''):
                raise ValueError('response is not SSE')
            while not state.done:
                remaining = self.timeout - (time.monotonic() - started)
                if remaining <= 0: raise TimeoutError('total request deadline')
                chunk = response.read1(65536)
                observed = time.monotonic() - started
                if observed > self.timeout: raise TimeoutError('total request deadline')
                for frame in parser.feed(chunk, final=not chunk):
                    state.observe(frame, observed)
                    if state.done: break
                if not chunk: break
            if not state.done: raise ValueError('stream ended without DONE')
        except Exception as error:
            failure = {'type': type(error).__name__, 'message': str(error)}
        finally:
            if response is not None:
                response.close()
            conn.close()
        row = state.result()
        row.update(started=started, finished=time.monotonic(), error=failure,
                   request_sha256=digest(encoded), sse_bytes=parser.bytes)
        return row


def declared_check_count(turn):
    return len(turn.get('contains_all', [])) + int('exact_answer' in turn)


def request_outcome(row, turn):
    checks = all(t in row.get('output', '') for t in turn.get('contains_all', []))
    if 'exact_answer' in turn:
        text = row.get('output', '').strip()
        checks = (checks and text.count('ANSWER:') == 1
                  and text.splitlines()[-1].strip() == 'ANSWER: ' + turn['exact_answer'])
    if row.get('error') or not row.get('done'):
        status = 'error'
    elif row.get('finish_reason') == 'length':
        status = 'truncated'
    elif row.get('finish_reason') != 'stop' or not row.get('output', '').strip() or not checks:
        status = 'invalid_answer'
    else:
        status = 'completed'
    return status, checks


def execute(trace, client, output, run_id, extra_body=None):
    """One thread/session, no concurrency semaphore or all-first-token barrier."""
    validate(trace); extra_body = extra_body or {}
    if set(extra_body) - {'chat_template_kwargs'}:
        raise ValueError('only chat_template_kwargs accepted as extra body')
    results = []; write_errors = []; lock = threading.Lock(); ready = threading.Barrier(len(trace['sessions']) + 1)
    epoch_box = []
    def session(s):
        ready.wait(); epoch = epoch_box[0]
        system = trace['system']
        if trace['cache_policy'] == 'run-isolated':
            system = 'Replay namespace: ' + run_id + '\n' + system
        history = [{'role': 'system', 'content': system}]
        previous_end = None; failed = False
        for index, turn in enumerate(s['turns']):
            row = {'id': f"{s['id']}:{index}", 'session': s['id'], 'turn': index,
                   'category': s['category'], 'max_tokens': turn['max_tokens'],
                   'declared_content_checks': declared_check_count(turn)}
            if failed:
                row.update(status='blocked_by_previous_turn', error={'type': 'DependencyFailure'})
            else:
                due = epoch + s['start_s'] if index == 0 else previous_end + turn.get('think_s', 0)
                time.sleep(max(0, due - time.monotonic()))
                history.append({'role': 'user', 'content': turn['user']})
                try:
                    response = client.stream(history, turn['max_tokens'], extra_body)
                except Exception as error:
                    now = time.monotonic()
                    response = {'started': now, 'finished': now, 'error': {'type': type(error).__name__, 'message': str(error)}}
                row.update(response)
                row['due_s'] = due - epoch
                row['dispatch_lag_s'] = max(0, row['started'] - due)
                row['started_s'] = row.pop('started') - epoch
                previous_end = row.pop('finished'); row['finished_s'] = previous_end - epoch
                row['client_e2e_s'] = previous_end - due
                row['user_visible_ttft_s'] = (row['dispatch_lag_s'] + row['first_visible_s']
                                              if row.get('first_visible_s') is not None else None)
                row['user_output_ttft_s'] = (row['dispatch_lag_s'] + row['first_output_s']
                                             if row.get('first_output_s') is not None else None)
                status, checks = request_outcome(row, turn)
                row['declared_content_checks_pass'] = checks
                row['status'] = status
                failed = status != 'completed'
                history.append({'role': 'assistant', 'content': row.get('output', '')})
            with lock:
                try:
                    save(output / f"request-{len(results)+1:04d}.json", row)
                except Exception as error:
                    write_errors.append(error)
                    return
                results.append(row)
    threads = [threading.Thread(target=session, args=(s,), daemon=False) for s in trace['sessions']]
    for t in threads: t.start()
    epoch_box.append(time.monotonic() + 0.1)
    ready.wait()
    for t in threads: t.join()
    if write_errors:
        raise RuntimeError('request receipt write failed') from write_errors[0]
    if len(results) != sum(len(s['turns']) for s in trace['sessions']):
        raise RuntimeError('missing request receipts')
    return sorted(results, key=lambda r: (r['session'], r['turn']))


def distribution(values):
    values = sorted(v for v in values if v is not None)
    if not values: return {'n': 0}
    out = {'n': len(values), 'median': statistics.median(values), 'max': max(values)}
    for percentile, minimum in ((95,20),(99,100)):
        if len(values) >= minimum:
            out[f'p{percentile}'] = values[math.ceil(len(values)*percentile/100)-1]
    return out


def interference(rows):
    """Client-observed delivery overlap. This does not identify GPU decode state."""
    result = []
    for arrival in rows:
        if arrival.get('first_output_s') is None or 'started_s' not in arrival: continue
        start = arrival['started_s']; end = start + arrival['first_output_s']
        incumbents = []
        for r in rows:
            if r is arrival or 'started_s' not in r or not r['started_s'] < start < r['finished_s']: continue
            times = [r['started_s']+e['seconds'] for e in r.get('events',[]) if e['visible_characters']]
            gaps = [b-a for a,b in zip(times,times[1:]) if a < end and b > start]
            before = [t for t in times if t <= start]
            censored = max(0, min(end,r['finished_s']) - max(start,times[-1])) if times else None
            counts = None
            delivery = r.get('token_delivery') or {}
            if delivery.get('status') == 'EXACT_COMPLETION_TOKEN_COUNTS':
                counts = sum(n for t, n in zip(delivery['event_seconds'], delivery['event_token_counts'])
                             if start <= r['started_s'] + t <= end)
            elif r.get('token_timeline_exact'):
                counts = sum(e['delta_token_count'] for e in r['events'] if start <= r['started_s']+e['seconds'] <= end)
            incumbents.append({'id': r['id'], 'visible_before_arrival': bool(before),
                'longest_intersecting_visible_gap_s': max(gaps,default=None),
                'right_censored_silence_in_window_s': censored,
                'visible_delivery_events_during_wait': sum(start <= t <= end for t in times),
                'generated_tokens_delivered_during_wait': counts})
        result.append({'arrival': arrival['id'], 'wait_to_first_output_s': end-start,
                       'outstanding_at_arrival': len(incumbents), 'incumbents': incumbents})
    return result


# SLO basis: 'visible' times the first answer text and gaps between answer deltas (the original definition);
# 'output' also counts reasoning deltas, for applications that stream a reasoning model's thinking to the user.
SLO_BASES = ('visible', 'output')
SLO_FIELDS = {'visible': ('user_visible_ttft_s', 'longest_visible_delivery_gap_s'),
              'output': ('user_output_ttft_s', 'longest_output_delivery_gap_s')}


def slo_basis(slo):
    basis = slo.get('basis', 'visible')         # absent in older receipts: visible
    if basis not in SLO_BASES:
        raise ValueError('unknown SLO basis')
    return basis


def score(rows, slo=None):
    attempted = [r for r in rows if 'started_s' in r]
    completed = [r for r in rows if r['status'] == 'completed']
    span = max((r['finished_s'] for r in attempted), default=0)
    statuses = {s: sum(r['status']==s for r in rows) for s in sorted({r['status'] for r in rows})}
    passed = []
    if slo:
        ttft, gap = SLO_FIELDS[slo_basis(slo)]
        passed = [r for r in completed if r.get(ttft) is not None
                  and r[ttft] <= slo['visible'] and r['client_e2e_s'] <= slo['total']
                  and (r.get(gap) or 0) <= slo['gap']]
    token_total = sum(r['usage']['completion_tokens'] for r in attempted if r.get('usage_valid'))
    timeline = sorted([(r['started_s'],1) for r in attempted] + [(r['finished_s'],-1) for r in attempted])
    outstanding = peak = 0
    for _, delta in timeline:
        outstanding += delta; peak = max(peak,outstanding)
    return {'planned_requests': len(rows), 'attempted_requests': len(attempted), 'status_counts': statuses,
            'completion_fraction': len(completed)/len(rows), 'common_epoch_makespan_s': span,
            'peak_client_outstanding': peak,
            'prompt_tokens': distribution([r['usage'].get('prompt_tokens') for r in attempted if r.get('usage_valid')]),
            'completion_tokens': distribution([r['usage'].get('completion_tokens') for r in attempted if r.get('usage_valid')]),
            'completed_requests_per_s': len(completed)/span if span else None,
            'reported_completion_tokens': token_total,
            'usage_coverage_requests': sum(bool(r.get('usage_valid')) for r in attempted),
            'completion_tokens_per_s': token_total/span if span and attempted and all(r.get('usage_valid') for r in attempted) else None,
            'visible_ttft_s': distribution([r.get('user_visible_ttft_s') for r in attempted]),
            'output_ttft_s': distribution([r.get('user_output_ttft_s') for r in attempted]),
            'e2e_s': distribution([r.get('client_e2e_s') for r in attempted]),
            'dispatch_lag_s': distribution([r.get('dispatch_lag_s') for r in attempted]),
            'longest_visible_gap_s': distribution([r.get('longest_visible_delivery_gap_s') for r in attempted]),
            'longest_output_gap_s': distribution([r.get('longest_output_delivery_gap_s') for r in attempted]),
            # Declared content checks (e.g. the quality sanity set): every planned request that declares checks is
            # in the denominator; blocked or failed requests count as misses.
            'content_checks_declared_requests': sum(bool(r.get('declared_content_checks')) for r in rows),
            'content_checks_pass_fraction': (sum(r.get('status') == 'completed' and r.get('declared_content_checks_pass') is True for r in rows
                                                 if r.get('declared_content_checks')) /
                                             sum(bool(r.get('declared_content_checks')) for r in rows)
                                             if any(r.get('declared_content_checks') for r in rows) else None),
            'token_delivery_exact_requests': sum((r.get('token_delivery') or {}).get('status') ==
                                                 'EXACT_COMPLETION_TOKEN_COUNTS' for r in attempted),
            'client_sse_events': sum(r.get('total_sse_events') or 0 for r in attempted),
            'client_sse_bytes': sum(r.get('sse_bytes') or 0 for r in attempted),
            'slo': slo, 'slo_good_requests': len(passed) if slo else None,
            'slo_goodput_per_s': len(passed)/span if slo and span else None,
            'slo_fraction_all_planned': len(passed)/len(rows) if slo else None,
            'interference': interference(rows),
            'semantics': 'Client delivery gaps, not token ITL; usage counts include reasoning. Missing token timelines remain null. Latencies include failed attempts where observable; all failures retained in denominator.'}


def summarise_run(rows, slo, max_dispatch_lag):
    summary = score(rows, slo)
    summary['client_schedule_valid'] = summary['dispatch_lag_s'].get('max', float('inf')) <= max_dispatch_lag
    summary['by_category'] = {c: score([r for r in rows if r['category'] == c], slo)
                              for c in sorted({r['category'] for r in rows})}
    return summary


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--trace', type=Path, required=True)
    ap.add_argument('--output', type=Path)
    ap.add_argument('--base-url')
    ap.add_argument('--model')
    ap.add_argument('--identity', type=Path)
    ap.add_argument('--run-id')
    ap.add_argument('--timeout', type=float, default=180)
    ap.add_argument('--extra-body', default='{}')
    ap.add_argument('--slo-visible', type=float)
    ap.add_argument('--slo-gap', type=float)
    ap.add_argument('--slo-total', type=float)
    ap.add_argument('--slo-basis', choices=SLO_BASES, default='visible',
                    help="first-output and gap basis of the SLO: visible answer text only, or any output including reasoning")
    ap.add_argument('--max-dispatch-lag', type=float, default=0.05, help='client validity bound in seconds; never silently throttle arrivals')
    ap.add_argument('--delivery-tokens', choices=DELIVERY_MODES, default='off',
                    help='exact completion-token delivery accounting (docs/token-delivery.md); off keeps plain requests')
    ap.add_argument('--run', action='store_true')
    a = ap.parse_args(); raw = a.trace.read_bytes(); trace = validate(json.loads(raw))
    number(a.timeout, 'timeout', 3600)
    number(a.max_dispatch_lag, 'max dispatch lag')
    if a.timeout == 0: ap.error('timeout must be positive')
    extra = json.loads(a.extra_body)
    if not isinstance(extra, dict) or set(extra) - {'chat_template_kwargs'}: ap.error('only chat_template_kwargs allowed')
    thresholds = (a.slo_visible, a.slo_gap, a.slo_total)
    slo = None
    if a.slo_basis != 'visible' and any(v is None for v in thresholds):
        ap.error('--slo-basis needs the three SLO thresholds')
    if any(v is not None for v in thresholds):
        if any(v is None for v in thresholds): ap.error('supply all three SLO thresholds')
        for v in thresholds: number(v, 'SLO')
        slo = dict(zip(('visible','gap','total'),thresholds))
        if a.slo_basis != 'visible': slo['basis'] = a.slo_basis   # visible receipts stay as before
    if not a.run:
        print(json.dumps({'status':'VALIDATED_NO_REQUESTS', 'trace_sha256':digest(raw),
                          'sessions':len(trace['sessions']), 'turns':sum(len(s['turns']) for s in trace['sessions'])})); return 0
    if not all((a.output,a.base_url,a.model,a.identity,a.run_id)):
        ap.error('--run requires output, base-url, model, identity, run-id')
    identity_raw = a.identity.read_bytes(); identity = json.loads(identity_raw)
    client = Client(a.base_url,a.model,a.timeout,os.getenv('OPENAI_API_KEY',''),a.delivery_tokens)
    a.output.mkdir(parents=True, exist_ok=False)
    (a.output/'trace.json').write_bytes(raw)
    (a.output/'identity.json').write_bytes(identity_raw)
    save(a.output/'manifest.json', {'receipt_version':1, 'protocol':PROTOCOL, 'trace_sha256':digest(raw), 'trace':trace,
        'runner_sha256':digest(Path(__file__).read_bytes()), 'identity':identity, 'identity_sha256':digest(identity_raw),
        'model':a.model,'run_id':a.run_id,'extra_body':extra,'timeout':a.timeout,
        'max_dispatch_lag_s':a.max_dispatch_lag, 'delivery_token_accounting':a.delivery_tokens,
        'cache_claim':'No cache flush. Run-isolated prefix is not proof of coldness.', 'slo':slo})
    try:
        cpu_started = time.process_time()
        rows = execute(trace,client,a.output,a.run_id,extra)
        client_cpu = time.process_time() - cpu_started
        summary = summarise_run(rows, slo, a.max_dispatch_lag)
        save(a.output/'score.json',summary)
        identity_unchanged = a.identity.read_bytes()==identity_raw
        passed = identity_unchanged and summary['client_schedule_valid'] and all(r['status']=='completed' for r in rows)
        save(a.output/'terminal.json',{'status':'COMPLETED' if passed else 'COMPLETED_WITH_FAILURES_NO_RETRY',
             'identity_file_unchanged':identity_unchanged,'client_schedule_valid':summary['client_schedule_valid'],'identity_scope':'supplied receipt only; owner must verify live runtime',
             'completed':sum(r['status']=='completed' for r in rows),'planned':len(rows),
             # Whole-process CPU of this client during the run (all threads): an overhead receipt, not a server metric.
             'client_cpu_seconds':round(client_cpu,6),
             'client_cpu_us_per_sse_event':(round(1e6*client_cpu/summary['client_sse_events'],3)
                                            if summary['client_sse_events'] else None)})
        return 0 if passed else 2
    except Exception as e:
        save(a.output/'terminal.json',{'status':'FAILED_NO_RETRY','type':type(e).__name__,'message':str(e)})
        return 2

if __name__ == '__main__':
    raise SystemExit(main())
