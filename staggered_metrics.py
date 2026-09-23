"""Versioned client-delivery evidence, not a reconstruction of GPU scheduling."""
from __future__ import annotations

import math
import statistics

VERSION = 2
EPS = 0.000012  # Independent six-decimal clock/offset rounding.
DECODE_KEYS = (
    'newcomer_ttft_seconds', 'newcomer_ttft_ratio_vs_solo',
    'newcomer_decode_tokens_per_second', 'incumbent_max_arrival_window_gap_seconds',
    'incumbent_max_p95_gap_seconds', 'incumbent_median_decode_tokens_per_second',
)
PREFILL_KEYS = (
    'newcomer_median_ttft_seconds', 'newcomer_max_ttft_seconds',
    'newcomer_ttft_ratio_vs_solo', 'long_ttft_seconds', 'long_ttft_ratio_vs_solo',
)


def number(value):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError('finite number required, excluding booleans')
    return value


def same(actual, expected, label, tolerance=EPS):
    if isinstance(expected, dict):
        if not isinstance(actual, dict) or actual.keys() != expected.keys():
            raise ValueError(label + ' has missing or unexpected fields')
        for key, value in expected.items():
            same(actual[key], value, label + '.' + key, tolerance)
    elif isinstance(expected, list):
        if not isinstance(actual, list) or len(actual) != len(expected):
            raise ValueError(label + ' list length differs')
        for i, value in enumerate(expected):
            same(actual[i], value, f'{label}[{i}]', tolerance)
    elif expected is None or type(expected) is bool or isinstance(expected, str):
        if type(actual) is not type(expected) or actual != expected:
            raise ValueError(label + ' differs from raw evidence')
    elif type(expected) is int:
        if type(actual) is not int or actual != expected:
            raise ValueError(label + ' count differs from raw evidence')
    elif abs(number(actual) - expected) > tolerance:
        raise ValueError(label + ' differs from raw evidence')


def timeline(row):
    start, first, finish = [number(row[k]) for k in (
        'started_monotonic_seconds', 'first_output_monotonic_seconds',
        'finished_monotonic_seconds')]
    offsets = row['event_seconds']
    if not isinstance(offsets, list) or not offsets:
        raise ValueError('nonempty event timestamps required')
    offsets = [number(v) for v in offsets]
    if offsets[0] < 0 or any(b < a for a, b in zip(offsets, offsets[1:])):
        raise ValueError('negative or reversed event timestamps')
    events = [start + v for v in offsets]
    if first < start or events[-1] > finish + EPS:
        raise ValueError('events outside request lifetime')
    same(row['measured_sse_events'], len(events), 'event count')
    for actual, expected, label in (
        (first, events[0], 'first output'),
        (row['ttft_seconds'], first-start, 'TTFT'),
        (row['wall_seconds'], finish-start, 'wall'),
        (row['decode_seconds'], events[-1]-first, 'decode window'),
        (row['time_to_last_output_seconds'], offsets[-1], 'last output'),
    ):
        same(actual, float(expected), label)
    return dict(start=start, first=first, last=events[-1], finish=finish, events=events)


def window_metrics(row, lo, hi):
    """Full and clipped *inter-event* gaps; final silence is separately censored.

    The legacy ending-in-window statistic remains unchanged in bench.py.
    A response's post-output tail is not another measured inter-event interval.
    """
    if number(hi) < number(lo):
        raise ValueError('reversed observation window')
    start = number(row['started_monotonic_seconds'])
    events = [start + number(v) for v in row['event_seconds']]
    if not events or any(b < a for a, b in zip(events, events[1:])):
        raise ValueError('nonempty ordered events required')
    t = dict(events=events, last=events[-1], finish=number(row['finished_monotonic_seconds']))
    pairs = [(a, b) for a, b in zip(t['events'], t['events'][1:])
             if b > lo and a < hi]
    full = [b-a for a, b in pairs]
    clipped = [min(b, hi)-max(a, lo) for a, b in pairs]
    censored = max(0., min(hi, t['finish']) - max(lo, t['last']))
    return dict(
        intersecting_gap_count=len(pairs),
        max_intersecting_gap_seconds=round(max(full), 6) if full else None,
        max_clipped_gap_seconds=round(max(clipped), 6) if clipped else None,
        terminal_silence_clipped_seconds=round(censored, 6),
        # Client text/reasoning delivery events; never call these tokens.
        output_events_in_window=sum(lo <= e < hi for e in t['events']),
    )


def derived(row, level, direction):
    """Recompute old scalars and the new evidence exclusively from raw streams."""
    solo = row['solo']; timeline(solo)
    single, many_key = (('newcomer', 'incumbents') if direction == 'decode_first'
                        else ('incumbent', 'newcomers'))
    one = row[single]; t = timeline(one)
    many = row[many_key]; ts = [timeline(r) for r in many]
    if len(many) != level-1 and not (direction == 'prefill_first' and not many):
        raise ValueError('wrong number of submitted streams')
    if number(solo['ttft_seconds']) <= 0:
        raise ValueError('positive solo TTFT required')
    all_t = [t, *ts]
    origin = min(r['start'] for r in all_t)
    finish = max(r['finish'] for r in all_t)
    evidence = dict(metrics_version=VERSION, submitted_requests=len(all_t),
        makespan_from_first_submission_seconds=round(finish-origin, 6),
        actual_completion_tokens=sum(r['completion_tokens'] for r in [one, *many]))
    if direction == 'decode_first':
        arrival, end = t['start'], t['first']
        open_count = sum(r['first'] <= arrival < r['finish'] for r in ts)
        output_count = sum(r['first'] <= arrival < r['last'] for r in ts)
        expected = dict(
            overlap_valid=open_count == level-1,
            arrival_after_last_incumbent_first_output_seconds=round(arrival-max(r['first'] for r in ts),6),
            incumbents_finished_before_newcomer_first_output=sum(r['finish'] <= end for r in ts),
            newcomer_ttft_seconds=one['ttft_seconds'],
            newcomer_solo_ttft_seconds=solo['ttft_seconds'],
            newcomer_ttft_ratio_vs_solo=round(one['ttft_seconds']/solo['ttft_seconds'],3),
            newcomer_decode_tokens_per_second=one['decode_tokens_per_second'],
            newcomer_wall_seconds=one['wall_seconds'],
            incumbent_median_decode_tokens_per_second=round(statistics.median(r['decode_tokens_per_second'] for r in many),3),
        )
        windows = [window_metrics(r, arrival, end) for r in many]
        evidence.update(open_incumbent_streams_at_arrival=open_count,
            incumbents_with_output_after_arrival=output_count,
            all_incumbents_output_live=output_count == level-1,
            elapsed_before_newcomer_seconds=round(arrival-origin,6),
            makespan_after_newcomer_seconds=round(finish-arrival,6),
            incumbent_windows=windows)
    else:
        count = sum(t['start'] <= r['start'] < t['first'] for r in ts)
        long_solo = number(row['long_solo_ttft_seconds'])
        if long_solo <= 0:
            raise ValueError('positive long solo TTFT required')
        expected = dict(overlap_valid=len(many) == level-1 and count == level-1,
            newcomers_started_during_prefill=count,
            long_ttft_seconds=one['ttft_seconds'],
            long_ttft_ratio_vs_solo=round(one['ttft_seconds']/long_solo,3),
            short_solo_ttft_seconds=solo['ttft_seconds'])
        if many:
            median = statistics.median(r['ttft_seconds'] for r in many)
            expected.update(newcomer_median_ttft_seconds=round(median,6),
                newcomer_max_ttft_seconds=round(max(r['ttft_seconds'] for r in many),6),
                newcomer_ttft_ratio_vs_solo=round(median/solo['ttft_seconds'],3),
                newcomer_median_decode_tokens_per_second=round(statistics.median(r['decode_tokens_per_second'] for r in many),3))
        evidence.update(short_requests_submitted_during_long_ttft=count,
            short_submission_offsets_seconds=[round(r['start']-origin,6) for r in ts],
            short_ttft_seconds=[r['ttft_seconds'] for r in many])
    return expected, evidence


def add_evidence(row, level, direction):
    expected, row['evidence'] = derived(row, level, direction)
    # Validity includes request start ordering as well as stream completion.
    row['overlap_valid'] = expected['overlap_valid']
    return row
