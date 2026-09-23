#!/usr/bin/env python3
"""Prepare inspectable application traces from explicitly selected local text."""
import argparse
import hashlib
import json
import math
import random
from pathlib import Path
from replay import PROTOCOL, validate

QUESTIONS = [
    # Revised 2026-09-23: the original wording had no way out when the code has no such bug, and GLM-5.3 at
    # reasoning_effort low then reasoned past a 4,096-token budget without a visible answer (4 of 4 such first turns in
    # one validation run). The task now names the no-bug outcome and bounds the answer.
    ('code', 'Review this code for a cancellation or concurrency bug. If you find one, describe one concrete failing sequence and the smallest fix; if you find none, say so and name the riskiest code path. Avoid style changes. Keep the answer under 300 words.'),
    ('document', 'Summarize the supplied material for an engineer joining the project. Give the main constraint, the evidence behind it and one unresolved question.'),
    ('code', 'Propose three focused regression tests for this implementation. Include inputs and expected outcomes; explain which failure each catches.'),
    ('document', 'Explain the operational trade-offs in the material in plain language. Separate measured findings from assumptions; do not invent numbers.'),
]
FOLLOWUPS = [
    'Now challenge your answer: which claim has the weakest evidence? Revise it and give one next check.',
    'Turn that into a concise handoff with the next action and its acceptance criteria.',
]
# Output budgets (max_tokens) per turn kind. The defaults are the original trace budgets. A reasoning model can spend a
# 1,024-token budget on reasoning alone and return no visible answer (status 'truncated'), so a replay of such a model
# declares larger budgets explicitly. The trace records the budgets it was prepared with.
DEFAULT_BUDGETS = {'initial': 1024, 'long': 1536, 'followup': 768}


def budgets(initial=None, long=None, followup=None):
    b = dict(DEFAULT_BUDGETS)
    for key, value in (('initial', initial), ('long', long), ('followup', followup)):
        if value is not None:
            if type(value) is not int or not 16 <= value <= 16384:
                raise ValueError(f'{key} output budget must be an integer in [16, 16384]')
            b[key] = value
    return b


def material(paths):
    texts=[]; provenance=[]
    for path in paths:
        raw=path.read_bytes(); text=raw.decode('utf-8')
        if not text.strip(): raise ValueError('empty input: '+str(path))
        texts.append(f'File: {path.name}\n```text\n{text}\n```')
        provenance.append({'name':path.name,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)})
    return '\n\n'.join(texts), provenance


def prepare(code, document, contexts, users, direction, delay, cache, output_budgets=None):
    c, pc=material([code]); d,pd=material([document]); long,pl=material(contexts)
    b=output_budgets or budgets()
    sessions=[]
    for i in range(users):
        is_long = i == users-1
        category, question = QUESTIONS[i%len(QUESTIONS)]
        if is_long:
            category='long-review'
            question='Review the complete material. Identify cross-file correctness risks, cite the relevant file names, then prioritize three actionable fixes. Do not repeat the material.'
        content=long if is_long else c if category=='code' else d
        # Context comes first so repeat users can exercise genuine shared-prefix reuse.
        user=content+'\n\nTask:\n'+question
        if direction=='long-first': start=0 if is_long else delay
        elif direction=='short-first': start=delay if is_long else 0
        else: start=[0,0.35,1.2,1.25,3.0,4.5][i%6]+(i//6)*5.0
        turns=[{'user':user,'max_tokens':b['long'] if is_long else b['initial'],'think_s':0}]
        if direction=='sessions':
            turns += [{'user':q,'max_tokens':b['followup'],'think_s':float(2+(i+j)%5)} for j,q in enumerate(FOLLOWUPS)]
        sessions.append({'id':f'user-{i+1:02d}','start_s':start,'category':category,'turns':turns})
    return validate({'protocol':PROTOCOL,'label':f'{direction}-{users}-users',
        'provenance':{'kind':'authored_application_scenario_not_production_trace',
                      'inputs':pc+pd+pl,'padding':'none','token_lengths':'measured from server usage, not assumed',
                      'output_budgets':b},
        'cache_policy':cache,'system':'Help an engineer solve the task using only the supplied material. Be precise and concise. Admit missing evidence.',
        'sessions':sessions})


def open_loop_starts(rate, duration, seed):
    """Seeded Poisson arrivals: one unit-rate exponential sequence scaled by 1/rate, so every rate replays the same
    prompts in the same order and only compresses time."""
    if not (type(rate) in (int, float) and math.isfinite(rate) and rate > 0):
        raise ValueError('rate must be a positive request rate')
    if not (type(duration) in (int, float) and math.isfinite(duration) and 0 < duration <= 3600):
        raise ValueError('duration must be in (0, 3600] seconds')
    rng = random.Random(seed); unit = 0.0; starts = []
    while True:
        unit += rng.expovariate(1.0)
        start = unit / rate
        if start >= duration:
            return starts
        starts.append(round(start, 6))
        if len(starts) > 128:
            raise ValueError('more than 128 arrivals; lower rate x duration (bounded client)')


def prepare_open_loop(code, document, contexts, rate, duration, seed, long_every, cache, output_budgets=None):
    """Single-turn arrivals at a seeded Poisson rate; every long_every-th arrival is the long-context review."""
    c, pc=material([code]); d,pd=material([document]); long,pl=material(contexts)
    b=output_budgets or budgets()
    if type(long_every) is not int or long_every < 0:
        raise ValueError('long_every must be a nonnegative integer')
    sessions=[]
    for i, start in enumerate(open_loop_starts(rate, duration, seed)):
        is_long = long_every > 0 and (i + 1) % long_every == 0
        category, question = QUESTIONS[i%len(QUESTIONS)]
        if is_long:
            category='long-review'
            question='Review the complete material. Identify cross-file correctness risks, cite the relevant file names, then prioritize three actionable fixes. Do not repeat the material.'
        content=long if is_long else c if category=='code' else d
        sessions.append({'id':f'arrival-{i+1:03d}','start_s':start,'category':category,
                         'turns':[{'user':content+'\n\nTask:\n'+question,'max_tokens':b['long'] if is_long else b['initial'],'think_s':0}]})
    if not sessions:
        raise ValueError('no arrivals in the window; raise rate x duration')
    return validate({'protocol':PROTOCOL,'label':f'open-loop-{rate:g}rps-{duration:g}s-seed{seed}',
        'provenance':{'kind':'authored_application_scenario_not_production_trace','inputs':pc+pd+pl,'padding':'none',
                      'arrivals':{'process':'poisson','rate_per_s':rate,'duration_s':duration,'seed':seed,
                                  'long_every':long_every},
                      'token_lengths':'measured from server usage, not assumed','output_budgets':{'initial':b['initial'],'long':b['long']}},
        'cache_policy':cache,'system':'Help an engineer solve the task using only the supplied material. Be precise and concise. Admit missing evidence.',
        'sessions':sessions})


def add_budget_arguments(p):
    p.add_argument('--max-tokens',type=int,help=f"output budget of each first turn (default {DEFAULT_BUDGETS['initial']})")
    p.add_argument('--long-max-tokens',type=int,help=f"output budget of the long-context review (default {DEFAULT_BUDGETS['long']})")
    p.add_argument('--followup-max-tokens',type=int,help=f"output budget of each follow-up turn (default {DEFAULT_BUDGETS['followup']})")


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--code',type=Path,required=True)
    p.add_argument('--document',type=Path,required=True)
    p.add_argument('--context',type=Path,action='append',required=True)
    p.add_argument('--users',type=int,default=6)
    p.add_argument('--direction',choices=('short-first','long-first','sessions','open-loop'),default='sessions')
    p.add_argument('--rate',type=float,help='open-loop: Poisson arrival rate in requests per second')
    p.add_argument('--duration',type=float,default=60,help='open-loop: arrival window in seconds')
    p.add_argument('--seed',type=int,default=1,help='open-loop: arrival seed (same seed = same prompts and order)')
    p.add_argument('--long-every',type=int,default=6,help='open-loop: every Nth arrival is the long review (0 = none)')
    p.add_argument('--delay',type=float,default=1)
    p.add_argument('--cache-policy',choices=('natural','run-isolated'),default='run-isolated')
    p.add_argument('--output',type=Path,required=True)
    add_budget_arguments(p)
    a=p.parse_args()
    try: b=budgets(a.max_tokens,a.long_max_tokens,a.followup_max_tokens)
    except ValueError as error: p.error(str(error))
    if a.direction=='open-loop':
        if a.rate is None: p.error('open-loop needs --rate')
        result=prepare_open_loop(a.code,a.document,a.context,a.rate,a.duration,a.seed,a.long_every,a.cache_policy,b)
    else:
        if not 2 <= a.users <= 128: p.error('users must be 2..128')
        result=prepare(a.code,a.document,a.context,a.users,a.direction,a.delay,a.cache_policy,b)
    with a.output.open('x') as f: f.write(json.dumps(result,indent=2,allow_nan=False)+'\n')
    print(json.dumps({'status':'PREPARED_NO_REQUESTS','output':str(a.output),'sessions':len(result['sessions']),
                      'sha256':hashlib.sha256(a.output.read_bytes()).hexdigest()}))
if __name__=='__main__': main()
