#!/usr/bin/env python3
"""Prepare inspectable application traces from explicitly selected local text."""
import argparse
import hashlib
import json
from pathlib import Path
from replay import PROTOCOL, validate

QUESTIONS = [
    ('code', 'Review this code for a cancellation or concurrency bug. Describe one concrete failing sequence and the smallest fix. Avoid style changes.'),
    ('document', 'Summarize the supplied material for an engineer joining the project. Give the main constraint, the evidence behind it and one unresolved question.'),
    ('code', 'Propose three focused regression tests for this implementation. Include inputs and expected outcomes; explain which failure each catches.'),
    ('document', 'Explain the operational trade-offs in the material in plain language. Separate measured findings from assumptions; do not invent numbers.'),
]
FOLLOWUPS = [
    'Now challenge your answer: which claim has the weakest evidence? Revise it and give one next check.',
    'Turn that into a concise handoff with the next action and its acceptance criteria.',
]


def material(paths):
    texts=[]; provenance=[]
    for path in paths:
        raw=path.read_bytes(); text=raw.decode('utf-8')
        if not text.strip(): raise ValueError('empty input: '+str(path))
        texts.append(f'File: {path.name}\n```text\n{text}\n```')
        provenance.append({'name':path.name,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)})
    return '\n\n'.join(texts), provenance


def prepare(code, document, contexts, users, direction, delay, cache):
    c, pc=material([code]); d,pd=material([document]); long,pl=material(contexts)
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
        turns=[{'user':user,'max_tokens':1536 if is_long else 1024,'think_s':0}]
        if direction=='sessions':
            turns += [{'user':q,'max_tokens':768,'think_s':float(2+(i+j)%5)} for j,q in enumerate(FOLLOWUPS)]
        sessions.append({'id':f'user-{i+1:02d}','start_s':start,'category':category,'turns':turns})
    return validate({'protocol':PROTOCOL,'label':f'{direction}-{users}-users',
        'provenance':{'kind':'authored_application_scenario_not_production_trace',
                      'inputs':pc+pd+pl,'padding':'none','token_lengths':'measured from server usage, not assumed'},
        'cache_policy':cache,'system':'Help an engineer solve the task using only the supplied material. Be precise and concise. Admit missing evidence.',
        'sessions':sessions})


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--code',type=Path,required=True)
    p.add_argument('--document',type=Path,required=True)
    p.add_argument('--context',type=Path,action='append',required=True)
    p.add_argument('--users',type=int,default=6)
    p.add_argument('--direction',choices=('short-first','long-first','sessions'),default='sessions')
    p.add_argument('--delay',type=float,default=1)
    p.add_argument('--cache-policy',choices=('natural','run-isolated'),default='run-isolated')
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    if not 2 <= a.users <= 128: p.error('users must be 2..128')
    result=prepare(a.code,a.document,a.context,a.users,a.direction,a.delay,a.cache_policy)
    with a.output.open('x') as f: f.write(json.dumps(result,indent=2,allow_nan=False)+'\n')
    print(json.dumps({'status':'PREPARED_NO_REQUESTS','output':str(a.output),'sessions':a.users,
                      'sha256':hashlib.sha256(a.output.read_bytes()).hexdigest()}))
if __name__=='__main__': main()
