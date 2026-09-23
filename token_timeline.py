"""Exact completion-token accounting at SSE delivery times, opt-in.

Text events are not tokens. Usage mode requires cumulative completion usage on
every text-bearing chunk. IDs mode requires choice.token_ids deltas. Both must
reconcile with final usage; unsupported streams return unavailable, never a guess.
"""
import math

class TokenTimeline:
    def __init__(self, mode):
        if mode not in ('usage','ids'):raise ValueError('unsupported token delivery mode')
        self.mode=mode;self.seconds=[];self.counts=[];self.total=0;self.errors=[]

    def observe(self, choice, usage, offset, has_text):
        if not math.isfinite(offset) or offset<0 or (self.seconds and offset<self.seconds[-1]):
            self.errors.append('invalid delivery timestamp');return
        if self.mode=='ids':
            values=choice.get('token_ids')
            if values is None:
                if has_text:self.errors.append('text chunk lacks token IDs')
                return
            if not isinstance(values,list) or any(type(x) is not int or x<0 for x in values):
                self.errors.append('invalid token IDs');return
            count=len(values)
        else:
            value=usage.get('completion_tokens') if isinstance(usage,dict) else None
            if value is None:
                if has_text:self.errors.append('text chunk lacks continuous usage')
                return
            if type(value) is not int or value<self.total:
                self.errors.append('invalid or decreasing cumulative usage');return
            count=value-self.total
        self.total+=count
        self.seconds.append(offset);self.counts.append(count)

    def finish(self, final_count):
        errors=list(self.errors)
        if type(final_count) is not int or final_count<0 or final_count!=self.total:
            errors.append('delivered counts do not reconcile with final usage')
        ok=not errors
        return {'schema':1,'status':'EXACT_COMPLETION_TOKEN_COUNTS' if ok else 'UNAVAILABLE',
            'mode':self.mode,'scope':'completion tokens reported at delivery; includes reasoning/control tokens, not visible-text tokens',
            'event_seconds':self.seconds if ok else None,'event_token_counts':self.counts if ok else None,
            'total_tokens':self.total if ok else None,'errors':sorted(set(errors))}


def window_tokens(row, start, end):
    d=row.get('token_delivery') or {}
    if d.get('status')!='EXACT_COMPLETION_TOKEN_COUNTS':return None
    times=d.get('event_seconds');counts=d.get('event_token_counts')
    if not isinstance(times,list) or not isinstance(counts,list) or len(times)!=len(counts):return None
    if any(type(n) is not int or n<0 for n in counts):return None
    if sum(counts)!=row.get('completion_tokens') or sum(counts)!=d.get('total_tokens'):return None
    if any(type(t) not in (int,float) or not math.isfinite(t) or t<0 for t in times):return None
    if times!=sorted(times):return None
    return sum(n for t,n in zip(times,counts) if start<=row['started_monotonic_seconds']+t<end)
