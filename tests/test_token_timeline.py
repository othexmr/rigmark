import json
import unittest
from unittest.mock import patch
import bench
from token_timeline import TokenTimeline,window_tokens

class Tests(unittest.TestCase):
    def test_four_tokens_in_one_event_and_half_open_window(self):
        t=TokenTimeline('usage')
        t.observe({}, {'completion_tokens':0},0,False)
        t.observe({}, {'completion_tokens':4},1,True)
        t.observe({}, {'completion_tokens':5},2,True)
        t.observe({}, {'completion_tokens':8},3,True)
        d=t.finish(8);self.assertEqual(d['event_token_counts'],[0,4,1,3])
        row={'token_delivery':d,'started_monotonic_seconds':10,'completion_tokens':8}
        self.assertEqual(window_tokens(row,11,13),5)
        self.assertEqual(window_tokens(row,13,14),3)

    def test_ids_include_empty_text_tokens(self):
        t=TokenTimeline('ids');t.observe({'token_ids':[7,8]},None,1,False)
        t.observe({'token_ids':[9,10,11]},None,2,True)
        self.assertEqual(t.finish(5)['total_tokens'],5)
        self.assertEqual(t.finish(6)['status'],'UNAVAILABLE')

    def test_final_usage_only_cannot_backfill_times(self):
        t=TokenTimeline('usage');t.observe({},None,1,True);t.observe({}, {'completion_tokens':8},3,True)
        self.assertEqual(t.finish(8)['status'],'UNAVAILABLE')
        self.assertIsNone(t.finish(8)['event_token_counts'])

    def test_missing_ids_cumulative_ids_and_invalid_usage(self):
        t=TokenTimeline('ids');t.observe({'token_ids':[1,2]},None,1,True);t.observe({'token_ids':[1,2,3]},None,2,True)
        self.assertEqual(t.finish(3)['status'],'UNAVAILABLE')
        for value in (-1,True,float('nan'),1.5):
            t=TokenTimeline('usage');t.observe({}, {'completion_tokens':value},1,True)
            self.assertEqual(t.finish(0)['status'],'UNAVAILABLE')
        t=TokenTimeline('usage');t.observe({}, {'completion_tokens':3},1,True);t.observe({}, {'completion_tokens':2},2,True)
        self.assertEqual(t.finish(3)['status'],'UNAVAILABLE')

    def test_client_integration_and_no_changes_to_input_payload(self):
        chunks=[{'choices':[{'index':0,'delta':{'role':'assistant'}}],'usage':{'completion_tokens':0}},
            {'choices':[{'index':0,'delta':{'content':'four tokens here now'}}],'usage':{'completion_tokens':4}},
            {'choices':[{'index':0,'delta':{'reasoning':'next three tokens'}}],'usage':{'completion_tokens':7}},
            {'choices':[],'usage':{'completion_tokens':7,'prompt_tokens':2}}]
        class Response:
            def __enter__(self):return iter([('data: '+json.dumps(c)+'\n').encode() for c in chunks]+[b'data: [DONE]\n'])
            def __exit__(self,*args):pass
        payload={'stream':True,'stream_options':{'include_usage':True}}
        seen=[]; starts=[]
        def open_(request,**kwargs):
            self.assertEqual(starts, [10])
            seen.append(json.loads(request.data));return Response()
        with patch('urllib.request.urlopen',side_effect=open_),patch('time.monotonic',side_effect=[10,10,11,12,13]):
            row=bench.Client('http://localhost:8000','',10,'usage').stream('/v1/chat/completions',payload,True,on_request_start=starts.append)
        self.assertEqual(row['token_delivery']['total_tokens'],7)
        self.assertEqual(len(row['event_seconds']),2)
        self.assertEqual(row['token_delivery']['event_token_counts'],[0,4,3])
        self.assertTrue(seen[0]['stream_options']['continuous_usage_stats'])
        self.assertNotIn('continuous_usage_stats',payload['stream_options'])
        self.assertEqual(bench.stall_analysis(row,10,12)['arrival_window_completion_tokens'],4)

    def test_old_receipts_unavailable_and_tampering_rejected(self):
        self.assertIsNone(window_tokens({'event_seconds':[1,2,3]},0,5))
        t=TokenTimeline('ids');t.observe({'token_ids':[1,2]},None,1,True)
        d=t.finish(2);d['event_token_counts']=[3]
        self.assertIsNone(window_tokens({'token_delivery':d,'completion_tokens':2,'started_monotonic_seconds':0},0,2))

class ComparisonTests(unittest.TestCase):
    def test_accounting_modes_cannot_mix(self):
        import compare
        from pathlib import Path
        source=json.loads(Path('results/reference/glm53-libert-nvfp4-tp2-low.json').read_text())
        key='settings.delivery_token_accounting'
        for mode in ('off', 'usage', 'ids', 'invalid', None):
            other=json.loads(json.dumps(source))
            other['settings']['delivery_token_accounting']=mode
            self.assertEqual(key in compare.comparable(source,other), mode != 'off')
        for mode in ('usage', 'ids'):
            source['settings']['delivery_token_accounting']=mode
            self.assertNotIn(key,compare.comparable(source,source))

if __name__=='__main__':unittest.main()
