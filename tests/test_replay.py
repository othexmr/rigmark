import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import replay as R
import replay_prepare as P
import replay_compare as C
import subprocess
import sys


def trace(turns=1):
    return {'protocol':R.PROTOCOL,'system':'Shared instructions','cache_policy':'run-isolated',
            'provenance':{'kind':'test'},'sessions':[{'id':'a','category':'code','start_s':0,
                'turns':[{'user':'First question','max_tokens':128,'think_s':0}]+
                        [{'user':'Follow up','max_tokens':128,'think_s':.01}]*(turns-1)},
                {'id':'b','category':'document','start_s':.015,'turns':[{'user':'Independent','max_tokens':128}]}]}


def event(content='',reasoning='',finish=None,usage=None,ids=None):
    obj={'choices':[{'index':0,'delta':{'content':content,'reasoning_content':reasoning},'finish_reason':finish}]}
    if usage is not None: obj['usage']=usage
    if ids is not None: obj['choices'][0]['token_ids']=ids
    return json.dumps(obj,ensure_ascii=False)


class FakeClient:
    def __init__(self, fail=False): self.calls=[]; self.fail=fail; self.lock=threading.Lock()
    def stream(self,messages,cap,extra):
        started=time.monotonic()
        with self.lock: self.calls.append((started,copy.deepcopy(messages)))
        time.sleep(.06 if messages[-1]['content']=='First question' else .005)
        state=R.StreamState()
        state.observe(event('answer café',finish='stop',usage={'prompt_tokens':10,'completion_tokens':3}),.003)
        state.observe('[DONE]',.004)
        row=state.result()
        row.update(started=started,finished=time.monotonic(),error={'type':'Simulated'} if self.fail else None)
        return row


class Validation(unittest.TestCase):
    def test_final_turn_receipt_write_failure_fails_execution(self):
        with tempfile.TemporaryDirectory() as d:
            output = Path(d)
            with patch.object(R, 'save', side_effect=OSError('disk full')):
                with self.assertRaisesRegex(RuntimeError, 'receipt write failed'):
                    R.execute(trace(), FakeClient(), output, 'write-failure')
            self.assertEqual(list(output.glob('request-*.json')), [])

    def test_bad_arrival_and_caps(self):
        for v in (-1,float('nan'),True):
            t=trace();t['sessions'][0]['start_s']=v
            with self.assertRaises(ValueError): R.validate(t)
        t=trace();t['sessions'][0]['turns'][0]['max_tokens']=True
        with self.assertRaises(ValueError): R.validate(t)
    def test_duplicate_ids(self):
        t=trace();t['sessions'][1]['id']='a'
        with self.assertRaises(ValueError): R.validate(t)
    def test_declared_cache_policy(self):
        t=trace();t['cache_policy']='cold'
        with self.assertRaises(ValueError): R.validate(t)
    def test_first_think_rejected(self):
        t=trace();t['sessions'][0]['turns'][0]['think_s']=1
        with self.assertRaises(ValueError): R.validate(t)
    def test_url_security(self):
        for url in ('http://user:key@localhost','ftp://localhost','http://localhost?a=b'):
            with self.assertRaises(ValueError): R.Client(url,'test')
    def test_prepare_uses_actual_text_without_padding(self):
        with tempfile.TemporaryDirectory() as d:
            f=Path(d)/'code.txt';f.write_text('def add(a,b): return a+b')
            g=Path(d)/'doc.txt';g.write_text('A real short document.')
            t=P.prepare(f,g,[f,g],6,'short-first',1,'natural')
            self.assertEqual(t['sessions'][-1]['start_s'],1)
            self.assertEqual(t['sessions'][-1]['turns'][0]['user'].count('def add'),1)
            self.assertEqual(t['provenance']['padding'],'none')


class Streams(unittest.TestCase):
    def test_utf8_crlf_and_fragmentation(self):
        wire=('data: '+event('café')+'\r\n\r\n: heartbeat\r\n\r\ndata: [DONE]\r\n\r\n').encode()
        p=R.SSE();out=[]
        for b in wire: out+=p.feed(bytes([b]))
        out+=p.feed(b'',True)
        self.assertEqual(json.loads(out[0])['choices'][0]['delta']['content'],'café')
        self.assertEqual(out[-1],'[DONE]')
    def test_multiline_event(self):
        self.assertEqual(R.SSE().feed(b'data: {\ndata: "a": 1}\n\n'),['{\n"a": 1}'])
    def test_partial_frame_rejected(self):
        p=R.SSE();p.feed(b'data: {')
        with self.assertRaises(ValueError): p.feed(b'',True)
    def test_reasoning_and_visible_separate(self):
        s=R.StreamState();s.observe(event(reasoning='think'),1);s.observe(event('answer'),4)
        r=s.result();self.assertEqual(r['first_output_s'],1);self.assertEqual(r['first_visible_s'],4)
    def test_no_fake_token_counts(self):
        s=R.StreamState();s.observe(event('six tokens in one event',usage={'completion_tokens':6,'prompt_tokens':9}),1)
        self.assertFalse(s.result()['token_timeline_exact'])
        self.assertIsNone(s.result()['longest_visible_delivery_gap_s'])
    def test_exact_counts_require_reconciliation(self):
        s=R.StreamState();s.observe(event('hi',ids=[1,2],usage={'completion_tokens':2,'prompt_tokens':9}),1)
        self.assertTrue(s.result()['token_timeline_exact'])
        s.usage['completion_tokens']=3
        self.assertFalse(s.result()['token_timeline_exact'])
    def test_malformed_json_rejected(self):
        with self.assertRaises(json.JSONDecodeError): R.StreamState().observe('{',1)


class Execution(unittest.TestCase):
    def run_trace(self,t=None,client=None):
        with tempfile.TemporaryDirectory() as d:
            rows=R.execute(t or trace(),client or FakeClient(),Path(d),'test-run')
            self.assertEqual(len(list(Path(d).glob('request-*'))),len(rows))
            return rows
    def test_independent_arrival_does_not_wait_for_first_output(self):
        rows=self.run_trace();a,b=rows
        self.assertLess(b['started_s'],a['finished_s'])
        self.assertAlmostEqual(b['due_s'],.015,places=5)
    def test_followup_uses_actual_answer_and_think_delay(self):
        c=FakeClient();rows=self.run_trace(trace(2),c)
        a0,a1=rows[:2]
        self.assertAlmostEqual(a1['due_s']-a0['finished_s'],.01,places=5)
        follow=next(messages for _,messages in c.calls if messages[-1]['content']=='Follow up')
        self.assertEqual(follow[-2],{'role':'assistant','content':'answer café'})
        self.assertIn('test-run',follow[0]['content'])
    def test_dependency_failure_kept_without_retry(self):
        c=FakeClient(True);rows=self.run_trace(trace(2),c)
        self.assertEqual(len(c.calls),2)
        self.assertEqual(rows[1]['status'],'blocked_by_previous_turn')
        self.assertEqual(R.score(rows)['completion_fraction'],0)
    def test_natural_cache_does_not_mutate_prefix(self):
        t=trace();t['cache_policy']='natural';c=FakeClient();self.run_trace(t,c)
        self.assertEqual(c.calls[0][1][0]['content'],'Shared instructions')
    def test_extra_body_cannot_force_eos_or_change_workload(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ValueError):
                R.execute(trace(),FakeClient(),Path(d),'id',{'ignore_eos':True})
    def test_truncated_answers_not_counted_as_success(self):
        class C(FakeClient):
            def stream(self,*args):
                r=super().stream(*args);r['finish_reason']='length';return r
        rows=self.run_trace(trace(2),C())
        self.assertEqual(rows[0]['status'],'truncated')
        self.assertEqual(rows[1]['status'],'blocked_by_previous_turn')
        self.assertEqual(R.score(rows)['completion_fraction'],0)


class Scores(unittest.TestCase):
    def test_sparse_percentiles_suppressed(self):
        self.assertNotIn('p95',R.distribution([1]*6))
        self.assertNotIn('p99',R.distribution([1]*25))
        self.assertEqual(R.distribution(list(range(100)))['p99'],98)
    def test_crossing_gap_and_no_token_estimation(self):
        inc={'id':'inc','started_s':0,'finished_s':10,'first_output_s':.1,
             'events':[{'seconds':x,'visible_characters':1,'delta_token_count':None} for x in (.1,1,7,9)]}
        arrival={'id':'new','started_s':2,'finished_s':9,'first_output_s':3,'events':[]}
        r=R.interference([inc,arrival])[1]['incumbents'][0]
        self.assertEqual(r['longest_intersecting_visible_gap_s'],6)
        self.assertIsNone(r['generated_tokens_delivered_during_wait'])
    def test_terminal_silence_after_an_in_window_delivery(self):
        inc = {'id': 'inc', 'started_s': 0., 'finished_s': 7., 'first_output_s': 0.,
               'events': [{'seconds': t, 'visible_characters': 1} for t in (0., 2.)]}
        arrival = {'id': 'new', 'started_s': 1., 'finished_s': 8., 'first_output_s': 5., 'events': []}
        evidence = R.interference([inc, arrival])[-1]['incumbents'][0]
        self.assertEqual(evidence['longest_intersecting_visible_gap_s'], 2.)
        self.assertEqual(evidence['right_censored_silence_in_window_s'], 4.)

    def test_censored_stall_and_queue_label(self):
        inc={'id':'inc','started_s':0,'finished_s':10,'first_output_s':.1,
             'events':[{'seconds':.1,'visible_characters':1,'delta_token_count':None}]}
        queued={'id':'queued','started_s':1,'finished_s':12,'first_output_s':10,'events':[]}
        new={'id':'new','started_s':2,'finished_s':9,'first_output_s':3,'events':[]}
        r=R.interference([inc,queued,new])[-1]['incumbents']
        self.assertEqual(r[0]['right_censored_silence_in_window_s'],3)
        self.assertFalse(r[1]['visible_before_arrival'])
    def test_slo_uses_all_planned_denominator(self):
        rows=[{'id':'a','status':'completed','started_s':0,'finished_s':5,'user_visible_ttft_s':2,'client_e2e_s':5,
               'usage_valid':False,'first_output_s':None}, {'id':'b','status':'blocked_by_previous_turn'}]
        r=R.score(rows,{'visible':3,'gap':1,'total':6})
        self.assertEqual(r['slo_fraction_all_planned'],.5)
        self.assertIsNone(r['completion_tokens_per_s'])


class WireIntegration(unittest.TestCase):
    def test_http_mock_server_single_chunk_valid_and_error_retained(self):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_POST(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                if body['messages'][0]['content']=='fail':
                    self.send_response(503);self.end_headers();return
                self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
                data=('data: '+event('café',finish='stop',usage={'prompt_tokens':9,'completion_tokens':3})+'\n\ndata: [DONE]\n\n').encode()
                for i in range(0,len(data),7): self.wfile.write(data[i:i+7]);self.wfile.flush()
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            c=R.Client(f'http://127.0.0.1:{server.server_port}/v1','mock',1)
            r=c.stream([{'role':'user','content':'hi'}],10,{})
            self.assertIsNone(r['error']);self.assertTrue(r['done']);self.assertEqual(r['output'],'café')
            r=c.stream([{'role':'user','content':'fail'}],10,{})
            self.assertIn('503',r['error']['message'])
        finally: server.shutdown();server.server_close();thread.join()

class CompareAndCLI(unittest.TestCase):
    def test_mismatched_schedule_refused(self):
        with tempfile.TemporaryDirectory() as d:
            a=Path(d)/'a';b=Path(d)/'b';a.mkdir();b.mkdir()
            for root,sha in ((a,'a'),(b,'b')):
                R.save(root/'manifest.json',{'protocol':R.PROTOCOL,'trace_sha256':sha})
            with self.assertRaises(ValueError): C.compare(a,b)

    def test_end_to_end_cli_and_comparison_with_local_server(self):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
                data='data: '+event('answer',finish='stop',usage={'prompt_tokens':10,'completion_tokens':3})+'\n\ndata: [DONE]\n\n'
                self.wfile.write(data.encode());self.wfile.flush()
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            with tempfile.TemporaryDirectory() as d:
                root=Path(d);R.save(root/'trace.json',trace(2));R.save(root/'identity.json',{'model':'local-mock'})
                for arm in ('control','candidate'):
                    argv=[sys.executable,'-B',str(Path(R.__file__).with_name('rigmark')), 'replay', '--trace',str(root/'trace.json'),
                        '--output',str(root/arm),'--base-url',f'http://127.0.0.1:{server.server_port}',
                        '--model','mock','--identity',str(root/'identity.json'),'--run-id',arm,
                        '--max-dispatch-lag','1','--run']
                    p=subprocess.run(argv,capture_output=True,text=True,timeout=10)
                    self.assertEqual(p.returncode,0,p.stderr)
                    self.assertEqual(json.loads((root/arm/'terminal.json').read_text())['status'],'COMPLETED')
                result=C.compare(root/'control',root/'candidate')
                self.assertEqual(result['overall']['completion_fraction']['candidate'],1)
                self.assertEqual(result['overall']['status_counts']['control'],{'completed':3})
                terminal_path = root/'candidate/terminal.json'
                terminal_bytes = terminal_path.read_bytes()
                for change in ({'status': 'COMPLETED_WITH_FAILURES_NO_RETRY'}, {'client_schedule_valid': False}):
                    terminal = json.loads(terminal_bytes); terminal.update(change)
                    R.save(terminal_path, terminal)
                    with self.assertRaisesRegex(ValueError, 'terminal status/validity'):
                        C.compare(root/'control', root/'candidate')
                terminal_path.write_bytes(terminal_bytes)
                request = next((root/'candidate').glob('request-*.json'))
                saved = request.read_bytes()
                for change in ({'output': ''}, {'finish_reason': 'length'}, {'done': False}):
                    bad = json.loads(saved); bad.update(change)
                    R.save(request, bad)
                    with self.assertRaisesRegex(ValueError, 'request status differs'):
                        C.compare(root/'control', root/'candidate')
                request.write_bytes(saved)
                bad = json.loads(saved); bad['output'] = 'different completed answer'
                R.save(request, bad)
                with self.assertRaisesRegex(ValueError, 'output hash differs'):
                    C.compare(root/'control', root/'candidate')
                request.write_bytes(saved)
                for field, value in (('due_s', -100.), ('dispatch_lag_s', 999.), ('user_visible_ttft_s', 99.)):
                    bad = json.loads(saved); bad[field] = value
                    R.save(request, bad)
                    with self.assertRaisesRegex(ValueError, 'timing differs'):
                        C.compare(root/'control', root/'candidate')
                bad = json.loads(saved); bad['events'][0]['seconds'] = 100.
                R.save(request, bad)
                with self.assertRaisesRegex(ValueError, 'event clock'):
                    C.compare(root/'control', root/'candidate')
                request.write_bytes(saved)
                bad = json.loads(saved); bad['client_e2e_s'] = 999.
                R.save(request, bad)
                with self.assertRaisesRegex(ValueError, 'timing differs'):
                    C.compare(root/'control', root/'candidate')
                request.write_bytes(saved)
                cli = subprocess.run([sys.executable, str(Path(R.__file__).with_name('rigmark')),
                    'compare-replay', str(root/'control'), str(root/'candidate')],
                    capture_output=True, text=True, timeout=10)
                self.assertEqual(cli.returncode, 0, cli.stderr)
                self.assertEqual(json.loads(cli.stdout)['protocol'], R.PROTOCOL)
                R.save(root/'candidate/score.json',{'client_schedule_valid':False})
                with self.assertRaisesRegex(ValueError,'score differs'):C.compare(root/'control',root/'candidate')
        finally:server.shutdown();server.server_close();thread.join()

    def test_total_deadline_retains_partial_receipt(self):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
                self.wfile.write(('data: '+event('partial')+'\n\n').encode());self.wfile.flush()
                time.sleep(.15)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            r=R.Client(f'http://127.0.0.1:{server.server_port}','mock',.05).stream([],10,{})
            self.assertEqual(r['output'],'partial');self.assertFalse(r['done'])
            self.assertIsNotNone(r['error'])
            self.assertLess(r['finished']-r['started'],.14)
        finally:server.shutdown();server.server_close();thread.join()

    def test_missing_done_preserves_partial_output(self):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                self.send_response(200);self.send_header('Content-Type','text/event-stream');self.end_headers()
                self.wfile.write(('data: '+event('partial')+'\n\n').encode())
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            r=R.Client(f'http://127.0.0.1:{server.server_port}','mock',1).stream([],10,{})
            self.assertEqual(r['output'],'partial');self.assertFalse(r['done']);self.assertIsNotNone(r['error'])
        finally:server.shutdown();server.server_close();thread.join()

if __name__=='__main__': unittest.main(verbosity=2)
