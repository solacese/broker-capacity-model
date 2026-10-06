"""Token/SEMP/SMF adapter for the dedicated, already-provisioned test broker."""
from __future__ import annotations
import hashlib
import json
import multiprocessing as mp
import queue
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote
from .cloud import SolaceCloudClient,read_token,token_claims
from .config_writer import CampaignConfigWriter,ConfigAccess
from .semp import SempClient
from .grid_workers import worker
from .grid_detection import assess,COUNTERS,synchronized_lag

SERVICE_ID='4qn20a1u6ny'
SERVICE_NAME='bcm-20261005-5k'
OWNER='usfos7cfqge'
ORG='seall'

class LiveBackend:
    def __init__(self,token_file,manifest):
        token=read_token(Path(token_file))
        claims=token_claims(token)
        if (claims.get('org'),claims.get('sub'))!=(ORG,OWNER):raise ValueError('token organization/owner mismatch')
        if manifest['broker_id']!=SERVICE_ID:raise ValueError('only the dedicated 5K is approved for this grid')
        self.credentials=SolaceCloudClient(token).service_credentials(SERVICE_ID,expected_owner_id=OWNER,expected_name=SERVICE_NAME)
        self.semp=SempClient(self.credentials.access,timeout=10)
        c=self.credentials
        if not c.semp_manager_username or not c.semp_manager_password:raise ValueError('SEMP manager credentials missing')
        self.writer=CampaignConfigWriter(ConfigAccess(SERVICE_ID,OWNER,c.access.msg_vpn_name,c.access.management_uri,c.semp_manager_username,c.semp_manager_password),timeout=15)
        self.root=f'/SEMP/v2/monitor/msgVpns/{quote(c.access.msg_vpn_name,safe="")}'
        self.manifest=manifest
        self.prefix='bcm-20261005-grid-'+manifest['campaign_id'][:10]
        self.allowed_queues={self.queue_name(f,n) for f in [1,2,5,10,20] for n in range(f)}
        self.config=self.semp.get(self.root.replace('/monitor/','/config/'))['data']
        self.spec=self.semp.get('/SEMP/v2/monitor/spec')
        props=self.spec.get('definitions',{}).get('MsgVpn',{}).get('properties',{})
        if not set(COUNTERS+('msgSpoolUsage','msgSpoolMsgCount')).issubset(props):raise ValueError('required SEMP fields unsupported')
    def queue_name(self,fanout,n):return f'{self.prefix}-f{fanout:02}-q{n:02}'
    def topic(self,work):return f'{self.prefix}-{work["mode"][0]}-f{work["fanout"]:02}'
    def snapshot(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            vf=pool.submit(self.semp.get,self.root)
            qf=pool.submit(lambda:list(self.semp.collection(self.root+'/queues',count=100)))
            vpn=vf.result()['data'];qs={x['queueName']:x for x in qf.result()}
        return {'t':time.monotonic(),'vpn':{k:vpn.get(k) for k in COUNTERS+('msgSpoolUsage','msgSpoolMsgCount')},
                'queues':{n:{k:q.get(k) for k in ('msgSpoolUsage','txUnackedMsgCount','maxMsgSpoolUsage')} for n,q in qs.items()}}
    def preflight(self):
        clients=list(self.semp.collection(self.root+'/clients'))
        if any(c.get('clientName')!='#client' for c in clients):raise RuntimeError('broker has connected clients; wait for prior workers to stop')
        for resource in ['bridges','dmrBridges','topicEndpoints']:
            if list(self.semp.collection(self.root+'/'+resource)):raise RuntimeError('broker isolation failed: '+resource)
        qs=list(self.semp.collection(self.root+'/queues'))
        for q in qs:
            name=q['queueName']
            if name not in self.allowed_queues:
                # Prior test resources may remain, but must be empty and route no
                # campaign traffic. They are never consumed or changed by this run.
                subs=list(self.semp.collection(self.root+'/queues/'+quote(name,safe='')+'/subscriptions'))
                if q.get('msgSpoolUsage')!=0 or q.get('txUnackedMsgCount')!=0:
                    raise RuntimeError('foreign queue contains messages; refusing to consume it')
                if any('*' in x.get('subscriptionTopic','') or '>' in x.get('subscriptionTopic','') or x.get('subscriptionTopic','').startswith(self.prefix) for x in subs):
                    raise RuntimeError('foreign subscription may match campaign traffic')
                continue
            suffix=name.rsplit('-q',1)[0].rsplit('-f',1)[1]
            expected=f'{self.prefix}-p-f{suffix}'
            subscriptions=list(self.semp.collection(self.root+'/queues/'+quote(name,safe='')+'/subscriptions'))
            if [s.get('subscriptionTopic') for s in subscriptions]!=[expected]:
                raise RuntimeError('campaign queue subscription changed')
        a=self.snapshot();time.sleep(.2);b=self.snapshot()
        if any(b['vpn'].get(k)!=a['vpn'].get(k) for k in ('dataRxMsgCount','dataTxMsgCount')):
            raise RuntimeError('broker has unexplained traffic')
        if b['vpn'].get('msgSpoolMsgCount') is None or b['vpn'].get('msgSpoolUsage') is None:
            raise RuntimeError('current spool gauges unavailable')
        return b
    def prepare(self,work):
        names=[]
        if work['mode']=='persistent':
            for n in range(work['fanout']):
                name=self.queue_name(work['fanout'],n)
                self.writer.ensure_exclusive_queue(name,self.topic(work),max_spool_mb=100)
                names.append(name)
        return names
    def metadata(self):
        return {'service_id':SERVICE_ID,'service_name':SERVICE_NAME,'semp_api_version':self.spec.get('info',{}).get('version'),
                'service_class':self.credentials.access.service_class,'transport':'SMF/TLS',
                'generator_placement':'local_to_runner_host','semp_sha256':hashlib.sha256(json.dumps(self.spec,sort_keys=True).encode()).hexdigest(),
                'broker_capacity_validated':False}
    def _execute(self,work,rate,warmup,seconds,run_dir,cancel,progress,recovery_names=None):
        run_dir.mkdir(parents=True,exist_ok=True)
        names=recovery_names if recovery_names is not None else self.prepare(work)
        recovery=recovery_names is not None
        ctx=mp.get_context('spawn');phase=ctx.Value('i',0);until=ctx.Value('d',0.);q=ctx.Queue();stop=ctx.Event()
        processes=[];latest={};worker_samples={};errors=[];telemetry=[];lag={};interrupted=False;guardrail=False
        measured_start=measured_end=0.;counter_bracket=[]
        c=self.credentials;tag=int(self.manifest['campaign_id'][:16],16);stage_tag=uuid.uuid4().int & ((1<<64)-1)
        fanout=len(names) if recovery else work['fanout']
        base=dict(campaign_tag=tag,stage_tag=stage_tag,mode=work['mode'],smf_uri=c.smf_uri,vpn=c.access.msg_vpn_name,
                  username=c.smf_username,password=c.smf_password,topic=self.topic(work),payload_bytes=work['payload_bytes'],
                  durations={'1':warmup,'3':seconds},rate=rate/max(1,work['publishers']),recovery=recovery)
        roles=[('receiver',i) for i in range(fanout)]+([] if recovery else [('publisher',i) for i in range(work['publishers'])])
        raw=(run_dir/'workers.jsonl').open('a',buffering=1);semplog=(run_dir/'semp.jsonl').open('a',buffering=1)
        def pump():
            while True:
                try:e=q.get_nowait()
                except queue.Empty:break
                raw.write(json.dumps(e)+'\n')
                if e['event']=='error':errors.append(e['error_type'])
                else:
                    latest[e['id']]=e
                    worker_samples.setdefault(e['id'],[]).append(e)
        def poll():
            nonlocal guardrail
            s=self.snapshot();semplog.write(json.dumps(s)+'\n')
            lim=self.config.get('maxMsgSpoolUsage')
            if not isinstance(lim,(int,float)) or lim<=0:raise RuntimeError('missing spool limit')
            if s['vpn']['msgSpoolUsage']>=.70*lim*1_000_000:guardrail=True
            for n in names:
                v=s['queues'].get(n,{})
                if v.get('msgSpoolUsage') is None:raise RuntimeError('missing owned queue gauge')
                if v['msgSpoolUsage']>=70_000_000:guardrail=True
            return s
        def drained(p):
            pubs=[v for v in latest.values() if v['role']=='publisher'];subs=[v for v in latest.values() if v['role']=='receiver']
            key='measure' if p==3 else 'warmup'
            sent=sum(v[key]['submitted'] for v in pubs)
            return len(subs)==fanout and len(pubs)==work['publishers'] and all(v[key]['received']==sent for v in subs) and (work['mode']=='direct' or sum(v[key]['acked'] for v in pubs)==sent)
        def drain(p,maximum):
            deadline=time.monotonic()+maximum;next_poll=0;empty=False
            while time.monotonic()<deadline:
                pump()
                if time.monotonic()>=next_poll:
                    s=poll();empty=s['vpn']['msgSpoolMsgCount']==0 and all(s['queues'].get(n,{}).get('txUnackedMsgCount')==0 for n in names)
                    next_poll=time.monotonic()+1
                    if empty and (recovery or drained(p)):return True
                if errors:break
                time.sleep(.05)
            return empty and (recovery or drained(p))
        def settled_counters():
            # SEMP counters can lag delivery/ACKs. Require a quiet plateau before
            # subtracting them; keep these drain samples out of in-window trends.
            started=time.monotonic();last=None;stable_since=started
            while time.monotonic()-started<20:
                pump();s=poll();now=time.monotonic()
                values=tuple(s['vpn'].get(k) for k in COUNTERS)
                if values!=last:last=values;stable_since=now
                if now-started>=4 and now-stable_since>=3:return s
                progress('settling broker counters',now-started,20)
                time.sleep(.25)
            raise RuntimeError('broker counters did not settle')
        try:
            for role,i in roles:
                config=dict(base,id=f'{role}-{i}',role=role,publisher_index=i,
                            client_name=f'{self.prefix}-{stage_tag:x}-{role[0]}{i}',queue=names[i] if role=='receiver' and names else None)
                proc=ctx.Process(target=worker,args=(config,phase,until,q,stop));proc.start();processes.append(proc)
            deadline=time.monotonic()+90
            while len(latest)<len(roles) and time.monotonic()<deadline and not errors:
                pump()
                if cancel():interrupted=True;break
                progress('connecting',len(latest),len(roles));time.sleep(.1)
            if len(latest)<len(roles):raise RuntimeError('worker startup incomplete')
            if recovery:
                phase.value=4
                if not drain(3,120):raise RuntimeError('campaign recovery did not drain all owned data')
            else:
                if warmup:
                    until.value=time.monotonic()+warmup;phase.value=1
                    while time.monotonic()<until.value and not errors:
                        pump()
                        if cancel():interrupted=True;break
                        progress('warmup',warmup-max(0,until.value-time.monotonic()),warmup);time.sleep(.05)
                    phase.value=2
                    if not drain(1,30):raise RuntimeError('warmup drain did not reconcile')
                if not interrupted and not errors:
                    baseline=settled_counters();counter_bracket.append(baseline);telemetry.append(baseline)
                    measured_start=time.monotonic();until.value=measured_start+seconds;measured_end=until.value;phase.value=3;next_poll=time.monotonic()+2
                    while time.monotonic()<until.value and not errors:
                        pump()
                        if cancel():interrupted=True;break
                        if time.monotonic()>=next_poll:
                            telemetry.append(poll());next_poll=time.monotonic()+2
                            if guardrail:break
                        progress('measure',seconds-max(0,until.value-time.monotonic()),seconds);time.sleep(.03)
                    phase.value=4;stop.set();telemetry.append(poll())
                phase.value=4;stop.set();progress('drain',0,30);drain(3,30)
                if counter_bracket and not interrupted and not guardrail:
                    counter_bracket.append(settled_counters())
        except KeyboardInterrupt:
            interrupted=True;phase.value=4;stop.set()
        except Exception as e:errors.append(type(e).__name__+':stage_failed')
        finally:
            phase.value=5;stop.set();deadline=time.monotonic()+15
            while any(p.is_alive() for p in processes) and time.monotonic()<deadline:
                pump();time.sleep(.05)
            for p in processes:
                if p.is_alive():p.terminate();p.join(3);errors.append('forced_worker_shutdown')
                if p.is_alive():p.kill();p.join(3)
                p.join(.1)
            pump();raw.close();semplog.close();q.close()
        lag=synchronized_lag(worker_samples,measured_start,measured_end)
        final=self.snapshot()
        empty=final['vpn'].get('msgSpoolMsgCount')==0 and all(final['queues'].get(n,{}).get('txUnackedMsgCount')==0 for n in names)
        clean=len(latest)==len(roles) and all(v.get('event')=='stopped' and v.get('clean_shutdown') for v in latest.values())
        evidence=dict(seconds=seconds,rate=rate,mode=work['mode'],publishers=work['publishers'],fanout=fanout,
                      payload_bytes=work['payload_bytes'],workers=latest,errors=errors,telemetry=telemetry,
                      receiver_lag_series=lag,counter_bracket=counter_bracket,queue_names=names,queues_empty=empty,clean_shutdown=clean,
                      interrupted=interrupted,guardrail=guardrail)
        (run_dir/'evidence.json').write_text(json.dumps(evidence,indent=2)+'\n')
        result=({'outcome':'RECOVERED','reasons':[]} if recovery and empty and clean else assess(evidence))
        (run_dir/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        return result
    def recover(self,run_dir,cancel,progress):
        s=self.preflight()
        if s['vpn']['msgSpoolMsgCount']==0:return
        names=[n for n,q in s['queues'].items() if q.get('msgSpoolUsage',0)>0 or q.get('txUnackedMsgCount',0)>0]
        if not names or any(n not in self.allowed_queues for n in names):raise RuntimeError('cannot identify owned backlog')
        work=dict(mode='persistent',fanout=len(names),publishers=0,payload_bytes=1024)
        r=self._execute(work,1,0,1,run_dir,cancel,progress,recovery_names=names)
        if r['outcome']!='RECOVERED':raise RuntimeError('owned backlog recovery failed')
    def run_stage(self,work,rate,seconds,warmup,run_dir,cancel,progress):
        self.recover(run_dir/'recovery',cancel,progress)
        return self._execute(work,rate,warmup,seconds,run_dir,cancel,progress)
