"""Resumable, explicitly bound AWS pilot for persistent egress/backlog transitions.

This pilot collects observations, not automatically certified broker capacity labels.
The original 5K campaign allowlist is unchanged. A new target requires an exact binding.
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import ipaddress
import json
import math
import os
import re
import signal
import socket
import statistics
import threading
import time
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from . import near_runtime
from .cloud import SolaceCloudClient, read_token, token_claims
from .config_writer import CampaignConfigWriter
from .daily_budget import DailyBudget
from .sdkperf import project_root
from .sdkperf_backend import SDKPerfBackend, save
from .sdkperf_campaign import lock, Progress
from .saturation_v2 import DEFAULTS, trend
from .campaign_costs import wire_reservation

ROOT = project_root()
ALLOWED_CLASSES = {'enterprise-5k-standalone', 'enterprise-5k',
                   'enterprise-10k-standalone', 'enterprise-10k'}
SOURCE_FILES = ['math_pilot.py','near_runtime.py','sdkperf_backend.py','sdkperf.py',
                'sdkperf_agent.py','saturation_v2.py','campaign_costs.py','daily_budget.py',
                'cloud.py','semp.py','config_writer.py','sdkperf_campaign.py']


def digest():
    return hashlib.sha256(b''.join(Path(__file__).with_name(n).read_bytes() for n in SOURCE_FILES)).hexdigest()


def utc():
    return datetime.now(timezone.utc).isoformat()


def verified_credentials(binding, token_file):
    required = {'service_id','name','owner_id','organization_id','service_class','msg_vpn','aws_region'}
    if not required <= binding.keys() or binding['service_class'] not in ALLOWED_CLASSES:
        raise ValueError('Incomplete or unsupported explicit target binding')
    token = read_token(Path(token_file)); claims = token_claims(token)
    if claims.get('sub') != binding['owner_id'] or claims.get('org') != binding['organization_id']:
        raise ValueError('Token owner/organization differs from the frozen binding')
    c = SolaceCloudClient(token).service_credentials(binding['service_id'],
        expected_owner_id=binding['owner_id'], expected_name=binding['name'])
    if c.access.service_class != binding['service_class'] or c.access.msg_vpn_name != binding['msg_vpn']:
        raise ValueError('Live broker class or VPN changed')
    if not c.semp_manager_username or not c.semp_manager_password:
        raise ValueError('Broker manager credentials needed for isolated campaign queues')
    if binding.get('broker_version') != c.access.broker_version:
        raise ValueError('Broker version changed; create a new measurement epoch')
    return c


def bind(directory, service_id, token_file):
    directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
    if (directory/'binding.json').exists():
        binding = json.loads((directory/'binding.json').read_text())
        if binding['service_id'] != service_id: raise ValueError('Refusing to replace a target binding')
        verified_credentials(binding, token_file)
        return binding
    token = read_token(Path(token_file)); claims = token_claims(token)
    detail = SolaceCloudClient(token).get('/api/v0/services/'+service_id)['data']
    if detail.get('userId') != claims.get('sub') or detail.get('serviceClassId') not in ALLOWED_CLASSES:
        raise ValueError('Target must be an owned, explicitly authorized 5K/10K service')
    if detail.get('creationState') != 'completed' or detail.get('adminState') != 'start':
        raise RuntimeError('Broker provisioning is not completed')
    c = SolaceCloudClient(token).service_credentials(service_id, expected_owner_id=claims['sub'], expected_name=detail['name'])
    dc = detail['datacenterId']
    # The reviewed initial transfer policy covers the existing regional AWS path only.
    if dc != 'eks-us-east-1a':
        raise ValueError('This pilot requires the reviewed us-east-1a region; reprice other routes first')
    host = urlparse(c.smf_uri).hostname
    ips = socket.gethostbyname_ex(host)[2]
    with urlopen('https://ip-ranges.amazonaws.com/ip-ranges.json',timeout=20) as response:
        ranges = json.load(response)
    if not ips or any(not any(r['region']=='us-east-1' and ipaddress.ip_address(ip) in ipaddress.ip_network(r['ip_prefix'])
                             for r in ranges['prefixes']) for ip in ips):
        raise ValueError('Broker IP region cannot be verified')
    binding = dict(service_id=service_id,name=detail['name'],owner_id=claims['sub'],
        organization_id=claims['org'],service_class=c.access.service_class,msg_vpn=c.access.msg_vpn_name,
        broker_version=c.access.broker_version,datacenter_id=dc,aws_region='us-east-1',aws_az='us-east-1a',
        bound_utc=utc(),broker_subscription_budget='separate',deletion_authorized=False)
    verified_credentials(binding, token_file)
    save(directory/'binding.json',binding)
    return binding


class ScopedWriter(CampaignConfigWriter):
    def __init__(self, credentials, prefix):
        self.prefix = prefix
        a = credentials.access
        # The caller has already verified the exact ID, owner, class, VPN and token organization.
        super().__init__(SimpleNamespace(service_id=a.service_id,owner_id=a.owner_id,
            msg_vpn=a.msg_vpn_name,management_uri=a.management_uri,
            username=credentials.semp_manager_username,password=credentials.semp_manager_password),timeout=15)

    def _validate_name(self, name):
        if name not in {self.prefix+'-p-f1-q0',self.prefix+'-p-f2-q0',self.prefix+'-p-f2-q1'}:
            raise ValueError('Queue outside the frozen pilot namespace')

    def _validate_topic(self, topic):
        if topic not in {self.prefix+'-p-f1',self.prefix+'-p-f2'}:
            raise ValueError('Subscription outside the exact pilot topic namespace')


class PilotBackend(SDKPerfBackend):
    def __init__(self, binding, token_file, manifest, runtime):
        from .semp import SempClient
        self.manifest,self.runtime,self.settings = manifest,runtime,manifest['settings']
        self.credentials = verified_credentials(binding,token_file)
        self.prefix = manifest['resource_prefix']
        c = self.credentials
        self.semp = SempClient(c.access,timeout=10)
        self.root = '/SEMP/v2/monitor/msgVpns/'+quote(c.access.msg_vpn_name,safe='')
        self.writer = ScopedWriter(c,self.prefix)
        self.vpn_config = self.semp.get(self.root.replace('/monitor/','/config/'))['data']

    def preflight(self):
        info = super().preflight()
        info.update(service_id=self.credentials.access.service_id,service_name=self.credentials.access.name)
        return info

    def run_stage(self, work, step, directory, cancel, progress):
        from .broker_sampler import parse_response
        directory.mkdir(parents=True, exist_ok=True)
        finished = threading.Event()
        c = self.credentials
        auth = 'Basic '+base64.b64encode((c.semp_manager_username+':'+c.semp_manager_password).encode()).decode()
        # This read preserves the broker's native field names and units. Absent disk
        # counters are unknown evidence, never inferred from increasing queue bytes.
        def observer():
            with (directory/'broker-spool.jsonl').open('a', buffering=1) as log:
                while not finished.is_set():
                    frame = dict(utc=utc(),monotonic=time.monotonic(),command='show message-spool detail')
                    try:
                        req = Request(c.access.management_uri+'/SEMP', method='POST',
                            data=b'<rpc><show><message-spool><detail/></message-spool></show></rpc>',
                            headers={'Authorization':auth,'Content-Type':'application/xml'})
                        with urlopen(req,timeout=8) as response:
                            frame['response']=parse_response(response.read().decode())
                    except Exception as exc:
                        frame['response']=dict(valid=False,error=type(exc).__name__)
                    log.write(json.dumps(frame)+'\n')
                    finished.wait(10)
        thread = threading.Thread(target=observer, daemon=True);thread.start()
        try:return super().run_stage(work,step,directory,cancel,progress)
        finally:finished.set();thread.join(timeout=10)


def prepare(directory):
    directory = Path(directory); directory.mkdir(parents=True,exist_ok=True)
    path = directory/'plan.json'
    if path.exists():
        plan = json.loads(path.read_text())
        if plan['source_sha256'] != digest():
            raise RuntimeError('Measurement source changed; use a new campaign directory')
        return plan
    # Rate tiers are interleaved across six shapes, so budget stops retain varied low-rate points.
    # They are experiment inputs, not an asserted baseline or a maximum capacity.
    shapes = [(64,1),(256,1),(1024,1),(64,2),(256,2),(1024,2)]
    rates = [1000,4000,16000,64000,256000]
    points = []
    for rate in rates:
        for size,fanout in shapes:
            points.append(dict(id=f'M{len(points)+1:03}',mode='persistent',payload_bytes=size,
                fanout=fanout,publishers=4,publish_window=255,subscriber_window=255,
                offered_rate=rate,seconds=300,shape_id=f'p{size}-f{fanout}'))
    settings = dict(DEFAULTS,warmup_seconds=10,startup_seconds=60,drain_seconds=120,
        recovery_seconds=600,scout_seconds=30,extended_seconds=45,confirm_seconds=300,
        poll_seconds=2,max_queue_spool_mb=2048,max_spool_fraction=.70)
    plan = dict(schema_version=1,campaign_id=uuid.uuid4().hex,created_utc=utc(),
        source_sha256=digest(),resource_prefix='bcm-math-'+uuid.uuid4().hex[:12],
        point_count=len(points),points=points,settings=settings,maximum_active_hours=3.6,
        budget_policy=near_runtime.POLICY,
        statement='Thirty requested five-minute rate observations, not thirty confirmed saturation boundaries.',
        policy='No invalid/appartus-limited point is a broker-capacity training label. Budget may stop the campaign before all points.',
        model_scope='SMF/TLS persistent per-message, per-byte and fanout effects; no protocol multiplier is fitted by this pilot.',
        ingress_stability_relative_tolerance=.05,minimum_egress_drop_fraction=.10,
        full_window_seconds=300,classification='Stable ingress plus growing backlog and egress decline is a candidate transition; disk-path mechanism requires independent evidence.',
        latency_status='NOT_MEASURED',automatic_broker_capacity_training=False)
    save(path,plan)
    return plan


def classify_transition(evidence, *, minimum_seconds=300):
    """Conservative diagnostic; does not infer disk behavior from queue growth alone."""
    answer = dict(outcome='INCONCLUSIVE',broker_capacity_validated=False,
                  capacity_training_eligible=False,slow_path_mechanism='NOT_PROVEN',metrics={})
    def done(outcome, reason):
        answer.update(outcome=outcome,reason=reason);return answer
    if evidence.get('interrupted'): return done('INTERRUPTED','Interrupted trial')
    if evidence.get('guardrail'): return done('CENSORED',evidence['guardrail'])
    if evidence.get('errors') or not evidence.get('ready') or not evidence.get('clean_shutdown'):
        return done('APPARATUS_LIMITED','Worker or measurement failure')
    if not evidence.get('counter_scope_valid'):
        return done('INCONCLUSIVE','Whole-trial ingress/egress reconciliation failed')
    samples = evidence.get('samples',[]);work = evidence['workload'];fanout = work['fanout']
    if len(samples)<20: return done('INCONCLUSIVE','Insufficient samples')
    span = samples[-1]['t']-samples[0]['t']
    if span < minimum_seconds: return done('INCONCLUSIVE','Full five-minute window not observed')
    for a,b in zip(samples,samples[1:]):
        if b['t']<=a['t'] or b['t']-a['t']>15: return done('INCONCLUSIVE','Time order or telemetry gap')
        if b['accepted']<a['accepted']: return done('INCONCLUSIVE','Ingress counter epoch anomaly')
        if len(a.get('received_by_destination',[]))!=fanout or len(b.get('received_by_destination',[]))!=fanout:
            return done('INCONCLUSIVE','Destination counter missing')
        if any(y<x for x,y in zip(a['received_by_destination'],b['received_by_destination'])):
            return done('INCONCLUSIVE','Egress counter epoch anomaly')
    # Use >=10-second disjoint intervals to limit asynchronous counter-update noise.
    intervals=[];anchor=samples[0]
    for sample in samples[1:]:
        dt=sample['t']-anchor['t']
        if dt>=10:
            intervals.append(dict(t=(sample['t']+anchor['t'])/2,
                ingress=(sample['accepted']-anchor['accepted'])/dt,
                egress=[(y-x)/dt for x,y in zip(anchor['received_by_destination'],sample['received_by_destination'])]))
            anchor=sample
    if len(intervals)<12:return done('INCONCLUSIVE','Insufficient rate blocks')
    rates=[x['ingress'] for x in intervals];mean=statistics.mean(rates)
    if mean<=0:return done('APPARATUS_LIMITED','No ingress')
    n=max(3,len(intervals)//3)
    ingress_change=statistics.median(rates[-n:])/max(statistics.median(rates[:n]),1)-1
    variation=statistics.pstdev(rates)/mean
    stable=abs(ingress_change)<=.05 and variation<=.05
    drops=[];lags=[];queue_slopes={}
    tolerance=max(2,mean*.002)
    for i in range(fanout):
        first=statistics.median(x['egress'][i] for x in intervals[:n])
        last=statistics.median(x['egress'][i] for x in intervals[-n:])
        drops.append(1-last/max(first,1))
        lags.append(trend([(s['t'],s['accepted']-s['received_by_destination'][i]) for s in samples],tolerance=tolerance))
    for name in evidence.get('queue_names',[]):
        if any(name not in s.get('queue_bytes',{}) for s in samples):return done('INCONCLUSIVE','Queue gauge missing')
        queue_slopes[name]=trend([(s['t'],s['queue_bytes'][name]) for s in samples],tolerance=max(1024,mean*work['payload_bytes']*.002))
    subscriber_pressure=[k for k,v in evidence.get('cpu_by_worker',{}).items()
        if k.startswith('s') and (v.get('max_effective_percent') or 0)>=85]
    transport_pressure=any(any(s.get('slow_subscriber_by_destination',[False]*fanout)) for s in samples)
    answer['metrics']=dict(ingress_msgps=mean,ingress_rate_cv=variation,ingress_relative_change=ingress_change,
        ingress_stable=stable,requested_rate=evidence['rate'],requested_rate_demonstrated=mean>=.98*evidence['rate'],
        egress_drop_by_destination=drops,delivery_deficit_trends=lags,queue_byte_trends=queue_slopes,
        maximum_queue_bytes=max((v for s in samples for v in s.get('queue_bytes',{}).values()),default=0),
        subscriber_cpu_pressure=bool(subscriber_pressure),consumer_transport_pressure=transport_pressure,
        egress_msgps_by_destination=[statistics.mean(x['egress'][i] for x in intervals) for i in range(fanout)])
    if not evidence.get('cpu_telemetry_valid'):return done('INCONCLUSIVE','Missing client CPU evidence')
    if subscriber_pressure or transport_pressure:return done('CLIENT_OR_PATH_LIMITED','Consumer CPU or transport pressure')
    pubs=evidence.get('publishers',[]);subs=evidence.get('subscribers',[])
    if len(pubs)!=work['publishers'] or len(subs)!=fanout:return done('INCONCLUSIVE','Missing process summaries')
    if any(s.get('crc_errors') is None or s.get('crc_errors')!=0 for s in subs):return done('INCONCLUSIVE','CRC evidence missing or failed')
    if not stable:return done('INGRESS_UNSTABLE','Cannot isolate an egress transition at stable ingress')
    if mean<.98*evidence['rate']:return done('UNDERDRIVEN_OR_BACKPRESSURED','Stable accepted ingress is below requested rate; cause unresolved')
    growing=any(x.get('decision')=='growing' for x in lags) and any(x.get('decision')=='growing' for x in queue_slopes.values())
    if growing and max(drops)>=.10:
        return done('EGRESS_DEGRADATION_WITH_BACKLOG','Candidate transition; independent generator/network controls and disk evidence still required')
    if growing:return done('BACKLOG_GROWTH_WITHOUT_EGRESS_DECLINE','Sustained delivery deficit; not necessarily a fast-to-slow transition')
    sent=sum(p.get('transmitted') or 0 for p in pubs)
    integrity=(sent>0 and all(s.get('received')==sent and s.get('duplicates')==0 and s.get('missing')==0
                  and s.get('checked',-1)>=sent and s.get('crc_ok',-1)>=sent for s in subs))
    if integrity and evidence.get('queues_empty') and all(x.get('decision')=='stable' for x in lags):
        return done('STABLE_FOR_FIVE_MINUTES','Finite-horizon stable observation, not a maximum-capacity estimate')
    return done('INCONCLUSIVE','Backlog, drain or integrity evidence is unresolved')


def results(directory):
    rows=[]
    for p in sorted((Path(directory)/'points').glob('*/attempt-*/observation.json')):
        row=json.loads(p.read_text());row['artifact_path']=str(p);rows.append(row)
    return rows


def export(directory, state=None):
    directory=Path(directory);rows=results(directory)
    # Interrupted/invalid attempts remain visible; one experiment may have several attempts.
    save(directory/'observations.json',dict(updated_utc=utc(),attempts=rows,broker_capacity_validated=False,
        note='No capacity regression is fitted from unbracketed or unattributed observations.'))
    fields=['experiment','shape_id','payload_bytes','fanout','publishers','requested_rate','seconds','outcome',
            'ingress_msgps','egress_msgps_by_destination','egress_drop_by_destination','queue_growth_bytes_per_second',
            'maximum_queue_bytes','reason','artifact_path']
    with (directory/'observations.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader()
        for row in rows:
            work=row['workload'];r=row['transition'];m=r.get('metrics',{})
            v=dict(experiment=work['id'],shape_id=work['shape_id'],payload_bytes=work['payload_bytes'],fanout=work['fanout'],
                publishers=work['publishers'],requested_rate=row['rate'],seconds=row['seconds'],outcome=r['outcome'],
                ingress_msgps=m.get('ingress_msgps'),egress_msgps_by_destination=json.dumps(m.get('egress_msgps_by_destination')),
                egress_drop_by_destination=json.dumps(m.get('egress_drop_by_destination')),reason=r.get('reason'),artifact_path=row['artifact_path'])
            v['queue_growth_bytes_per_second']=json.dumps({k:t.get('slope_per_second') for k,t in m.get('queue_byte_trends',{}).items()})
            v['maximum_queue_bytes']=m.get('maximum_queue_bytes')
            writer.writerow(v)
    shapes={}
    for row in rows:
        work=row['workload'];r=row['transition'];s=shapes.setdefault(work['shape_id'],dict(
            payload_bytes=work['payload_bytes'],fanout=work['fanout'],stable_observed_lower_rate=None,
            candidate_transition_rate=None,broker_capacity_validated=False))
        if r['outcome']=='STABLE_FOR_FIVE_MINUTES':
            s['stable_observed_lower_rate']=max(s['stable_observed_lower_rate'] or 0,r['metrics']['ingress_msgps'])
        elif r['outcome']=='EGRESS_DEGRADATION_WITH_BACKLOG':s['candidate_transition_rate']=r['metrics']['ingress_msgps']
    save(directory/'model-readiness.json',dict(shapes=shapes,capacity_model_fitted=False,
        reason='Finite-horizon lower observations and transition candidates are not confirmed broker-capacity targets.'))
    if state:save(directory/'controller.json',dict(updated_utc=utc(),pid=os.getpid(),**state))
    return rows


def network_check(runtime,directory):
    """Record sustained worker baseline and relevant ENA evidence; do not infer baseline from 'up to'."""
    # Host/NIC and TCP diagnostics are collected by SDKPerfBackend during each point.
    return dict(required=True,source='per-point host_by_role and connection_diagnostics',
                qualified=False,note='A quiet average NIC is not proof against PPS/flow limits; a candidate transition needs hardware controls.')


def run(directory,token_file):
    directory=Path(directory).resolve();plan=prepare(directory)
    binding=json.loads((directory/'binding.json').read_text())
    verified_credentials(binding,token_file)
    state=dict(state='preflight',requested_experiments=len(plan['points']),completed_experiments=0)
    export(directory,state)
    stopped=False
    def stop(*_):
        nonlocal stopped
        stopped=True
    signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop)
    fleet_path=None;budget=None
    lease_end=None
    with lock(ROOT/'.pilot/locks/sdkperf-live.lock'),lock(ROOT/'.pilot/locks'/('broker-'+binding['service_id']+'.lock')):
        try:
            # Check broker isolation before launching paid AWS workers or writing queues.
            probe=PilotBackend(binding,token_file,plan,{})
            save(directory/'preflight.json',probe.preflight())
            budget=DailyBudget(ROOT/'.pilot/cost-control.sqlite',plan['budget_policy'])
            for p in (ROOT/'data').glob('*/manifest.json'):
                if not json.loads(p.read_text()).get('simulation',True):budget.import_campaign(p.parent)
            if not budget.admission(5)['allowed']:
                export(directory,dict(state='budget_paused',budget=budget.status()));return
            if stopped:return
            run_id='bcm-near-math-'+uuid.uuid4().hex[:10]
            fleet_path=near_runtime.provision(run_id,broker_binding=binding,token_file=token_file)
            save(directory/'fleet-reference.json',dict(path=str(fleet_path),run_id=run_id))
            # Time includes bootstrap. Both AWS Scheduler and host shutdown independently bound compute.
            fleet=json.loads(fleet_path.read_text())
            lease_end=min(time.monotonic()+plan['maximum_active_hours']*3600,
                time.monotonic()+datetime.fromisoformat(fleet['deadline_utc']).timestamp()-time.time()-600)
            export(directory,dict(state='worker_bootstrap',fleet_path=str(fleet_path)))
            ready_until=time.monotonic()+900
            while time.monotonic()<ready_until:
                if stopped:raise KeyboardInterrupt
                try:runtime_path=near_runtime.connect(fleet_path);break
                except Exception as exc:
                    print('Worker bootstrap: '+type(exc).__name__,flush=True);time.sleep(10)
            else:raise RuntimeError('Worker bootstrap timed out')
            runtime=json.loads(runtime_path.read_text())
            backend=PilotBackend(binding,token_file,plan,runtime)
            backend.doctor(directory/'doctor')
            save(directory/'runtime.json',runtime)
            for fanout in [1,2]:backend.resources(dict(mode='persistent',fanout=fanout))
            save(directory/'inventory-after-setup.json',backend.snapshot())
            # Recover owned backlog only, preserving data; book a conservative envelope first.
            def reserve_recovery(size,lifetime,kind):
                r=budget.reserve(run_id+':recovery:'+uuid.uuid4().hex,size,lifetime,kind)
                if not r['allowed']:raise RuntimeError('Budget cannot admit backlog recovery; data retained')
            backend.reserve_recovery=reserve_recovery
            for work in plan['points']:
                if stopped:raise KeyboardInterrupt
                if digest()!=plan['source_sha256']:raise RuntimeError('Measurement source changed; stopped before mixing epochs')
                prior=[r for r in results(directory) if r['workload']['id']==work['id']]
                if any(r['transition']['outcome'] not in {'INTERRUPTED','INCONCLUSIVE'} for r in prior):continue
                if time.monotonic()+work['seconds']+360>=lease_end:
                    state=dict(state='lease_paused',reason='Independent cleanup deadline');break
                # Each actual retry gets a distinct reservation and artifact path.
                attempt=directory/'points'/work['id']/('attempt-'+uuid.uuid4().hex[:8])
                reserved=wire_reservation(work,work['offered_rate'],work['seconds'])
                admission=budget.reserve(str(attempt),reserved,work['seconds']+600,'math_pilot_point')
                if not admission['allowed']:
                    state=dict(state='budget_paused',next_experiment=work['id'],budget=budget.status());break
                state=dict(state='measuring',current_experiment=work['id'],workload=work,budget=budget.status(),fleet_path=str(fleet_path))
                export(directory,state)
                print(json.dumps(state),flush=True)
                backend.run_stage(work,dict(rate=work['offered_rate'],seconds=300,purpose='math_pilot'),attempt,
                    lambda:stopped or time.monotonic()>=lease_end,Progress(work['id']))
                e=json.loads((attempt/'evidence.json').read_text())
                transition=classify_transition(e)
                save(attempt/'observation.json',dict(workload=work,rate=work['offered_rate'],seconds=e['measurement_seconds'],
                    transition=transition,source_sha256=plan['source_sha256'],recorded_utc=utc(),reserved_bytes=reserved,
                    fleet_path=str(fleet_path)))
                rows=export(directory,state)
                print(work['id']+': '+transition['outcome'],flush=True)
                if transition['outcome'] in {'APPARATUS_LIMITED','CLIENT_OR_PATH_LIMITED','INCONCLUSIVE','INGRESS_UNSTABLE',
                                            'UNDERDRIVEN_OR_BACKPRESSURED','CENSORED','EGRESS_DEGRADATION_WITH_BACKLOG',
                                            'BACKLOG_GROWTH_WITHOUT_EGRESS_DECLINE'}:
                    state=dict(state='review_required',last_experiment=work['id'],reason=transition['reason'],
                        observations=len(rows),broker_capacity_validated=False)
                    break
            else:state=dict(state='complete',observations=len(results(directory)),broker_capacity_validated=False)
        except KeyboardInterrupt:
            state=dict(state='interrupted',broker_capacity_validated=False)
        except Exception as exc:
            state=dict(state='stopped_error',error=type(exc).__name__,message=str(exc)[:500],broker_capacity_validated=False)
        finally:
            if fleet_path:
                try:
                    termination=near_runtime.terminate(fleet_path)
                    state['aws_termination_requested']=True
                    save(directory/'worker-termination.json',dict(ids=[x['id'] for x in termination['instances']],requested_utc=utc()))
                except Exception as exc:
                    state['termination_error']=type(exc).__name__
            if budget:
                state['budget']=budget.status();budget.close()
            export(directory,state)
            print(json.dumps(state,indent=2),flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=['prepare','bind','run','status','stop'])
    p.add_argument('--directory',type=Path,default=ROOT/'data/math-pilot-20261102')
    p.add_argument('--token-file',type=Path,default=ROOT/'.pilot/solace-cloud-pilot.token')
    p.add_argument('--service-id')
    a=p.parse_args();d=a.directory.resolve()
    if a.command=='prepare':
        plan=prepare(d);print(json.dumps(dict(directory=str(d),points=len(plan['points']),seconds_per_point=300),indent=2))
    elif a.command=='bind':
        if not a.service_id:p.error('--service-id is required')
        prepare(d);print(json.dumps(bind(d,a.service_id,a.token_file),indent=2))
    elif a.command=='run':run(d,a.token_file)
    elif a.command=='stop':
        status=json.loads((d/'controller.json').read_text())
        import psutil
        proc=psutil.Process(status['pid'])
        if 'broker_capacity.math_pilot' not in ' '.join(proc.cmdline()):raise RuntimeError('Controller identity mismatch')
        proc.send_signal(signal.SIGTERM)
    else:
        print(json.dumps(dict(controller=json.loads((d/'controller.json').read_text()) if (d/'controller.json').exists() else None,
                             observations=len(results(d)),directory=str(d)),indent=2))


if __name__=='__main__':main()
