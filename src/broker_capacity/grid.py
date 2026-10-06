"""Resumable deterministic grid + adaptive saturation search. No LLM dependencies."""
from __future__ import annotations
import argparse
import fcntl
import hashlib
import itertools
import json
import math
import random
import signal
import sqlite3
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from .grid_live import LiveBackend,SERVICE_ID
from .orchestrator import broker_lease

DEFAULTS=dict(warmup_seconds=10,probe_seconds=30,confirm_seconds=120,max_probe_stages=16,
              max_rate=100000.,aggregate_mib_per_second=128.,initial_rate=100.,relative_width=.10)

def make_grid():
    values=[dict(mode=m,payload_bytes=b,fanout=f,publishers=p) for m,b,f,p in
            itertools.product(['direct','persistent'],[64,256,1024,10240,102400],[1,2,5,10,20],[1,4])]
    random.Random(20261005).shuffle(values)
    anchors=[dict(mode=m,payload_bytes=1024,fanout=1,publishers=1) for m in ['direct','persistent']]
    values=anchors+[w for w in values if w not in anchors]
    return [dict(id=f'G{i:03}',**w) for i,w in enumerate(values,1)]

def rate_cap(work,settings):
    return max(1.,min(settings['max_rate'],settings['aggregate_mib_per_second']*1024**2/(work['payload_bytes']*(1+work['fanout']))))

def bounds(history,kind):
    good=[h['rate'] for h in history if h['kind']==kind and h['result']['outcome']=='SUSTAINABLE']
    bad=[h['rate'] for h in history if h['kind']==kind and h['result']['outcome']=='UNSUSTAINABLE']
    return max(good,default=None),min(bad,default=None)

def next_step(history,work,settings):
    """Pure restartable state machine. Invalid observations NEVER move a boundary."""
    cap=rate_cap(work,settings);probes=[h for h in history if h['kind']=='probe']
    lo,hi=bounds(history,'probe')
    def finish(reason):return {'done':True,'reason':reason}
    def step(kind,rate):return {'kind':kind,'rate':rate,'seconds':settings['probe_seconds'] if kind=='probe' else settings['confirm_seconds']}
    if lo is not None and hi is not None and hi<=lo:return finish('NON_MONOTONIC')
    if probes and probes[-1]['result']['outcome'] in ('INVALID','INCONCLUSIVE'):
        if len(probes)>1 and probes[-2]['result']['outcome'] in ('INVALID','INCONCLUSIVE'):
            return finish('CLIENT_OR_EVIDENCE_LIMIT')
        if len(probes)<settings['max_probe_stages']:return step('probe',probes[-1]['rate'])
    bracketed=lo is not None and hi is not None and (hi-lo)/lo<=settings['relative_width']
    exhausted=len(probes)>=settings['max_probe_stages'] or (lo is not None and lo>=cap) or (hi is not None and hi<=1)
    if not bracketed and not exhausted:
        if lo is not None and hi is not None:rate=(lo+hi)/2
        elif lo is not None:rate=min(cap,lo*2)
        elif hi is not None:rate=max(1.,hi/2)
        else:rate=min(cap,settings['initial_rate'])
        return step('probe',rate)
    confirmations=[h for h in history if h['kind']!='probe']
    if any(h['result']['outcome'] in ('INVALID','INCONCLUSIVE') for h in confirmations):return finish('CONFIRMATION_INCONCLUSIVE')
    low=[h for h in confirmations if h['kind']=='confirm_low']
    if any(h['result']['outcome']!='SUSTAINABLE' for h in low):return finish('HORIZON_DEPENDENT')
    if lo is not None and len(low)<2:return step('confirm_low',lo)
    high=[h for h in confirmations if h['kind']=='confirm_high']
    if hi is not None and not high:return step('confirm_high',hi)
    if high and high[-1]['result']['outcome']=='SUSTAINABLE':return finish('CONFIRMED_UPPER_DISAGREES')
    return finish('MEASURED' if lo is not None or hi is not None else 'NO_VALID_EVIDENCE')

def summarize(history,reason):
    lo,hi=bounds(history,'probe');cl,ch=None,None
    lows=[h['rate'] for h in history if h['kind']=='confirm_low' and h['result']['outcome']=='SUSTAINABLE']
    highs=[h for h in history if h['kind']=='confirm_high']
    if len(lows)>=2:cl=max(lows)
    for h in highs:
        if h['result']['outcome']=='UNSUSTAINABLE':ch=h['rate']
        elif h['result']['outcome']=='SUSTAINABLE':ch=None
    return dict(reason=reason,scout_lower=lo,scout_upper=hi,confirmed_lower=cl,confirmed_upper=ch,
                interval_kind='interval' if cl is not None and ch is not None and cl<ch else
                'right_censored' if cl is not None else 'left_censored' if ch is not None else 'unresolved',
                confirmed_relative_width=(ch-cl)/cl if cl is not None and ch is not None and cl>0 else None,
                boundary_scope='end_to_end_measured_path',broker_capacity_validated=False,stages=len(history))

@contextmanager
def file_lock(path):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('a') as h:
        try:fcntl.flock(h,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise SystemExit('Another runner is active: '+str(path)) from None
        try:yield
        finally:fcntl.flock(h,fcntl.LOCK_UN)

def atomic_json(path,data):
    tmp=path.with_suffix(path.suffix+'.tmp');tmp.write_text(json.dumps(data,indent=2)+'\n');tmp.replace(path)

def source_hash():
    base=Path(__file__).parent
    names=['grid.py','grid_live.py','grid_detection.py','grid_workers.py','cloud.py','config_writer.py','loadgen.py','semp.py']
    return hashlib.sha256(b''.join((base/n).read_bytes() for n in names)).hexdigest()

class Ledger:
    def __init__(self,path):
        self.db=sqlite3.connect(path);self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''CREATE TABLE IF NOT EXISTS experiments(id TEXT PRIMARY KEY, workload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', summary TEXT);
        CREATE TABLE IF NOT EXISTS stages(id INTEGER PRIMARY KEY AUTOINCREMENT, experiment TEXT NOT NULL, kind TEXT NOT NULL, rate REAL NOT NULL, seconds REAL NOT NULL, state TEXT NOT NULL, path TEXT NOT NULL, result TEXT, started REAL NOT NULL, finished REAL);''')
    def populate(self,grid):
        with self.db:self.db.executemany('INSERT OR IGNORE INTO experiments(id,workload) VALUES (?,?)',[(w['id'],json.dumps(w)) for w in grid])
    def recover(self):
        with self.db:
            self.db.execute("UPDATE stages SET state='interrupted',finished=? WHERE state='running'",(time.time(),))
            self.db.execute("UPDATE experiments SET status='pending' WHERE status='running'")
    def history(self,id):
        return [dict(kind=k,rate=r,result=json.loads(v)) for k,r,v in self.db.execute("SELECT kind,rate,result FROM stages WHERE experiment=? AND state='complete' ORDER BY id",(id,))]
    def start(self,id,step,path):
        with self.db:
            self.db.execute("UPDATE experiments SET status='running' WHERE id=?",(id,))
            c=self.db.execute('INSERT INTO stages(experiment,kind,rate,seconds,state,path,started) VALUES (?,?,?,?,?,?,?)',(id,step['kind'],step['rate'],step['seconds'],'running',str(path),time.time()))
        return c.lastrowid
    def finish(self,id,result):
        with self.db:self.db.execute('UPDATE stages SET state=?,result=?,finished=? WHERE id=?',('interrupted' if result['outcome']=='INTERRUPTED' else 'complete',json.dumps(result),time.time(),id))
    def pause(self):
        with self.db:
            self.db.execute("UPDATE stages SET state='interrupted',finished=? WHERE state='running'",(time.time(),))
            self.db.execute("UPDATE experiments SET status='pending' WHERE status='running'")
    def complete(self,id,summary):
        with self.db:self.db.execute("UPDATE experiments SET status='done',summary=? WHERE id=?",(json.dumps(summary),id))
    def rows(self):return list(self.db.execute('SELECT id,workload,status,summary FROM experiments ORDER BY id'))
    def export(self,path,manifest):
        out={'simulation':manifest['simulation'],'campaign_id':manifest['campaign_id'],'broker_id':manifest['broker_id'],
             'settings':manifest['settings'],'broker_capacity_validated':False,
             'experiments':[{'id':i,'workload':json.loads(w),'status':s,'summary':json.loads(v) if v else None} for i,w,s,v in self.rows()]}
        atomic_json(path/'results.json',out)
        done=sum(r[2]=='done' for r in self.rows());stages=self.db.execute('SELECT count(*) FROM stages WHERE state=\'complete\'').fetchone()[0]
        report=f'# Grid campaign results\n\n{"SIMULATED evidence only" if manifest["simulation"] else "Measured end-to-end path; intrinsic broker capacity remains unvalidated"}.\n\n{done}/{len(self.rows())} experiments finished; {stages} rate stages recorded.\n\n| ID | Status | Scout interval msg/s | Confirmed interval msg/s | Reason |\n|---|---|---|---|---|\n'
        for i,w,s,v in self.rows():
            v=json.loads(v) if v else {}
            fmt=lambda x:'?' if x is None else f'{x:.1f}'
            report+=f'| {i} | {s} | {fmt(v.get("scout_lower"))} – {fmt(v.get("scout_upper"))} | {fmt(v.get("confirmed_lower"))} – {fmt(v.get("confirmed_upper"))} | {v.get("reason","")} |\n'
        (path/'report.md').write_text(report)

class SimBackend:
    def metadata(self):return {'simulation':True,'broker_capacity_validated':False}
    def run_stage(self,work,rate,seconds,warmup,run_dir,cancel,progress):
        run_dir.mkdir(parents=True,exist_ok=True)
        cap=min(12500 if work['mode']=='direct' else 7500,12_000_000/(work['payload_bytes']*(1+work['fanout'])))
        progress('SIMULATED',seconds,seconds)
        r=dict(outcome='INTERRUPTED' if cancel() else 'SUSTAINABLE' if rate<=cap else 'UNSUSTAINABLE',reasons=['synthetic_validation_fixture'],measurement_seconds=seconds,simulation=True)
        atomic_json(run_dir/'result.json',r);return r

class Progress:
    def __init__(self,label):self.label=label;self.last=0
    def __call__(self,phase,current,total):
        now=time.monotonic()
        if now-self.last< (1 if sys.stdout.isatty() else 10):return
        self.last=now
        text=f'  {self.label} {phase}: {current:.0f}/{total:.0f}'
        print(('\r'+text+' '*15) if sys.stdout.isatty() else text,end='' if sys.stdout.isatty() else '\n',flush=True)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='command',required=True)
    plan=sub.add_parser('plan',help='show the 100-workload grid without network access')
    plan.add_argument('--json',action='store_true')
    status=sub.add_parser('status',help='read saved progress without network access')
    status.add_argument('--directory',type=Path,default=Path('data/grid-100'))
    run=sub.add_parser('run',help='create or resume the 100-experiment campaign')
    run.add_argument('--directory',type=Path,default=Path('data/grid-100'))
    run.add_argument('--token-file',type=Path,default=Path('.env'))
    run.add_argument('--experiments',type=int,choices=[100],default=100)
    run.add_argument('--limit',type=int,help='finish at most this many unfinished experiments this invocation')
    run.add_argument('--only',help='comma-separated workload IDs to run now; the complete 100-row grid remains saved')
    run.add_argument('--simulate',action='store_true',help='synthetic integration test; never connects to a broker')
    run.add_argument('--accept-code-change',action='store_true',help='record a new code revision when resuming after a repair')
    for key in DEFAULTS:
        run.add_argument('--'+key.replace('_','-'),type=float if key in ('max_rate','aggregate_mib_per_second','initial_rate','relative_width') else int)
    args=parser.parse_args(argv)
    if args.command=='plan':
        grid=make_grid()
        if args.json:print(json.dumps(grid,indent=2))
        else:
            print('100 unique SMF/TLS workloads: 2 modes × 5 payloads × 5 fanouts × 2 publisher counts')
            for w in grid:print(f'{w["id"]} {w["mode"]:10} {w["payload_bytes"]:6}B fanout={w["fanout"]:2} publishers={w["publishers"]}')
        return 0
    path=args.directory.resolve()
    if args.command=='status':
        if not (path/'ledger.sqlite').exists():parser.error('campaign does not exist')
        db=sqlite3.connect('file:'+str(path/'ledger.sqlite')+'?mode=ro',uri=True)
        print('Experiments:',dict(db.execute('SELECT status,count(*) FROM experiments GROUP BY status')))
        print('Stages:',dict(db.execute('SELECT state,count(*) FROM stages GROUP BY state')))
        for row in db.execute('SELECT experiment,kind,rate,state FROM stages ORDER BY id DESC LIMIT 5'):print(row)
        print('Report:',path/'report.md');db.close();return 0
    if args.limit is not None and args.limit<1:parser.error('--limit must be positive')
    selected=set(args.only.split(',')) if args.only else None
    if selected and not selected.issubset({w['id'] for w in make_grid()}):parser.error('unknown --only workload ID')
    path.mkdir(parents=True,exist_ok=True)
    with file_lock(path/'runner.lock'):
        mf=path/'manifest.json'
        if mf.exists():
            manifest=json.loads(mf.read_text())
            if manifest['simulation']!=args.simulate:parser.error('cannot mix simulated and live evidence in a directory')
            for k,v in manifest['settings'].items():
                value=getattr(args,k)
                if value is not None and value!=v:parser.error('resume settings are immutable; choose a new directory to change '+k)
            if manifest['source_sha256']!=source_hash():
                if not args.accept_code_change:parser.error('code changed; use --accept-code-change to record the revision or choose a new directory')
                manifest.setdefault('prior_source_hashes',[]).append(manifest['source_sha256']);manifest['source_sha256']=source_hash();atomic_json(mf,manifest)
        else:
            settings={k:getattr(args,k) if getattr(args,k) is not None else v for k,v in DEFAULTS.items()}
            if any(not math.isfinite(v) or v<=0 for k,v in settings.items() if k!='warmup_seconds') or settings['warmup_seconds']<0:parser.error('settings must be positive and finite')
            if settings['probe_seconds']<12 or settings['confirm_seconds']<settings['probe_seconds']:parser.error('probe must be >=12s and confirmation >= probe')
            if settings['relative_width']>=1:parser.error('relative width must be <1')
            manifest=dict(schema_version=1,campaign_id=uuid.uuid4().hex,broker_id=SERVICE_ID,simulation=args.simulate,
                          source_sha256=source_hash(),settings=settings,grid=make_grid(),created=time.time())
            atomic_json(mf,manifest)
        ledger=Ledger(path/'ledger.sqlite');ledger.populate(manifest['grid']);ledger.recover();ledger.export(path,manifest)
        cancelled=False
        def cancel_signal(signum,frame):
            nonlocal cancelled
            if cancelled:raise KeyboardInterrupt
            cancelled=True;print('\nStopping publishers and draining campaign data; checkpoint will be saved. Ctrl-C again forces stop.',flush=True)
        old_int=signal.signal(signal.SIGINT,cancel_signal);old_term=signal.signal(signal.SIGTERM,cancel_signal)
        count=0
        def execute():
            nonlocal count
            backend=SimBackend() if args.simulate else LiveBackend(args.token_file,manifest)
            atomic_json(path/'broker.json',backend.metadata())
            for id,w,status,summary in ledger.rows():
                if status=='done' or (selected is not None and id not in selected):continue
                if cancelled or (args.limit and count>=args.limit):break
                work=json.loads(w)
                done=sum(r[2]=='done' for r in ledger.rows())
                print(f'\n[{done+1}/100] {id} {work["mode"]} {work["payload_bytes"]}B fanout={work["fanout"]} publishers={work["publishers"]}',flush=True)
                while not cancelled:
                    history=ledger.history(id);step=next_step(history,work,manifest['settings'])
                    if step.get('done'):
                        result=summarize(history,step['reason']);ledger.complete(id,result);ledger.export(path,manifest);count+=1
                        print(f'  DONE {result["reason"]}: confirmed [{result["confirmed_lower"]}, {result["confirmed_upper"]}] msg/s',flush=True);break
                    attempt=ledger.db.execute('SELECT count(*) FROM stages WHERE experiment=?',(id,)).fetchone()[0]+1
                    run_dir=path/'raw'/id/f'{attempt:03}-{step["kind"]}'
                    run_dir.mkdir(parents=True,exist_ok=True)
                    atomic_json(run_dir/'stage.json',dict(workload=work,**step,source_sha256=manifest['source_sha256'],warmup_seconds=manifest['settings']['warmup_seconds']))
                    stage_id=ledger.start(id,step,run_dir)
                    print(f'  stage {attempt}: {step["kind"]} {step["rate"]:.1f} msg/s, {step["seconds"]}s measurement',flush=True)
                    try:r=backend.run_stage(work,step['rate'],step['seconds'],manifest['settings']['warmup_seconds'],run_dir,lambda:cancelled,Progress(id))
                    except BaseException:
                        # Preserve the interrupted row for safe retry, never turn infrastructure failure into a capacity label.
                        ledger.export(path,manifest);raise
                    ledger.finish(stage_id,r);ledger.export(path,manifest)
                    print(f'\n  {r["outcome"]}: {", ".join(r["reasons"])}',flush=True)
                    if r['outcome']=='INTERRUPTED':break
        try:
            if args.simulate:execute()
            else:
                with file_lock(Path.home()/'.cache/broker-capacity-model/generator.lock'),broker_lease(SERVICE_ID):execute()
        except KeyboardInterrupt:
            cancelled=True;print('\nInterrupted. Run the same command to resume.',flush=True)
        except Exception as e:
            print(f'\nPAUSED: {type(e).__name__}: {e}\nProgress saved. Fix the blocker and run the same command.',file=sys.stderr)
            return 1
        finally:
            ledger.pause();ledger.export(path,manifest);ledger.db.close()
            signal.signal(signal.SIGINT,old_int);signal.signal(signal.SIGTERM,old_term)
        print(f'\nCheckpoint: {path}\nResume with the same command. Status: ./run-grid status --directory {path}',flush=True)
        return 130 if cancelled else 0

if __name__=='__main__':raise SystemExit(main())
