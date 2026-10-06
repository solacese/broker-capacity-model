"""Bounded transfer checks against three supplied AWS 5K benchmark anchors.

Uses existing dedicated broker only. This does not reproduce the HA/100-subscriber
reference environment. Read the recorded mismatches before comparing rates.
"""
import json
import signal
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from broker_capacity.grid import atomic_json, file_lock, Progress, source_hash
from broker_capacity.grid_live import LiveBackend, SERVICE_ID
from broker_capacity.orchestrator import broker_lease

CASES = [
    dict(id='V01', mode='direct', payload_bytes=1024, fanout=1, publishers=20,
         reference_rate=138026, reference_cell='D34'),
    dict(id='V02', mode='persistent', payload_bytes=1024, fanout=1, publishers=10,
         reference_rate=54923, reference_cell='D44'),
    dict(id='V03', mode='persistent', payload_bytes=204800, fanout=1, publishers=10,
         reference_rate=349, reference_cell='H44'),
]

def main():
    root = Path('data/archive-validation-20261006')
    root.mkdir(parents=True, exist_ok=True)
    cancelled = False
    def stop(signum, frame):
        nonlocal cancelled
        if cancelled: raise KeyboardInterrupt
        cancelled = True
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    with file_lock(root/'runner.lock'), file_lock(Path.home()/'.cache/broker-capacity-model/generator.lock'), broker_lease(SERVICE_ID):
        mf = root/'manifest.json'
        if mf.exists(): manifest=json.loads(mf.read_text())
        else:
            manifest=dict(campaign_id=uuid.uuid4().hex,broker_id=SERVICE_ID,
                          source_sha256=source_hash(),cases=CASES)
            atomic_json(mf,manifest)
        backend=LiveBackend(Path('.env'),manifest)
        atomic_json(root/'broker.json',dict(backend.metadata(),broker_version=backend.credentials.access.broker_version))
        results=[]
        for c in CASES:
            if cancelled:break
            cap=min(100000.,128*1024**2/(c['payload_bytes']*(1+c['fanout'])))
            stages=[('qualification',100.),('reference_probe',min(.9*c['reference_rate'],cap))]
            for label,rate in stages:
                if cancelled:break
                folder=root/c['id']/label
                saved=folder/'comparison.json'
                if saved.exists():record=json.loads(saved.read_text())
                else:
                    print(f"{c['id']} {label}: {c['mode']} {c['payload_bytes']} B, {c['publishers']} publishers, {rate:.1f} msg/s",flush=True)
                    r=backend.run_stage(c,rate,30,3,folder,lambda:cancelled,Progress(c['id']))
                    record=dict(case=c,stage=label,offered_rate=rate,reference_fraction=rate/c['reference_rate'],
                                result=r,archived_reference_validated=False,
                                setup_differences='Standalone versus HA; one subscriber/topic versus 100; Python SDK versus CCSMP; local WAN; different broker build; windows/cipher not matched')
                    if r['outcome']!='INTERRUPTED':atomic_json(saved,record)
                results.append(record)
                atomic_json(root/'results.json',results)
                print(record['result']['outcome'],record['result']['reasons'],flush=True)
                if record['result']['outcome']!='SUSTAINABLE':break
        if not cancelled:
            backend.recover(root/'final-recovery',lambda:cancelled,Progress('cleanup'))
            atomic_json(root/'cleanup.json',{'owned_queues_empty':True,'campaign_finished':True})
    return 130 if cancelled else 0

if __name__=='__main__':raise SystemExit(main())
