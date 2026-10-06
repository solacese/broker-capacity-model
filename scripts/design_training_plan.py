"""Reproducible 2,000-configuration design and workbook input preparation."""
import hashlib
import itertools
import json
import random
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
TEMP=ROOT/'.pilot/benchmark-review'
SEED=20261006
SIZES=[64,100,256,512,1024,2048,10240,20480,65536,102400]
FANOUTS=[1,2,3,4,5,8,10,12,16,20]
PUBLISHERS=[1,2,4,10,20]

def main():
    grid=json.loads((ROOT/'campaigns/grid-100.json').read_text())
    keys={(w['mode'],w['payload_bytes'],w['fanout'],w['publishers']):w['id'] for w in grid}
    pilot_groups={k[:3] for k in keys}
    rng=random.Random(SEED);splits={};groups={}
    for mode in ['direct','persistent']:
        candidates=list(itertools.product([mode],SIZES,FANOUTS))
        forced=[k for k in candidates if k in pilot_groups]
        rest=[k for k in candidates if k not in pilot_groups];rng.shuffle(rest)
        ordered=forced+rest
        for i,k in enumerate(ordered):
            splits[k]='Train' if i<70 else 'Validation' if i<85 else 'Test'
            groups[k]=f'F{len(groups)+1:03}'
    configs=[]
    for tier,mode,size,fanout,pubs in itertools.product(['5K','1K'],['direct','persistent'],SIZES,FANOUTS,PUBLISHERS):
        base=keys.get((mode,size,fanout,pubs)) if tier=='5K' else None
        configs.append(dict(id=f'T{len(configs)+1:04}',broker_tier=tier,mode=mode,payload_bytes=size,
                            fanout=fanout,publishers=pubs,consumers=fanout,split=splits[(mode,size,fanout)],
                            group_id=groups[(mode,size,fanout)],grid_100_id=base,
                            deployment='Standalone',protocol='SMF/TLS',
                            execution='Existing 100-grid entry' if base else 'Runner extension needed' if tier=='5K' else 'Isolated 1K and runner extension needed'))
    assert len(configs)==2000
    assert len({(r['broker_tier'],r['mode'],r['payload_bytes'],r['fanout'],r['publishers']) for r in configs})==2000
    assert Counter(r['split'] for r in configs)=={'Train':1400,'Validation':300,'Test':300}
    assert len([r for r in configs if r['grid_100_id']])==100
    assert all(len({r['split'] for r in configs if r['group_id']==g})==1 for g in groups.values())
    plan=dict(schema_version=1,seed=SEED,scope='Proposed SMF/TLS standalone 1K and 5K; no provisioning authorized',
              experiment_unit='One broker/workload boundary search; rate stages and repeats are not separate configurations',
              split_rule='Group mode + payload + fanout across every publisher count and both brokers; all pilot families forced into Train',
              payload_bytes=SIZES,fanouts=FANOUTS,publisher_counts=PUBLISHERS,experiments=configs)
    (ROOT/'campaigns/grid-2000.json').write_text(json.dumps(plan,indent=2)+'\n')
    archive=json.loads((TEMP/'archive-data.json').read_text())
    aws=next(w for w in archive if 'New-Instances' in w['file'])
    normalized=[];inventory=[]
    for w in archive:
        for s in w['sheets']:
            inventory.append([w['file'],s['name'],len(s['rows']),
                              'Normalized on AWS Benchmarks' if w is aws else 'Inventoried; original workbook retained'])
    inventory.append(['Cloud-Performance-Test-Descriptions.pdf','Pages 1–4',4,'Read; streaming topology and unspooling/replay/tracing conditions'])
    for s in aws['sheets']:
        byrow={r['row']:r['cells'] for r in s['rows']}
        tier=s['name'].replace('Solace-Cloud-','').upper();settings={}
        for r in s['rows']:
            c=r['cells']
            if r['row']<30 and isinstance(c.get('B'),str):settings[c['B']]=c.get('C')
        for header,rows,scenario in [(33,range(34,39),'Direct'),(43,range(44,49),'Guaranteed streaming'),
                                      (53,range(54,58),'Streaming + unspooling'),(62,range(63,67),'Streaming + unspooling + replay'),
                                      (71,range(72,76),'Streaming + unspooling + tracing')]:
            for r in rows:
                for ci,co in zip('CDEFGH','KLMNOP'):
                    a=byrow.get(r,{});v=a.get(ci)
                    if not isinstance(v,(int,float)):continue
                    normalized.append(dict(id=f'AWS{len(normalized)+1:04}',tier=tier,scenario=scenario,
                        payload_bytes=byrow[header][ci],fanout=a['B'],ingress=v,egress=a.get(co),
                        publishers=20 if scenario=='Direct' else 10,subscribers=100,broker_mode='HA',
                        instance=settings.get('Instance Type'),memory=settings.get('Memory'),
                        disk=settings.get('Spool Disk Size'),build='10.8.1.241',api='CCSMP',
                        sheet=s['name'],ingress_cell=f'{ci}{r}',egress_cell=f'{co}{r}'))
    assert len(normalized)==792,len(normalized)
    snapshot=json.loads((ROOT/'data/grid-100/results.json').read_text())
    validation_path=ROOT/'data/archive-validation-20261006/results.json'
    validation=json.loads(validation_path.read_text()) if validation_path.exists() else []
    data=dict(created=datetime.now(ZoneInfo('Europe/Rome')).isoformat(timespec='seconds'),
              grid=grid,plan=plan,archive_inventory=inventory,benchmarks=normalized,
              archive_name=aws['file'],archive_sha256=hashlib.sha256((TEMP/aws['file']).read_bytes()).hexdigest(),
              campaign_snapshot=snapshot,validation=validation)
    (TEMP/'workbook-data.json').write_text(json.dumps(data,indent=2)+'\n')
    print(json.dumps({'grid':len(grid),'training':len(configs),'splits':dict(Counter(r['split'] for r in configs)),
                      'benchmarks':len(normalized),'archive_sheets':len(inventory)-1,'validation_stages':len(validation)}))

if __name__=='__main__':main()
