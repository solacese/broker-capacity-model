"""Deterministic, horizon-specific classification for the standalone grid runner."""
from __future__ import annotations
import math
from bisect import bisect_left
from statistics import mean

COUNTERS = ('dataRxMsgCount', 'dataTxMsgCount', 'dataRxByteCount', 'dataTxByteCount',
            'discardedRxMsgCount', 'discardedTxMsgCount')


def drift(values: list[tuple[float, float]], floor: float) -> bool:
    """Require growing means in three time blocks, not merely an endpoint difference."""
    if len(values) < 6:
        return False
    start, end = values[0][0], values[-1][0]
    if end <= start:
        return False
    blocks = [[], [], []]
    for t, v in values:
        blocks[min(2, int(3 * (t-start)/(end-start)))].append(v)
    if any(not b for b in blocks):
        return False
    a, b, c = map(mean, blocks)
    return b-a > floor and c-b > floor


def synchronized_lag(samples: dict[str,list[dict]], start: float, end: float) -> dict[str,list[tuple[float,float]]]:
    """Compare cumulative counts at the SAME monotonic timestamp, with bounded interpolation.

    Latest publisher minus latest receiver is invalid when their one-second clocks are offset.
    Never extrapolate, bridge missing samples, or include drain time in the trend.
    """
    publishers=[];receivers={}
    for key,events in samples.items():
        events=sorted({e['t']:e for e in events if 'measure' in e}.values(),key=lambda e:e['t'])
        if not events:continue
        if events[0]['role']=='publisher':
            publishers.append(([e['t'] for e in events],[e['measure']['submitted'] for e in events]))
        else:receivers[key]=events
    result={}
    for key,events in receivers.items():
        result[key]=[]
        for e in events:
            t=e['t']
            if not start<=t<=end:continue
            sent=0.;valid=bool(publishers)
            for times,counts in publishers:
                i=bisect_left(times,t)
                if i<len(times) and times[i]==t:sent+=counts[i];continue
                if i==0 or i==len(times) or times[i]-times[i-1]>2.5:
                    valid=False;break
                if counts[i]<counts[i-1]:valid=False;break
                ratio=(t-times[i-1])/(times[i]-times[i-1])
                sent+=counts[i-1]+ratio*(counts[i]-counts[i-1])
            if valid:result[key].append((t,max(0.,sent-e['measure']['received'])))
    return result


def assess(evidence: dict) -> dict:
    """No after-drain receipts are allowed to erase in-window throughput/backlog drift."""
    reasons, metrics = [], {}
    def result(outcome, why):
        return {'outcome': outcome, 'reasons': why, 'metrics': metrics,
                'measurement_seconds': evidence['seconds'],
                'boundary_scope': 'end_to_end_measured_path',
                'broker_capacity_validated': False}
    if evidence.get('interrupted'):
        return result('INTERRUPTED', ['operator_interrupt'])
    if evidence.get('guardrail'):
        return result('INCONCLUSIVE', ['resource_guardrail_censored'])
    if evidence.get('errors') or not evidence.get('clean_shutdown'):
        return result('INVALID', ['worker_or_telemetry_failure'] + evidence.get('errors', []))
    workers = evidence['workers']
    pubs = [w for w in workers.values() if w['role']=='publisher']
    subs = [w for w in workers.values() if w['role']=='receiver']
    if len(pubs)!=evidence['publishers'] or len(subs)!=evidence['fanout']:
        return result('INCONCLUSIVE', ['missing_worker_evidence'])
    if any(not w.get('measurement_done') for w in pubs):
        return result('INCONCLUSIVE', ['measurement_incomplete'])
    requested = int(evidence['rate']*evidence['seconds'])
    attempted = sum(w['measure']['attempted'] for w in pubs)
    submitted = sum(w['measure']['submitted'] for w in pubs)
    acked = sum(w['measure']['acked'] for w in pubs)
    rejected = sum(w['measure']['rejected'] for w in pubs)
    nacks = sum(w['measure']['nacks'] for w in pubs)
    max_cpu = max((w.get('max_cpu',0) for w in workers.values()), default=0)
    metrics.update(requested=requested, attempted=attempted, submitted=submitted,
                   acknowledged=acked, rejected=rejected, nacks=nacks,
                   generator_max_process_cpu_percent=max_cpu,
                   received_by_destination={w['id']:w['measure']['received'] for w in subs})
    if requested<=0 or not submitted:
        return result('INVALID', ['zero_traffic'])
    if max_cpu>=90:
        return result('INVALID', ['generator_core_limit'])
    if attempted < requested*.98:
        return result('INVALID', ['generator_cannot_schedule_requested_rate'])
    if any(w['measure']['unexpected'] for w in subs):
        return result('INVALID', ['unexpected_message_identity'])
    if any(w['measure']['duplicates'] for w in subs):
        reasons.append('duplicate_delivery')
    telemetry=evidence['telemetry']
    if len(telemetry)<6:
        return result('INCONCLUSIVE', ['insufficient_broker_samples'])
    ts=[s['t'] for s in telemetry]
    if any(b<=a or b-a>15 for a,b in zip(ts,ts[1:])):
        return result('INCONCLUSIVE', ['broker_sample_gap_or_order'])
    if ts[-1]-ts[0] < evidence['seconds']*.90:
        return result('INCONCLUSIVE', ['broker_measurement_coverage'])
    bracket=evidence.get('counter_bracket',[])
    if (len(bracket)!=2 or bracket[0]['t']>ts[0]
            or bracket[-1]['t']<ts[-1]):
        return result('INCONCLUSIVE', ['missing_settled_counter_bracket'])
    for k in COUNTERS:
        vals=[s['vpn'].get(k) for s in [bracket[0],*telemetry,bracket[-1]]]
        if any(not isinstance(v,(int,float)) or not math.isfinite(v) for v in vals):
            return result('INCONCLUSIVE', ['missing_counter:'+k])
        if any(b<a for a,b in zip(vals,vals[1:])):
            return result('INCONCLUSIVE', ['counter_reset:'+k])
        metrics[k+'_delta']=vals[-1]-vals[0]
    # Broker counts bracket the publish interval; do not infer Direct acceptance from API calls.
    accepted=metrics['dataRxMsgCount_delta']
    metrics['broker_accepted_rate']=accepted/evidence['seconds']
    if accepted>submitted*1.02+2:
        return result('INVALID', ['unrelated_ingress_or_counter_scope'])
    if accepted<submitted*.98:
        reasons.append('broker_ingress_below_submitted')
    if rejected or nacks or submitted<attempted*.98:
        reasons.append('publisher_backpressure_or_rejection')
    if evidence['mode']=='persistent' and acked<submitted*.99:
        reasons.append('publisher_ack_shortfall')
    for k in ('discardedRxMsgCount','discardedTxMsgCount'):
        if metrics[k+'_delta']>0: reasons.append(k)
    for w in subs:
        count=w['measure']['received']
        if count>submitted:
            return result('INVALID', ['impossible_unique_receipts'])
        # Loss-free criterion after bounded drain; record actual observed receipts only.
        if count!=submitted: reasons.append('delivery_shortfall:'+w['id'])
    for name in evidence.get('queue_names', []):
        for field,floor in [('msgSpoolUsage',max(4096,evidence['payload_bytes']*2)),('txUnackedMsgCount',2)]:
            vals=[]
            for s in telemetry:
                v=s['queues'].get(name,{}).get(field)
                if not isinstance(v,(int,float)):
                    return result('INCONCLUSIVE', ['missing_queue_gauge:'+name+':'+field])
                vals.append((s['t'],v))
            if drift(vals,floor): reasons.append('sustained_queue_growth:'+name+':'+field)
    # Receiver counts measured during publishing, excluding drain. Persistent accumulation
    # can sit in client buffers even while SEMP spool gauges look flat.
    lag_series=evidence.get('receiver_lag_series',{})
    if set(lag_series)!={w['id'] for w in subs}:
        return result('INCONCLUSIVE', ['missing_receiver_time_series'])
    for key, series in lag_series.items():
        if (len(series)<6 or series[-1][0]-series[0][0]<evidence['seconds']*.70
                or any(b[0]<=a[0] or b[0]-a[0]>5 for a,b in zip(series,series[1:]))):
            return result('INCONCLUSIVE', ['insufficient_aligned_receiver_samples:'+key])
        if drift(series,max(10,evidence['rate']*.05)):
            reasons.append('sustained_delivery_lag:'+key)
    if not evidence.get('queues_empty',False): reasons.append('queue_not_drained')
    return result('UNSUSTAINABLE' if reasons else 'SUSTAINABLE', reasons or ['all_checks_passed_for_measured_horizon'])
