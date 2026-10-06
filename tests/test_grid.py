import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from broker_capacity.grid import DEFAULTS,Ledger,make_grid,next_step,summarize,rate_cap
from broker_capacity.grid_detection import assess,drift,COUNTERS
from broker_capacity.grid_workers import Seen


def evidence():
    pub=dict(id='publisher-0',role='publisher',measurement_done=True,max_cpu=10,measure=dict(attempted=3000,submitted=3000,acked=3000,rejected=0,nacks=0))
    sub=dict(id='receiver-0',role='receiver',max_cpu=10,measure=dict(received=3000,unexpected=0,duplicates=0))
    telemetry=[dict(t=float(i),vpn={**{k:0 for k in COUNTERS},'dataRxMsgCount':i*100,'dataTxMsgCount':i*100},queues={'q':dict(msgSpoolUsage=0,txUnackedMsgCount=0)}) for i in range(0,31,2)]
    return dict(seconds=30,rate=100,mode='persistent',publishers=1,fanout=1,payload_bytes=1024,workers={'p':pub,'r':sub},
                telemetry=telemetry,counter_bracket=[copy.deepcopy(telemetry[0]),copy.deepcopy(telemetry[-1])],queue_names=['q'],receiver_lag_series={'receiver-0':[(i,10.) for i in range(0,31,2)]},queues_empty=True,clean_shutdown=True,errors=[])

def observation(rate,outcome,kind='probe'):
    return dict(kind=kind,rate=rate,result={'outcome':outcome})

class GridTests(unittest.TestCase):
    def test_exactly_100_unique_workloads_and_deterministic_order(self):
        grid=make_grid();self.assertEqual(len(grid),100);self.assertEqual(grid,make_grid())
        self.assertEqual(len({(w['mode'],w['payload_bytes'],w['fanout'],w['publishers']) for w in grid}),100)
    def test_invalid_does_not_establish_boundary(self):
        w=make_grid()[0];h=[observation(100,'SUSTAINABLE'),observation(200,'INVALID')]
        self.assertEqual(next_step(h,w,DEFAULTS)['rate'],200)
        h.append(observation(200,'INVALID'));self.assertEqual(next_step(h,w,DEFAULTS)['reason'],'CLIENT_OR_EVIDENCE_LIMIT')
        self.assertIsNone(summarize(h,'limited')['confirmed_upper'])
    def test_confirmation_required_and_horizon_change_detected(self):
        s={**DEFAULTS,'max_probe_stages':2};h=[observation(100,'SUSTAINABLE'),observation(200,'UNSUSTAINABLE')]
        n=next_step(h,make_grid()[0],s);self.assertEqual(n['kind'],'confirm_low')
        h.append(observation(100,'UNSUSTAINABLE','confirm_low'))
        self.assertEqual(next_step(h,make_grid()[0],s)['reason'],'HORIZON_DEPENDENT')
    def test_non_monotonic_search_stops(self):
        h=[observation(100,'UNSUSTAINABLE'),observation(200,'SUSTAINABLE')]
        self.assertEqual(next_step(h,make_grid()[0],DEFAULTS)['reason'],'NON_MONOTONIC')
    def test_passing_upper_confirmation_does_not_become_false_boundary(self):
        h=[observation(100,'SUSTAINABLE'),observation(200,'UNSUSTAINABLE'),
           observation(100,'SUSTAINABLE','confirm_low'),observation(100,'SUSTAINABLE','confirm_low'),
           observation(200,'SUSTAINABLE','confirm_high')]
        reason=next_step(h,make_grid()[0],{**DEFAULTS,'max_probe_stages':2})['reason']
        self.assertEqual(reason,'CONFIRMED_UPPER_DISAGREES')
        summary=summarize(h,reason)
        self.assertEqual(summary['confirmed_lower'],100)
        self.assertIsNone(summary['confirmed_upper'])
        self.assertEqual(summary['interval_kind'],'right_censored')
    def test_byte_limit_and_rate_limit(self):
        w=dict(payload_bytes=102400,fanout=20)
        self.assertLess(rate_cap(w,DEFAULTS),100)
    def test_default_search_can_reach_its_configured_rate_ceiling(self):
        w=dict(mode='direct',payload_bytes=64,fanout=1,publishers=4);h=[]
        for _ in range(30):
            n=next_step(h,w,DEFAULTS)
            if n.get('done'):break
            h.append(observation(n['rate'],'SUSTAINABLE',n['kind']))
        self.assertTrue(n.get('done'))
        self.assertEqual(summarize(h,n['reason'])['confirmed_lower'],DEFAULTS['max_rate'])
    def test_database_interruption_preserves_completed_stages(self):
        with tempfile.TemporaryDirectory() as d:
            db=Ledger(Path(d)/'state.sqlite');db.populate(make_grid())
            step=dict(kind='probe',rate=100,seconds=30)
            n=db.start('G001',step,Path(d));db.finish(n,{'outcome':'SUSTAINABLE'})
            db.start('G001',dict(step,rate=200),Path(d));db.db.close()
            db=Ledger(Path(d)/'state.sqlite');db.recover()
            self.assertEqual(len(db.history('G001')),1)
            self.assertEqual(db.db.execute("select count(*) from stages where state='interrupted'").fetchone()[0],1);db.db.close()
    def test_bounded_dedup_handles_reordering_and_duplicate(self):
        seen=Seen();self.assertTrue(seen.add(1,100));self.assertTrue(seen.add(1,0));self.assertFalse(seen.add(1,100));self.assertTrue(seen.add(2,100))

class GridDetectorTests(unittest.TestCase):
    def test_healthy_measured_window(self):self.assertEqual(assess(evidence())['outcome'],'SUSTAINABLE')
    def test_zero_traffic(self):
        e=evidence();e['workers']['p']['measure']['submitted']=0
        self.assertEqual(assess(e)['outcome'],'INVALID')
    def test_generator_limit_not_broker_fail(self):
        e=evidence();e['workers']['p']['max_cpu']=95
        self.assertEqual(assess(e)['outcome'],'INVALID')
    def test_mid_window_reset(self):
        e=evidence();e['telemetry'][6]['vpn']['dataRxMsgCount']=0
        self.assertEqual(assess(e)['outcome'],'INCONCLUSIVE')
    def test_discards_fail(self):
        e=evidence();e['telemetry'][-1]['vpn']['discardedTxMsgCount']=1
        e['counter_bracket'][-1]['vpn']['discardedTxMsgCount']=1
        self.assertEqual(assess(e)['outcome'],'UNSUSTAINABLE')
    def test_late_broker_stats_use_settled_counter_bracket(self):
        e=evidence();e['telemetry'][-1]['vpn']['dataRxMsgCount']=2900
        self.assertEqual(assess(e)['outcome'],'SUSTAINABLE')
        del e['counter_bracket']
        self.assertEqual(assess(e)['outcome'],'INCONCLUSIVE')
    def test_control_traffic_is_not_application_ingress(self):
        e=evidence()
        for s in [*e['telemetry'],*e['counter_bracket']]:
            s['vpn']['rxMsgCount']=s['t']*1000
            s['vpn']['controlRxMsgCount']=s['t']*900
        self.assertEqual(assess(e)['outcome'],'SUSTAINABLE')
    def test_growing_queue_cannot_be_hidden_by_drain(self):
        e=evidence()
        for i,s in enumerate(e['telemetry']):s['queues']['q']['msgSpoolUsage']=i*10000
        self.assertEqual(assess(e)['outcome'],'UNSUSTAINABLE')
    def test_one_destination_loss_not_masked(self):
        e=evidence();e['fanout']=2;e['workers']['r2']=copy.deepcopy(e['workers']['r']);e['workers']['r2']['id']='receiver-1';e['workers']['r2']['measure']['received']=2999
        e['receiver_lag_series']['receiver-1']=e['receiver_lag_series']['receiver-0']
        self.assertEqual(assess(e)['outcome'],'UNSUSTAINABLE')
    def test_full_drain_does_not_erase_in_window_growth(self):
        e=evidence();e['receiver_lag_series']={'receiver-0':[(i,float(i*100)) for i in range(0,31,2)]}
        self.assertEqual(assess(e)['outcome'],'UNSUSTAINABLE')
    def test_missing_or_sparse_aligned_lag_never_passes(self):
        e=evidence();e['receiver_lag_series']={}
        self.assertEqual(assess(e)['outcome'],'INCONCLUSIVE')
        e['receiver_lag_series']={'receiver-0':[(0,0),(30,0)]}
        self.assertEqual(assess(e)['outcome'],'INCONCLUSIVE')
    def test_falling_gauge_is_not_counter_reset(self):
        self.assertFalse(drift([(i,300-i*10) for i in range(12)],1))
    def test_missing_evidence_never_passes(self):
        e=evidence();del e['telemetry'][2]['vpn']['dataRxByteCount']
        self.assertEqual(assess(e)['outcome'],'INCONCLUSIVE')
    def test_guardrail_censors_and_interrupt_does_not_label(self):
        e=evidence();e['guardrail']=True;self.assertEqual(assess(e)['outcome'],'INCONCLUSIVE')
        e['interrupted']=True;self.assertEqual(assess(e)['outcome'],'INTERRUPTED')

class ConfigMissingResourceTests(unittest.TestCase):
    def test_semp_named_not_found_is_not_a_validation_error(self):
        import io
        from unittest.mock import patch
        from urllib.error import HTTPError
        from broker_capacity.config_writer import CampaignConfigWriter,ConfigAccess
        writer=CampaignConfigWriter(ConfigAccess('4qn20a1u6ny','usfos7cfqge','bcm-20261005-5k','https://example.invalid','u','p'))
        err=HTTPError('https://example.invalid',400,'Bad request',{},io.BytesIO(b'{"meta":{"error":{"status":"NOT_FOUND"}}}'))
        with patch('broker_capacity.config_writer.urlopen',side_effect=err):
            self.assertIsNone(writer._request('GET','/queue',allow_404=True))
        err=HTTPError('https://example.invalid',400,'Bad request',{},io.BytesIO(b'{"meta":{"error":{"status":"INVALID_PARAMETER"}}}'))
        with patch('broker_capacity.config_writer.urlopen',side_effect=err):
            with self.assertRaises(HTTPError):writer._request('GET','/queue',allow_404=True)

class TimestampAlignmentTests(unittest.TestCase):
    def test_offset_healthy_samples_have_stable_lag(self):
        from broker_capacity.grid_detection import synchronized_lag
        pub=[dict(t=float(i),role='publisher',measure={'submitted':i*100}) for i in range(32)]
        sub=[dict(t=i+.8,role='receiver',measure={'received':i*100+60}) for i in range(31)]
        lag=synchronized_lag({'p':pub,'r':sub},1,30)['r']
        self.assertGreater(len(lag),20)
        self.assertTrue(all(abs(v-20)<1e-9 for _,v in lag))
        self.assertFalse(drift(lag,10))
    def test_no_extrapolation_or_interpolation_across_missing_samples(self):
        from broker_capacity.grid_detection import synchronized_lag
        pub=[dict(t=0.,role='publisher',measure={'submitted':0}),dict(t=10.,role='publisher',measure={'submitted':1000})]
        sub=[dict(t=5.,role='receiver',measure={'received':100})]
        self.assertEqual(synchronized_lag({'p':pub,'r':sub},0,10)['r'],[])

if __name__=='__main__':unittest.main()
