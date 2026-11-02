import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from broker_capacity.math_pilot import classify_transition, prepare, verified_credentials, ScopedWriter, run


def evidence(degrade=False, flat_deficit=False):
    rate=1000
    samples=[]
    delivered=0
    for t in range(0,301,2):
        if t:
            egress=rate if not degrade and not flat_deficit else 800 if flat_deficit else rate*(1-.4*t/300)
            delivered+=egress*2
        accepted=t*rate
        lag=max(0,accepted-delivered)
        samples.append(dict(t=t,accepted=accepted,received_by_destination=[delivered],
            queue_bytes={'q':lag*1024},slow_subscriber_by_destination=[False]))
    summary=dict(transmitted=0,received=300000,checked=300000,crc_ok=300000,
                 crc_errors=0,duplicates=0,missing=0)
    return dict(workload=dict(publishers=1,fanout=1,payload_bytes=1024),rate=rate,
        ready=True,clean_shutdown=True,counter_scope_valid=True,errors=[],measurement_seconds=300,
        samples=samples,queue_names=['q'],cpu_telemetry_valid=True,
        cpu_by_worker={'p0':dict(max_effective_percent=100),'s0':dict(max_effective_percent=20)},
        publishers=[dict(summary,transmitted=300000)],subscribers=[summary],queues_empty=True)


class TransitionTests(unittest.TestCase):
    def test_stable_is_not_a_capacity_or_disk_label(self):
        r=classify_transition(evidence())
        self.assertEqual(r['outcome'],'STABLE_FOR_FIVE_MINUTES')
        self.assertFalse(r['broker_capacity_validated'])
        self.assertFalse(r['capacity_training_eligible'])
        self.assertEqual(r['slow_path_mechanism'],'NOT_PROVEN')

    def test_stable_ingress_egress_decline_and_queue_growth_is_candidate(self):
        r=classify_transition(evidence(degrade=True))
        self.assertEqual(r['outcome'],'EGRESS_DEGRADATION_WITH_BACKLOG')
        self.assertGreater(r['metrics']['egress_drop_by_destination'][0],.10)
        self.assertFalse(r['broker_capacity_validated'])

    def test_queue_growth_without_decline_is_distinct(self):
        r=classify_transition(evidence(flat_deficit=True))
        self.assertEqual(r['outcome'],'BACKLOG_GROWTH_WITHOUT_EGRESS_DECLINE')
        self.assertGreaterEqual(r['metrics']['tail_egress_deficit_fraction_by_destination'][0],.20)
        self.assertFalse(r['broker_capacity_validated'])

    def test_deficit_and_queue_growth_must_be_on_same_destination(self):
        e=evidence(flat_deficit=True);e['workload']['fanout']=2;e['queue_names']=['q0','q1']
        e['subscribers']*=2
        for sample in e['samples']:
            sample['received_by_destination'].append(sample['accepted'])
            sample['slow_subscriber_by_destination'].append(False)
            sample['queue_bytes']={'q0':0,'q1':(sample['accepted']-sample['received_by_destination'][0])*1024}
        self.assertEqual(classify_transition(e)['outcome'],'INCONCLUSIVE')

    def test_consumer_pressure_blocks_broker_candidate(self):
        e=evidence(degrade=True);e['cpu_by_worker']['s0']['max_effective_percent']=95
        self.assertEqual(classify_transition(e)['outcome'],'CLIENT_OR_PATH_LIMITED')

    def test_slow_transport_blocks_broker_candidate(self):
        e=evidence(degrade=True);e['samples'][-1]['slow_subscriber_by_destination']=[True]
        self.assertEqual(classify_transition(e)['outcome'],'CLIENT_OR_PATH_LIMITED')

    def test_counter_reset_is_unknown(self):
        e=evidence();e['samples'][50]['accepted']=0
        self.assertEqual(classify_transition(e)['outcome'],'INCONCLUSIVE')

    def test_short_or_missing_measurement_cannot_pass(self):
        e=evidence();e['samples']=e['samples'][:-1]
        self.assertEqual(classify_transition(e)['outcome'],'INCONCLUSIVE')
        e=evidence();e['cpu_telemetry_valid']=False
        self.assertEqual(classify_transition(e)['outcome'],'INCONCLUSIVE')

    def test_rate_undertracking_is_not_broker_capacity(self):
        e=evidence();e['rate']=2000
        self.assertEqual(classify_transition(e)['outcome'],'UNDERDRIVEN_OR_BACKPRESSURED')

    def test_guardrail_is_censoring(self):
        e=evidence(degrade=True);e['guardrail']='queue_spool_budget'
        self.assertEqual(classify_transition(e)['outcome'],'CENSORED')

    def test_fanout_normalization_uses_each_destination(self):
        e=evidence();e['workload']['fanout']=2
        e['queue_names']=['q0','q1']
        for s in e['samples']:
            s['received_by_destination']*=2;s['slow_subscriber_by_destination']*=2
            s['queue_bytes']={'q0':s['queue_bytes']['q'],'q1':s['queue_bytes']['q']}
        e['subscribers']*=2
        self.assertEqual(classify_transition(e)['outcome'],'STABLE_FOR_FIVE_MINUTES')

    def test_crc_error_is_not_a_valid_stable_point(self):
        e=evidence();e['subscribers'][0]['crc_errors']=1
        self.assertEqual(classify_transition(e)['outcome'],'INCONCLUSIVE')


class PilotDesignTests(unittest.TestCase):
    def test_plan_is_offline_idempotent_and_thirty_five_minute_points(self):
        with tempfile.TemporaryDirectory() as folder,patch('socket.socket',side_effect=AssertionError('network')):
            p=prepare(Path(folder));again=prepare(Path(folder))
            self.assertEqual(p,again);self.assertEqual(len(p['points']),30)
            self.assertEqual(len({x['id'] for x in p['points']}),30)
            self.assertTrue(all(x['seconds']==300 for x in p['points']))
            self.assertTrue(all(x['mode']=='persistent' for x in p['points']))
            self.assertEqual({x['payload_bytes'] for x in p['points']},{64,256,1024})
            self.assertFalse(p['automatic_broker_capacity_training'])

    def test_invalid_binding_is_rejected_before_network(self):
        with patch('socket.socket',side_effect=AssertionError('network')):
            with self.assertRaises(ValueError):verified_credentials({},Path('unused'))

    def test_unreviewed_worker_profile_is_rejected_before_launch(self):
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder);plan=prepare(directory)
            plan['worker_profile']={'publisher':'c7i.4xlarge','subscriber':'c7i.xlarge'}
            (directory/'plan.json').write_text(json.dumps(plan))
            with patch('broker_capacity.math_pilot.near_runtime.provision') as provision:
                with self.assertRaises(ValueError):run(directory,directory/'unused')
                provision.assert_not_called()

    def test_writer_rejects_foreign_namespace(self):
        w=object.__new__(ScopedWriter);w.prefix='bcm-math-unit'
        w._validate_name('bcm-math-unit-p-f2-q0')
        with self.assertRaises(ValueError):w._validate_name('foreign-p-f2-q0')
        with self.assertRaises(ValueError):w._validate_topic('bcm-math-unit-p-f2/>')
        with self.assertRaises(ValueError):w._validate_name('bcm-math-unit-p-f2-q100')

    def test_controller_records_failed_preflight_and_never_launches_workers(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);campaign=root/'campaign';campaign.mkdir()
            (campaign/'binding.json').write_text(json.dumps(dict(service_id='test')))
            with patch('broker_capacity.math_pilot.ROOT',root), \
                 patch('broker_capacity.math_pilot.verified_credentials'), \
                 patch('broker_capacity.math_pilot.signal.signal'), \
                 patch('broker_capacity.math_pilot.PilotBackend') as backend, \
                 patch('broker_capacity.math_pilot.near_runtime.provision') as provision:
                backend.return_value.preflight.side_effect=RuntimeError('Foreign traffic')
                run(campaign,root/'unused')
                provision.assert_not_called()
            self.assertEqual(json.loads((campaign/'controller.json').read_text())['state'],'stopped_error')

if __name__=='__main__':unittest.main()
