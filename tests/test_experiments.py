from __future__ import annotations

import math
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from broker_capacity.experiments import (
    AcceptedEvidence,
    BrokerSample,
    DeliveryMode,
    GeneratorSample,
    QueueSample,
    StageOutcome,
    StageRecord,
    Workload,
)
from broker_capacity.search import AdaptiveBracketSearch, BoundaryKind
from broker_capacity.sustainability import ClassificationPolicy, classify_stage, cumulative_delta


START = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)


def generator_sample(
    seconds: int,
    count: int,
    *,
    destinations: dict[str, int | None] | None = None,
    cpu: float | None = 20.0,
    errors: tuple[str, ...] = (),
) -> GeneratorSample:
    return GeneratorSample(
        timestamp=START + timedelta(seconds=seconds),
        requested_publishes=count,
        scheduled_publishes=count,
        attempted_publishes=count,
        successful_publishes=count,
        accepted_unique_messages=count,
        accepted_evidence=AcceptedEvidence.DIRECT_PUBLISH_SUCCESS,
        acknowledgements=None,
        negative_acknowledgements=0,
        unique_receipts_by_destination=(
            destinations if destinations is not None else {"d1": count}
        ),
        retransmitted_receipts=0,
        receipts_inflight_by_destination={
            destination: 0 for destination in (destinations or {"d1": count})
        },
        cpu_percent=cpu,
        raw_process_cpu_percent=cpu,
        logical_cpu_count=1,
        network_tx_bytes=count * 1024,
        network_rx_bytes=count * 64,
        errors=errors,
    )


def broker_sample(
    seconds: int,
    backlog: int = 0,
    *,
    queues: tuple[QueueSample, ...] | None = None,
    counters: dict[str, int | None] | None = None,
) -> BrokerSample:
    return BrokerSample(
        timestamp=START + timedelta(seconds=seconds),
        cumulative_counters=counters
        or {
            "rxMsgCount": 10 + seconds,
            "txMsgCount": 10 + seconds,
            "rxByteCount": 100 + seconds,
            "txByteCount": 100 + seconds,
            "discardedRxMsgCount": 10,
            "discardedTxMsgCount": 10,
        },
        queues=queues or (QueueSample("q1", backlog, 0, backlog * 1024),),
    )


def stage(
    samples: tuple[GeneratorSample, ...] | None = None,
    *,
    broker_samples: tuple[BrokerSample, ...] | None = None,
    fanout: int = 1,
    destinations: tuple[str, ...] = ("d1",),
    requested_rate: float = 100,
) -> StageRecord:
    return StageRecord(
        run_id="run",
        stage_id="stage",
        service_id="avfk5i15jtm",
        msg_vpn="msgvpn-avfk5i15jtm",
        workload=Workload(
            DeliveryMode.DIRECT,
            "smf",
            "smf",
            1024,
            fanout,
            destinations,
            requested_rate,
        ),
        warmup_seconds=15,
        measurement_seconds=60,
        measurement_started_at=START,
        measurement_ended_at=START + timedelta(seconds=60),
        generator_samples=samples
        or (generator_sample(0, 0), generator_sample(60, 6000)),
        broker_samples=broker_samples
        or (broker_sample(0), broker_sample(60)),
        provenance={},
        generator_processes_stopped=True,
        delivery_drain_completed=True,
        drained_unique_receipts_by_destination={
            destination: 6000 for destination in destinations
        },
        drained_retransmitted_receipts=0,
        queues_drained=True,
    )


class SustainabilityTests(unittest.TestCase):
    policy = ClassificationPolicy(maximum_sample_gap_seconds=60)

    def assess(self, record: StageRecord):
        return classify_stage(record, self.policy)

    def test_positive_backlog_drift_is_unsustainable(self) -> None:
        result = self.assess(
            stage(broker_samples=(broker_sample(0), broker_sample(60, backlog=3)))
        )
        self.assertEqual(result.outcome, StageOutcome.UNSUSTAINABLE)
        self.assertIn("positive_backlog_drift:q1", result.reasons)

    def test_one_growing_queue_is_not_hidden_by_one_draining(self) -> None:
        first = broker_sample(
            0,
            queues=(QueueSample("grow", 0, 0, 0), QueueSample("drain", 10, 0, 100)),
        )
        last = broker_sample(
            60,
            queues=(QueueSample("grow", 5, 0, 50), QueueSample("drain", 0, 0, 0)),
        )
        result = self.assess(stage(broker_samples=(first, last)))
        self.assertEqual(result.outcome, StageOutcome.UNSUSTAINABLE)
        self.assertIn("positive_backlog_drift:grow", result.reasons)

    def test_falling_gauge_is_not_a_counter_reset(self) -> None:
        result = self.assess(
            stage(broker_samples=(broker_sample(0, backlog=10), broker_sample(60)))
        )
        self.assertEqual(result.outcome, StageOutcome.SUSTAINABLE)

    def test_transient_gauge_rise_that_recovers_is_sustainable(self) -> None:
        samples = (
            broker_sample(0, backlog=0),
            broker_sample(30, backlog=5),
            broker_sample(60, backlog=0),
        )
        self.assertEqual(self.assess(stage(broker_samples=samples)).outcome, StageOutcome.SUSTAINABLE)

    def test_fanout_reconciles_each_destination(self) -> None:
        start = generator_sample(0, 0, destinations={"d1": 0, "d2": 0})
        end = generator_sample(60, 6000, destinations={"d1": 6000, "d2": 6000})
        result = self.assess(stage((start, end), fanout=2, destinations=("d1", "d2")))
        self.assertEqual(result.outcome, StageOutcome.SUSTAINABLE)
        self.assertEqual(result.metrics["expected_deliveries"], 12000)

    def test_aggregate_receipts_cannot_hide_destination_imbalance(self) -> None:
        start = generator_sample(0, 0, destinations={"d1": 0, "d2": 0})
        end = generator_sample(60, 6000, destinations={"d1": 12000, "d2": 0})
        result = self.assess(stage((start, end), fanout=2, destinations=("d1", "d2")))
        record = stage((start, end), fanout=2, destinations=("d1", "d2"))
        record = replace(
            record,
            drained_unique_receipts_by_destination={"d1": 12000, "d2": 0},
        )
        result = self.assess(record)
        self.assertEqual(result.outcome, StageOutcome.UNSUSTAINABLE)
        self.assertIn("destination_over_delivery:d1", result.reasons)
        self.assertIn("destination_under_delivery:d2", result.reasons)

    def test_mid_window_counter_reset_is_inconclusive(self) -> None:
        samples = (
            generator_sample(0, 0),
            generator_sample(30, 4000),
            generator_sample(60, 6000),
        )
        samples = (samples[0], samples[1], replace(samples[2], successful_publishes=2000))
        result = self.assess(stage(samples))
        self.assertEqual(result.outcome, StageOutcome.INCONCLUSIVE)
        self.assertIn("successful_publishes_counter_reset", result.reasons)

    def test_increasing_discards_is_unsustainable(self) -> None:
        first = broker_sample(0)
        last = broker_sample(
            60,
            counters={**broker_sample(60).cumulative_counters, "discardedRxMsgCount": 11},
        )
        result = self.assess(stage(broker_samples=(first, last)))
        self.assertEqual(result.outcome, StageOutcome.UNSUSTAINABLE)
        self.assertIn("positive_discardedRxMsgCount", result.reasons)

    def test_mid_window_broker_counter_reset_is_inconclusive(self) -> None:
        first = broker_sample(0)
        middle = broker_sample(30)
        last = broker_sample(60)
        middle = replace(
            middle,
            cumulative_counters={**middle.cumulative_counters, "rxMsgCount": 1000},
        )
        result = self.assess(stage(broker_samples=(first, middle, last)))
        self.assertEqual(result.outcome, StageOutcome.INCONCLUSIVE)
        self.assertIn("broker_counter_rxMsgCount_counter_reset", result.reasons)

    def test_missing_broker_counter_is_inconclusive(self) -> None:
        record = stage()
        missing = replace(
            record.broker_samples[-1],
            cumulative_counters={
                **record.broker_samples[-1].cumulative_counters,
                "discardedTxMsgCount": None,
            },
        )
        result = self.assess(replace(record, broker_samples=(record.broker_samples[0], missing)))
        self.assertEqual(result.outcome, StageOutcome.INCONCLUSIVE)
        self.assertIn("broker_counter_discardedTxMsgCount_missing", result.reasons)

    def test_zero_traffic_at_nonzero_rate_is_inconclusive(self) -> None:
        result = self.assess(stage((generator_sample(0, 0), generator_sample(60, 0))))
        self.assertEqual(result.outcome, StageOutcome.INCONCLUSIVE)
        self.assertIn("requested_count_rate_mismatch", result.reasons)

    def test_unordered_or_out_of_window_samples_are_inconclusive(self) -> None:
        unordered = stage((generator_sample(30, 3000), generator_sample(0, 0), generator_sample(60, 6000)))
        self.assertIn("generator_unordered_samples", self.assess(unordered).reasons)
        outside = stage((generator_sample(-1, 0), generator_sample(60, 6000)))
        self.assertIn("generator_sample_outside_measurement_window", self.assess(outside).reasons)

    def test_client_limitation_requires_corroborating_evidence(self) -> None:
        saturated = stage((generator_sample(0, 0), generator_sample(60, 6000, cpu=96.0)))
        result = self.assess(saturated)
        self.assertEqual(result.outcome, StageOutcome.INVALID)
        self.assertIn("generator_cpu_limit", result.reasons)

    def test_publish_shortfall_without_generator_evidence_is_not_invalid(self) -> None:
        end = replace(
            generator_sample(60, 6000),
            attempted_publishes=5000,
            successful_publishes=4900,
            accepted_unique_messages=4900,
            unique_receipts_by_destination={"d1": 4900},
        )
        result = self.assess(stage((generator_sample(0, 0), end)))
        self.assertEqual(result.outcome, StageOutcome.UNSUSTAINABLE)
        self.assertIn("attempted_rate_below_scheduled", result.reasons)

    def test_persistent_mode_requires_acknowledgements(self) -> None:
        direct = stage()
        persistent_workload = replace(
            direct.workload,
            delivery_mode=DeliveryMode.PERSISTENT,
            queue_type="exclusive",
            acknowledgement_mode="client",
        )
        samples = tuple(
            replace(sample, accepted_evidence=AcceptedEvidence.BROKER_ACK)
            for sample in direct.generator_samples
        )
        result = self.assess(
            replace(direct, workload=persistent_workload, generator_samples=samples)
        )
        self.assertEqual(result.outcome, StageOutcome.INCONCLUSIVE)
        self.assertIn("acknowledgements_missing", result.reasons)

    def test_direct_mode_rejects_broker_ack_evidence(self) -> None:
        samples = (
            replace(generator_sample(0, 0), accepted_evidence=AcceptedEvidence.BROKER_ACK),
            replace(generator_sample(60, 6000), accepted_evidence=AcceptedEvidence.BROKER_ACK),
        )
        result = self.assess(stage(samples))
        self.assertEqual(result.outcome, StageOutcome.INCONCLUSIVE)
        self.assertIn("accepted_evidence_invalid_for_delivery_mode", result.reasons)

    def test_counter_delta_reports_reset_and_missing(self) -> None:
        self.assertEqual(cumulative_delta(10, 2).reason, "counter_reset")
        self.assertEqual(cumulative_delta(None, 2).reason, "missing")


class AdaptiveSearchTests(unittest.TestCase):
    def test_interval_result_ignores_invalid_observation(self) -> None:
        search = AdaptiveBracketSearch(100, relative_tolerance=0.10)
        search.observe(100, StageOutcome.SUSTAINABLE)
        search.observe(200, StageOutcome.UNSUSTAINABLE)
        search.observe(150, StageOutcome.INVALID)
        self.assertEqual(search.next_rate(), 150)
        search.observe(150, StageOutcome.SUSTAINABLE)
        self.assertEqual(search.next_rate(), 175)
        search.observe(175, StageOutcome.UNSUSTAINABLE)
        estimate = search.estimate()
        self.assertEqual(estimate.kind, BoundaryKind.INTERVAL)
        self.assertEqual((estimate.sustainable_rate, estimate.unsustainable_rate), (150, 175))

    def test_budget_stop_produces_lower_censored_result(self) -> None:
        search = AdaptiveBracketSearch(100, maximum_rate=200)
        search.observe(100, StageOutcome.SUSTAINABLE)
        search.observe(200, StageOutcome.SUSTAINABLE)
        self.assertIsNone(search.next_rate())
        self.assertEqual(search.estimate().kind, BoundaryKind.LOWER_CENSORED)

    def test_non_monotonic_valid_evidence_is_rejected(self) -> None:
        search = AdaptiveBracketSearch(100)
        search.observe(100, StageOutcome.UNSUSTAINABLE)
        with self.assertRaisesRegex(ValueError, "non-monotonic"):
            search.observe(200, StageOutcome.SUSTAINABLE)

    def test_non_finite_rates_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            AdaptiveBracketSearch(math.nan)
        search = AdaptiveBracketSearch(100)
        with self.assertRaises(ValueError):
            search.observe(math.inf, StageOutcome.SUSTAINABLE)

    def test_invalid_retry_respects_maximum_rate(self) -> None:
        search = AdaptiveBracketSearch(300, maximum_rate=200)
        search.observe(200, StageOutcome.INVALID)
        self.assertEqual(search.next_rate(), 200)

    def test_stage_budget_stops_repeated_invalid_retries(self) -> None:
        search = AdaptiveBracketSearch(100, maximum_stages=2)
        search.observe(100, StageOutcome.INVALID)
        self.assertEqual(search.next_rate(), 100)
        search.observe(100, StageOutcome.INCONCLUSIVE)
        self.assertIsNone(search.next_rate())
        self.assertEqual(search.estimate().kind, BoundaryKind.UNKNOWN_CENSORED)


if __name__ == "__main__":
    unittest.main()
