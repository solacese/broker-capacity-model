from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, TypeVar

from .experiments import (
    AcceptedEvidence,
    BrokerSample,
    DeliveryMode,
    GeneratorSample,
    QueueSample,
    StageOutcome,
    StageRecord,
)


_REQUIRED_BROKER_COUNTERS = (
    "rxMsgCount",
    "txMsgCount",
    "rxByteCount",
    "txByteCount",
    "discardedRxMsgCount",
    "discardedTxMsgCount",
)
_T = TypeVar("_T", GeneratorSample, BrokerSample)


@dataclass(frozen=True)
class ClassificationPolicy:
    minimum_acceptance_ratio: float = 0.98
    minimum_delivery_ratio: float = 0.98
    maximum_delivery_ratio: float = 1.02
    requested_count_tolerance: float = 0.02
    backlog_growth_tolerance: int = 0
    unacked_growth_tolerance: int = 0
    spool_growth_tolerance_bytes: int = 0
    maximum_generator_cpu_percent: float = 90.0
    maximum_sample_gap_seconds: float = 15.0
    timestamp_tolerance_seconds: float = 1.0


@dataclass(frozen=True)
class CounterDelta:
    value: int | None
    reason: str | None = None


@dataclass(frozen=True)
class StageAssessment:
    outcome: StageOutcome
    reasons: tuple[str, ...]
    metrics: dict[str, float | int | None] = field(default_factory=dict)


def cumulative_delta(start: int | None, end: int | None) -> CounterDelta:
    if start is None or end is None:
        return CounterDelta(None, "missing")
    if end < start:
        return CounterDelta(None, "counter_reset")
    return CounterDelta(end - start)


def _ordered_window_samples(
    samples: tuple[_T, ...],
    started_at: datetime,
    ended_at: datetime,
    maximum_gap_seconds: float,
    tolerance_seconds: float,
) -> tuple[tuple[_T, ...] | None, str | None]:
    if len(samples) < 2:
        return None, "insufficient_samples"
    timestamps = [sample.timestamp for sample in samples]
    if timestamps != sorted(timestamps) or len(set(timestamps)) != len(timestamps):
        return None, "unordered_samples"
    tolerance = tolerance_seconds
    if any(timestamp < started_at or timestamp > ended_at for timestamp in timestamps):
        return None, "sample_outside_measurement_window"
    if (timestamps[0] - started_at).total_seconds() > tolerance:
        return None, "measurement_start_not_covered"
    if (ended_at - timestamps[-1]).total_seconds() > tolerance:
        return None, "measurement_end_not_covered"
    gaps = [
        (later - earlier).total_seconds()
        for earlier, later in zip(timestamps, timestamps[1:])
    ]
    if any(gap > maximum_gap_seconds for gap in gaps):
        return None, "measurement_sampling_gap"
    return samples, None


def _series_delta(samples: tuple[GeneratorSample, ...], field_name: str) -> CounterDelta:
    values = [getattr(sample, field_name) for sample in samples]
    if any(value is None for value in values):
        return CounterDelta(None, "missing")
    if any(later < earlier for earlier, later in zip(values, values[1:])):
        return CounterDelta(None, "counter_reset")
    return CounterDelta(values[-1] - values[0])


def _broker_counter_delta(samples: tuple[BrokerSample, ...], field_name: str) -> CounterDelta:
    values = [sample.cumulative_counters.get(field_name) for sample in samples]
    if any(value is None for value in values):
        return CounterDelta(None, "missing")
    if any(later < earlier for earlier, later in zip(values, values[1:])):
        return CounterDelta(None, "counter_reset")
    return CounterDelta(values[-1] - values[0])


def _destination_deltas(
    samples: tuple[GeneratorSample, ...], destinations: tuple[str, ...]
) -> tuple[dict[str, int] | None, str | None]:
    result: dict[str, int] = {}
    for destination in destinations:
        values = [sample.unique_receipts_by_destination.get(destination) for sample in samples]
        if any(value is None for value in values):
            return None, f"receipts_{destination}_missing"
        if any(later < earlier for earlier, later in zip(values, values[1:])):
            return None, f"receipts_{destination}_counter_reset"
        result[destination] = values[-1] - values[0]
    return result, None


def _destination_inflight_changes(
    samples: tuple[GeneratorSample, ...], destinations: tuple[str, ...]
) -> tuple[dict[str, int] | None, str | None]:
    result: dict[str, int] = {}
    for destination in destinations:
        start = samples[0].receipts_inflight_by_destination.get(destination)
        end = samples[-1].receipts_inflight_by_destination.get(destination)
        if start is None or end is None:
            return None, f"inflight_{destination}_missing"
        if start < 0 or end < 0:
            return None, f"inflight_{destination}_negative"
        result[destination] = end - start
    return result, None


def _queue_series(
    samples: tuple[BrokerSample, ...], field_name: str
) -> tuple[dict[str, tuple[int, ...]] | None, str | None]:
    inventories = [{queue.queue_name for queue in sample.queues} for sample in samples]
    if any(inventory != inventories[0] for inventory in inventories[1:]):
        return None, "queue_inventory_changed"
    series: dict[str, list[int]] = {name: [] for name in inventories[0]}
    for sample in samples:
        queues = {queue.queue_name: queue for queue in sample.queues}
        for name in series:
            value = getattr(queues[name], field_name)
            if value is None:
                return None, f"{field_name}_{name}_missing"
            series[name].append(value)
    return {name: tuple(values) for name, values in series.items()}, None


def _positive_drift_destinations(series: dict[str, tuple[int, ...]], tolerance: int) -> list[str]:
    growing = []
    for name, values in series.items():
        interval_changes = [later - earlier for earlier, later in zip(values, values[1:])]
        if not interval_changes:
            continue
        half = max(1, len(interval_changes) // 2)
        recent_changes = interval_changes[-half:]
        sustained = (
            values[-1] - values[0] > tolerance
            and sum(recent_changes) > tolerance
            and all(change >= 0 for change in recent_changes)
        )
        if sustained:
            growing.append(name)
    return growing


def _max_present(values: Iterable[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return max(present) if present else None


def classify_stage(
    record: StageRecord, policy: ClassificationPolicy | None = None
) -> StageAssessment:
    policy = policy or ClassificationPolicy()
    reasons: list[str] = []
    metrics: dict[str, float | int | None] = {}
    duration = (record.measurement_ended_at - record.measurement_started_at).total_seconds()
    if abs(duration - record.measurement_seconds) > policy.timestamp_tolerance_seconds:
        reasons.append("measurement_window_duration_mismatch")

    generator_samples, generator_window_error = _ordered_window_samples(
        record.generator_samples,
        record.measurement_started_at,
        record.measurement_ended_at,
        policy.maximum_sample_gap_seconds,
        policy.timestamp_tolerance_seconds,
    )
    broker_samples, broker_window_error = _ordered_window_samples(
        record.broker_samples,
        record.measurement_started_at,
        record.measurement_ended_at,
        policy.maximum_sample_gap_seconds,
        policy.timestamp_tolerance_seconds,
    )
    if generator_window_error:
        reasons.append(f"generator_{generator_window_error}")
    if broker_window_error:
        reasons.append(f"broker_{broker_window_error}")
    generator_samples = generator_samples or ()
    broker_samples = broker_samples or ()

    generator_fields = (
        "requested_publishes",
        "scheduled_publishes",
        "attempted_publishes",
        "successful_publishes",
        "accepted_unique_messages",
        "retransmitted_receipts",
    )
    if record.workload.delivery_mode == DeliveryMode.PERSISTENT:
        generator_fields += ("acknowledgements", "negative_acknowledgements")
    required = {
        name: _series_delta(generator_samples, name)
        if generator_samples
        else CounterDelta(None, "invalid_window")
        for name in generator_fields
    }
    for name, delta in required.items():
        if delta.value is None:
            reasons.append(f"{name}_{delta.reason}")

    receipt_deltas, receipt_error = (
        _destination_deltas(generator_samples, record.workload.destination_ids)
        if generator_samples
        else (None, "invalid_window")
    )
    if receipt_error:
        reasons.append(receipt_error)
    inflight_changes, inflight_error = (
        _destination_inflight_changes(generator_samples, record.workload.destination_ids)
        if generator_samples
        else (None, "invalid_window")
    )
    if inflight_error:
        reasons.append(inflight_error)

    broker_counter_deltas = {
        name: _broker_counter_delta(broker_samples, name)
        if broker_samples
        else CounterDelta(None, "invalid_window")
        for name in _REQUIRED_BROKER_COUNTERS
    }
    for name, delta in broker_counter_deltas.items():
        if delta.value is None:
            reasons.append(f"broker_counter_{name}_{delta.reason}")

    queue_series: dict[str, dict[str, tuple[int, ...]]] = {}
    for field_name in ("backlog_messages", "unacked_messages", "spool_bytes"):
        series, error = (
            _queue_series(broker_samples, field_name)
            if broker_samples
            else (None, "invalid_window")
        )
        if error:
            reasons.append(error)
        elif series is not None:
            queue_series[field_name] = series

    max_cpu = _max_present(sample.cpu_percent for sample in generator_samples)
    max_raw_cpu = _max_present(
        sample.raw_process_cpu_percent for sample in generator_samples
    )
    if max_cpu is None:
        reasons.append("generator_cpu_missing")
    if not generator_samples or any(
        sample.network_tx_bytes is None or sample.network_rx_bytes is None
        for sample in generator_samples
    ):
        reasons.append("generator_network_missing")
    errors = [error for sample in generator_samples for error in sample.errors]
    if errors:
        reasons.append("generator_errors")
    if not record.generator_processes_stopped:
        reasons.append("generator_processes_not_stopped")
    if not record.delivery_drain_completed:
        reasons.append("delivery_drain_incomplete")
    if record.queues_drained is False:
        reasons.append("queues_not_drained")

    evidence_values = {sample.accepted_evidence for sample in generator_samples}
    expected_evidence = (
        AcceptedEvidence.DIRECT_PUBLISH_SUCCESS
        if record.workload.delivery_mode == DeliveryMode.DIRECT
        else AcceptedEvidence.BROKER_ACK
    )
    if evidence_values != {expected_evidence}:
        reasons.append("accepted_evidence_invalid_for_delivery_mode")

    requested = required["requested_publishes"].value
    scheduled = required["scheduled_publishes"].value
    attempted = required["attempted_publishes"].value
    successful = required["successful_publishes"].value
    accepted = required["accepted_unique_messages"].value
    nacks = (
        required["negative_acknowledgements"].value
        if "negative_acknowledgements" in required
        else None
    )
    retransmits = required["retransmitted_receipts"].value
    delivered = sum(receipt_deltas.values()) if receipt_deltas is not None else None
    drained_receipts = record.drained_unique_receipts_by_destination
    expected_deliveries = accepted * record.workload.fanout if accepted is not None else None
    requested_expected = record.workload.requested_rate * duration
    metrics.update(
        {
            "measurement_seconds": duration,
            "requested_publishes": requested,
            "scheduled_publishes": scheduled,
            "attempted_publishes": attempted,
            "successful_publishes": successful,
            "accepted_unique_messages": accepted,
            "actual_accepted_rate": accepted / duration if accepted is not None else None,
            "expected_deliveries": expected_deliveries,
            "unique_receipts": delivered,
            "retransmitted_receipts": retransmits,
            "requested_expected": requested_expected,
            "max_generator_host_normalized_cpu_percent": max_cpu,
            "max_generator_raw_process_cpu_percent": max_raw_cpu,
            "logical_cpu_count": (
                generator_samples[-1].logical_cpu_count if generator_samples else None
            ),
            "nacks": nacks,
            **{
                f"broker_delta_{name}": delta.value
                for name, delta in broker_counter_deltas.items()
            },
        }
    )

    counts = (requested, scheduled, attempted, successful, accepted)
    if all(value is not None for value in counts):
        if any(value < 0 for value in counts):
            reasons.append("negative_publish_count")
        if not requested >= scheduled >= attempted >= successful >= accepted:
            reasons.append("publish_count_ordering_violation")
        if abs(requested - requested_expected) > max(1, requested_expected * policy.requested_count_tolerance):
            reasons.append("requested_count_rate_mismatch")
        if requested_expected > 0 and requested == 0:
            reasons.append("zero_traffic_at_nonzero_rate")

    invalid_reasons: list[str] = []
    if max_cpu is not None and max_cpu >= policy.maximum_generator_cpu_percent:
        invalid_reasons.append("generator_cpu_limit")
    if errors:
        invalid_reasons.append("generator_errors")
    if invalid_reasons:
        return StageAssessment(StageOutcome.INVALID, tuple(dict.fromkeys(invalid_reasons)), metrics)
    if reasons:
        return StageAssessment(StageOutcome.INCONCLUSIVE, tuple(dict.fromkeys(reasons)), metrics)

    unsustainable_reasons: list[str] = []
    if scheduled and accepted is not None and accepted / scheduled < policy.minimum_acceptance_ratio:
        unsustainable_reasons.append("accepted_rate_below_scheduled")
    if scheduled and attempted is not None and attempted / scheduled < policy.minimum_acceptance_ratio:
        unsustainable_reasons.append("attempted_rate_below_scheduled")
    if attempted and successful is not None and successful / attempted < policy.minimum_acceptance_ratio:
        unsustainable_reasons.append("publish_success_below_attempted")
    if nacks:
        unsustainable_reasons.append("negative_acknowledgements")
    if record.workload.delivery_mode == DeliveryMode.PERSISTENT:
        acknowledgements = required["acknowledgements"].value
        metrics["acknowledgements"] = acknowledgements
        if accepted is not None and acknowledgements != accepted:
            unsustainable_reasons.append("publisher_acknowledgements_do_not_match_accepted")

    if expected_deliveries is not None and receipt_deltas is not None:
        observed_receipts = drained_receipts or receipt_deltas
        if set(observed_receipts) != set(record.workload.destination_ids):
            return StageAssessment(
                StageOutcome.INCONCLUSIVE,
                ("drained_destination_evidence_missing",),
                metrics,
            )
        destination_ratios = {
            destination: observed_receipts[destination] / accepted if accepted else None
            for destination in record.workload.destination_ids
        }
        metrics.update(
            {f"delivery_ratio_{destination}": ratio for destination, ratio in destination_ratios.items()}
        )
        for destination, ratio in destination_ratios.items():
            if ratio is None or ratio < policy.minimum_delivery_ratio:
                unsustainable_reasons.append(f"destination_under_delivery:{destination}")
            elif ratio > policy.maximum_delivery_ratio:
                unsustainable_reasons.append(f"destination_over_delivery:{destination}")
        reconciled_deliveries = sum(observed_receipts.values())
        metrics["drained_unique_receipts"] = reconciled_deliveries
        metrics["delivery_reconciliation_ratio"] = (
            reconciled_deliveries / expected_deliveries if expected_deliveries else None
        )
        if record.drained_retransmitted_receipts:
            unsustainable_reasons.append("retransmitted_receipts")

    for name in ("discardedRxMsgCount", "discardedTxMsgCount"):
        delta = broker_counter_deltas[name]
        if delta.value:
            unsustainable_reasons.append(f"positive_{name}")

    drift_specs = (
        ("backlog_messages", policy.backlog_growth_tolerance, "positive_backlog_drift"),
        ("unacked_messages", policy.unacked_growth_tolerance, "positive_unacked_drift"),
        ("spool_bytes", policy.spool_growth_tolerance_bytes, "positive_spool_drift"),
    )
    for field_name, tolerance, reason in drift_specs:
        growing = _positive_drift_destinations(queue_series[field_name], tolerance)
        if growing:
            metrics[f"{field_name}_growing_destinations"] = len(growing)
            unsustainable_reasons.extend(f"{reason}:{destination}" for destination in growing)

    if unsustainable_reasons:
        return StageAssessment(
            StageOutcome.UNSUSTAINABLE, tuple(dict.fromkeys(unsustainable_reasons)), metrics
        )
    return StageAssessment(StageOutcome.SUSTAINABLE, ("all_required_evidence_passed",), metrics)
