from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class DeliveryMode(StrEnum):
    DIRECT = "direct"
    PERSISTENT = "persistent"


class StageOutcome(StrEnum):
    SUSTAINABLE = "SUSTAINABLE"
    UNSUSTAINABLE = "UNSUSTAINABLE"
    INVALID = "INVALID"
    INCONCLUSIVE = "INCONCLUSIVE"


class AcceptedEvidence(StrEnum):
    DIRECT_PUBLISH_SUCCESS = "direct_publish_success"
    BROKER_ACK = "broker_ack"


@dataclass(frozen=True)
class FieldProvenance:
    endpoint: str
    json_pointer: str
    unit: str | None
    schema_url: str
    schema_version: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class Workload:
    delivery_mode: DeliveryMode
    ingress_protocol: str
    egress_protocol: str
    message_size_bytes: int
    fanout: int
    destination_ids: tuple[str, ...]
    requested_rate: float
    queue_type: str | None = None
    partition_count: int | None = None
    acknowledgement_mode: str | None = None
    composition: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.ingress_protocol or not self.egress_protocol:
            raise ValueError("ingress_protocol and egress_protocol are required")
        if self.message_size_bytes <= 0:
            raise ValueError("message_size_bytes must be positive")
        if self.fanout <= 0:
            raise ValueError("fanout must be positive")
        if len(self.destination_ids) != self.fanout:
            raise ValueError("destination_ids must contain exactly fanout destinations")
        if not math.isfinite(self.requested_rate) or self.requested_rate <= 0:
            raise ValueError("requested_rate must be finite and positive")
        if self.partition_count is not None and self.partition_count <= 0:
            raise ValueError("partition_count must be positive")


@dataclass(frozen=True)
class GeneratorSample:
    timestamp: datetime
    requested_publishes: int | None
    scheduled_publishes: int | None
    attempted_publishes: int | None
    successful_publishes: int | None
    accepted_unique_messages: int | None
    accepted_evidence: AcceptedEvidence | None
    acknowledgements: int | None
    negative_acknowledgements: int | None
    unique_receipts_by_destination: dict[str, int | None]
    retransmitted_receipts: int | None
    receipts_inflight_by_destination: dict[str, int | None]
    cpu_percent: float | None
    network_tx_bytes: int | None
    network_rx_bytes: int | None
    errors: tuple[str, ...] = ()
    raw_process_cpu_percent: float | None = None
    logical_cpu_count: int | None = None


@dataclass(frozen=True)
class QueueSample:
    queue_name: str
    backlog_messages: int | None
    unacked_messages: int | None
    spool_bytes: int | None


@dataclass(frozen=True)
class BrokerSample:
    timestamp: datetime
    cumulative_counters: dict[str, int | None]
    queues: tuple[QueueSample, ...]


@dataclass(frozen=True)
class StageRecord:
    run_id: str
    stage_id: str
    service_id: str
    msg_vpn: str
    workload: Workload
    warmup_seconds: int
    measurement_seconds: int
    measurement_started_at: datetime
    measurement_ended_at: datetime
    generator_samples: tuple[GeneratorSample, ...]
    broker_samples: tuple[BrokerSample, ...]
    provenance: dict[str, FieldProvenance]
    generator_processes_stopped: bool
    delivery_drain_completed: bool
    drained_unique_receipts_by_destination: dict[str, int] | None
    drained_retransmitted_receipts: int | None
    queues_drained: bool | None
    broker_version: str | None = None
    service_class: str | None = None
    broker_config_sha256: str | None = None
    workload_config_sha256: str | None = None
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.warmup_seconds < 0 or self.measurement_seconds <= 0:
            raise ValueError("stage durations must be non-negative with a positive measurement")
        if self.measurement_ended_at <= self.measurement_started_at:
            raise ValueError("measurement window must have positive duration")

    def as_dict(self) -> dict[str, Any]:
        return _json_value(asdict(self))


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value
