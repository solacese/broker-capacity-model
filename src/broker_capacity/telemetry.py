from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .experiments import BrokerSample, FieldProvenance, QueueSample
from .sustainability import CounterDelta, cumulative_delta


VPN_COUNTER_FIELDS = (
    "rxMsgCount",
    "txMsgCount",
    "rxByteCount",
    "txByteCount",
    "discardedRxMsgCount",
    "discardedTxMsgCount",
)


@dataclass(frozen=True)
class WindowDelta:
    started_at: datetime
    ended_at: datetime
    counters: dict[str, CounterDelta]
    queue_backlog: dict[str, CounterDelta]
    queue_unacked: dict[str, CounterDelta]
    queue_spool_bytes: dict[str, CounterDelta]


def broker_sample_from_semp(
    *,
    timestamp: datetime,
    vpn: dict[str, Any],
    queues: list[dict[str, Any]],
    queue_backlog_messages: dict[str, int | None] | None = None,
) -> BrokerSample:
    return BrokerSample(
        timestamp=timestamp,
        cumulative_counters={field: vpn.get(field) for field in VPN_COUNTER_FIELDS},
        queues=tuple(
            QueueSample(
                queue_name=str(queue["queueName"]),
                backlog_messages=(
                    _number(queue_backlog_messages.get(str(queue["queueName"])))
                    if queue_backlog_messages is not None
                    else None
                ),
                unacked_messages=_number(queue.get("txUnackedMsgCount")),
                spool_bytes=_number(queue.get("msgSpoolUsage")),
            )
            for queue in queues
        ),
    )


def measurement_delta(start: BrokerSample, end: BrokerSample) -> WindowDelta:
    start_queues = {queue.queue_name: queue for queue in start.queues}
    end_queues = {queue.queue_name: queue for queue in end.queues}
    queue_names = start_queues.keys() & end_queues.keys()
    return WindowDelta(
        started_at=start.timestamp,
        ended_at=end.timestamp,
        counters={
            field: cumulative_delta(
                start.cumulative_counters.get(field), end.cumulative_counters.get(field)
            )
            for field in set(start.cumulative_counters) | set(end.cumulative_counters)
        },
        queue_backlog={
            name: gauge_change(
                start_queues[name].backlog_messages, end_queues[name].backlog_messages
            )
            for name in queue_names
        },
        queue_unacked={
            name: gauge_change(
                start_queues[name].unacked_messages, end_queues[name].unacked_messages
            )
            for name in queue_names
        },
        queue_spool_bytes={
            name: gauge_change(start_queues[name].spool_bytes, end_queues[name].spool_bytes)
            for name in queue_names
        },
    )


def provenance_from_openapi(
    spec: dict[str, Any],
    *,
    schema_name: str,
    fields: tuple[str, ...],
    endpoint: str,
    schema_url: str,
) -> dict[str, FieldProvenance]:
    definitions = spec.get("definitions", {})
    properties = definitions.get(schema_name, {}).get("properties", {})
    version = spec.get("info", {}).get("version")
    result: dict[str, FieldProvenance] = {}
    for field in fields:
        definition = properties.get(field)
        if not definition:
            continue
        description = definition.get("description")
        result[field] = FieldProvenance(
            endpoint=endpoint,
            json_pointer=f"/data/{field}",
            unit=_unit_from_description(description),
            schema_url=schema_url,
            schema_version=version,
            description=description,
        )
    return result


def gauge_change(start: int | None, end: int | None) -> CounterDelta:
    if start is None or end is None:
        return CounterDelta(None, "missing")
    return CounterDelta(end - start)


def _number(value: Any) -> int | float | None:
    return value if isinstance(value, (int, float)) else None


def _unit_from_description(description: str | None) -> str | None:
    if not description:
        return None
    lowered = description.casefold()
    if "bytes (b)" in lowered or "in bytes" in lowered:
        return "bytes"
    if "messages" in lowered or "number of" in lowered:
        return "count"
    return None
