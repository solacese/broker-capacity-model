from __future__ import annotations

from typing import Any


_BYTES_PER_MB = 1_000_000


def _ratio(current: int | float | None, maximum: int | float | None) -> float | None:
    if current is None or maximum is None or maximum <= 0:
        return None
    return current / maximum


def _sum_known(items: list[dict[str, Any]], field: str) -> int | float | None:
    values = [item.get(field) for item in items]
    if any(not isinstance(value, (int, float)) for value in values):
        return None
    return sum(values)


def _max_known(items: list[dict[str, Any]], field: str) -> int | float | None:
    if not items:
        return 0
    values = [item.get(field) for item in items]
    if any(not isinstance(value, (int, float)) for value in values):
        return None
    return max(values)


def capacity_snapshot(
    *,
    service: dict[str, Any],
    vpn_monitor: dict[str, Any],
    vpn_config: dict[str, Any],
    clients: list[dict[str, Any]],
    queues: list[dict[str, Any]],
    topic_endpoints: list[dict[str, Any]],
    transactions: list[dict[str, Any]],
    bridges: list[dict[str, Any]],
) -> dict[str, Any]:
    endpoints = queues + topic_endpoints
    endpoint_spool_bytes = _sum_known(endpoints, "msgSpoolUsage")
    spool_bytes = vpn_monitor.get("msgSpoolUsage")
    if not isinstance(spool_bytes, (int, float)):
        spool_bytes = endpoint_spool_bytes
    spool_messages = vpn_monitor.get("msgSpoolMsgCount")
    if not isinstance(spool_messages, (int, float)):
        spool_messages = None
    delivered_unacked_messages = _sum_known(endpoints, "txUnackedMsgCount")
    spool_limit_mb = vpn_config.get("maxMsgSpoolUsage")
    spool_limit_bytes = (
        spool_limit_mb * _BYTES_PER_MB if isinstance(spool_limit_mb, (int, float)) else None
    )
    queue_spool_ratios = []
    for queue in queues:
        limit_mb = queue.get("maxMsgSpoolUsage")
        limit_bytes = (
            limit_mb * _BYTES_PER_MB if isinstance(limit_mb, (int, float)) else None
        )
        ratio = _ratio(queue.get("msgSpoolUsage"), limit_bytes)
        if ratio is not None:
            queue_spool_ratios.append((queue.get("queueName", "<unnamed>"), ratio))
    limiting_queue = max(queue_spool_ratios, key=lambda item: item[1], default=None)

    current = {
        "connections": len(clients),
        "endpoints": len(endpoints),
        "queues": len(queues),
        "topic_endpoints": len(topic_endpoints),
        "transactions": len(transactions),
        "bridges": len(bridges),
        "spool_bytes": spool_bytes,
        "spool_messages": spool_messages,
        "delivered_unacked_messages": delivered_unacked_messages,
    }
    limits = {
        "connections": vpn_config.get("maxConnectionCount"),
        "endpoints": vpn_config.get("maxEndpointCount"),
        "subscriptions": vpn_config.get("maxSubscriptionCount"),
        "ingress_flows": vpn_config.get("maxIngressFlowCount"),
        "egress_flows": vpn_config.get("maxEgressFlowCount"),
        "transactions": vpn_config.get("maxTransactionCount"),
        "spool_mb": spool_limit_mb,
        "spool_bytes": spool_limit_bytes,
    }
    utilization = {
        "connections": _ratio(current["connections"], limits["connections"]),
        "endpoints": _ratio(current["endpoints"], limits["endpoints"]),
        "transactions": _ratio(current["transactions"], limits["transactions"]),
        "spool": _ratio(current["spool_bytes"], limits["spool_bytes"]),
        "most_utilized_queue_spool": limiting_queue[1] if limiting_queue else None,
    }
    available = {key: value for key, value in utilization.items() if value is not None}
    limiting_factor = max(available, key=available.get) if available else None

    return {
        "id": service["id"],
        "name": service["name"],
        "owner_id": service["owner"],
        "msg_vpn": service["msgVpnName"],
        "service_class": service["serviceClassId"],
        "broker_version": service["eventBrokerVersion"],
        "current": current,
        "limits": limits,
        "utilization": utilization,
        "deterministic_utilization": available.get(limiting_factor) if limiting_factor else None,
        "limiting_factor": limiting_factor,
        "traffic": {
            key: vpn_monitor.get(key)
            for key in (
                "rxMsgRate",
                "txMsgRate",
                "rxByteRate",
                "txByteRate",
                "averageRxMsgRate",
                "averageTxMsgRate",
                "averageRxByteRate",
                "averageTxByteRate",
                "discardedRxMsgCount",
                "discardedTxMsgCount",
            )
        },
        "queue_summary": {
            "count": len(queues),
            "non_empty": None,
            "max_depth_messages": None,
            "historical_spooled_messages": _sum_known(queues, "spooledMsgCount"),
            "max_usage_bytes": _max_known(queues, "msgSpoolUsage"),
            "most_utilized_queue": limiting_queue[0] if limiting_queue else None,
            "most_utilized_queue_spool": limiting_queue[1] if limiting_queue else None,
            "historical_spool_limit_discards": _sum_known(
                queues, "maxMsgSpoolUsageExceededDiscardedMsgCount"
            ),
        },
        "topic_endpoint_summary": {
            "count": len(topic_endpoints),
            "spool_bytes": _sum_known(topic_endpoints, "msgSpoolUsage"),
            "spool_messages": None,
            "historical_spooled_messages": _sum_known(
                topic_endpoints, "spooledMsgCount"
            ),
            "delivered_unacked_messages": _sum_known(
                topic_endpoints, "txUnackedMsgCount"
            ),
        },
    }
