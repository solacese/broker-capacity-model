from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from .capacity import capacity_snapshot
from .semp import BrokerAccess, SempClient


RESOURCES = ("clients", "queues", "topicEndpoints", "transactions", "bridges")


def collect_broker(access: BrokerAccess) -> dict[str, Any]:
    client = SempClient(access)
    vpn = quote(access.msg_vpn_name, safe="")
    root = f"/SEMP/v2/monitor/msgVpns/{vpn}"
    monitor = client.get(root).get("data", {})
    config = client.get(f"/SEMP/v2/config/msgVpns/{vpn}").get("data", {})

    def collect_resource(resource: str) -> tuple[str, list[dict[str, Any]]]:
        return resource, list(client.collection(f"{root}/{resource}"))

    with ThreadPoolExecutor(max_workers=len(RESOURCES)) as executor:
        resources = dict(executor.map(collect_resource, RESOURCES))

    service = {
        "id": access.service_id,
        "name": access.name,
        "owner": access.owner_id,
        "msgVpnName": access.msg_vpn_name,
        "serviceClassId": access.service_class,
        "eventBrokerVersion": access.broker_version,
    }
    return capacity_snapshot(
        service=service,
        vpn_monitor=monitor,
        vpn_config=config,
        clients=resources["clients"],
        queues=resources["queues"],
        topic_endpoints=resources["topicEndpoints"],
        transactions=resources["transactions"],
        bridges=resources["bridges"],
    )


def collect_all(raw_access: list[dict[str, Any]], expected_owner_id: str) -> dict[str, Any]:
    access = [BrokerAccess.from_dict(item, expected_owner_id) for item in raw_access]
    with ThreadPoolExecutor(max_workers=min(5, len(access) or 1)) as executor:
        brokers = list(executor.map(collect_broker, access))
    return {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "owner_id": expected_owner_id,
        "brokers": brokers,
    }
