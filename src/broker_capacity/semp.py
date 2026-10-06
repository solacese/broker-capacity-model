from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Iterator
from urllib.parse import urljoin
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class BrokerAccess:
    service_id: str
    name: str
    owner_id: str
    msg_vpn_name: str
    service_class: str
    broker_version: str
    management_uri: str
    username: str
    password: str

    @classmethod
    def from_dict(cls, value: dict[str, Any], expected_owner_id: str) -> BrokerAccess:
        owner_id = str(value.get("owner", ""))
        if owner_id != expected_owner_id:
            raise ValueError(
                f"Refusing broker {value.get('id', '<unknown>')}: owner {owner_id!r} "
                f"does not match {expected_owner_id!r}"
            )
        return cls(
            service_id=str(value["id"]),
            name=str(value["name"]),
            owner_id=owner_id,
            msg_vpn_name=str(value["msgVpnName"]),
            service_class=str(value["serviceClassId"]),
            broker_version=str(value["eventBrokerVersion"]),
            management_uri=str(value["uri"]).rstrip("/"),
            username=str(value["username"]),
            password=str(value["password"]),
        )


class SempClient:
    """Minimal GET-only SEMP v2 client."""

    def __init__(self, access: BrokerAccess, timeout: float = 30.0):
        self.access = access
        self.timeout = timeout
        credentials = f"{access.username}:{access.password}".encode()
        self._authorization = "Basic " + base64.b64encode(credentials).decode()

    def get(self, path_or_url: str) -> dict[str, Any]:
        url = (
            path_or_url
            if path_or_url.startswith("https://")
            else urljoin(self.access.management_uri + "/", path_or_url.lstrip("/"))
        )
        request = Request(
            url,
            method="GET",
            headers={"Authorization": self._authorization, "Accept": "application/json"},
        )
        with urlopen(request, timeout=self.timeout) as response:
            return json.load(response)

    def collection(self, path: str, count: int = 100) -> Iterator[dict[str, Any]]:
        separator = "&" if "?" in path else "?"
        next_url: str | None = f"{path}{separator}count={count}"
        while next_url:
            payload = self.get(next_url)
            data = payload.get("data", [])
            if isinstance(data, list):
                yield from data
            elif isinstance(data, dict):
                yield data
            paging = payload.get("meta", {}).get("paging") or payload.get("paging") or {}
            next_url = paging.get("nextPageUri")
