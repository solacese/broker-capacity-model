from __future__ import annotations

import base64
import json
import ssl
from urllib.error import HTTPError
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urljoin
from urllib.request import Request, urlopen


RESOURCE_PREFIX = "bcm-20261005-"
TARGET_SERVICE_ID = "4qn20a1u6ny"
TARGET_OWNER_ID = "usfos7cfqge"
TARGET_VPN = "bcm-20261005-5k"


@dataclass(frozen=True)
class ConfigAccess:
    service_id: str
    owner_id: str
    msg_vpn: str
    management_uri: str
    username: str
    password: str

    def __post_init__(self) -> None:
        if self.service_id != TARGET_SERVICE_ID or self.owner_id != TARGET_OWNER_ID:
            raise ValueError("configuration writes are restricted to the dedicated 5K service")
        if self.msg_vpn != TARGET_VPN:
            raise ValueError("configuration writes are restricted to the dedicated 5K VPN")


class CampaignConfigWriter:
    """Narrow SEMP writer for idempotently creating campaign-owned resources."""

    def __init__(self, access: ConfigAccess, timeout: float = 30.0):
        self.access = access
        self.timeout = timeout
        credentials = f"{access.username}:{access.password}".encode()
        self._authorization = "Basic " + base64.b64encode(credentials).decode()

    def ensure_exclusive_queue(
        self,
        queue_name: str,
        subscription_topic: str,
        *,
        max_spool_mb: int = 100,
    ) -> dict[str, Any]:
        self._validate_name(queue_name)
        self._validate_topic(subscription_topic)
        vpn = quote(self.access.msg_vpn, safe="")
        queue = quote(queue_name, safe="")
        existing = self._request("GET", f"/SEMP/v2/config/msgVpns/{vpn}/queues/{queue}", allow_404=True)
        expected = {
            "queueName": queue_name,
            "accessType": "exclusive",
            "ingressEnabled": True,
            "egressEnabled": True,
            "permission": "consume",
            "maxMsgSpoolUsage": max_spool_mb,
        }
        if existing is None:
            self._request("POST", f"/SEMP/v2/config/msgVpns/{vpn}/queues", body=expected)
        else:
            data = existing.get("data", {})
            mismatches = {
                key: (data.get(key), value)
                for key, value in expected.items()
                if data.get(key) != value
            }
            if mismatches:
                raise ValueError(f"existing campaign queue configuration mismatch: {mismatches}")

        encoded_topic = quote(subscription_topic, safe="")
        subscription_path = (
            f"/SEMP/v2/config/msgVpns/{vpn}/queues/{queue}/subscriptions/{encoded_topic}"
        )
        if self._request("GET", subscription_path, allow_404=True) is None:
            self._request(
                "POST",
                f"/SEMP/v2/config/msgVpns/{vpn}/queues/{queue}/subscriptions",
                body={"subscriptionTopic": subscription_topic},
            )
        return {"queue_name": queue_name, "subscription_topic": subscription_topic}

    def _validate_name(self, name: str) -> None:
        if not name.startswith(RESOURCE_PREFIX):
            raise ValueError(f"resource names must start with {RESOURCE_PREFIX}")

    def _validate_topic(self, topic: str) -> None:
        if not topic.startswith(RESOURCE_PREFIX) or "*" in topic or ">" in topic:
            raise ValueError("subscription must be an exact campaign-prefixed topic")

    def _request(
        self, method: str, path: str, *, body: dict[str, Any] | None = None, allow_404: bool = False
    ) -> dict[str, Any] | None:
        data = json.dumps(body).encode() if body is not None else None
        request = Request(
            urljoin(self.access.management_uri + "/", path.lstrip("/")),
            method=method,
            data=data,
            headers={
                "Authorization": self._authorization,
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        try:
            with urlopen(
                request, timeout=self.timeout, context=ssl.create_default_context()
            ) as response:
                return json.load(response)
        except HTTPError as error:
            if allow_404 and error.code == 404:
                return None
            if allow_404 and error.code == 400:
                # SEMP reports a missing named resource as HTTP 400 / NOT_FOUND.
                # Other 400 responses must still fail rather than trigger a write.
                try:
                    payload = json.loads(error.read())
                except (ValueError, OSError):
                    raise error
                if payload.get("meta", {}).get("error", {}).get("status") == "NOT_FOUND":
                    return None
            raise
