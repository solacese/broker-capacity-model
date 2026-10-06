from __future__ import annotations

import base64
import json
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .semp import BrokerAccess


CLOUD_API = "https://production-api.solace.cloud"


@dataclass(frozen=True)
class ServiceCredentials:
    access: BrokerAccess
    smf_uri: str
    smf_username: str
    smf_password: str
    semp_read_only_spec_url: str
    semp_manager_spec_url: str | None
    semp_manager_username: str | None
    semp_manager_password: str | None


def read_token(path: Path) -> str:
    token = path.read_text().strip()
    if token.count(".") != 2:
        raise ValueError("token file does not contain a raw JWT")
    return token


def token_claims(token: str) -> dict[str, Any]:
    encoded = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))


class SolaceCloudClient:
    def __init__(self, token: str, base_url: str = CLOUD_API, timeout: float = 45.0):
        self._token = token
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get(self, path: str) -> dict[str, Any]:
        request = Request(
            self.base_url + path,
            method="GET",
            headers={"Authorization": f"Bearer {self._token}", "Accept": "application/json"},
        )
        with urlopen(request, timeout=self.timeout, context=ssl.create_default_context()) as response:
            return json.load(response)

    def service_credentials(
        self,
        service_id: str,
        *,
        expected_owner_id: str,
        expected_name: str,
    ) -> ServiceCredentials:
        detail = self.get(f"/api/v0/services/{service_id}").get("data", {})
        actual_id = detail.get("serviceId", detail.get("id"))
        if actual_id != service_id or detail.get("userId") != expected_owner_id:
            raise ValueError("service ID or owner mismatch")
        if detail.get("name") != expected_name:
            raise ValueError("service name mismatch")

        read_only = _protocol(detail, "SEMP Read-Only")
        manager = _protocol(detail, "SEMP Manager", required=False)
        smf = _protocol(detail, "SMF")
        read_only_spec = _uri_with_path(read_only, "/SEMP/v2/config/spec")
        management_origin = _origin(read_only_spec)
        smf_uri = next(
            uri
            for endpoint in smf.get("endPoints", [])
            for uri in endpoint.get("uris", [])
            if uri.startswith("tcps://")
        )
        return ServiceCredentials(
            access=BrokerAccess(
                service_id=service_id,
                name=str(detail["name"]),
                owner_id=expected_owner_id,
                msg_vpn_name=str(detail["msgVpnName"]),
                service_class=str(detail["serviceClassId"]),
                broker_version=str(detail["eventBrokerVersion"]),
                management_uri=management_origin,
                username=str(read_only["username"]),
                password=str(read_only["password"]),
            ),
            smf_uri=smf_uri,
            smf_username=str(smf["username"]),
            smf_password=str(smf["password"]),
            semp_read_only_spec_url=read_only_spec,
            semp_manager_spec_url=(
                _uri_with_path(manager, "/SEMP/v2/config/spec") if manager else None
            ),
            semp_manager_username=str(manager["username"]) if manager else None,
            semp_manager_password=str(manager["password"]) if manager else None,
        )


def _protocol(detail: dict[str, Any], name: str, *, required: bool = True) -> dict[str, Any] | None:
    groups = detail.get("messagingProtocols", []) if name == "SMF" else detail.get("managementProtocols", [])
    result = next((protocol for protocol in groups if protocol.get("name") == name), None)
    if result is None and required:
        raise ValueError(f"service did not return {name} credentials")
    return result


def _uri_with_path(protocol: dict[str, Any], path: str) -> str:
    return next(
        uri
        for endpoint in protocol.get("endPoints", [])
        for uri in endpoint.get("uris", [])
        if urlparse(uri).path == path
    )


def _origin(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"
