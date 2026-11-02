"""Read-only Solace Cloud lookup for the capacity page.

The Cloud token is supplied for one request and is never persisted or returned.
Only Solace's fixed API host and the service's SEMP Read-Only endpoint are used.
"""
from __future__ import annotations

import base64
import json
import os
import re
import ssl
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen


API = "https://production-api.solace.cloud"
SERVICE_ID = re.compile(r"^[a-z0-9]{6,32}$")
TLS = ssl.create_default_context()


def _json_get(url, authorization, timeout=5):
    req = Request(url, headers={"Authorization": authorization, "Accept": "application/json"})
    with urlopen(req, timeout=timeout, context=TLS) as response:
        return json.load(response)


def _response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "content-type": "application/json; charset=utf-8",
            "cache-control": "no-store",
            "x-content-type-options": "nosniff",
        },
        "body": json.dumps(body, separators=(",", ":")),
    }


def _read_only_protocol(service):
    return next((p for p in service.get("managementProtocols", []) if p.get("name") == "SEMP Read-Only"), None)


def _semp_origin(protocol):
    for endpoint in protocol.get("endPoints", []):
        for uri in endpoint.get("uris", []):
            parsed = urlparse(uri)
            if parsed.scheme == "https" and parsed.hostname and parsed.hostname.endswith(".messaging.solace.cloud") and parsed.path == "/SEMP/v2/config/spec":
                return f"https://{parsed.netloc}"
    return None


def _finite_number(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def handler(event, context):
    # A long random CloudFront origin header prevents public direct invocation of
    # the function URL. Do not expose its value in the site or logs.
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    if headers.get("x-bcm-origin") != os.environ.get("BCM_ORIGIN_SECRET"):
        return _response(403, {"error": "Forbidden"})
    if event.get("requestContext", {}).get("http", {}).get("method") != "POST":
        return _response(405, {"error": "POST required"})
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        try:
            raw = base64.b64decode(raw, validate=True).decode("utf-8")
        except (ValueError, UnicodeError):
            return _response(400, {"error": "Invalid request"})
    if len(raw) > 6000:
        return _response(413, {"error": "Request too large"})
    try:
        body = json.loads(raw)
        service_id = body["serviceId"].strip()
        token = body["token"].strip()
    except (ValueError, KeyError, AttributeError, TypeError):
        return _response(400, {"error": "Service ID and Cloud API token required"})
    if not SERVICE_ID.fullmatch(service_id) or len(token) > 4500 or token.count(".") != 2:
        return _response(400, {"error": "Invalid service ID or token format"})
    try:
        service = _json_get(f"{API}/api/v0/services/{service_id}", f"Bearer {token}").get("data", {})
        if service.get("serviceId") != service_id:
            return _response(404, {"error": "Service not found"})
        output = {
            "serviceId": service_id,
            "name": str(service.get("name", ""))[:120],
            "serviceClassId": str(service.get("serviceClassId", ""))[:80],
            "version": str(service.get("eventBrokerVersion", ""))[:40],
            "datacenterId": str(service.get("datacenterId", ""))[:80],
            "msgVpnName": str(service.get("msgVpnName", ""))[:120],
            "networkSpeed": str(service.get("serviceClassDisplayedAttributes", {}).get("Network Speed", ""))[:40],
            "queriedUtc": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
            "sempConnected": False,
            "limits": {},
            "current": {},
        }
        protocol = _read_only_protocol(service)
        origin = _semp_origin(protocol) if protocol else None
        if not (protocol and origin and protocol.get("username") and protocol.get("password") and output["msgVpnName"]):
            return _response(502, {"error": "SEMP Read-Only endpoint unavailable for this service"})
        basic = base64.b64encode(f"{protocol['username']}:{protocol['password']}".encode()).decode()
        prefix = f"{origin}/SEMP/v2"
        vpn = quote(output["msgVpnName"], safe="")
        auth = f"Basic {basic}"
        config = _json_get(f"{prefix}/config/msgVpns/{vpn}", auth).get("data", {})
        monitor = _json_get(f"{prefix}/monitor/msgVpns/{vpn}", auth).get("data", {})
        for key in ("maxConnectionCount", "maxMsgSpoolUsage", "maxSubscriptionCount", "maxEgressFlowCount", "maxIngressFlowCount"):
            output["limits"][key] = _finite_number(config.get(key))
        for key in ("rxMsgRate", "txMsgRate", "msgSpoolUsage", "msgSpoolMsgCount"):
            output["current"][key] = _finite_number(monitor.get(key))
        output["current"]["clientCount"] = _finite_number(monitor.get("msgVpnConnections"))
        output["current"]["subscriptionCount"] = _finite_number(monitor.get("msgVpnTotalUniqueSubscriptions"))
        if output["current"]["clientCount"] is None:
            clients = _json_get(f"{prefix}/monitor/msgVpns/{vpn}/clients?count=1", auth)
            output["current"]["clientCount"] = _finite_number(clients.get("meta", {}).get("count"))
        output["sempConnected"] = True
        return _response(200, output)
    except HTTPError as error:
        if error.code in (401, 403):
            return _response(401, {"error": "Token or broker access was rejected"})
        if error.code == 404:
            return _response(404, {"error": "Service not found"})
        return _response(502, {"error": "Broker lookup failed"})
    except (URLError, TimeoutError, ValueError, KeyError, OSError):
        return _response(502, {"error": "Broker lookup unavailable"})
