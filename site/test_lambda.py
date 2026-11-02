"""Contract checks for the read-only SEMP snapshot response."""
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
import lambda_function as backend


SERVICE = {
    "serviceId": "abc123",
    "name": "test-broker",
    "serviceClassId": "10k",
    "msgVpnName": "vpn-a",
    "managementProtocols": [{
        "name": "SEMP Read-Only",
        "username": "reader",
        "password": "secret",
        "endPoints": [{"uris": ["https://example.messaging.solace.cloud/SEMP/v2/config/spec"]}],
    }],
}


def event():
    return {
        "headers": {"x-bcm-origin": "test-origin"},
        "requestContext": {"http": {"method": "POST"}},
        "body": json.dumps({"serviceId": "abc123", "token": "header.payload.signature"}),
    }


class SempSnapshotTests(unittest.TestCase):
    def test_returns_real_semp_counters_without_credentials(self):
        def get(url, authorization, timeout=5):
            if url.endswith("/api/v0/services/abc123"):
                return {"data": SERVICE}
            if "/config/msgVpns/" in url:
                return {"data": {"maxConnectionCount": 100, "maxMsgSpoolUsage": 1000, "maxSubscriptionCount": 500}}
            if "/monitor/msgVpns/" in url:
                return {"data": {"rxMsgRate": 16000, "txMsgRate": 15000, "msgVpnConnections": 90, "msgSpoolUsage": 750, "msgVpnTotalUniqueSubscriptions": 120}}
            raise AssertionError(url)

        with patch.dict(os.environ, {"BCM_ORIGIN_SECRET": "test-origin"}), patch.object(backend, "_json_get", side_effect=get):
            response = backend.handler(event(), None)
        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertIs(body["sempConnected"], True)
        self.assertEqual(body["current"]["clientCount"], 90)
        self.assertEqual(body["current"]["subscriptionCount"], 120)
        self.assertEqual(body["limits"]["maxSubscriptionCount"], 500)
        self.assertNotIn("secret", response["body"])
        self.assertNotIn("header.payload.signature", response["body"])

    def test_rejects_cloud_metadata_without_semp(self):
        service = {**SERVICE, "managementProtocols": []}
        with patch.dict(os.environ, {"BCM_ORIGIN_SECRET": "test-origin"}), patch.object(backend, "_json_get", return_value={"data": service}):
            response = backend.handler(event(), None)
        self.assertEqual(response["statusCode"], 502)
        self.assertIn("SEMP Read-Only endpoint unavailable", response["body"])


if __name__ == "__main__":
    unittest.main()
