import unittest

from broker_capacity.capacity import capacity_snapshot
from broker_capacity.semp import BrokerAccess


class CapacitySnapshotTests(unittest.TestCase):
    def test_computes_unit_aware_ratios(self) -> None:
        snapshot = capacity_snapshot(
            service={
                "id": "svc",
                "name": "broker",
                "owner": "owner",
                "msgVpnName": "vpn",
                "serviceClassId": "enterprise-250-standalone",
                "eventBrokerVersion": "10.25",
            },
            vpn_monitor={"msgSpoolUsage": 45_000_000, "msgSpoolMsgCount": 3},
            vpn_config={
                "maxConnectionCount": 250,
                "maxEndpointCount": 250,
                "maxTransactionCount": 2500,
                "maxMsgSpoolUsage": 50_000,
            },
            clients=[{}],
            queues=[
                {
                    "queueName": "near-limit",
                    "msgSpoolUsage": 45_000_000,
                    "spooledMsgCount": 3,
                    "maxMsgSpoolUsage": 50,
                }
            ],
            topic_endpoints=[],
            transactions=[],
            bridges=[],
        )

        self.assertEqual(snapshot["utilization"]["connections"], 1 / 250)
        self.assertEqual(snapshot["utilization"]["endpoints"], 1 / 250)
        self.assertEqual(snapshot["utilization"]["spool"], 45_000_000 / 50_000_000_000)
        self.assertEqual(snapshot["utilization"]["most_utilized_queue_spool"], 0.9)
        self.assertEqual(snapshot["queue_summary"]["most_utilized_queue"], "near-limit")
        self.assertEqual(snapshot["limiting_factor"], "most_utilized_queue_spool")

    def test_includes_topic_endpoints_in_spool_totals(self) -> None:
        snapshot = capacity_snapshot(
            service={
                "id": "svc",
                "name": "broker",
                "owner": "owner",
                "msgVpnName": "vpn",
                "serviceClassId": "enterprise-250-standalone",
                "eventBrokerVersion": "10.26",
            },
            vpn_monitor={"msgSpoolUsage": 5_000_000, "msgSpoolMsgCount": 5},
            vpn_config={"maxMsgSpoolUsage": 100},
            clients=[],
            queues=[
                {
                    "queueName": "q",
                    "msgSpoolUsage": 2_000_000,
                    "spooledMsgCount": 2,
                    "txUnackedMsgCount": 1,
                    "maxMsgSpoolUsage": 10,
                    "maxMsgSpoolUsageExceededDiscardedMsgCount": 0,
                }
            ],
            topic_endpoints=[
                {
                    "topicEndpointName": "te",
                    "msgSpoolUsage": 3_000_000,
                    "spooledMsgCount": 3,
                    "txUnackedMsgCount": 2,
                }
            ],
            transactions=[],
            bridges=[],
        )
        self.assertEqual(snapshot["current"]["spool_bytes"], 5_000_000)
        self.assertEqual(snapshot["current"]["spool_messages"], 5)
        self.assertEqual(snapshot["current"]["delivered_unacked_messages"], 3)
        self.assertEqual(snapshot["utilization"]["spool"], 0.05)
        self.assertEqual(snapshot["queue_summary"]["historical_spooled_messages"], 2)
        self.assertIsNone(snapshot["queue_summary"]["max_depth_messages"])

    def test_missing_counters_remain_unknown(self) -> None:
        snapshot = capacity_snapshot(
            service={
                "id": "svc",
                "name": "broker",
                "owner": "owner",
                "msgVpnName": "vpn",
                "serviceClassId": "developer",
                "eventBrokerVersion": "10.26",
            },
            vpn_monitor={},
            vpn_config={"maxMsgSpoolUsage": 100},
            clients=[],
            queues=[{"queueName": "q", "maxMsgSpoolUsage": 10}],
            topic_endpoints=[],
            transactions=[],
            bridges=[],
        )
        self.assertIsNone(snapshot["current"]["spool_bytes"])
        self.assertIsNone(snapshot["current"]["spool_messages"])
        self.assertIsNone(snapshot["current"]["delivered_unacked_messages"])
        self.assertIsNone(snapshot["queue_summary"]["non_empty"])
        self.assertIsNone(snapshot["utilization"]["spool"])

    def test_rejects_wrong_owner(self) -> None:
        value = {
            "id": "svc",
            "name": "broker",
            "owner": "someone-else",
            "msgVpnName": "vpn",
            "serviceClassId": "developer",
            "eventBrokerVersion": "10.26",
            "uri": "https://example.com:943",
            "username": "reader",
            "password": "secret",
        }
        with self.assertRaisesRegex(ValueError, "Refusing broker"):
            BrokerAccess.from_dict(value, "raphael")


if __name__ == "__main__":
    unittest.main()
