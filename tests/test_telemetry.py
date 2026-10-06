from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from broker_capacity.telemetry import (
    broker_sample_from_semp,
    measurement_delta,
    provenance_from_openapi,
)


class TelemetryTests(unittest.TestCase):
    def test_computes_measurement_window_deltas_and_preserves_missingness(self) -> None:
        started = datetime(2026, 10, 5, tzinfo=timezone.utc)
        first = broker_sample_from_semp(
            timestamp=started,
            vpn={"rxMsgCount": 100, "txMsgCount": 200},
            queues=[{"queueName": "q", "txUnackedMsgCount": 2}],
            queue_backlog_messages={"q": 10},
        )
        last = broker_sample_from_semp(
            timestamp=started + timedelta(seconds=60),
            vpn={"rxMsgCount": 160, "txMsgCount": 10},
            queues=[{"queueName": "q", "txUnackedMsgCount": 1}],
            queue_backlog_messages={"q": 12},
        )
        delta = measurement_delta(first, last)
        self.assertEqual(delta.counters["rxMsgCount"].value, 60)
        self.assertEqual(delta.counters["txMsgCount"].reason, "counter_reset")
        self.assertEqual(delta.counters["rxByteCount"].reason, "missing")
        self.assertEqual(delta.queue_backlog["q"].value, 2)
        self.assertEqual(delta.queue_unacked["q"].value, -1)
        self.assertEqual(delta.queue_spool_bytes["q"].reason, "missing")

    def test_provenance_is_derived_from_openapi(self) -> None:
        spec = {
            "info": {"version": "10.26.0.8894"},
            "definitions": {
                "MsgVpn": {
                    "properties": {
                        "rxMsgCount": {
                            "type": "integer",
                            "description": "The number of messages received.",
                        },
                        "rxByteCount": {
                            "type": "integer",
                            "description": "The number of bytes received, in bytes (B).",
                        },
                    }
                }
            },
        }
        result = provenance_from_openapi(
            spec,
            schema_name="MsgVpn",
            fields=("rxMsgCount", "rxByteCount", "missing"),
            endpoint="/SEMP/v2/monitor/msgVpns/{msgVpnName}",
            schema_url="returned://SEMP/v2/monitor/spec",
        )
        self.assertEqual(result["rxMsgCount"].unit, "count")
        self.assertEqual(result["rxByteCount"].unit, "bytes")
        self.assertEqual(result["rxMsgCount"].schema_version, "10.26.0.8894")
        self.assertNotIn("missing", result)


if __name__ == "__main__":
    unittest.main()
