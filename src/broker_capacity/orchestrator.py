from __future__ import annotations

import fcntl
import hashlib
import json
import multiprocessing
import os
import platform
import queue
import socket
import time
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote

from .cloud import ServiceCredentials, SolaceCloudClient, read_token, token_claims
from .config_writer import CampaignConfigWriter, ConfigAccess
from .experiments import (
    AcceptedEvidence,
    BrokerSample,
    DeliveryMode,
    GeneratorSample,
    QueueSample,
    StageRecord,
    Workload,
)
from .loadgen import GeneratorConfig, run_generator
from .semp import SempClient
from .sustainability import ClassificationPolicy, classify_stage
from .telemetry import VPN_COUNTER_FIELDS, provenance_from_openapi


TARGET_SERVICE_ID = "4qn20a1u6ny"
TARGET_SERVICE_NAME = "bcm-20261005-5k"
TARGET_OWNER_ID = "usfos7cfqge"
TARGET_ORGANIZATION = "seall"
RESOURCE_PREFIX = "bcm-20261005-"


@contextmanager
def broker_lease(service_id: str) -> Iterator[None]:
    path = Path(".pilot/locks") / f"{service_id}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f"broker {service_id} already has an active local lease") from error
        handle.write(f"pid={os.getpid()}\n")
        handle.flush()
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


class CampaignOrchestrator:
    def __init__(self, token_file: Path, output_root: Path = Path("data/campaign")):
        self.token = read_token(token_file)
        claims = token_claims(self.token)
        if (claims.get("org"), claims.get("sub")) != (
            TARGET_ORGANIZATION,
            TARGET_OWNER_ID,
        ):
            raise ValueError("Cloud token organization or owner does not match campaign scope")
        self.cloud = SolaceCloudClient(self.token)
        self.credentials = self.cloud.service_credentials(
            TARGET_SERVICE_ID,
            expected_owner_id=TARGET_OWNER_ID,
            expected_name=TARGET_SERVICE_NAME,
        )
        self.semp = SempClient(self.credentials.access)
        self.output_root = output_root

    def discover(self) -> dict[str, Any]:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-5k-discovery"
        run_dir = self.output_root / run_id
        run_dir.mkdir(parents=True)
        config_spec = self.semp.get(self.credentials.semp_read_only_spec_url)
        monitor_spec = self.semp.get("/SEMP/v2/monitor/spec")
        self._write_json(run_dir / "config-openapi.json", config_spec)
        self._write_json(run_dir / "monitor-openapi.json", monitor_spec)
        inventory = self._inventory()
        self._write_json(run_dir / "inventory.json", inventory)
        summary = {
            "run_id": run_id,
            "service_id": TARGET_SERVICE_ID,
            "service_name": TARGET_SERVICE_NAME,
            "owner_id": TARGET_OWNER_ID,
            "msg_vpn": self.credentials.access.msg_vpn_name,
            "service_class": self.credentials.access.service_class,
            "broker_version_summary": self.credentials.access.broker_version,
            "semp_version": monitor_spec.get("info", {}).get("version"),
            "config_spec_sha256": _sha256_json(config_spec),
            "monitor_spec_sha256": _sha256_json(monitor_spec),
            "inventory": _inventory_summary(inventory),
        }
        self._write_json(run_dir / "summary.json", summary)
        return summary

    def ensure_g03_resources(self) -> dict[str, Any]:
        credentials = self.credentials
        if not all(
            (
                credentials.semp_manager_spec_url,
                credentials.semp_manager_username,
                credentials.semp_manager_password,
            )
        ):
            raise RuntimeError("SEMP Manager credentials are unavailable")
        writer = CampaignConfigWriter(
            ConfigAccess(
                service_id=TARGET_SERVICE_ID,
                owner_id=TARGET_OWNER_ID,
                msg_vpn=credentials.access.msg_vpn_name,
                management_uri=credentials.access.management_uri,
                username=credentials.semp_manager_username,
                password=credentials.semp_manager_password,
            )
        )
        return writer.ensure_exclusive_queue(
            "bcm-20261005-g03-q", "bcm-20261005-g03", max_spool_mb=100
        )

    def run_calibration(self, calibration_id: str) -> dict[str, Any]:
        plans = {
            "G01": (DeliveryMode.DIRECT, 100.0, None),
            "G02": (DeliveryMode.DIRECT, 1000.0, None),
            "G03": (DeliveryMode.PERSISTENT, 100.0, "bcm-20261005-g03-q"),
        }
        if calibration_id not in plans:
            raise ValueError("calibration_id must be G01, G02, or G03")
        mode, rate, queue_name = plans[calibration_id]
        with broker_lease(TARGET_SERVICE_ID):
            preflight = self._preflight(calibration_id, queue_name)
            if not preflight["eligible"]:
                return self._blocked_result(calibration_id, preflight)
            if calibration_id == "G03":
                resource = self.ensure_g03_resources()
            else:
                resource = None
            return self._run_stage(calibration_id, mode, rate, queue_name, resource)

    def _preflight(self, calibration_id: str, queue_name: str | None) -> dict[str, Any]:
        inventory = self._inventory()
        campaign_queues = [
            item for item in inventory["queues"] if item.get("queueName", "").startswith(RESOURCE_PREFIX)
        ]
        unrelated_clients = [
            item for item in inventory["clients"] if item.get("clientName") != "#client"
        ]
        foreign_resources = [
            item for item in campaign_queues if item.get("queueName") != queue_name
        ]
        vpn = inventory["vpn_monitor"]
        idle = all(vpn.get(field) == 0 for field in ("rxMsgRate", "txMsgRate", "rxByteRate", "txByteRate"))
        eligible = not any(
            (
                unrelated_clients,
                inventory["bridges"],
                inventory["dmr_bridges"],
                inventory["topic_endpoints"],
                foreign_resources,
                not idle,
                vpn.get("msgSpoolUsage") not in (0, None),
                vpn.get("msgSpoolMsgCount") not in (0, None),
            )
        )
        return {
            "calibration_id": calibration_id,
            "eligible": eligible,
            "unrelated_clients": len(unrelated_clients),
            "bridges": len(inventory["bridges"]),
            "dmr_bridges": len(inventory["dmr_bridges"]),
            "topic_endpoints": len(inventory["topic_endpoints"]),
            "foreign_campaign_queues": len(foreign_resources),
            "idle_rates": idle,
            "vpn_spool_usage": vpn.get("msgSpoolUsage"),
            "vpn_spool_messages": vpn.get("msgSpoolMsgCount"),
        }

    def _run_stage(
        self,
        calibration_id: str,
        mode: DeliveryMode,
        rate: float,
        queue_name: str | None,
        resource: dict[str, Any] | None,
    ) -> dict[str, Any]:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{calibration_id.lower()}"
        run_dir = self.output_root / run_id
        run_dir.mkdir(parents=True)
        topic = f"{RESOURCE_PREFIX}{calibration_id.lower()}"
        config_spec = self.semp.get(self.credentials.semp_read_only_spec_url)
        monitor_spec = self.semp.get("/SEMP/v2/monitor/spec")
        inventory = self._inventory()
        workload = Workload(
            delivery_mode=mode,
            ingress_protocol="SMF",
            egress_protocol="SMF",
            message_size_bytes=1024,
            fanout=1,
            destination_ids=(topic,),
            requested_rate=rate,
            queue_type="exclusive" if queue_name else None,
            acknowledgement_mode="client" if queue_name else None,
            composition={"publishers": 1, "consumers_per_destination": 1, "tls": True},
        )
        manifest = {
            "schema_version": 1,
            "kind": "wiring_calibration",
            "capacity_label_allowed": False,
            "run_id": run_id,
            "stage_id": calibration_id,
            "service_id": TARGET_SERVICE_ID,
            "service_name": TARGET_SERVICE_NAME,
            "owner_id": TARGET_OWNER_ID,
            "msg_vpn": self.credentials.access.msg_vpn_name,
            "service_class": self.credentials.access.service_class,
            "broker_version": monitor_spec.get("info", {}).get("version"),
            "workload": _safe(asdict(workload)),
            "resource": resource,
            "generator": {
                "placement": "local_wan",
                "hostname": socket.gethostname(),
                "python": platform.python_version(),
                "adapter": "solace-pubsubplus-python",
                "process_isolated": True,
            },
            "warmup_seconds": 15,
            "measurement_seconds": 60,
            "drain_seconds": 10,
            "config_spec_sha256": _sha256_json(config_spec),
            "monitor_spec_sha256": _sha256_json(monitor_spec),
            "broker_config_sha256": _sha256_json(inventory["vpn_config"]),
            "workload_config_sha256": _sha256_json(_safe(asdict(workload))),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write_json(run_dir / "manifest.json", manifest)
        self._write_json(
            run_dir / "provenance.json",
            {
                key: _safe(asdict(value))
                for key, value in provenance_from_openapi(
                    monitor_spec,
                    schema_name="MsgVpn",
                    fields=VPN_COUNTER_FIELDS,
                    endpoint="/SEMP/v2/monitor/msgVpns/{msgVpnName}",
                    schema_url="returned SEMP origin + /SEMP/v2/monitor/spec",
                ).items()
            },
        )

        ctx = multiprocessing.get_context("spawn")
        events = ctx.Queue()
        start_measurement = ctx.Event()
        generator = GeneratorConfig(
            run_id=run_id,
            stage_id=calibration_id,
            delivery_mode=mode.value,
            smf_uri=self.credentials.smf_uri,
            msg_vpn=self.credentials.access.msg_vpn_name,
            username=self.credentials.smf_username,
            password=self.credentials.smf_password,
            topic=topic,
            queue_name=queue_name,
            payload_bytes=1024,
            requested_rate=rate,
            warmup_seconds=15,
            measurement_seconds=60,
        )
        process = ctx.Process(target=run_generator, args=(generator, events, start_measurement))
        process.start()
        generator_events: list[dict[str, Any]] = []
        broker_events: list[dict[str, Any]] = []
        measurement_started: datetime | None = None
        measurement_ended: datetime | None = None
        stopped = False
        forced_stop = False
        deadline = time.monotonic() + 15 + 60 + 10 + 45
        next_broker_poll = 0.0
        try:
            while time.monotonic() < deadline:
                while True:
                    try:
                        event = events.get_nowait()
                    except queue.Empty:
                        break
                    generator_events.append(event)
                    if event["event"] == "measurement_ready":
                        broker_events.append(self._broker_event(queue_name))
                        measurement_started = datetime.fromisoformat(
                            broker_events[-1]["timestamp"]
                        )
                        next_broker_poll = time.monotonic() + 2
                        start_measurement.set()
                    elif event["event"] == "measurement_finished":
                        broker_events.append(self._broker_event(queue_name))
                        measurement_ended = datetime.fromisoformat(
                            broker_events[-1]["timestamp"]
                        )
                    elif event["event"] == "error":
                        forced_stop = True
                    elif event["event"] == "stopped":
                        stopped = True
                        break
                if stopped or forced_stop:
                    break
                if (
                    measurement_started
                    and measurement_ended is None
                    and time.monotonic() >= next_broker_poll
                ):
                    broker_events.append(self._broker_event(queue_name))
                    next_broker_poll = time.monotonic() + 2
                    if self._guardrail_hit(broker_events[-1], inventory["vpn_config"], queue_name):
                        forced_stop = True
                        break
                time.sleep(0.05)
        finally:
            if process.is_alive():
                if forced_stop or time.monotonic() >= deadline:
                    process.terminate()
                process.join(timeout=15)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=5)
            stopped = stopped or not process.is_alive()
        self._write_jsonl(run_dir / "generator.jsonl", generator_events)
        self._write_jsonl(run_dir / "semp.jsonl", broker_events)

        result = self._assess(
            run_id,
            calibration_id,
            workload,
            generator_events,
            broker_events,
            measurement_started,
            measurement_ended,
            stopped,
            queue_name,
            monitor_spec,
            manifest,
            forced_stop,
        )
        self._write_json(run_dir / "result.json", result)
        return result

    def _assess(
        self,
        run_id: str,
        stage_id: str,
        workload: Workload,
        generator_events: list[dict[str, Any]],
        broker_events: list[dict[str, Any]],
        measurement_started: datetime | None,
        measurement_ended: datetime | None,
        stopped: bool,
        queue_name: str | None,
        monitor_spec: dict[str, Any],
        manifest: dict[str, Any],
        forced_stop: bool,
    ) -> dict[str, Any]:
        samples = [event for event in generator_events if event.get("event") == "generator_sample"]
        if measurement_started is None or measurement_ended is None or len(samples) < 2:
            return {
                "run_id": run_id,
                "stage_id": stage_id,
                "outcome": "INCONCLUSIVE",
                "reasons": ["generator_stage_incomplete"],
                "processes_stopped": stopped,
                "forced_stop": forced_stop,
            }
        drain_event = next(
            (event for event in generator_events if event.get("event") == "drain_finished"),
            None,
        )
        terminal_error = next(
            (event for event in generator_events if event.get("event") == "error"),
            None,
        )
        generator_samples = tuple(
            GeneratorSample(
                timestamp=datetime.fromisoformat(item["timestamp"]),
                requested_publishes=item.get("requested_publishes"),
                scheduled_publishes=item.get("scheduled_publishes"),
                attempted_publishes=item.get("attempted_publishes"),
                successful_publishes=item.get("successful_publishes"),
                accepted_unique_messages=item.get("accepted_unique_messages"),
                accepted_evidence=AcceptedEvidence(item["accepted_evidence"]),
                acknowledgements=item.get("acknowledgements"),
                negative_acknowledgements=item.get("negative_acknowledgements"),
                unique_receipts_by_destination=item.get("unique_receipts_by_destination", {}),
                retransmitted_receipts=item.get("retransmitted_receipts"),
                receipts_inflight_by_destination=item.get("receipts_inflight_by_destination", {}),
                cpu_percent=item.get("cpu_percent"),
                network_tx_bytes=item.get("network_tx_bytes"),
                network_rx_bytes=item.get("network_rx_bytes"),
                errors=tuple(item.get("errors", ())),
                raw_process_cpu_percent=item.get("raw_process_cpu_percent"),
                logical_cpu_count=item.get("logical_cpu_count"),
            )
            for item in samples
        )
        broker_samples = tuple(_broker_sample(item) for item in broker_events)
        queue_empty = self._queue_depth(queue_name) == 0 if queue_name else True
        provenance = provenance_from_openapi(
            monitor_spec,
            schema_name="MsgVpn",
            fields=VPN_COUNTER_FIELDS,
            endpoint="/SEMP/v2/monitor/msgVpns/{msgVpnName}",
            schema_url="returned SEMP origin + /SEMP/v2/monitor/spec",
        )
        record = StageRecord(
            run_id=run_id,
            stage_id=stage_id,
            service_id=TARGET_SERVICE_ID,
            msg_vpn=self.credentials.access.msg_vpn_name,
            workload=workload,
            warmup_seconds=15,
            measurement_seconds=60,
            measurement_started_at=measurement_started,
            measurement_ended_at=measurement_ended,
            generator_samples=generator_samples,
            broker_samples=broker_samples,
            provenance=provenance,
            generator_processes_stopped=stopped,
            delivery_drain_completed=drain_event is not None and terminal_error is None,
            drained_unique_receipts_by_destination=(
                {workload.destination_ids[0]: drain_event["unique_receipts"]}
                if drain_event is not None
                else None
            ),
            drained_retransmitted_receipts=(
                drain_event.get("retransmitted_receipts")
                if drain_event is not None
                else None
            ),
            queues_drained=queue_empty,
            broker_version=manifest["broker_version"],
            service_class=manifest["service_class"],
            broker_config_sha256=manifest["broker_config_sha256"],
            workload_config_sha256=manifest["workload_config_sha256"],
            notes=("local WAN wiring calibration; not broker capacity",),
        )
        assessment = classify_stage(
            record,
            ClassificationPolicy(
                minimum_acceptance_ratio=0.98,
                minimum_delivery_ratio=0.98,
                maximum_sample_gap_seconds=5,
                timestamp_tolerance_seconds=2,
            ),
        )
        outcome = assessment.outcome
        reasons = list(assessment.reasons)
        if terminal_error is not None:
            outcome = "INVALID"
            reasons.append(f"generator_teardown_error:{terminal_error['error_type']}")
        return {
            "run_id": run_id,
            "stage_id": stage_id,
            "outcome": outcome,
            "reasons": tuple(dict.fromkeys(reasons)),
            "metrics": assessment.metrics,
            "capacity_label_allowed": False,
            "generator_placement": "local_wan",
            "processes_stopped": stopped,
            "queue_empty": queue_empty,
            "forced_stop": forced_stop,
        }

    def _inventory(self) -> dict[str, Any]:
        vpn = quote(self.credentials.access.msg_vpn_name, safe="")
        monitor = self.semp.get(f"/SEMP/v2/monitor/msgVpns/{vpn}").get("data", {})
        config = self.semp.get(f"/SEMP/v2/config/msgVpns/{vpn}").get("data", {})
        return {
            "vpn_monitor": monitor,
            "vpn_config": config,
            "clients": list(self.semp.collection(f"/SEMP/v2/monitor/msgVpns/{vpn}/clients")),
            "queues": list(self.semp.collection(f"/SEMP/v2/config/msgVpns/{vpn}/queues")),
            "topic_endpoints": list(
                self.semp.collection(f"/SEMP/v2/config/msgVpns/{vpn}/topicEndpoints")
            ),
            "bridges": list(self.semp.collection(f"/SEMP/v2/config/msgVpns/{vpn}/bridges")),
            "dmr_bridges": list(
                self.semp.collection(f"/SEMP/v2/config/msgVpns/{vpn}/dmrBridges")
            ),
        }

    def _broker_event(self, queue_name: str | None) -> dict[str, Any]:
        vpn = quote(self.credentials.access.msg_vpn_name, safe="")
        monitor = self.semp.get(f"/SEMP/v2/monitor/msgVpns/{vpn}").get("data", {})
        queues = list(self.semp.collection(f"/SEMP/v2/monitor/msgVpns/{vpn}/queues"))
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "vpn": {field: monitor.get(field) for field in VPN_COUNTER_FIELDS},
            "vpn_gauges": {
                key: monitor.get(key)
                for key in ("msgSpoolUsage", "msgSpoolMsgCount", "rxMsgRate", "txMsgRate")
            },
            "queues": [
                {
                    "queueName": item.get("queueName"),
                    "backlogMessages": self._queue_depth(item.get("queueName")),
                    "txUnackedMsgCount": item.get("txUnackedMsgCount"),
                    "msgSpoolUsage": item.get("msgSpoolUsage"),
                }
                for item in queues
                if item.get("queueName", "").startswith(RESOURCE_PREFIX)
            ],
        }

    def _queue_depth(self, queue_name: str | None) -> int | None:
        if queue_name is None:
            return 0
        vpn = quote(self.credentials.access.msg_vpn_name, safe="")
        queue_name_encoded = quote(queue_name, safe="")
        payload = self.semp.get(
            f"/SEMP/v2/monitor/msgVpns/{vpn}/queues/{queue_name_encoded}/msgs?count=1"
        )
        count = payload.get("meta", {}).get("count")
        if isinstance(count, int):
            return count
        data = payload.get("data", [])
        if not payload.get("meta", {}).get("paging", {}).get("nextPageUri"):
            return len(data)
        return None

    @staticmethod
    def _guardrail_hit(
        event: dict[str, Any], vpn_config: dict[str, Any], queue_name: str | None
    ) -> bool:
        usage = event["vpn_gauges"].get("msgSpoolUsage")
        limit_mb = vpn_config.get("maxMsgSpoolUsage")
        if isinstance(usage, (int, float)) and isinstance(limit_mb, (int, float)):
            if usage >= limit_mb * 1_000_000 * 0.70:
                return True
        for queue_item in event["queues"]:
            if queue_name and queue_item.get("queueName") == queue_name:
                if queue_item.get("backlogMessages") is None:
                    return True
        return False

    def _blocked_result(self, calibration_id: str, preflight: dict[str, Any]) -> dict[str, Any]:
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{calibration_id.lower()}-blocked"
        result = {
            "run_id": run_id,
            "stage_id": calibration_id,
            "outcome": "INCONCLUSIVE",
            "reasons": ["preflight_isolation_failed"],
            "preflight": preflight,
            "processes_stopped": True,
            "queue_empty": None,
            "capacity_label_allowed": False,
        }
        run_dir = self.output_root / run_id
        run_dir.mkdir(parents=True)
        self._write_json(run_dir / "result.json", result)
        return result

    @staticmethod
    def _write_json(path: Path, value: Any) -> None:
        path.write_text(json.dumps(_safe(value), indent=2, sort_keys=True) + "\n")

    @staticmethod
    def _write_jsonl(path: Path, values: list[dict[str, Any]]) -> None:
        path.write_text("".join(json.dumps(_safe(value), sort_keys=True) + "\n" for value in values))


def _broker_sample(event: dict[str, Any]) -> BrokerSample:
    return BrokerSample(
        timestamp=datetime.fromisoformat(event["timestamp"]),
        cumulative_counters=event["vpn"],
        queues=tuple(
            QueueSample(
                queue_name=item["queueName"],
                backlog_messages=item.get("backlogMessages"),
                unacked_messages=item.get("txUnackedMsgCount"),
                spool_bytes=item.get("msgSpoolUsage"),
            )
            for item in event["queues"]
        ),
    )


def _inventory_summary(inventory: dict[str, Any]) -> dict[str, Any]:
    vpn = inventory["vpn_monitor"]
    return {
        "clients": len(inventory["clients"]),
        "queues": len(inventory["queues"]),
        "topic_endpoints": len(inventory["topic_endpoints"]),
        "bridges": len(inventory["bridges"]),
        "dmr_bridges": len(inventory["dmr_bridges"]),
        "rx_msg_rate": vpn.get("rxMsgRate"),
        "tx_msg_rate": vpn.get("txMsgRate"),
        "spool_bytes": vpn.get("msgSpoolUsage"),
        "spool_messages": vpn.get("msgSpoolMsgCount"),
    }


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value") and isinstance(value.value, str):
        return value.value
    if isinstance(value, dict):
        return {key: _safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe(item) for item in value]
    return value
