from __future__ import annotations

import hashlib
import os
import ssl
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from multiprocessing.queues import Queue as ProcessQueue
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import psutil
from solace.messaging.config.authentication_strategy import BasicUserNamePassword
from solace.messaging.config.retry_strategy import RetryStrategy
from solace.messaging.config.solace_properties import service_properties, transport_layer_properties
from solace.messaging.config.transport_security_strategy import TLS
from solace.messaging.messaging_service import MessagingService
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener
from solace.messaging.resources.queue import Queue
from solace.messaging.resources.topic import Topic
from solace.messaging.resources.topic_subscription import TopicSubscription


@dataclass(frozen=True)
class GeneratorConfig:
    run_id: str
    stage_id: str
    delivery_mode: str
    smf_uri: str
    msg_vpn: str
    username: str
    password: str
    topic: str
    queue_name: str | None
    payload_bytes: int
    requested_rate: float
    warmup_seconds: int
    measurement_seconds: int
    drain_seconds: int = 10


class _DirectFailureListener(PublishFailureListener):
    def __init__(self, state: dict[str, Any], lock: threading.Lock):
        self.state = state
        self.lock = lock

    def on_failed_publish(self, failed_publish_event):
        with self.lock:
            self.state["nacks"] += 1
            if len(self.state["errors"]) < 20:
                self.state["errors"].append(type(failed_publish_event.get_exception()).__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _payload(prefix: bytes, sequence: int, size: int) -> bytearray:
    header = prefix + b":" + str(sequence).encode() + b":"
    if len(header) > size:
        raise ValueError("payload size is too small for the run identifier")
    digest = hashlib.sha256(header).digest()
    return bytearray(
        header
        + (digest * ((size - len(header) + len(digest) - 1) // len(digest)))[
            : size - len(header)
        ]
    )


def _sequence(message, prefix: bytes) -> int | None:
    payload = message.get_payload_as_bytes()
    if payload is None:
        return None
    raw = bytes(payload)
    if not raw.startswith(prefix + b":"):
        return None
    try:
        return int(raw[len(prefix) + 1 :].split(b":", 1)[0])
    except (ValueError, IndexError):
        return None


def _materialize_system_trust_store(directory: Path) -> None:
    candidates = [ssl.get_default_verify_paths().cafile]
    try:
        import certifi

        candidates.append(certifi.where())
    except ImportError:
        pass
    bundle = next((Path(path) for path in candidates if path and Path(path).is_file()), None)
    if bundle is None:
        raise RuntimeError("no system CA bundle is available")
    written = 0
    for index, certificate in enumerate(bundle.read_text().split("-----END CERTIFICATE-----")):
        if "-----BEGIN CERTIFICATE-----" not in certificate:
            continue
        (directory / f"ca-{index:03}.pem").write_text(
            certificate.strip() + "\n-----END CERTIFICATE-----\n"
        )
        written += 1
    if not written:
        raise RuntimeError("system CA bundle contained no PEM certificates")
    subprocess.run(["openssl", "rehash", str(directory)], check=True, capture_output=True)


def _service(config: GeneratorConfig, trust_store_path: str):
    properties = {
        transport_layer_properties.HOST: config.smf_uri,
        service_properties.VPN_NAME: config.msg_vpn,
    }
    return (
        MessagingService.builder()
        .from_properties(properties)
        .with_authentication_strategy(
            BasicUserNamePassword.of(config.username, config.password)
        )
        .with_connection_retry_strategy(RetryStrategy.parametrized_retry(2, 500))
        .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(2, 500))
        .with_transport_security_strategy(
            TLS.create().with_certificate_validation(
                False, validate_server_name=True, trust_store_file_path=trust_store_path
            )
        )
        .build(f"{config.run_id}-{config.stage_id}")
    )


def run_generator(config: GeneratorConfig, events: ProcessQueue, start_measurement) -> None:
    state: dict[str, Any] = {
        "received": set(),
        "retransmits": 0,
        "nacks": 0,
        "errors": [],
        "measurement_active": False,
    }
    lock = threading.Lock()
    service = receiver = publisher = None
    process = psutil.Process(os.getpid())
    logical_cpu_count = psutil.cpu_count(logical=True) or 1
    warmup_prefix = f"{config.run_id}:{config.stage_id}:warmup".encode()
    measurement_prefix = f"{config.run_id}:{config.stage_id}:measure".encode()
    topic = Topic.of(config.topic)
    counts = _zero_counts()

    def receive_loop() -> None:
        while receiver is not None and not receiver.is_terminated():
            try:
                message = receiver.receive_message(250)
                if message is None:
                    continue
                prefix = measurement_prefix if state["measurement_active"] else warmup_prefix
                sequence = _sequence(message, prefix)
                if sequence is None:
                    with lock:
                        state["errors"].append("unexpected_message")
                    continue
                with lock:
                    if sequence in state["received"]:
                        state["retransmits"] += 1
                    else:
                        state["received"].add(sequence)
                if config.delivery_mode == "persistent":
                    receiver.ack(message)
            except Exception as error:
                if receiver is not None and not receiver.is_terminating():
                    with lock:
                        state["errors"].append(type(error).__name__)

    def emit_sample(sequence: int, network_start) -> None:
        network = psutil.net_io_counters()
        with lock:
            received = len(state["received"])
            retransmits = state["retransmits"]
            asynchronous_nacks = state["nacks"]
            errors = tuple(state["errors"])
        raw_cpu_percent = process.cpu_percent(None)
        events.put(
            {
                "event": "generator_sample",
                "timestamp": _utc_now().isoformat(),
                "requested_publishes": sequence,
                "scheduled_publishes": counts["scheduled"],
                "attempted_publishes": counts["attempted"],
                "successful_publishes": counts["successful"],
                "accepted_unique_messages": counts["accepted"],
                "accepted_evidence": (
                    "broker_ack" if config.delivery_mode == "persistent" else "direct_publish_success"
                ),
                "acknowledgements": counts["acks"] if config.delivery_mode == "persistent" else None,
                "negative_acknowledgements": (
                    counts["nacks"] + asynchronous_nacks
                    if config.delivery_mode == "persistent"
                    else None
                ),
                "unique_receipts_by_destination": {config.topic: received},
                "retransmitted_receipts": retransmits,
                "receipts_inflight_by_destination": {
                    config.topic: max(0, counts["accepted"] - received)
                },
                "cpu_percent": raw_cpu_percent / logical_cpu_count,
                "raw_process_cpu_percent": raw_cpu_percent,
                "logical_cpu_count": logical_cpu_count,
                "network_tx_bytes": network.bytes_sent - network_start.bytes_sent,
                "network_rx_bytes": network.bytes_recv - network_start.bytes_recv,
                "errors": errors,
            }
        )

    def publish_phase(seconds: int, measurement: bool) -> None:
        started = time.monotonic()
        next_sample = started
        sequence = 0
        network_start = psutil.net_io_counters()
        process.cpu_percent(None)
        if measurement:
            time.sleep(0.05)
            emit_sample(0, network_start)
        while True:
            elapsed = time.monotonic() - started
            target = min(int(elapsed * config.requested_rate), int(seconds * config.requested_rate))
            counts["requested"] = target
            while sequence < target:
                counts["scheduled"] += 1
                counts["attempted"] += 1
                prefix = measurement_prefix if measurement else warmup_prefix
                message = _payload(prefix, sequence, config.payload_bytes)
                try:
                    if config.delivery_mode == "persistent":
                        publisher.publish_await_acknowledgement(message, topic, 5000, None)
                        counts["acks"] += 1
                    else:
                        publisher.publish(message, topic)
                    counts["successful"] += 1
                    counts["accepted"] += 1
                except Exception as error:
                    counts["nacks"] += 1
                    with lock:
                        if len(state["errors"]) < 20:
                            state["errors"].append(type(error).__name__)
                sequence += 1
            now = time.monotonic()
            if measurement and now >= next_sample:
                emit_sample(sequence, network_start)
                next_sample += 1
            if elapsed >= seconds:
                break
            seconds_until_next = (sequence + 1) / config.requested_rate - elapsed
            if seconds_until_next > 0:
                time.sleep(min(0.01, seconds_until_next))
        if measurement:
            emit_sample(sequence, network_start)

    try:
        with TemporaryDirectory(prefix="bcm-ca-") as trust_store:
            _materialize_system_trust_store(Path(trust_store))
            service = _service(config, trust_store)
            service.connect()
            if config.delivery_mode == "direct":
                receiver = (
                    service.create_direct_message_receiver_builder()
                    .with_subscriptions([TopicSubscription.of(config.topic)])
                    .build()
                )
                publisher = (
                    service.create_direct_message_publisher_builder()
                    .on_back_pressure_reject(10_000)
                    .build()
                )
                publisher.set_publish_failure_listener(_DirectFailureListener(state, lock))
            else:
                receiver = (
                    service.create_persistent_message_receiver_builder()
                    .with_message_client_acknowledgement()
                    .build(Queue.durable_exclusive_queue(str(config.queue_name)))
                )
                publisher = (
                    service.create_persistent_message_publisher_builder()
                    .on_back_pressure_reject(10_000)
                    .build()
                )
            receiver.start()
            publisher.start()
            receive_thread = threading.Thread(target=receive_loop, daemon=True)
            receive_thread.start()
            events.put({"event": "ready", "timestamp": _utc_now().isoformat()})
            publish_phase(config.warmup_seconds, False)
            warmup_deadline = time.monotonic() + config.drain_seconds
            while time.monotonic() < warmup_deadline:
                with lock:
                    if len(state["received"]) >= counts["accepted"]:
                        break
                time.sleep(0.05)
            counts = _zero_counts()
            with lock:
                state["received"].clear()
                state["retransmits"] = 0
                state["nacks"] = 0
                state["errors"].clear()
                state["measurement_active"] = True
            events.put({"event": "measurement_ready", "timestamp": _utc_now().isoformat()})
            if not start_measurement.wait(30):
                raise TimeoutError("orchestrator did not start measurement")
            events.put({"event": "measurement_started", "timestamp": _utc_now().isoformat()})
            publish_phase(config.measurement_seconds, True)
            events.put({"event": "measurement_finished", "timestamp": _utc_now().isoformat()})
            drain_deadline = time.monotonic() + config.drain_seconds
            while time.monotonic() < drain_deadline:
                with lock:
                    if len(state["received"]) >= counts["accepted"]:
                        break
                time.sleep(0.05)
            with lock:
                drain_receipts = len(state["received"])
                drain_retransmits = state["retransmits"]
                drain_errors = tuple(state["errors"])
            publisher.terminate(30_000)
            publisher = None
            events.put(
                {
                    "event": "drain_finished",
                    "timestamp": _utc_now().isoformat(),
                    "unique_receipts": drain_receipts,
                    "retransmitted_receipts": drain_retransmits,
                    "errors": drain_errors,
                }
            )
    except Exception as error:
        events.put(
            {"event": "error", "error_type": type(error).__name__, "message": str(error)[:500]}
        )
    finally:
        for component in (publisher, receiver):
            if component is not None:
                try:
                    component.terminate(10_000)
                except Exception:
                    pass
        if service is not None:
            try:
                service.disconnect()
            except Exception:
                pass
        events.put({"event": "stopped", "timestamp": _utc_now().isoformat()})


def _zero_counts() -> dict[str, int]:
    return {
        name: 0
        for name in ("requested", "scheduled", "attempted", "successful", "accepted", "acks", "nacks")
    }
