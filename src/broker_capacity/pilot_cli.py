from __future__ import annotations

import argparse
import json
from pathlib import Path

from .experiments import StageOutcome
from .orchestrator import CampaignOrchestrator
from .search import AdaptiveBracketSearch


TARGET_SERVICE_ID = "avfk5i15jtm"
TARGET_OWNER_ID = "usfos7cfqge"
RESOURCE_PREFIX = "bcm-20261005-"
DIRECT_RATES = (100, 1000)
WARMUP_SECONDS = 15
MEASUREMENT_SECONDS = 60


def calibration_plan(service_id: str, owner_id: str) -> dict[str, object]:
    if service_id != TARGET_SERVICE_ID:
        raise ValueError(f"pilot is restricted to service {TARGET_SERVICE_ID}")
    if owner_id != TARGET_OWNER_ID:
        raise ValueError(f"pilot is restricted to owner {TARGET_OWNER_ID}")
    return {
        "kind": "wiring_calibration",
        "saturation_result_allowed": False,
        "service_id": service_id,
        "owner_id": owner_id,
        "resource_prefix": RESOURCE_PREFIX,
        "preconditions": [
            "owner_and_service_match",
            "inventory_captured",
            "no_unrelated_traffic",
            "wildcard_subscriptions_clear",
            "bridges_clear",
            "dmr_clear",
            "telemetry_schema_verified",
            "consumers_attached_before_publishers",
        ],
        "stop_conditions": [
            "unrelated_traffic",
            "generator_error",
            "positive_unexplained_backlog_drift",
            "resource_guardrail",
            "generator_limitation",
        ],
        "stages": [
            {
                "delivery_mode": "direct",
                "ingress_protocol": "smf",
                "egress_protocol": "smf",
                "message_size_bytes": 1024,
                "fanout": 1,
                "requested_rate": rate,
                "warmup_seconds": WARMUP_SECONDS,
                "measurement_seconds": MEASUREMENT_SECONDS,
            }
            for rate in DIRECT_RATES
        ],
        "postconditions": ["all_processes_stopped", "test_queues_empty"],
    }


def _write_json(value: object, output: Path | None) -> None:
    serialized = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if output is None:
        print(serialized, end="")
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(serialized)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plan and summarize the bounded 2026-10-05 broker wiring pilot"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan", help="emit the immutable bounded calibration plan")
    plan.add_argument("--service-id", default=TARGET_SERVICE_ID)
    plan.add_argument("--owner-id", default=TARGET_OWNER_ID)
    plan.add_argument("--output", type=Path)

    discover = subparsers.add_parser(
        "discover", help="run scoped token-based read-only discovery on the dedicated 5K"
    )
    discover.add_argument("--token-file", type=Path, default=Path(".env"))
    discover.add_argument("--output-root", type=Path, default=Path("data/campaign"))

    run = subparsers.add_parser(
        "run-calibration", help="execute one bounded G01, G02, or G03 calibration"
    )
    run.add_argument("calibration_id", choices=("G01", "G02", "G03"))
    run.add_argument("--token-file", type=Path, default=Path(".env"))
    run.add_argument("--output-root", type=Path, default=Path("data/campaign"))

    bracket = subparsers.add_parser(
        "bracket", help="summarize explicit stage outcomes without running load"
    )
    bracket.add_argument(
        "observation",
        nargs="+",
        help="RATE=OUTCOME, where outcome is SUSTAINABLE, UNSUSTAINABLE, INVALID, or INCONCLUSIVE",
    )
    bracket.add_argument("--maximum-rate", type=float)
    bracket.add_argument("--maximum-stages", type=int, default=10)
    bracket.add_argument("--output", type=Path)

    args = parser.parse_args()
    if args.command == "plan":
        _write_json(calibration_plan(args.service_id, args.owner_id), args.output)
        return
    if args.command == "discover":
        result = CampaignOrchestrator(args.token_file, args.output_root).discover()
        _write_json(result, None)
        return
    if args.command == "run-calibration":
        result = CampaignOrchestrator(args.token_file, args.output_root).run_calibration(
            args.calibration_id
        )
        _write_json(result, None)
        return

    search = AdaptiveBracketSearch(
        100, maximum_rate=args.maximum_rate, maximum_stages=args.maximum_stages
    )
    for observation in args.observation:
        rate_text, separator, outcome_text = observation.partition("=")
        if not separator:
            parser.error(f"invalid observation {observation!r}")
        search.observe(float(rate_text), StageOutcome(outcome_text.upper()))
    estimate = search.estimate()
    _write_json(
        {
            "kind": estimate.kind,
            "sustainable_rate": estimate.sustainable_rate,
            "unsustainable_rate": estimate.unsustainable_rate,
            "relative_width": estimate.relative_width,
            "next_rate": search.next_rate(),
            "observations": estimate.observations,
        },
        args.output,
    )


if __name__ == "__main__":
    main()
