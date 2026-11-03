"""Offline randomized paired-study plan and interval-aware effect analysis.

This module neither reads credentials nor provisions brokers or workers.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import random
import statistics
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "campaigns/paired-multisize-v1.json"
SCHEDULE_FIELDS = ["slot_id", "class", "service_id", "session", "position", "kind",
                   "pair_id", "order", "arm", "factor_value", "payload_bytes", "fanout",
                   "publisher_connections", "sdkperf_arguments", "offered_rate", "measurement_seconds", "status"]
RESULT_FIELDS = ["slot_id", "status", "attribution", "lower_msgps", "upper_msgps",
                 "artifact_path", "artifact_sha256"]


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def load_spec(path: Path = SPEC) -> dict:
    spec = json.loads(Path(path).read_text())
    if spec["execution_authorized"] or spec["provisioning_authorized"]:
        raise ValueError("Preparation must not authorize cloud actions")
    factor = spec["first_factor"]
    if (factor["pair_repeats_per_class"] != 12 or factor["sessions_per_class"] != 4
            or factor["pairs_per_session"] != 3
            or factor["order_balance_per_class"] != {"AB": 6, "BA": 6}):
        raise ValueError("Unexpected preregistered replication or order balance")
    if {t["class"] for t in spec["targets"]} != {"1K", "5K", "10K"}:
        raise ValueError("Study must contain one target slot per requested class")
    return spec


def design(spec: dict) -> dict:
    factor, work = spec["first_factor"], spec["reference_workload"]
    rows = []
    for target in spec["targets"]:
        broker_class = target["class"]
        seed = int(hashlib.sha256(f'{spec["random_seed"]}:{broker_class}'.encode()).hexdigest()[:16], 16)
        rng = random.Random(seed)
        ab_per_session = [2, 2, 1, 1]
        rng.shuffle(ab_per_session)
        orders = []
        for ab_count in ab_per_session:
            block = ["AB"] * ab_count + ["BA"] * (3 - ab_count)
            rng.shuffle(block)
            orders.extend(block)
        position = 0

        def add(session: int, kind: str, pair: str = "", order: str = "", arm: str = "A") -> None:
            nonlocal position
            position += 1
            value = factor["arm_a" if arm == "A" else "arm_b"][factor["id"]]
            slot = f"{broker_class}-S{session:02}-P{position:02}-{arm}"
            args = (f'-mt=persistent -msa={work["payload_bytes"]} '
                    f'-apw={work["publisher_ack_window"]} -asw={value} '
                    '-mr=<controller-selected-rate>')
            rows.append(dict(slot_id=slot, **{"class": broker_class},
                             service_id=target["service_id"] or "", session=session,
                             position=position, kind=kind, pair_id=pair, order=order,
                             arm=arm, factor_value=value, payload_bytes=work["payload_bytes"],
                             fanout=work["fanout"], publisher_connections=work["publisher_connections"],
                             sdkperf_arguments=args,
                             offered_rate="ADAPTIVE" if kind == "BOUNDARY_SEARCH" else "VALIDATED_SAFE_REFERENCE",
                             measurement_seconds="ADAPTIVE" if kind == "BOUNDARY_SEARCH" else 60,
                             status="TODO"))

        for session in range(1, 5):
            add(session, "DRIFT_REFERENCE")
            pair_numbers = list(range((session - 1) * 3 + 1, session * 3 + 1))
            rng.shuffle(pair_numbers)
            for number in pair_numbers:
                order = orders[number - 1]
                pair = f"{broker_class}-{factor['id']}-P{number:02}"
                for arm in order:
                    add(session, "BOUNDARY_SEARCH", pair, order, arm)
            add(session, "DRIFT_REFERENCE")

    plan = {"schema_version": 1, "kind": "OFFLINE_PAIRED_STUDY", "study_id": spec["study_id"],
            "spec_sha256": _digest(spec), "random_seed": spec["random_seed"],
            "execution_authorized": False, "provisioning_authorized": False,
            "factor_id": factor["id"], "rows": rows,
            "counts": {"broker_classes": 3, "paired_blocks_per_class": 12,
                       "paired_blocks_total": 36, "boundary_search_slots": 72,
                       "drift_reference_slots": 24, "total_slots": 96},
            "note": "A boundary-search slot may require multiple SDKPerf rate trials. No slot is a completed measurement."}
    validate(plan, spec)
    return plan


def validate(plan: dict, spec: dict) -> None:
    if plan["spec_sha256"] != _digest(spec):
        raise ValueError("Plan and study specification differ")
    rows = plan["rows"]
    if len(rows) != 96 or len({r["slot_id"] for r in rows}) != 96:
        raise ValueError("Missing or duplicated slots")
    if any(r["status"] != "TODO" or
           r["offered_rate"] != ("ADAPTIVE" if r["kind"] == "BOUNDARY_SEARCH" else "VALIDATED_SAFE_REFERENCE") or
           r["measurement_seconds"] != ("ADAPTIVE" if r["kind"] == "BOUNDARY_SEARCH" else 60)
           for r in rows):
        raise ValueError("Preparation contains execution data")
    if any(r["kind"] == "BOUNDARY_SEARCH" and (r["arm"] not in "AB" or not r["pair_id"])
           for r in rows):
        raise ValueError("Invalid A/B arm")
    for target in spec["targets"]:
        broker_class = target["class"]
        class_rows = [r for r in rows if r["class"] == broker_class]
        pairs = defaultdict(list)
        for row in class_rows:
            if row["pair_id"]:
                pairs[row["pair_id"]].append(row)
        if len(pairs) != 12 or any(len(v) != 2 or {x["arm"] for x in v} != {"A", "B"}
                                    or "".join(x["arm"] for x in v) != v[0]["order"] for v in pairs.values()):
            raise ValueError(f"Broken paired blocks for {broker_class}")
        if sorted(v[0]["order"] for v in pairs.values()) != ["AB"] * 6 + ["BA"] * 6:
            raise ValueError(f"Order imbalance for {broker_class}")
        for session in range(1, 5):
            session_rows = [r for r in class_rows if r["session"] == session]
            if (len(session_rows) != 8 or session_rows[0]["kind"] != "DRIFT_REFERENCE"
                    or session_rows[-1]["kind"] != "DRIFT_REFERENCE"
                    or sum(r["kind"] == "BOUNDARY_SEARCH" for r in session_rows) != 6):
                raise ValueError(f"Broken session block for {broker_class}")
            if {r["order"] for r in session_rows if r["kind"] == "BOUNDARY_SEARCH"} != {"AB", "BA"}:
                raise ValueError(f"A session lacks both arm orders for {broker_class}")
    if plan["execution_authorized"] or plan["provisioning_authorized"]:
        raise ValueError("Plan must remain offline")


def prepare(directory: Path, spec_path: Path = SPEC) -> dict:
    spec = load_spec(spec_path)
    plan = design(spec)
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "plan.json"
    if path.exists():
        existing = json.loads(path.read_text())
        if existing != plan:
            raise ValueError("Frozen plan differs; choose a new directory")
        return plan
    path.write_text(json.dumps(plan, indent=2) + "\n")
    with (directory / "schedule.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, SCHEDULE_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(plan["rows"])
    with (directory / "results-template.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, RESULT_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows({"slot_id": r["slot_id"], "status": "TODO"} for r in plan["rows"])
    return plan


def analyze(plan: dict, results_path: Path, *, margin_fraction: float = .05) -> dict:
    """Conservative paired log-ratio intervals; never promote censored bounds."""
    if not 0 < margin_fraction < .5:
        raise ValueError("Invalid practical equivalence margin")
    with Path(results_path).open(newline="") as source:
        records = list(csv.DictReader(source))
    valid_ids = {r["slot_id"] for r in plan["rows"]}
    if len({r["slot_id"] for r in records}) != len(records) or any(r["slot_id"] not in valid_ids for r in records):
        raise ValueError("Duplicate or foreign result slot")
    by_id = {r["slot_id"]: r for r in records}
    classes = {}
    for broker_class in ["1K", "5K", "10K"]:
        pairs = defaultdict(dict)
        drift_slots = []
        for slot in plan["rows"]:
            if slot["class"] == broker_class and slot["kind"] == "BOUNDARY_SEARCH":
                pairs[slot["pair_id"]][slot["arm"]] = (slot, by_id.get(slot["slot_id"]))
            elif slot["class"] == broker_class and slot["kind"] == "DRIFT_REFERENCE":
                drift_slots.append((slot, by_id.get(slot["slot_id"])))
        drift_valid = 0
        for slot, result in drift_slots:
            if (not result or result.get("status") != "VALID" or not slot["service_id"]
                    or not result.get("artifact_path") or not result.get("artifact_sha256")):
                continue
            path = Path(result["artifact_path"])
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != result["artifact_sha256"]:
                continue
            try:
                proof = json.loads(path.read_text())
            except (ValueError, UnicodeDecodeError):
                continue
            if (proof.get("slot_id") == slot["slot_id"] and proof.get("service_id") == slot["service_id"]
                    and proof.get("drift_reference_passed") is True and proof.get("source_sha256")):
                drift_valid += 1
        effects = []
        reasons = defaultdict(int)
        for pair_id, arms in sorted(pairs.items()):
            parsed = {}
            for arm in "AB":
                slot, result = arms[arm]
                if not result or result.get("status") != "VALID":
                    reasons["missing_or_invalid"] += 1
                    break
                if result.get("attribution") != "BROKER_BOUND":
                    reasons["not_broker_attributed"] += 1
                    break
                try:
                    lower, upper = float(result["lower_msgps"]), float(result["upper_msgps"])
                except (TypeError, ValueError):
                    reasons["missing_interval"] += 1
                    break
                artifact_path = Path(result.get("artifact_path") or "")
                if (not slot["service_id"] or not result.get("artifact_path")
                        or not artifact_path.is_file() or not result.get("artifact_sha256")):
                    reasons["missing_verified_artifact"] += 1
                    break
                raw = artifact_path.read_bytes()
                if hashlib.sha256(raw).hexdigest() != result["artifact_sha256"]:
                    reasons["artifact_digest_mismatch"] += 1
                    break
                try:
                    proof = json.loads(raw)
                except (ValueError, UnicodeDecodeError):
                    reasons["invalid_artifact"] += 1
                    break
                if (proof.get("slot_id") != slot["slot_id"]
                        or proof.get("service_id") != slot["service_id"]
                        or proof.get("broker_capacity_validated") is not True
                        or proof.get("apparatus_control_passed") is not True
                        or not proof.get("source_sha256")
                        or proof.get("lower_msgps") != lower
                        or proof.get("upper_msgps") != upper):
                    reasons["artifact_not_broker_bound"] += 1
                    break
                if (not math.isfinite(lower) or not math.isfinite(upper) or lower <= 0
                        or upper < lower or upper / lower > 1.01):
                    reasons["invalid_or_wide_interval"] += 1
                    break
                parsed[arm] = (lower, upper)
            if len(parsed) != 2:
                continue
            a_lo, a_hi = parsed["A"]
            b_lo, b_hi = parsed["B"]
            midpoint = math.log(math.sqrt(b_lo * b_hi) / math.sqrt(a_lo * a_hi))
            effects.append(dict(pair_id=pair_id, order=arms["A"][0]["order"],
                                session=arms["A"][0]["session"], midpoint_log=midpoint,
                                minimum_log=math.log(b_lo / a_hi),
                                maximum_log=math.log(b_hi / a_lo)))
        report = {"valid_pairs": len(effects), "required_pairs": 12,
                  "valid_drift_references": drift_valid, "required_drift_references": 8,
                  "excluded_pair_reasons": dict(reasons), "decision": "NOT_READY",
                  "scope": "this broker class and the 1,024-byte, fanout-two reference workload only"}
        if len(effects) == 12 and drift_valid != 8:
            report["decision"] = "DRIFT_GATES_INCOMPLETE"
        if len(effects) == 12 and drift_valid == 8:
            mid = [x["midpoint_log"] for x in effects]
            average = statistics.mean(mid)
            se = statistics.stdev(mid) / math.sqrt(len(mid))
            session_means = [statistics.mean(x["midpoint_log"] for x in effects if x["session"] == s)
                             for s in range(1, 5)]
            session_se = statistics.stdev(session_means) / math.sqrt(4)
            base_lo = statistics.mean(x["minimum_log"] for x in effects)
            base_hi = statistics.mean(x["maximum_log"] for x in effects)
            # Fixed design: 12 pairs (11 df), four sessions (3 df).
            t90_pairs, t90_sessions = 1.79588481870367, 2.35336343480183
            t95_pairs, t95_sessions = 2.20098516008295, 3.18244630528426
            half90 = max(t90_pairs * se, t90_sessions * session_se)
            half95 = max(t95_pairs * se, t95_sessions * session_se)
            ci90 = [base_lo - half90, base_hi + half90]
            ci95 = [base_lo - half95, base_hi + half95]
            ab = statistics.mean(x["midpoint_log"] for x in effects if x["order"] == "AB")
            ba = statistics.mean(x["midpoint_log"] for x in effects if x["order"] == "BA")
            order_flag = abs(ab - ba) > math.log(1 + margin_fraction)
            per_session = {str(s): session_means[s - 1] for s in range(1, 5)}
            lower_equiv, upper_equiv = math.log(1 - margin_fraction), math.log(1 + margin_fraction)
            if order_flag:
                decision = "ORDER_SENSITIVITY_REVIEW"
            elif ci90[0] > lower_equiv and ci90[1] < upper_equiv:
                decision = "EQUIVALENT_ON_REFERENCE_WORKLOAD"
            elif ci95[0] > upper_equiv:
                decision = "MATERIAL_INCREASE_ON_REFERENCE_WORKLOAD"
            elif ci95[1] < lower_equiv:
                decision = "MATERIAL_DECREASE_ON_REFERENCE_WORKLOAD"
            else:
                decision = "UNRESOLVED"
            report.update(decision=decision,
                          estimate_percent=(math.exp(average) - 1) * 100,
                          paired_log_sd=statistics.stdev(mid),
                          session_log_sd=statistics.stdev(session_means),
                          effect_interval_90_percent=[(math.exp(x) - 1) * 100 for x in ci90],
                          effect_interval_95_percent=[(math.exp(x) - 1) * 100 for x in ci95],
                          order_contrast_percent=(math.exp(ab - ba) - 1) * 100,
                          session_effect_percent={k: (math.exp(v) - 1) * 100 for k, v in per_session.items()})
        classes[broker_class] = report
    return {"study_id": plan["study_id"], "factor_id": plan["factor_id"],
            "practical_margin_fraction": margin_fraction, "by_class": classes,
            "global_coefficient_status": "NOT_ESTIMATED",
            "note": "A class result is not portable to other payload/fanout contexts or independent broker instances."}
