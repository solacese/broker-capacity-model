"""Build the capacity explorer and its public experiment catalog."""
import csv
import io
import json
import math
from datetime import datetime, timezone
from pathlib import Path


ALLOWED_STATES = {"TODO", "RUNNING", "DONE"}


def describe_experiment(row: dict) -> str:
    workload = (f"{row['service_class']} {row['delivery_mode']} {row['client_api']}, "
                f"{int(row['message_bytes']):,} B messages, fanout {row['queue_fanout']}, "
                f"{row['publisher_connections']} publisher connections")
    stage = row["stage"]
    feature = row["feature_under_test"]
    if stage == "qualification":
        purpose = "Qualify generator headroom, consumer drain, and broker telemetry before searching for saturation."
    elif stage in {"surface", "transfer_surface", "class_check_surface"}:
        purpose = "Measure the stable publish-rate boundary for this message-size and fanout point."
    elif stage in {"protocol_delivery", "transfer_protocol", "class_check_protocol"}:
        purpose = "Compare the client API and delivery mode with matched reference workloads."
    elif stage in {"feature", "transfer_feature", "class_check_feature"}:
        purpose = f"Measure the effect of {feature.replace('_', ' ')} {row['feature_level']} against a matched reference."
    elif stage == "workload_density":
        purpose = f"Isolate {row['notes'].removeprefix('Isolate ')} while holding the other workload settings fixed."
    elif stage == "interaction":
        purpose = (f"Test the combined effect of {feature.replace('+', ' and ')}."
                   if feature else "Test how message size, fanout, and publisher concurrency interact.")
    elif stage == "heldout":
        purpose = "Use an independent holdout point to check interpolation and repeatability."
    elif stage == "transfer_limit":
        purpose = "Check whether the model transfers to this broker class under a changed workload limit."
    else:
        purpose = "Repeat a reference workload to check class transfer and drift."
    return f"{purpose} Configuration: {workload}."


def experiment_catalog(campaign: Path, progress_file: Path) -> dict:
    with campaign.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    if len(rows) != 300 or len({row["id"] for row in rows}) != 300:
        raise ValueError("Expected 300 distinct experiment IDs")
    progress = {"experiments": {}, "updated_at": None}
    if progress_file.exists():
        progress = json.loads(progress_file.read_text(encoding="utf-8"))
    updates = progress.get("experiments", {})
    if not isinstance(updates, dict):
        raise ValueError("Experiment progress must be keyed by experiment ID")
    unknown = set(updates) - {row["id"] for row in rows}
    if unknown:
        raise ValueError(f"Unknown experiment progress IDs: {sorted(unknown)}")
    catalog = []
    for row in rows:
        update = updates.get(row["id"], {})
        status = update.get("status", "TODO")
        if status not in ALLOWED_STATES:
            raise ValueError(f"Invalid status for {row['id']}: {status}")
        if status == "DONE" and not update.get("result_ref"):
            raise ValueError(f"DONE needs a result_ref for {row['id']}")
        if status == "RUNNING" and not update.get("started_at"):
            raise ValueError(f"RUNNING needs a started_at for {row['id']}")
        bounds = update.get("capacity_interval_msg_s")
        if bounds is not None:
            if status != "DONE" or not isinstance(bounds, dict) or update.get("broker_attributed") is not True:
                raise ValueError(f"Capacity interval needs DONE status and broker attribution for {row['id']}")
            lower, upper = bounds.get("lower"), bounds.get("upper")
            if (not all(isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
                        for value in (lower, upper)) or lower < 0 or upper < lower
                    or upper - lower > int(row["target_resolution_msg_s"])):
                raise ValueError(f"Capacity interval is wider than target resolution for {row['id']}")
        catalog.append({
            "id": row["id"], "stage": row["stage"], "service_class": row["service_class"],
            "description": describe_experiment(row),
            "client_api": row["client_api"], "delivery_mode": row["delivery_mode"],
            "message_bytes": int(row["message_bytes"]), "queue_fanout": int(row["queue_fanout"]),
            "publisher_connections": int(row["publisher_connections"]),
            "target_resolution_msg_s": int(row["target_resolution_msg_s"]),
            "published_topics": int(row["published_topics"]),
            "subscriptions_per_queue": int(row["subscriptions_per_queue"]),
            "user_property_bytes": int(row["user_property_bytes"]),
            "publisher_rate_skew": float(row["publisher_rate_skew"]),
            "other_active_queues": int(row["other_active_queues"]),
            "connection_churn_per_sec": int(row["connection_churn_per_sec"]),
            "feature_flags": json.loads(row["feature_flags_json"]),
            "feature_under_test": row["feature_under_test"], "feature_level": row["feature_level"],
            "readiness": row["status"], "status": status,
            "status_updated_at": update.get("updated_at"), "result_ref": update.get("result_ref"),
            "capacity_interval_msg_s": bounds,
            "broker_attributed": update.get("broker_attributed") is True,
            "publisher_command_template": row["publisher_command_template"],
            "consumer_command_template": row["consumer_command_template"],
            "broker_setup": row["broker_setup"], "required_adapter_or_gate": row["required_adapter_or_gate"],
            "notes": row["notes"],
        })
    return {"generated_at": datetime.now(timezone.utc).isoformat(),
            "progress_updated_at": progress.get("updated_at"), "experiments": catalog}


def downloadable_csv(campaign: Path, catalog: dict) -> str:
    with campaign.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        fieldnames = ["readiness" if name == "status" else name for name in reader.fieldnames]
        fieldnames += ["experiment_status", "status_updated_at", "result_ref",
                       "capacity_lower_msg_s", "capacity_upper_msg_s", "broker_attributed"]
        source_rows = list(reader)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for source, work in zip(source_rows, catalog["experiments"], strict=True):
        if source["id"] != work["id"]:
            raise ValueError("Campaign and status catalog order differ")
        row = {("readiness" if key == "status" else key): value for key, value in source.items()}
        row.update(experiment_status=work["status"],
                   status_updated_at=work["status_updated_at"] or "",
                   result_ref=work["result_ref"] or "",
                   capacity_lower_msg_s=(work["capacity_interval_msg_s"] or {}).get("lower", ""),
                   capacity_upper_msg_s=(work["capacity_interval_msg_s"] or {}).get("upper", ""),
                   broker_attributed="true" if work["broker_attributed"] else "")
        writer.writerow(row)
    return output.getvalue()


def main() -> None:
    site = Path(__file__).parent
    repo = site.parent
    template = (site / "index.template.html").read_text()
    dist = site / "dist"
    dist.mkdir(parents=True, exist_ok=True)
    (dist / "index.html").write_text(template, encoding="utf-8")
    campaign = repo / "campaigns" / "site-model-300.csv"
    catalog = experiment_catalog(campaign, repo / "campaigns" / "site-model-300-progress.json")
    (dist / "experiments.json").write_text(json.dumps(catalog, separators=(",", ":")) + "\n", encoding="utf-8")
    (dist / "experiments.csv").write_text(downloadable_csv(campaign, catalog), encoding="utf-8")
    print(f"Built {dist / 'index.html'} and {len(catalog['experiments'])} experiments")


if __name__ == "__main__":
    main()
