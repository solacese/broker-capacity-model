from __future__ import annotations

import copy
import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from broker_capacity.paired_study import RESULT_FIELDS, analyze, design, load_spec, prepare, validate


class PairedStudyTests(unittest.TestCase):
    def setUp(self):
        self.spec = load_spec()

    def test_balanced_reproducible_offline_plan(self):
        plan = design(self.spec)
        validate(plan, self.spec)
        self.assertEqual(plan["counts"]["paired_blocks_total"], 36)
        self.assertEqual(plan["counts"]["boundary_search_slots"], 72)
        self.assertEqual(plan["counts"]["drift_reference_slots"], 24)
        self.assertFalse(plan["execution_authorized"])
        self.assertEqual(plan, design(self.spec))
        changed = copy.deepcopy(self.spec)
        changed["random_seed"] += 1
        self.assertNotEqual([r["order"] for r in plan["rows"]],
                            [r["order"] for r in design(changed)["rows"]])

    def test_preparation_is_idempotent_and_does_not_touch_cloud(self):
        with tempfile.TemporaryDirectory() as tmp, \
             patch("socket.socket", side_effect=AssertionError("No network in preparation")), \
             patch("subprocess.Popen", side_effect=AssertionError("No worker in preparation")):
            directory = Path(tmp)
            first = prepare(directory)
            second = prepare(directory)
            self.assertEqual(first, second)
            self.assertEqual(len((directory / "schedule.csv").read_text().splitlines()), 97)
            self.assertEqual(len((directory / "results-template.csv").read_text().splitlines()), 97)

    def _results(self, directory: Path, plan: dict, b_multiplier: float = 1.0,
                 bad_artifact: bool = False) -> Path:
        rows = []
        for slot in plan["rows"]:
            if slot["kind"] == "DRIFT_REFERENCE":
                proof = {"slot_id": slot["slot_id"], "service_id": slot["service_id"],
                         "drift_reference_passed": True, "source_sha256": "test-source"}
                path = directory / (slot["slot_id"] + ".json")
                path.write_text(json.dumps(proof))
                rows.append({"slot_id": slot["slot_id"], "status": "VALID",
                             "artifact_path": str(path),
                             "artifact_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
                continue
            center = 40000 * (b_multiplier if slot["arm"] == "B" else 1)
            lower, upper = center * .9975, center * 1.0025
            proof = {"slot_id": slot["slot_id"], "service_id": slot["service_id"],
                     "broker_capacity_validated": not (bad_artifact and slot["pair_id"].endswith("P01")
                                                       and slot["arm"] == "A"),
                     "apparatus_control_passed": True, "source_sha256": "test-source",
                     "lower_msgps": lower, "upper_msgps": upper}
            path = directory / (slot["slot_id"] + ".json")
            path.write_text(json.dumps(proof))
            rows.append({"slot_id": slot["slot_id"], "status": "VALID", "attribution": "BROKER_BOUND",
                         "lower_msgps": lower, "upper_msgps": upper,
                         "artifact_path": str(path), "artifact_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        result_path = directory / "results.csv"
        with result_path.open("w", newline="") as output:
            writer = csv.DictWriter(output, RESULT_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        return result_path

    def test_equivalence_and_material_change_require_twelve_verified_pairs(self):
        spec = copy.deepcopy(self.spec)
        for target in spec["targets"]:
            target["service_id"] = "test-" + target["class"]
        plan = design(spec)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            same = analyze(plan, self._results(root, plan))
            self.assertTrue(all(r["decision"] == "EQUIVALENT_ON_REFERENCE_WORKLOAD"
                                for r in same["by_class"].values()))
            larger = analyze(plan, self._results(root, plan, 1.10))
            self.assertTrue(all(r["decision"] == "MATERIAL_INCREASE_ON_REFERENCE_WORKLOAD"
                                for r in larger["by_class"].values()))
            no_drift = root / "results-no-drift.csv"
            drift_ids = {slot["slot_id"] for slot in plan["rows"]
                         if slot["kind"] == "DRIFT_REFERENCE"}
            with (root / "results.csv").open(newline="") as source:
                rows = list(csv.DictReader(source))
            with no_drift.open("w", newline="") as output:
                writer = csv.DictWriter(output, RESULT_FIELDS)
                writer.writeheader()
                writer.writerows(r for r in rows if r["slot_id"] not in drift_ids)
            drift_report = analyze(plan, no_drift)
            self.assertTrue(all(r["decision"] == "DRIFT_GATES_INCOMPLETE"
                                for r in drift_report["by_class"].values()))
            invalid = analyze(plan, self._results(root, plan, 1.10, bad_artifact=True))
            self.assertTrue(all(r["decision"] == "NOT_READY" for r in invalid["by_class"].values()))
            self.assertTrue(all(r["valid_pairs"] == 11 for r in invalid["by_class"].values()))

    def test_unbound_targets_and_template_cannot_claim_effect(self):
        plan = design(self.spec)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prepare(root)
            report = analyze(plan, root / "results-template.csv")
            self.assertTrue(all(r["decision"] == "NOT_READY" for r in report["by_class"].values()))


if __name__ == "__main__":
    unittest.main()
