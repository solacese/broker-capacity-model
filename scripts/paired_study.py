#!/usr/bin/env python3
"""Prepare, validate, or analyze the offline 1K/5K/10K paired study."""
import argparse
import json
from pathlib import Path

from broker_capacity.paired_study import analyze, design, load_spec, prepare, validate


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["prepare", "validate", "analyze"])
    parser.add_argument("--directory", type=Path, default=Path("data/paired-multisize-v1"))
    parser.add_argument("--spec", type=Path, default=Path("campaigns/paired-multisize-v1.json"))
    parser.add_argument("--results", type=Path)
    args = parser.parse_args()
    spec = load_spec(args.spec)
    if args.command == "prepare":
        plan = prepare(args.directory, args.spec)
        print(json.dumps({"directory": str(args.directory), "counts": plan["counts"],
                          "execution_authorized": False}, indent=2))
    elif args.command == "validate":
        path = args.directory / "plan.json"
        plan = json.loads(path.read_text()) if path.exists() else design(spec)
        validate(plan, spec)
        print(json.dumps({"valid": True, "counts": plan["counts"],
                          "execution_authorized": False}, indent=2))
    else:
        plan = json.loads((args.directory / "plan.json").read_text())
        validate(plan, spec)
        results = args.results or args.directory / "results-template.csv"
        print(json.dumps(analyze(plan, results,
                                 margin_fraction=spec["first_factor"]["practical_equivalence_margin_fraction"]),
                         indent=2))


if __name__ == "__main__":
    main()
