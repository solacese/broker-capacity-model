from __future__ import annotations

import argparse
import json
from pathlib import Path

from .collector import collect_all


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect read-only Solace capacity data")
    parser.add_argument("access_file", type=Path, help="Temporary JSON with read-only broker access")
    parser.add_argument("--owner-id", required=True, help="Required Solace service owner ID")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    try:
        access = json.loads(args.access_file.read_text())
        snapshot = collect_all(access, args.owner_id)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    finally:
        args.access_file.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
