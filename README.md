# Broker capacity model

Solace PubSub+ inventory, workload experiments, and saturation-boundary modeling.

## Run the autonomous 100-experiment grid

```bash
./run-grid run --directory data/grid-100
```

Run the same command to resume. Ctrl-C stops and checkpoints; `./run-grid status --directory data/grid-100` shows saved progress. [Runner documentation](docs/terminal-grid-runner.md) covers the exact grid, detector, output files, timing controls, and current scope. This is a standalone program with no LLM dependency. It targets the dedicated 5K and preserves shared demo brokers.
## Current status

The initial discovery pass covers five services in the **Solace SEs** workspace whose `ownedBy` value exactly matches Raphael Caillon's verified user ID. See [docs/initial-findings.md](docs/initial-findings.md) and [data/initial_snapshot.json](data/initial_snapshot.json).

The collector:

- accepts only explicitly exported read-only SEMP credentials;
- rejects every broker whose owner does not match `--owner-id`;
- performs only HTTP `GET` requests;
- follows SEMP cursor pagination;
- removes the temporary credential file even if collection fails;
- writes only sanitized telemetry and metadata.

## Run the collector

```bash
PYTHONPATH=src python -m broker_capacity.cli /tmp/solace-semp-readonly.json \
  --owner-id usfos7cfqge \
  --output data/snapshot.json
```

The temporary access file is intentionally not committed and is deleted after each run.

## Tests

```bash
PYTHONPATH=src python -m unittest discover -s tests
```

## Saturation experiment campaign

The initial snapshot is discovery evidence, not a measured performance boundary. See [the reviewed campaign](docs/saturation-campaign.md) for the 24 baseline configurations, targeted follow-ups, mixed-workload rays, broker ladder, measurement protocol and model-validation criteria. The machine-readable design is [campaigns/saturation-v1.json](campaigns/saturation-v1.json).

The earlier Claude pilot has been stopped. Its [pilot brief](docs/claude-pilot-brief.md), [pilot status](docs/pilot-status.md), and [review findings](docs/pilot-review-checkpoint.md) are historical context. The [experiment list through 5K](docs/experiments-through-5k.md) defines the broader research plan; the terminal runner above implements the current 100-workload SMF grid on dedicated 5K service `4qn20a1u6ny`.

The earlier token-only [Claude handoff](docs/claude-autonomous-campaign.md) is retained for reference. The terminal grid does not require that supervisor or an LLM. Local process/log state is under ignored `.pilot/`; no credentials are copied into reports.

The token in ignored `.env` now includes `services:post` alongside self-service discovery/management access. See [verified token access](campaigns/token-access-status.json); token values are never stored in reports.
