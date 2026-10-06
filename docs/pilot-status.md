# Pilot status

Updated: 2026-10-05

## Scope and safety

- Checkpoint target is restricted to service `avfk5i15jtm` (`swlb-20260928-121042-c-data-1k`) owned by `usfos7cfqge` in organization `seall`.
- No broker lifecycle changes, deletion, existing-queue consumption, or broader saturation work are authorized.
- Discovery remains GET-only. Any future configuration writer must be separate, target the exact service/VPN, and restrict created resource names to `bcm-20261005-`.
- Direct calibration is permitted only after owner, inventory, idle traffic, wildcard/bridge/DMR isolation, endpoint mappings, and telemetry are verified. Persistent calibration additionally requires a new exclusive prefixed queue and proof it is empty before and after.

## Progress

- Read the project brief and initial findings.
- Inventoried the current Python collector, deterministic capacity engine, CLI, tests, and sanitized initial snapshot.
- Confirmed the initial snapshot is discovery-only: target service `avfk5i15jtm` was idle at collection time, with four existing queues and no measured performance boundary.
- Added reusable experiment records, measurement-window telemetry, sustainability classification, adaptive bracket search, and a bounded pilot CLI.
- Corrected deterministic spool accounting to include topic endpoints and to preserve unknown counters instead of coercing them to zero.
- Verified target ownership and fetched the exact returned SEMP URLs. Config and monitor OpenAPI version `10.26.0.8894` expose 177 and 234 paths; all selected inventory endpoints returned HTTP 200.
- Saved sanitized discovery, isolation, activity, raw idle-window, and schema-provenance samples under `data/pilot/`.
- Closed the reviewer blockers with adversarial tests: adjacent-sample counter resets, strict measurement-window coverage, signed per-queue gauges, offered-load and per-destination conservation, mode-specific accepted evidence/ACK handling, non-monotonic search rejection, finite bounds, and a stage budget.

## Commands

Commands shown here contain no credentials.

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m broker_capacity.pilot_cli plan --output /tmp/bcm-pilot-plan.json
PYTHONPATH=src python3 -m broker_capacity.pilot_cli bracket 100=SUSTAINABLE 200=UNSUSTAINABLE 150=INCONCLUSIVE
```

Read-only discovery used the Cloud service detail endpoint to obtain the exact SEMP URLs, then issued GETs to those returned URLs and schema-proven paths. Raw responses were sanitized before being saved under `data/pilot/<run-id>/`.

## Current blockers

- Live isolation failed the calibration gate. Four pre-existing AMQP clients are connected, including publisher/subscriber identities, and the broker has one enabled internal DMR link.
- The first 11-second idle window had zero instantaneous rates but cumulative VPN transmit counters increased by 2 messages / 16 bytes. A second per-client window was flat, so the earlier activity remains unexplained.
- All four queues report nonzero historical `spooledMsgCount` counters (`185`, `196`, `4,585,322`, and `218,196`), but current VPN/queue spool gauges are zero. The schema describes `spooledMsgCount` as cumulative, not queue depth; current per-queue backlog therefore remains unknown without a separately verified source. No existing queue was consumed or altered.
- Queue subscriptions contain workload-specific `>` wildcards. System client subscriptions also contain `>` wildcards; although the candidate pilot prefix does not match the workload-specific filters, the active clients/DMR and unexplained counter movement make the shared broker unsuitable for this checkpoint.
- No `sdkperf` binary is installed. The Solace Python client and `psutil` are installed, but a Python generator ceiling would only be a client limit, not broker capacity.

No live traffic was sent and no broker configuration was changed.

## Next checkpoint

Reviewer should inspect the telemetry/classifier contract and the isolation evidence. The next executable command is:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Before any calibration is enabled, obtain an isolated target (or a documented maintenance window with DMR and unrelated clients absent), then rerun read-only isolation discovery. Do not run the broader saturation campaign.
