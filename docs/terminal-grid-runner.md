# Resumable 100-experiment saturation runner

This is a standalone Python/SMF/SEMP program. It does not call Claude, Codex, or any LLM.

## Start or resume

From this repository:

```bash
./run-grid run --directory data/grid-100
```

On this Mac, `caffeinate -i ./run-grid run --directory data/grid-100` also prevents idle sleep while the command runs. Keep the terminal open; if it closes or the host restarts, rerun the same command to recover.

The same command resumes the same campaign. The token is read from ignored `.env` in memory; it is not passed on the command line or written to results. Run on one controller host at a time. On this machine the dependencies are installed; on a clean Python 3.11+ host, create a virtual environment and install the project with `python -m pip install -e .` first. A CA certificate bundle and `openssl rehash` are required by the TLS adapter.

Press **Ctrl-C once** to stop publishers, allow a bounded drain, and checkpoint. Pressing it again forces interruption. A later run retries the interrupted stage and skips completed stages/configurations. After an abrupt crash, recovery waits for old clients to leave and drains only this campaign's named queues with matching campaign-tagged messages. It never purges or deletes queues. If another client, foreign backlog or conflicting routing remains, the runner pauses with saved progress.

Inspect progress from a second terminal:

```bash
./run-grid status --directory data/grid-100
```

Preview the complete workload list without connecting:

```bash
./run-grid plan
```

To stop after a small batch, use `--limit 4`. To run particular workload IDs first, use `--only G001,G002,G013,G067`. All 100 definitions remain in the campaign; a later invocation without these filters resumes the rest. Flags controlling rate/duration are immutable for a campaign; use a new directory to change them. After an intentional code repair, `--accept-code-change` explicitly records the source revision change; it does not silently relabel old measurements.

## The 100 workload configurations

The Cartesian product is:

- Delivery: Direct, persistent Guaranteed.
- Application payload: 64, 256, 1,024, 10,240, 102,400 bytes.
- Fanout: 1, 2, 5, 10, 20 independent destinations.
- Publishers: 1 or 4, sharing the total offered rate equally.

2 × 5 × 5 × 2 = **100 distinct configurations**. Each gets an adaptive search of offered rate. A rate stage is not counted as another experiment. The order is reproducibly shuffled, with Direct and persistent 1KiB 1:1 first.

The exact workload list is also saved in [`campaigns/grid-100.json`](../campaigns/grid-100.json).

V1 implements **SMF over TLS**. Direct destinations are independent client processes; persistent destinations are separate exclusive queues subscribed to one campaign topic. More publisher processes do not increase fanout. AMQP, MQTT, PQ, selectors, mixed traffic and cross-host generator agents are not implemented by this CLI; their earlier planning entries are future work, not claims of supported execution.

## Target and scope

The live runner is pinned to the dedicated existing Enterprise 5K service `4qn20a1u6ny` / `bcm-20261005-5k`, owner `usfos7cfqge`, organization `seall`. It verifies identity through Cloud API responses, retrieves credentials, and uses SEMP directly. It creates no brokers and incurs no additional broker-provisioning charge.

The other existing brokers are excluded from load testing because they are shared/demo systems. Empty queues from another campaign may remain if their subscriptions cannot route current test traffic; they are neither modified nor consumed. Non-empty foreign queues or wildcard matches stop the run. Campaign queues stay empty and inventoried after clean completion; the no-deletion constraint is preserved.

Locks prevent duplicate runners on this controller and simultaneous use of the local load generator. **Locks are host-local, not a distributed scheduler.** Do not launch two controller hosts against this broker. One workload search runs on the broker at a time. Publishers and consumers within that workload run concurrently as separate OS processes.

## Search and detection

Defaults:

| Setting | Value |
|---|---:|
| Warmup | 10 seconds per stage, followed by a bounded drain |
| Exploratory measurement | 30 seconds |
| Boundary confirmation | 120 seconds |
| Maximum exploratory stages | 16 per workload, including retries |
| Safe-side confirmations | 2 independent stages |
| Unsafe-side confirmation | 1 stage when an upper bound exists |
| Search | Start at 100 msg/s, double until failure, then bisect |
| Target exploratory bracket width | 10% of lower bound |
| Configured ingress rate ceiling | 100,000 msg/s |
| Aggregate payload ingress + egress ceiling | 128 MiB/s (protocol/TLS overhead is additional) |
| Spool stop | 70% of campaign queue or VPN allocation |

The byte ceiling is divided by payload size × (1 + fanout). It is a campaign guardrail, not a measured broker limit. A configuration that hits a rate/byte ceiling while still passing is recorded as a censored lower bound. A narrow bracket is not promised when the stage budget is exhausted.

For each stage the detector requires scheduled load tracking, per-destination observed unique receipts, persistent publisher ACKs, broker ingress/egress/discard counter deltas, bounded sample gaps, per-queue spool/unacked gauges and delivery-lag trends, and generator process CPU. Publisher backpressure, loss, NACKs, sustained growth or undrained queues fail sustainability. CPU saturation or inability to generate the offered rate invalidates the broker inference. Missing fields, counter resets and guardrail stops are inconclusive. Invalid/inconclusive results do not update the search bracket. Two consecutive invalid/inconclusive probes end that workload with an unresolved/censored result instead of looping indefinitely.

SEMP counters may update after delivery receipts arrive. The runner uses `dataRxMsgCount`, `dataTxMsgCount`, and their byte counters; total VPN counters include control traffic and are unsuitable for application-message reconciliation. It waits for a quiet data-counter plateau before and after measurement, bounded to 20 seconds each, and saves those snapshots separately. Queue and delivery-lag trends still use only the measurement window. Publisher and receiver counts are aligned by monotonic timestamp; missing aligned samples make the result inconclusive.

Window-local trends and bounded observed drain are both retained: catching up afterward does not erase a growing queue during measurement. End-to-end latency mean/max are recorded from same-host monotonic clocks; percentile latency SLOs are not a v1 classifier input. Low-rate or slowly developing effects may need longer measurement settings. A pass is a statement about its recorded duration, not an indefinite stability guarantee.

Short-horizon search bounds and longer confirmed bounds are saved separately. A longer confirmation failure at a short-pass rate is reported as `HORIZON_DEPENDENT`. Contradictory monotonic search labels stop refinement. Results from a local WAN generator describe the **measured end-to-end path**. They do not prove intrinsic broker processing capacity; every report preserves `broker_capacity_validated: false`. Qualifying independent same-region generators and network capacity is separate work.

## Files and recovery

The output directory contains:

- `manifest.json`: immutable grid, settings, campaign ID, target and source hash.
- `ledger.sqlite`: WAL-mode SQLite transactions for experiment and stage state.
- `broker.json`: non-secret service metadata, SEMP API version and schema hash.
- `raw/Gxxx/NNN-kind/`: stage definition, flushed worker/SEMP JSONL, final evidence and classification.
- `results.json` and `report.md`: refreshed after every completed stage and on exit.

Each stage is marked running before clients start and committed after classification. An interrupted stage is retried; it cannot become a completed capacity observation. Keep the entire directory to resume, including the SQLite files. A campaign does not silently change timing or mix synthetic/live evidence.

A synthetic integration test is available and explicitly marks all its outputs as simulated:

```bash
./run-grid run --simulate --directory /tmp/bcm-grid-simulation
```

It exercises the scheduler and ledger without connecting to any broker; it does not validate load generation or real broker behavior.
