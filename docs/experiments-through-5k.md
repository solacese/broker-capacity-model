# Experiment list: Solace broker capacity through Enterprise 5K

2026-10-05. Scope: **Enterprise 1K standalone → Enterprise 5K standalone**, same AWS US East region and matched 10.26 build where possible. No broker above 5K is part of this list.

**Main target:** `bcm-20261005-5k`, service `4qn20a1u6ny`, creation accepted and deploying at list creation. Readiness must be verified before publishing. **1K references:** existing c-data / b-data only after isolation is proven. c-data currently fails that gate: pre-existing AMQP clients, non-empty queues and an enabled DMR link. Do not reuse its queues or disable its topology. If it cannot be isolated, defer the 1K comparisons; the 5K campaign can proceed independently.

The initial fleet snapshot contains no saturation measurements. All entries below are planned, not completed results.


Token access update: `.env` now holds the four-permission provisioning token, including `services:post`. Discovery and management-credential retrieval returned HTTP 200. Read `campaigns/token-access-status.json`; the old missing-permission blocker is resolved. No additional broker spending is authorized by this update.

## Execution order and size

| Block | Broker | Unique broker/workload configurations | Release condition |
|---|---|---:|---|
| G0: wiring and measurement checks | Dedicated 5K | 3 fixed-rate calibration stages; not capacity searches | Broker ready and schema/isolation checks pass |
| A: core baseline | Dedicated 5K | 24 boundary searches | Reviewed detector and qualified load generators |
| R: small-broker anchors | Isolated 1K | 8 boundary searches | Suitable isolated 1K available |
| I: targeted interactions | Dedicated 5K | Up to 24 additional boundary searches | Choose informative cases from baseline results |
| M: mixed-workload boundary | Dedicated 5K | 12 boundary searches | Measured single-stream intercepts available |

**Maximum: 68 broker/workload boundary searches**, plus calibration, repeated controls, boundary confirmations and soaks. Start with four 5K searches; review before releasing the rest. This is a cap, not a requirement to run every follow-up. A repeat of the same setup is a replicate, not a new workload configuration.

## Fixed experimental contract

Unless overridden in a row: SMF publisher → SMF receiver, TLS enabled, 1,024-byte application payload, one publisher, one receiver per destination, exact topic matching, immediate ACK, no tracing/replay/selector, initially empty queue. Persistent means explicitly persistent, not nonpersistent or Direct. Payload bytes exclude protocol headers; also measure actual wire bytes. Record payload entropy and metadata.

Direct uses one subscription per independent destination. Persistent fanout uses one independently subscribed queue per destination. Multiple consumers on one nonexclusive queue compete for one copy: fanout remains one. Publisher/consumer counts are workload variables, while generator process/host count is independently increased until the generator can deliver the requested load without saturating.

Per configuration, search offered rate geometrically then bisect, retaining [highest confirmed sustainable, lowest confirmed unsustainable]. Full stages start with 60s warmup + 300s measurement; target bracket width <=10% of lower bound. Require three independent boundary confirmations and a 15-minute safe-side soak for each major bottleneck regime. Short calibration runs do not establish capacity. Record requested/attempted/accepted/unique delivered rates, bytes, ACK/NACKs/discards, latency, per-queue backlog/spool/unacked slopes, and generator health. Missing data or generator limitation cannot yield a passing broker-capacity label.

## G0 — calibration, no saturation claims

| ID | Workload | Offered rate | Duration | Pass evidence |
|---|---|---:|---|---|
| G01 | Direct 1KiB 1:1 | 100 msg/s | 15s warmup + 60s measurement | Scheduled, accepted evidence and unique receipts reconcile; no unexplained loss |
| G02 | Direct 1KiB 1:1 | 1,000 msg/s | Same | Same, plus stable generator scheduling |
| G03 | Persistent 1KiB, new exclusive test queue | 100 msg/s | Same | Publish ACKs, unique receipts, empty test queue afterward; no sustained drift |

Before G01: verify fresh owner, schema/units, private campaign namespace, no matching unrelated subscriptions, no enabled external routing, and an exclusive experiment lease. Consumers attach first. Only campaign-generated messages may be consumed.

## A — 24 baseline boundary searches on 5K

| ID | Delivery / topology | Payload bytes | Fanout | Publishers | Consumers | Question / comparison |
|---|---|---:|---:|---:|---|---|
| B01 | Direct | 64 | 1 | 1 | 1 | Small-message ceiling |
| B02 | Direct | 1,024 | 1 | 1 | 1 | Direct reference |
| B03 | Direct | 10,240 | 1 | 1 | 1 | Message/byte crossover |
| B04 | Direct | 102,400 | 1 | 1 | 1 | Large-message byte ceiling |
| B05 | Persistent / exclusive | 64 | 1 | 1 | 1 | Persistence cost against B01 |
| B06 | Persistent / exclusive | 1,024 | 1 | 1 | 1 | Persistent reference against B02 |
| B07 | Persistent / exclusive | 10,240 | 1 | 1 | 1 | Persistence × size against B03 |
| B08 | Persistent / exclusive | 102,400 | 1 | 1 | 1 | Persistence/byte ceiling against B04 |
| B09 | Direct | 1,024 | 5 | 1 | 5 independent destinations | Low fanout scaling |
| B10 | Direct | 1,024 | 20 | 1 | 20 independent destinations | Delivery-rate ceiling |
| B11 | Direct | 1,024 | 100 | 1 | 100 independent destinations | High fanout nonlinearities |
| B12 | Persistent / 5 exclusive queues | 1,024 | 5 | 1 | 1 per queue | Queue-copy cost |
| B13 | Persistent / 20 exclusive queues | 1,024 | 20 | 1 | 1 per queue | Persistent delivery ceiling |
| B14 | Persistent / 100 exclusive queues | 1,024 | 100 | 1 | 1 per queue | High persistent fanout |
| B15 | Direct | 10,240 | 20 | 1 | 20 independent destinations | Size × fanout against B10 |
| B16 | Persistent / 20 exclusive queues | 10,240 | 20 | 1 | 1 per queue | Size × persistence × fanout against B13 |
| B17 | Persistent / exclusive | 1,024 | 1 | 10 | 1 | Publisher concurrency against B06 |
| B18 | Persistent / exclusive | 1,024 | 1 | 50 | 1 | More ingress concurrency against B17 |
| B19 | Persistent / nonexclusive | 1,024 | 1 | 1 | 10 competing consumers | Consumer concurrency; shared-queue mode is a confound until matched I07 |
| B20 | Persistent / nonexclusive | 1,024 | 1 | 1 | 50 competing consumers | More egress concurrency against B19 |
| B21 | Persistent / 12-partition PQ | 1,024 | 1 | 1 | 12 | Uniform distribution, 10,000 keys |
| B22 | Persistent / 12-partition PQ | 1,024 | 1 | 1 | 12 | One key gets 90%; other 10% uniform over remaining keys |
| B23 | Persistent / exclusive, AMQP→SMF | 1,024 | 1 | 1 | 1 | Ingress protocol effect against B06 |
| B24 | Persistent / exclusive, SMF→AMQP | 1,024 | 1 | 1 | 1 | Egress protocol effect against B06 |

First release: **B02, B06, B04, B08**. These establish Direct/persistent and message/byte regimes. Randomize subsequent runs within setup blocks using seed 20261005; repeat B02/B06 every fourth search and at the end of a block to detect drift. Doubling generator resources near a suspected ceiling is a required validity check, not a new broker workload.

## R — eight matching 1K anchors

Run the same exact definitions for **B01, B04, B05, B08, B10, B13, B21 and B23** on an isolated Enterprise 1K. Prefix results with broker instance, e.g. `1k-c/B01`, rather than inventing new workload IDs.

These compare message/byte, persistence, fanout, partition and cross-protocol regimes. Hold provider/region/build/TLS/generator placement fixed; record remaining differences. Fit on 1K and freeze predictions before 5K if the 1K data becomes available first. Otherwise freeze a 5K-trained model and evaluate 1K as a reverse-transfer test before fitting on both. Do not call a model validated across sizes until these out-of-sample predictions have actually been scored.

## I — up to 24 targeted 5K follow-ups

This list supersedes the broad optional follow-up grouping in the original campaign for the through-5K scope. It adds necessary matched controls and prioritizes boundary geometry over buying more service classes. Choose batches of four based on baseline uncertainty/effect size. A skipped test must retain its reason.

| ID | Exact variation | Matched reference | Question |
|---|---|---|---|
| I01 | Persistent PQ, 1 partition, 1 consumer, uniform 10,000 keys | I02 | Partition overhead with consumer count fixed |
| I02 | Persistent PQ, 12 partitions, 1 consumer, uniform keys | I01 / B21 | Partitions vs consumer parallelism |
| I03 | Persistent PQ, 12 partitions, 3 consumers, uniform keys | I02 / B21 | Consumer scaling on fixed partition count |
| I04 | Persistent PQ, 48 partitions, 12 consumers, uniform keys | B21 | Extra partitions at fixed consumers |
| I05 | B21 with 50% on one key | B21 / B22 | Locate onset of skew bottleneck |
| I06 | B21 with 99% on one key | B22 | Near-single-partition limit |
| I07 | Persistent nonexclusive queue, 1 consumer | B06 / B19 / B20 | Remove exclusive-vs-shared confound |
| I08 | Persistent 12 separate nonexclusive queues, 1 consumer each; route each message to exactly one queue uniformly | B21 | PQ vs ordinary sharding at same destination work |
| I09 | Direct 1KiB: 1,000 installed exact subscriptions, exactly one match per message | B02 | Routing-table size at fixed fanout |
| I10 | Same with 50,000 exact subscriptions | I09 | Large routing-table effect |
| I11 | Direct 1KiB: 1,000 shallow wildcard subscriptions, exactly one match per message | I09 | Wildcard cost with equal delivery work |
| I12 | Same with six-level deep wildcard patterns | I11 | Matching depth; persist exact generated patterns |
| I13 | Persistent 1KiB: each message ACK delayed 1ms by a concurrent timer, unlimited timer concurrency within recorded inflight cap | B06 | Moderate ACK residence time |
| I14 | Same ACK delay 10ms | I13 | ACK residence × unacked working set |
| I15 | Persistent 1KiB, immediate ACK, maximum outstanding client window 32 | B06 / I16 | Small window; record actual negotiated value |
| I16 | Same window 255 if supported, otherwise explicit unsupported | I15 | Window-limited throughput, distinguish client/config boundary |
| I17 | MQTT QoS 0 publisher → SMF Direct receiver, 1KiB 1:1 | B02 | MQTT ingress, best-effort semantics |
| I18 | MQTT QoS 1 publisher → SMF persistent queue receiver, 1KiB 1:1 | B06 | QoS/durability ingress; verify actual semantics and deduplicate |
| I19 | SMF publisher → MQTT QoS 0 subscriber, 1KiB 1:1 | B02 | MQTT egress; verify publication/delivery mode |
| I20 | SMF persistent publisher → MQTT QoS 1 subscriber, 1KiB 1:1 | B06 | MQTT ACK/durable-session behavior; verify actual semantics |
| I21 | B06 with tracing enabled at a fixed supported recorded sample rate | B06 | Tracing overhead; same external collector availability |
| I22 | Persistent: bounded seeded queue backlog = 10% of test queue limit, fixed selector matching 10% of seeded messages | I23 | Selector behavior under shallow depth; finite-horizon workload |
| I23 | Same selector/workload, seeded backlog = 50% | I22 | Depth × selector; matched generated payloads and measured drift |
| I24 | B06 offered rate in 2× bursts for 1s followed by 1s idle; search mean rate | B06 | Burst sensitivity; compare transient envelope separately from steady state |

I13–I16 are application/window-conditioned limits, not intrinsic broker processing labels. I22–I23 form a paired deep-queue diagnostic: unmatched backlog must be accounted for, and any growing retained population makes the workload unsustainable at that horizon. Never label a short prefilled run as an infinite sustainable throughput result. If a required window/selector/tracing/MQTT mode is unsupported, record it and spend that slot on the highest-uncertainty supported candidate. TLS-off experiments are deferred because current connection placement is public; they are not silently run over a public link.

For I09–I12, hold connections and delivery matches fixed; predeclare exact subscription grammar and topic generator and confirm cardinality from SEMP. For I08, every message goes to one shard only, so fanout remains one. These controls are necessary for a causal comparison.

## M — 12 mixed-workload rays on 5K

Each pair is run at **25:75, 50:50 and 75:25**, normalized by the component's measured standalone sustainable capacity. If those capacities are Ca and Cb, requested component rates are `lambda × w × Ca` and `lambda × (1-w) × Cb`. Search lambda; do not use raw msg/s weights that let a large-message component dominate trivially.

| IDs | Concurrent pair | What the surface tells us |
|---|---|---|
| M01–M03 | B01 Direct 64B + B04 Direct 100KiB | Shared message-processing versus byte-rate resources |
| M04–M06 | B02 Direct 1KiB + B06 persistent 1KiB | Whether delivery modes share a processing budget |
| M07–M09 | B06 persistent 1:1 + B13 persistent fanout 20 | Ingress versus fanout/persistence competition |
| M10–M12 | B10 Direct fanout 20 + B13 persistent fanout 20 | Common egress ceiling across delivery modes |

Recheck the two single-stream intercepts in each block. Track each stream independently: aggregate balance cannot hide starvation. Fit and compare a shared linear budget, independent axis limits, and curved/interacting boundaries. Do not assume convexity. At selected knees, repeat upward and downward approaches to detect state dependence; count these as repetitions, not new workload definitions.

## What every completed search must deliver

1. Raw sanitized client and SEMP time series, exact workload/config/build/spec hashes and generator placement.
2. Sustainable/unsustainable interval or an explicitly censored bound; no guessed precise maximum.
3. Limiting mechanism with evidence: hard limit, broker processing, bytes, fanout, persistence, routing, hot key, application or generator/network. Unknown is valid.
4. Three-repeat variation and safe-side soak result where required; loss/backlog/latency plots.
5. SEMP-only features separated from lab-only workload intent (e.g. key skew/ACK timing may be unobservable in production).
6. Next experiment selected because it resolves an uncertainty, tests an interaction or covers an unseen regime.

A baseline search usually costs 6–10 load stages, approximately 36–60 minutes before reset/drain/repeats. Thus 24 initial configurations are about 14–24 active stage-hours, not 24 short load tests. Run serially per broker. Provisioning and generator availability determine calendar time; this is not a completion-time promise.

## E — 16 additional candidates after the core campaign

Added for the autonomous Claude handoff. These are optional information-driven experiments on the same 5K; they do not require another broker. Combined ceiling becomes **84 experiment entries** (68 core searches + 16 extra candidates), before replicates. Do not execute all 84 blindly. Complete/review the first four baseline curves, then prioritize experiments that change the inferred boundary or reduce held-out uncertainty.

| ID | Exact workload variation | Comparison / hypothesis |
|---|---|---|
| E01 | Persistent 1KiB steady 1:1, publish to 12 ordinary queues with a 90% hot shard and 1 consumer per queue | I08 and B22: hot ordinary shard vs hot partition |
| E02 | Direct payload mixture: 50% 64B + 50% 1,984B, one stream, 1KiB mean | B02: does equal mean payload hide a variance effect? |
| E03 | Direct payload mixture: 99% 64B + 1% 96,064B, one stream, 1KiB mean | E02/B02: rare large messages and head-of-line effects |
| E04 | Persistent version of E02, exclusive queue | B06: size variance with persistence |
| E05 | Persistent version of E03, exclusive queue | E04/B06: rare large persistent messages |
| E06 | Direct B02, 2× instantaneous rate for 1s / 1s idle | B02: burst tolerance without queue buffering |
| E07 | Direct B02, 10× instantaneous rate for 100ms / 900ms idle | E06: same mean, different peak/duty cycle |
| E08 | Persistent B06, 10× instantaneous rate for 100ms / 900ms idle | I24: burst scale with persistence |
| E09 | Persistent B06 plus 100 connected idle clients | B06: idle connections vs active traffic |
| E10 | Persistent B06 plus 1,000 connected idle clients | E09: connection-state overhead; all are campaign-owned |
| E11 | Persistent B06 plus 10 campaign client connect/disconnect cycles per second, keeping 100 extra concurrently connected | E09: churn cost at matched connection occupancy |
| E12 | Direct B02 plus 100 subscription add/remove cycles per second on one campaign client, fixed inventory about 1,000 | I09: routing-update overhead; only remove campaign-created subscriptions |
| E13 | B21 at 70% of its safe rate, restart one of 12 campaign consumers once per 60s for 10min | PQ rebalance/recovery, not a steady capacity label |
| E14 | B21 at 70% of safe rate, stop six campaign consumers for 30s, restore, repeat three times | Recovery time, partition redistribution and bounded backlog |
| E15 | B06 safe-side boundary replay for 60min, stable composition | Longer-horizon drift; a confirmation/soak rather than a new capacity target |
| E16 | B13 safe-side boundary replay for 60min, stable composition | Long fanout/persistence stability; same distinction |

E13–E16 are dynamic/reliability or long-horizon observations and must live in their own task/label family; do not mix them into steady-boundary regression as independent maxima. The 84-entry ceiling is a backlog count; at most 80 entries are new steady/transient search configurations because E13–E16 are diagnostic/soak entries. Count workload IDs, rate stages and repeated runs separately in reports.

The inherited no-deletion rule still applies. E12 is **deferred** until removal of these explicitly disposable campaign-created subscriptions is authorized; do not infer permission from this design. Disconnecting campaign clients and stopping campaign consumers after tests is normal workload teardown; never disconnect existing clients. Do not alter broker-wide settings to make a test easier.

Autonomous execution scope is documented in [claude-autonomous-campaign.md](claude-autonomous-campaign.md). The token is referenced from `.env`, never copied into this document, prompts or logs.
