# Solace saturation-boundary campaign

Decision record: 2026-10-05. Goal: measure the boundary of sustainable operation and predict it for an unseen broker/workload using SEMP-accessible features. User approved provisioning **one Enterprise 5K test broker first**. No larger provisioning is authorized by this plan.

## Review of the initial work

Evidence reviewed: `prompt.md`, all five source modules, `tests/test_capacity.py`, `data/initial_snapshot.json`, `docs/initial-findings.md`, and the local Claude session through its 14:20 UTC interruption.

- Five owned services were inventoried; all were idle. There are **zero measured saturation curves**, no sustained benchmark dataset, and no trained capacity model in this initial work.
- Useful findings: endpoint-level limits can dominate VPN-level ratios; spool units differ (bytes vs decimal MB); historical discard counts require deltas; schema availability varies by version. The ~90.9% queue-spool finding is a snapshot of a configured limit, not evidence of processing saturation.
- Collector implementation is GET-only and rejects a mismatched supplied owner. Two unit tests passed on review. Ownership metadata must additionally be verified from the current Cloud response, not merely trusted from an access-file assertion.
- Coverage gaps: total spool sums queues but omits topic endpoints; several missing values become zero; subscriptions/flows lack current gauges; no raw time series, complete schema registry, workload generator, sustainability detector, interval search, generator validation, or model validation exists yet. Treat the published deterministic percentage as a partial lower bound, not a complete broker utilization score.
- Scope matters: the existing brokers serve demo/workload-balancer topologies. Ownership and idle traffic do not prove isolation. Wildcard subscriptions and bridges can route a new topic into existing queues. Do not consume, drain, purge, reconfigure or delete their existing resources.

## What boundary are we finding?

For frozen workload composition w and broker/configuration b, scale all component input rates by lambda. Record the sustainable interval along that ray: [L, U], where L is the highest independently confirmed sustainable rate and U the lowest confirmed unsustainable rate. Keep ingress messages/s, ingress bytes/s, egress unique deliveries/s, egress bytes/s, persistent ingress, fanout and routing work alongside the scalar rate. One throughput number cannot describe the whole boundary.

Separate three labels: (1) hard configured resource limit, (2) broker performance limit, (3) application/client/generator/network limit. If client or network limitation prevents observing broker saturation, the broker result is censored, not a measured capacity. Slow consumers define an application-conditioned sustainable region; they must not train an intrinsic broker-throughput ceiling without that distinction.

Sustainability is horizon- and SLO-dependent. Report loss-free/stable-backlog throughput separately from latency-constrained capacity. Do not invent a universal latency SLO: establish a low-load baseline, record p50/p95/p99, and retain a relative-latency knee as a diagnostic until an application SLO is supplied. Use same-clock RTT or demonstrably synchronized clocks; do not subtract timestamps from unrelated unsynchronized hosts.

## Broker ladder and deployment controls

| Role | Broker | Purpose |
|---|---|---|
| Calibration/reference | Existing c-data Enterprise 1K, `avfk5i15jtm` | Only if isolation is demonstrated; otherwise no live load |
| Independent instance | Existing b-data Enterprise 1K, `t95ezd27d6e` | Replicate selected anchors after the same isolation checks |
| Dedicated first test target | `bcm-20261005-5k`, Enterprise 5K standalone | User-approved creation; run main campaign here |
| Later family holdout | Enterprise 50K standalone | Proposed only; test unseen processing family |
| Later topology holdout | Enterprise 5K HA | Proposed only; isolate HA effect against 5K standalone |

Match provider, region, exact broker build, TLS mode, generator placement and spool settings where possible. Existing SWLB services are in `eks-us-east-1a`, environment `ccucmdicy0i`; request the new 5K there, initially 10.26 and standalone. Record the full resolved patch version. If the same patch cannot be selected, version is a confound, not a size effect. Do not reuse production/demo queues. Generate payloads with recorded entropy; constant compressible payloads may hide byte-rate costs.

Solace's current service-class table maps 250/1K to underlying 1K, 5K/10K to underlying 10K, and 50K/100K to underlying 100K. These are useful sampling strata, not assertions of linear CPU or throughput scaling. Prefer 5K then 50K over buying every connection tier. Service limits and shaping can differ even within a family. Source: https://docs.solace.com/Cloud/service-class-limits.htm (checked 2026-10-05).

The direct creation attempt for the approved 5K returned HTTP 403: the present token lacks `services:post`. Exact request and sanitized response are in `campaigns/provision-5k*.json`. The subsequent authorized console creation succeeded: `bcm-20261005-5k`, ID `4qn20a1u6ny`, is provisioning. Readiness must still be verified before load. Creation API: https://api.solace.dev/cloud/reference/createservice.

## Stage 0 — instrument before searching

Entry gate for the campaign:

1. Current service owner/organization verified, dedicated broker or proven routing isolation, no concurrent load experiment. Use a per-broker lease and persist a resource manifest. One active load stage per broker, generator pool exclusive while measuring.
2. Cache config/monitor OpenAPI documents and hashes; derive valid endpoints and units. Record broker/config snapshots and missingness. Baseline/drift probes before and after each block. Minimize collection overhead; compare polling on/off at a control point.
3. Collect scheduled/attempted/accepted publishes, unique receipts per expected destination, publisher ACK/NACKs, errors, counter-reset epochs, per-queue depth/spool/unacked, and generator CPU/network/GC or scheduler lag. Raw counters with monotonic interval timestamps and UTC run metadata, at roughly 1-second client and 2–5-second SEMP cadence.
4. Prove fanout: N subscribers or N independently subscribed queues gives N deliveries per accepted message. N competing consumers on one queue gives one delivery, not N. Verify persistence on the wire/API and exact SDKPerf flags (explicit `-mt=persistent`; default is direct).
5. Run 100 and 1,000 msg/s Direct 1KiB calibrations; then a 100 msg/s persistent calibration if a new isolated queue is safe. These short runs verify wiring only. SDKPerf must be validated against its installed help and actual flags. Python generators are acceptable for calibration, but not proof of high-rate broker capacity.
6. Qualify generators: process/core/network headroom, offered-rate tracking, publish/receive reconciliation, and near-boundary repeat with doubled generator resources on independent hosts. If the measured ceiling rises, classify the earlier result as generator-limited. Remote clients over the laptop WAN cannot establish broker/NIC capacity. Same-region external load hosts are a required main-campaign dependency.

SDKPerf documentation: https://docs.solace.com/API/SDKPerf/Command-Line-Options.htm and https://docs.solace.com/API/SDKPerf/SDKPerf.htm. Generator-host provisioning is separate from the one approved broker and requires a concrete infrastructure choice/cost scope if no suitable host already exists.

## Stage 1 — 24 boundary configurations on the dedicated 5K

Defaults: 1KiB application payload, SMF→SMF, TLS on, immediate ACK, one publisher, one receiver per destination, empty initial queue, no tracing/replication/DMR/replay, exact routing, unchanged configuration during rate search. Direct has no durable queue; Guaranteed means persistent to an exclusive regular queue unless specified. Fanout counts destinations. Use seeded shuffled order within each setup block and rerun a reference every fourth search. Message-size numbers below are exact bytes, not ambiguous MB/KB.

| IDs | Configurations | Count | Scientific question |
|---|---|---:|---|
| B01–B04 | Direct 1:1; 64 / 1,024 / 10,240 / 102,400 bytes | 4 | Message-rate to byte-rate crossover |
| B05–B08 | Persistent regular queue 1:1; same four sizes | 4 | Cost of durable ingress/egress versus payload size |
| B09–B11 | Direct 1KiB; fanout 5 / 20 / 100 | 3 | Is egress delivery rate the binding axis? |
| B12–B14 | Persistent 1KiB; 5 / 20 / 100 separate subscribed queues | 3 | Does fanout add persistence/egress cost? |
| B15–B16 | Direct 1KiB fanout 20 and persistent fanout 20, now 10KiB | 2 | Size × fanout interaction, compared with B10/B13 |
| B17–B18 | Persistent 1KiB, one destination, 10 / 50 publishers | 2 | Ingress concurrency versus single-client limitation |
| B19–B20 | Persistent 1KiB, one nonexclusive queue, 10 / 50 competing consumers | 2 | Egress concurrency, holding true fanout at one |
| B21–B22 | Persistent PQ, 12 partitions, 12 consumers; uniform keys / 90% one hot key | 2 | Parallel partitioning and hot-key constraint |
| B23–B24 | AMQP→SMF / SMF→AMQP, persistent 1KiB regular queue 1:1 | 2 | Direction-specific protocol cost |

This is a structured initial screen plus controls, not a statistically orthogonal full factorial. Record confounds: B21 versus B05/B06 changes topology and consumer count. Make causal partition-count claims only after Stage 2 matched tests. Skip unsupported configurations with a reason; do not silently substitute another protocol, persistence mode, queue type or partition count.

## Stage 2 — up to 24 informative follow-ups

**Scope update:** the concrete [through-5K list](experiments-through-5k.md) supersedes the I01–I24 grouping below for current execution. The grouping below remains a broader future candidate menu; do not mix its IDs with the scoped manifest.

Run only after the first screen; select on effect/noise, model disagreement, coverage and bottleneck transitions. The catalog stores candidates, not an order to execute all combinations.

| IDs | Proposed family | Maximum new configurations |
|---|---|---:|
| I01–I04 | PQ: 1 vs 12 partitions at 1 consumer, then 12 partitions/3 consumers and 48 partitions/12 consumers; uniform keys | 4 |
| I05–I08 | Exact subscription count 1,000/50,000; wildcard shallow/deep at 1,000 with equal match fanout | 4 |
| I09–I12 | TLS off versus on at 64B/100KiB in Direct/persistent modes, only on a private approved test network | 4 |
| I13–I16 | Persistent consumer ACK delay 1/10ms; outstanding window low/high at constant other settings | 4 |
| I17–I20 | MQTT QoS 0/1 to SMF and SMF to MQTT QoS 0/1, clearly record resulting delivery semantics | 4 |
| I21–I24 | Persistent controlled initial queue occupancy 10%/50%, tracing on, selector selectivity probe | 4 |

Store exact workload profiles before dispatch: per-consumer ACK/batch/window settings, key cardinality (default 10,000), hot-key fraction, wildcard grammar, subscription matches, protocol version, client library/version, TLS/cipher, payload metadata/entropy and generator count. Queue occupancy is finite state: record horizon and drift; prefilled experiments must consume only campaign-generated messages. Background backlog must not be quietly drained to create a passing stage. Tracing/selector tests require a matched off/control and feature support. Never apply TLS-off tests over public links.

## Stage 3 — find the actual surface with mixed workloads

An isolated maximum for each workload does not identify the joint feasible region. On the 5K, run 12 mixture rays: three interior mixtures (25:75, 50:50, 75:25 of each stream's independently measured capacity) for four pairs:

1. Direct 64B + Direct 100KiB: message-rate vs byte-rate resource sharing.
2. Direct 1KiB + persistent 1KiB: shared vs separate processing constraints.
3. Persistent 1:1 + persistent fanout 20: ingress vs delivery/spool work.
4. Direct fanout 20 + persistent fanout 20: common egress ceiling.

Scale both component rates by one lambda; record requested and realized composition. Recheck both axis intercepts in the same block. Compare linear shared-budget, rectangular independent-budget and curved/interacting boundaries; do not assume convexity. Test ascending and descending approaches at six selected knees after resetting campaign state to expose hysteresis. Bursty tests (steady, 2× for 1s/1s idle, 10× for 100ms/900ms idle at matched means) are a later transient-envelope family, not interchangeable with steady capacity labels.

## Stage 4 — cross-broker generalization

Use eight anchors B01, B04, B05, B08, B10, B13, B21, B23. On each additional qualified broker, rerun these eight, then up to four uncertainty-selected cases. Existing 1K b/c provide same-class instance variability if safely isolated. Proposed 50K supplies an unseen processing family; proposed 5K HA isolates topology. Two sizes support early transfer measurements, not a universal capacity claim.

Freeze a model before the first 5K measurements if the 1K baseline exists; score those predictions before adding 5K data. Later leave one full size/family out, and hold out entire instances and versions. A patch-version comparison must keep size/topology fixed. Include a same-class independent instance to avoid equating class with one physical allocation.

## Search, labels and repetition

- Pilot: 15s warmup + 60s measurement, calibration only. Full stage: initial 60s warmup + 300s measured steady state; extend when diagnostics have not stabilized. No burst catch-up in measurement.
- Start known-safe, double until a valid failure, then bisect. Target `(U-L)/L <= 0.10` (~±5% around midpoint). Record interval, never just a guessed midpoint. Usually budget 6–10 stages, permit up to 12; if budget exhausted preserve the wider interval.
- Invalid/inconclusive does not move either bound. If all passes, result is right-censored `capacity >= L`; if the first point fails with no pass, result is left-censored. Contradictory labels trigger repetitions and nonstationarity review, not forced bisection.
- Three independently restarted confirmations at the safe/unsafe edge, separate time blocks; a 15-minute safe-side soak per canonical regime. Same-run windows are not independent repetitions. Report variation and bracket uncertainty separately. If variation >10%, diagnose or increase repetitions before refining to 5%.
- Pre-register initial acceptance: generator attempted load within 2% of schedule; accepted >=99% of attempted unless loss policy explicitly differs; delivery conservation at every destination; no new unexplained discards/NACKs; no persistent backlog/spool/unacked drift across at least three consecutive 60s windows. Use confidence intervals and an absolute drift floor from idle measurement. A flat total with one growing queue is not stable. Low-count stages use counts/intervals rather than unstable percentages.
- Positive unexplained drift invalidates sustainability even when total throughput looks correct. Delayed ACKs can have a stable nonzero unacked plateau. Queue drain after a stage is diagnostic and does not erase growth during measurement.
- Stop publishing if any campaign queue reaches 70% of its configured spool limit, the VPN reaches 70%, unrelated traffic appears, a health alarm fires, or a generator fails. If the guardrail prevents reaching performance saturation, label the result censored/guardrail-limited. The near-full autoscale queue is out of scope. Leave test resources inventoried and empty, no deletion under the inherited constraint.

## Data and model contract

Keep immutable manifest + sanitized raw JSONL/Parquet samples + per-stage labels + boundary summary. Include run ID, seed, code revision and dirty-tree hash, exact redacted invocation, config/spec hashes, service/instance/build, resource manifest, UTC and monotonic time, scheduler/attempted/accepted/received counts, generator host placement, broker counters, reasons, limits, censoring, confidence and model version. Never store credentials in datasets/logs. A local DuckDB index can be added without replacing raw evidence.

Maintain two feature sets: (1) production SEMP-only features with explicit missingness; (2) lab enrichment. Exact key skew, publisher intent, payload entropy and ACK timing may be invisible to SEMP. Record them for experimental explanation, but do not silently make them required production inputs. In particular, overloaded observed egress/ingress can understate intended fanout; freeze configured composition for prediction and mark unidentifiable production workloads low-confidence.

Compare simple message/byte/delivery bottleneck envelopes, capacity regression (tree baseline followed by CatBoost/LightGBM if justified), and a calibrated sustainability classifier searched along workload rays. Use interval/censored labels appropriately; do not feed censored ceilings as exact regression targets. Growth/error features can diagnose a current overloaded state but must not leak outcome-only information into prospective capacity predictions. Keep workload IDs, replicates, all stages of a curve and future telemetry together in splits.

Pre-register practical validation targets, not promised outcomes: median absolute relative capacity error <=10%, 90th percentile <=20%, nominal 90% intervals with evaluated coverage, and <=5% unsafe recommendations on independent near-boundary holdouts (report a confidence interval and sample size; small samples cannot certify this). Also report worst overestimation, sustainability calibration, monotonicity, abstention/OOD rate and per-size results. Use grouped bootstrap by workload/instance; avoid row-wise random splits.

Performance utilization along a supported direction is `current_load / predicted_capacity = 1/lambda*`. Report uncertainty; OOD workloads can abstain. Overall is the max of known deterministic ratios and the learned ratio. Missing boundaries must remain visible; a clipped 100% display must retain raw over-limit ratios in output. At idle with unknown workload composition, do not invent a meaningful directional headroom forecast.

## Budget and Claude checkpoints

First milestone: 24 baseline configurations (about 14–24 stage-hours at 6–10 six-minute stages each, before drain/setup/repeats). Checkpoint after the first four trustworthy curves; do not spend the whole budget if the generator is limiting. Next: up to 24 targeted follow-ups + 12 mixture rays = at most 60 unique configurations on the first dedicated broker. Three additional broker/topology targets × up to 12 gives at most 96 core configurations; repetitions, soaks, hysteresis and later active-learning batches are separately counted. Add batches of 4–8 only while held-out error/coverage improves. This is an informative campaign, not a quota of 200 nearly duplicate tests. Instance-hours/cost need real provisioning prices before quoting currency.

Claude execution checkpoints:

1. Repair accounting + implement/test records/detector/search; read-only schema/isolation checks; bounded calibration. Review `docs/pilot-status.md` and raw evidence.
2. Qualify generator placement and dedicated 5K; four canonical searches, reviewed by Codex for conservation, drift, resource scope and censoring.
3. Release remaining B01–B24 in blocks of four; update run ledger and concise findings after every block. Stop on validity failure.
4. Fit grouped baseline models and rank uncertainty; choose Stage 2/3 batches with explicit hypotheses.
5. Freeze predictions, provision only authorized targets, execute transfer holdouts, update model card.

Codex has dispatched one Claude Code worker with `docs/claude-pilot-brief.md`. Its initial scope ends at checkpoint 1; this plan does not claim later checkpoints have run. No concurrent broker-load workers or nested agent waves.
