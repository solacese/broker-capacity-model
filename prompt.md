You are building a complete Solace PubSub+ broker capacity-modeling system.

The end goal is to produce a single broker utilization score from 0–100% that represents how close a broker is to its FIRST meaningful capacity boundary.

This must combine:

1. deterministic resource utilization;
2. learned, non-deterministic performance utilization.

The system should work across different Solace broker sizes, versions, workloads and configurations.

For the first phase, you will have access to a single broker. Use it to build the full framework, discover available data, validate the methodology, perform efficient saturation experiments, and produce the first model. The architecture must be designed so that additional brokers can later be added without redesigning the system.

# 1. Core product definition

The final product receives:

- SEMP v2 configuration;
- SEMP v2 monitoring data;
- standard broker statistics;
- broker/service-class identity;
- optionally a short history of those statistics.

It should return something like:

Overall broker utilization: 94%

Limiting factor:
Subscriptions — 94%

Performance utilization:
68%

Connections:
61%

Message spool:
43%

Ingress flows:
22%

Egress flows:
51%

Delivered-unacknowledged:
18%

Estimated performance headroom:
+47k msg/s under the current workload composition

Likely performance bottleneck:
Guaranteed egress / fanout

Confidence:
High

Explanation:
Performance prediction is driven primarily by egress delivery rate, persistent messaging, fanout and message size.

The fundamental definition is:

overall_utilization =
max(
    all deterministic utilization ratios,
    learned performance utilization
)

Example:

connections = 70%
subscriptions = 99%
spool = 30%
flows = 40%
ML performance utilization = 62%

Overall utilization = 99%.

This is intentional.

The common denominator is:

"How close are we to the first capacity boundary?"

A broker cannot be called 62% utilized if one enforceable resource is already at 99%.

# 2. What the project must solve

There are two fundamentally different capacity problems.

## A. Deterministic capacity

Some resources have known current values and known limits.

Examples may include:

- connections;
- protocol-specific connections;
- SSL connections;
- subscriptions;
- queues;
- topic endpoints;
- ingress flows;
- egress flows;
- delivered-unacknowledged messages;
- message spool;
- queue message capacity;
- transactions;
- transacted sessions;
- REST resources;
- MQTT resources;
- bridges;
- Kafka bridge resources;
- replay resources;
- other broker-enforced limits.

For these:

utilization = current / maximum

No machine learning should be used when the true denominator is known.

## B. Non-deterministic performance capacity

Other limits depend on workload shape.

For example, the same broker may sustain very different throughput depending on:

- message size;
- Direct vs Guaranteed messaging;
- ingress protocol;
- egress protocol;
- fanout;
- publisher count;
- consumer count;
- subscription complexity;
- queue topology;
- partitioned queues;
- partition count;
- key skew;
- slow consumers;
- ACK behavior;
- TLS;
- tracing;
- HA;
- replication;
- DMR;
- queue depth;
- selector usage;
- disk/spool pressure;
- message metadata;
- other interactions.

These cannot be expressed as one static published limit.

The project must learn this performance capacity boundary experimentally.

# 3. Production-data constraint

Design the project under the assumption that, in many real customer environments, the only inputs available will be:

- SEMP v2 config;
- SEMP v2 monitor;
- normal broker statistics;
- broker/service-class identity.

Do NOT make the production model dependent on:

- exact CPU model;
- exact vCPU allocation;
- exact RAM;
- cloud VM type;
- physical NIC model;
- storage SKU;
- disk IOPS;
- hypervisor information.

These may be recorded in the lab if available, but they must be optional enrichment features.

Always maintain a "SEMP-only" model.

If richer infrastructure metadata materially improves accuracy, compare both models explicitly.

# 4. High-level architecture

Build the project as six logical layers.

## Layer 1 — SEMP discovery and collection

Automatically discover available SEMP fields from the broker.

Use:

/SEMP/v2/config
/SEMP/v2/monitor

and obtain the OpenAPI specification when possible.

Do not hard-code assumptions that all Solace versions expose exactly the same fields.

Maintain a registry containing:

feature name
description
SEMP endpoint
JSON field/path
scope
unit
static/dynamic
deterministic/ML input
availability
broker-version compatibility

## Layer 2 — Deterministic capacity engine

Discover every resource where both:

current usage

and

maximum capacity

are available.

Compute utilization exactly.

Example:

connections = active_connections / max_connections
subscriptions = current_subscriptions / max_subscriptions
spool = spool_used / spool_limit
flows = active_flows / max_flows

Store all ratios between 0 and 1.

The deterministic engine should be independently useful before any ML exists.

## Layer 3 — Experiment and telemetry system

Build a controlled benchmark framework capable of:

- configuring workloads;
- starting publishers/consumers;
- changing offered load;
- polling SEMP;
- recording load-generator statistics;
- detecting whether a workload is sustainable;
- finding its saturation boundary;
- resetting/draining broker state;
- storing all experiment metadata.

## Layer 4 — Performance-capacity model

Learn the non-deterministic sustainable capacity boundary.

The model should answer:

"Given this broker/configuration/workload, how close are we to the sustainable performance boundary?"

## Layer 5 — Unified utilization engine

Combine:

deterministic ratios

and

learned performance utilization

using the common denominator:

overall broker utilization =
distance to the closest capacity boundary.

At minimum:

overall_utilization =
max(deterministic ratios, performance utilization)

If later evidence shows that some boundaries need more nuanced combination logic, document and validate it rather than silently changing the definition.

## Layer 6 — Explanation layer

Every result should explain:

- limiting factor;
- current utilization;
- deterministic vs learned nature;
- remaining headroom;
- confidence;
- primary features contributing to performance saturation.

Avoid black-box answers.

# 5. Definition of performance saturation

Define a workload as sustainable only when it remains stable for a sufficient observation period.

At minimum consider:

- offered publishing rate;
- accepted publishing rate;
- delivered rate;
- publisher ACKs/NACKs;
- ingress discards;
- egress discards;
- queue depth;
- queue-depth growth;
- spool utilization;
- spool growth;
- delivered-unacknowledged messages;
- delivered-unacknowledged growth;
- latency where available;
- broker health statistics;
- load-generator health.

A broker is NOT considered sustainable merely because it has not crashed.

A workload becomes unsustainable when one or more persistent symptoms appear, such as:

- accepted throughput cannot track offered load;
- delivered throughput cannot track the expected delivery workload;
- backlog grows continuously;
- spool grows continuously;
- unacknowledged deliveries grow uncontrollably;
- latency sharply deteriorates;
- discards appear materially;
- publisher backpressure dominates;
- broker health degrades significantly.

For each configuration estimate:

T_max = maximum sustainable workload.

Do not assume T_max is only ingress messages/sec.

The effective boundary may depend jointly on:

- ingress msg/s;
- ingress bytes/s;
- egress deliveries/s;
- egress bytes/s;
- persistent messaging rate;
- routing work;
- fanout;
- spool work.

# 6. Performance-model formulation

Do NOT begin by training an arbitrary neural network that directly maps features to a 0–1 utilization score.

The percentage should have a physical interpretation.

Evaluate at least two model formulations.

## Model A — Capacity regression

Input:

broker configuration
workload configuration

Output:

maximum sustainable capacity or capacity boundary.

Example:

estimated T_max = 182k msg/s under current workload composition.

Then:

performance_utilization =
current_effective_load / predicted_capacity

## Model B — Sustainability classifier

Input:

broker configuration
workload configuration
operating point

Output:

P(sustainable)

For example:

100k msg/s -> 0.99 sustainable
150k -> 0.92
180k -> 0.55
210k -> 0.08

Numerically search along the current workload direction to locate the sustainable boundary.

If the current workload can be multiplied by λ before reaching that boundary:

performance_utilization ≈ 1 / λ

Example:

current workload can increase by 1.25× before saturation

therefore:

utilization ≈ 1 / 1.25 = 80%

This gives the percentage a meaningful interpretation.

Test both approaches and choose based on:

- validation accuracy;
- calibration;
- stability;
- interpretability;
- ability to generalize.

# 7. Preferred ML models

Start with tabular models.

Evaluate:

- CatBoost;
- LightGBM;
- XGBoost;
- Extra Trees / Random Forest;
- uncertainty-aware surrogate models where useful.

Do not default to an MLP.

Reasons:

- dataset will initially be relatively small;
- many variables are categorical;
- many variables will be missing depending on broker/version;
- strong nonlinear interactions are expected;
- tree models offer good interpretability;
- SHAP explanations are useful.

Only evaluate an MLP or temporal neural network later if there is enough data and it demonstrably improves generalization.

# 8. Candidate SEMP-first performance features

Build a complete feature registry.

At minimum investigate:

## Broker identity

- service class;
- system scaling values;
- broker version;
- cloud/software/appliance when known;
- standalone/HA;
- available configured capacities.

## Connections

- total active connections;
- connections by protocol;
- connection fraction;
- SSL/TLS connections;
- publisher connections;
- consumer connections;
- connection churn.

## Traffic

- ingress msg/s;
- egress msg/s;
- ingress bytes/s;
- egress bytes/s;
- Direct ingress;
- Direct egress;
- Guaranteed ingress;
- Guaranteed egress;
- discard rates;
- average message size.

## Fanout

Derive:

fanout_messages =
egress_messages / ingress_messages

and where useful:

fanout_bytes =
egress_bytes / ingress_bytes

Also measure:

- direct matches/message;
- queue matches/message;
- remote matches/message;
- total deliveries/message.

Fanout should be treated as a major candidate predictor.

## Subscriptions

- current subscriptions;
- maximum subscriptions;
- local subscriptions;
- remote subscriptions;
- shared subscriptions;
- subscriptions/client;
- wildcard subscriptions;
- wildcard depth;
- subscription exceptions where measurable;
- subscription churn.

## Queues and endpoints

- queue count;
- topic endpoint count;
- active vs inactive;
- exclusive/non-exclusive;
- queue depth;
- total queue depth;
- largest queue depth;
- number of non-empty queues;
- queue depth distribution;
- number of growing queues;
- queue-growth rate.

## Guaranteed messaging

- spool used;
- spool max;
- spool percentage;
- spool-growth rate;
- ingress flows;
- egress flows;
- delivered-unacknowledged;
- delivered-unacknowledged gradient;
- ACK-related statistics;
- slow-consumer indicators;
- persistent publish rate;
- persistent delivery rate.

## Partitioned queues

- PQ count;
- partitions;
- consumers;
- partitions/consumer;
- partition imbalance if exposed;
- key cardinality if known from workload;
- key skew;
- rebalance events;
- consumer churn.

## Protocol

Treat ingress and egress protocol independently.

Potential protocols include:

- SMF;
- AMQP;
- MQTT;
- REST;
- Web Messaging;
- others exposed by the environment.

## Broker topology/features

Where available through SEMP/config:

- HA;
- replication;
- DMR;
- bridges;
- replay;
- tracing;
- selectors;
- TTL;
- priority;
- compression;
- TLS;
- MQTT QoS/retained behavior;
- REST delivery resources;
- Kafka bridges.

## Health metrics

If available:

- compute latency;
- disk latency;
- network latency;
- mate-link latency;
- interface utilization;
- broker health alarms.

Treat these as optional but valuable.

# 9. Derived features

Create derived features rather than feeding only raw counters.

Examples:

connection_fraction
subscription_fraction
queue_fraction
spool_fraction
flow_fraction
unacked_fraction

fanout
bytes_fanout

avg_ingress_message_size
avg_egress_message_size

messages_per_connection
messages_per_publisher
deliveries_per_consumer

subscriptions_per_connection
queues_per_connection

persistent_fraction
direct_fraction

queue_growth_10s
queue_growth_30s
queue_growth_120s

spool_growth_10s
spool_growth_30s
spool_growth_120s

unacked_growth

accepted/offered
delivered/accepted

egress_msg_per_ingress_msg
egress_bytes_per_ingress_bytes

current / rolling_mean
rolling_mean
rolling_max
slope

Normalize useful variables by broker size/capacity to help cross-broker generalization later.

# 10. Experimental philosophy

The full project must NOT rely on brute-force enumeration.

The configuration space is too large.

Use experimental design and active learning to discover the important structure with as few runs as possible.

The goal is not:

"test every Solace configuration."

The goal is:

"identify which dimensions materially change the performance boundary, model those dimensions, and ignore dimensions whose effects are negligible."

# 11. First-broker mission

You currently have one broker.

Use it to build the complete framework and perform the first screening campaign.

The first broker should teach us:

- which SEMP fields are actually accessible;
- which deterministic limits can be computed;
- which non-deterministic variables matter most;
- what form the saturation boundary seems to take;
- how noisy benchmarks are;
- how many repetitions are necessary;
- whether a simple boosted-tree model is sufficient;
- how many experiments are likely required for a multi-broker model.

Do not pretend that one broker is sufficient to prove generalization across service classes.

Explicitly separate:

what we can learn from one broker

from

what must later be validated across multiple broker sizes.

# 12. Experiment stages

## Phase A — baseline characterization

Establish a small set of canonical baselines.

At minimum investigate:

- Direct 1:1;
- Guaranteed 1:1;
- Direct fanout;
- Guaranteed fanout;
- regular queue;
- partitioned queue if supported.

Use a few representative message sizes.

The goal is to discover basic regimes:

message-rate limited
byte-rate limited
delivery/fanout limited
spool/persistence limited
other.

## Phase B — main-effect screening

Vary important candidate variables aggressively enough to reveal effects.

Examples:

message size:
64B / 1KB / 10KB / 100KB / possibly 1MB

fanout:
1 / 5 / 20 / 100

publishers:
1 / 10 / 50 or similar

consumers:
1 / 10 / 100 / 200 where practical

delivery:
Direct / Guaranteed

queue:
regular / PQ

partitions:
small / medium / large

key distribution:
uniform / skewed / hot-key

subscription counts:
low / medium / high

consumer speed:
healthy / mildly slow / severely slow

protocol:
SMF / AMQP / MQTT where available

Do not use a complete Cartesian product.

Use a statistically efficient design such as:

- fractional factorial;
- definitive screening design;
- D-optimal design;
- Latin hypercube;
- sequential model-based design.

For each factor estimate:

- effect on T_max;
- confidence;
- nonlinear behavior;
- interaction hints.

Drop factors whose effect is below benchmark noise unless there is a strong architectural reason to retain them.

## Phase C — interaction testing

Only test interactions that are plausible or suggested by the data.

Examples:

message_size × throughput
message_size × fanout
fanout × persistence
fanout × subscriptions
fanout × tracing
queue_depth × selectors
persistence × spool pressure
partitions × consumers
partitions × key skew
protocol × persistence
TLS × message size
slow consumers × unacked
HA × persistent load
replication × RTT

Do not test every pair.

## Phase D — active learning

Once a provisional model exists:

generate candidate experiments

estimate:

prediction
uncertainty
distance from observed data
expected information gain

choose the next experiment where it will most improve the model.

Prefer experiments that:

- probe uncertain regions;
- explore transitions between bottleneck types;
- test strong nonlinearities;
- discriminate competing models;
- cover previously unseen operating regimes.

Avoid near-duplicate experiments.

# 13. Efficient saturation search

Do NOT benchmark every rate linearly.

For each workload configuration:

1. choose a known-safe load;
2. increase aggressively until failure;
3. bracket the boundary;
4. binary-search or use another adaptive search method.

Example:

50k PASS
100k PASS
200k FAIL

150k PASS
175k FAIL
162k PASS
169k FAIL

T_max ≈ 165k

Target approximately ±5% capacity precision initially.

Only increase precision if needed.

This should reduce each saturation curve to roughly 6–10 useful load stages rather than dozens.

# 14. Experimental validity

A run is invalid if the load generator becomes the bottleneck first.

Monitor the generators.

Record:

- offered rate;
- successful publish rate;
- received rate;
- ACK/NACK rates;
- client CPU;
- client network;
- latency;
- connection failures;
- generator-side errors.

A broker-capacity benchmark must not accidentally measure sdkperf, client CPU, NIC saturation or GC limits.

# 15. Bottleneck classification

Every saturation result should attempt to classify the limiting mechanism.

Candidate classes:

- deterministic hard limit;
- compute/message-processing;
- ingress msg-rate;
- egress delivery-rate;
- ingress byte-rate;
- egress byte-rate;
- routing/subscription matching;
- fanout;
- Guaranteed ingress;
- Guaranteed egress;
- spool/disk;
- slow consumer;
- delivered-unacked;
- partition/hot-key;
- protocol overhead;
- HA/mate;
- replication;
- DMR;
- selector/deep queue;
- tracing;
- client/load generator;
- unknown.

The model should eventually output the likely bottleneck together with utilization.

# 16. Unified broker score

The final score should be based on the nearest boundary.

If:

connections = 35%
subscriptions = 72%
spool = 41%
egress flows = 81%
performance = 66%

then:

overall broker utilization = 81%

limiting resource = egress flows.

If:

connections = 40%
subscriptions = 25%
spool = 30%
performance = 91%

then:

overall broker utilization = 91%

limiting resource = learned performance boundary.

This is the conceptual foundation of the entire project.

# 17. Confidence and out-of-distribution behavior

Never output false precision.

The system should estimate confidence based on:

- proximity to training examples;
- model disagreement;
- uncertainty estimates;
- broker/service class coverage;
- workload coverage.

Example:

Estimated performance utilization: 76%
Confidence: High

or:

Estimated performance utilization: 71–88%
Confidence: Low
Reason: workload has much higher fanout than any benchmarked configuration.

If the workload is far outside the experimental domain, explicitly say so.

# 18. Validation strategy

For the first broker:

- hold out complete workload configurations, not just random rows from the same saturation curve;
- validate T_max prediction;
- validate sustainability classification;
- validate bottleneck prediction;
- validate utilization monotonicity.

Later, when more brokers are added:

- hold out entire broker sizes;
- hold out broker versions;
- test cross-service-class generalization.

A universal model must prove it can generalize rather than memorize service classes.

# 19. Data storage

For every run save:

experiment_id
timestamp
broker_id
broker_version
broker/service class
SEMP version/spec
full relevant config snapshot
workload definition
load-generator config
offered load
SEMP time series
load-generator time series
derived metrics
PASS/FAIL sustainability
estimated T_max
bottleneck class
run validity
notes
model version

Prefer raw data in Parquet and a searchable metadata/index layer such as DuckDB or Postgres.

Do not discard raw telemetry after feature extraction.

# 20. Feature registry

Maintain a living registry with:

feature
definition
unit
source
SEMP path
static/dynamic
deterministic/ML
tested range
importance
interactions
missingness
status:
keep / drop / uncertain

This is a major project artifact.

The project should progressively reduce hundreds of available broker fields into the smallest useful production feature set.

# 21. Expected first-broker outputs

After working with the first broker, produce:

1. SEMP inventory;
2. deterministic-capacity table;
3. complete candidate feature registry;
4. benchmark harness;
5. saturation detector;
6. experiment catalog;
7. baseline saturation curves;
8. screening results;
9. ranked feature importance;
10. discovered interactions;
11. first ML performance model;
12. bottleneck classifier;
13. unified 0–100 broker utilization engine;
14. confidence/OOD mechanism;
15. model-validation report;
16. recommendation for the next broker size to test;
17. recommended minimum multi-broker campaign.

# 22. Desired experiment budget

Be frugal.

For the first broker, target roughly:

15–30 saturation configurations initially.

Expand only if the early results show that additional experiments are informative.

Do not spend 100 tests proving an obvious effect.

The first goal is to identify:

- the major workload dimensions;
- the useful SEMP features;
- the shape of the performance boundary;
- the bottleneck regimes.

The later universal campaign may contain roughly 100–200 carefully selected saturation configurations across multiple brokers, but this is a guideline, not a quota.

Use active learning to minimize the final count.

# 23. Development priorities

Build in this order:

1. inspect the actual broker and SEMP schema;
2. build the feature registry;
3. implement the deterministic capacity engine;
4. implement reliable telemetry collection;
5. implement benchmark orchestration;
6. implement sustainability detection;
7. implement adaptive saturation search;
8. run baseline experiments;
9. run screening experiments;
10. train first capacity model;
11. combine deterministic + learned utilization;
12. add explainability;
13. use model uncertainty to select the next experiments.

Do not spend time building a sophisticated frontend before the scientific model works.

# 24. Success criteria

The project is successful if, given only normal broker/SEMP information, it can answer:

"How full is this Solace broker?"

with:

- one meaningful 0–100 overall utilization number;
- the limiting capacity dimension;
- exact deterministic ratios where available;
- learned performance utilization;
- estimated remaining headroom;
- likely bottleneck;
- uncertainty/confidence;
- understandable explanation.

The score must be actionable and physically interpretable.

The project should ultimately support many Solace broker sizes and configurations, but the current single broker is the bootstrap environment for discovering the methodology and building the reusable system.

Begin by inspecting the available SEMP APIs and broker configuration, then build the experiment and modeling framework described above.