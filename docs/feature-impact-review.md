# Feature impact review for the fast-path publish-rate model

Reviewed 2026-11-02. The archived 5K pilot has ten five-minute stable
observations, one interrupted attempt, and **no broker-attributed saturation
boundary or feature contrast**. It cannot calibrate a feature percentage. The
remaining percentages in the web page are scenario coefficients only.

## How to read this review

"Low", "medium", and "potentially dominant" are **test priorities**, not
measured slowdown bands. A feature can matter greatly for operational behavior
while having little effect on this particular steady-state fast-path metric.
The comparison must hold message size, fanout, delivery mode, publishers,
consumer capacity, topology, and broker version constant. An idle feature and
an actively exercised feature are different experiments.

For the requested fast-path ceiling, the highest-value explanatory variables
are **delivery mode, message bytes, destination copies, and consumer drain
rate**. Publisher window/concurrency and client or network limits must be
checked so that a generator ceiling is not mistaken for broker saturation.
Service class is a resource and admission envelope, not a documented fixed
publish-rate multiplier. [Solace Cloud service class limits](https://docs.solace.com/Cloud/service-class-limits.htm)
list connection, subscription, queue and message-size allowances; they do not
provide a universal maximum messages/s. [SDKPerf's controls](https://docs.solace.com/API/SDKPerf/Command-Line-Options.htm)
define the offered workload, not broker capacity by themselves.

| Factor | Fast-path impact assessment | First contrast to run |
| --- | --- | --- |
| [TLS client transport](https://docs.solace.com/Cloud/Security/security-client-connectivity.htm) | Medium for small messages or heavy connection churn; CPU/cipher dependent. | Plain vs TLS at fixed established connections, then a separate churn trial. |
| [High availability](https://docs.solace.com/Features/HA-Redundancy/Configuring-HA-Groups.htm) | Conditional, potentially important for Guaranteed messaging through mate-link work. An HA toggle alone does not define a percent. | Matched standalone and HA service with identical client workload; measure mate-link latency and encryption separately. |
| [DR replication](https://docs.solace.com/Features/DR-Replication/Sync-Asynch-Replication.htm) | **Potentially dominant if synchronous**; asynchronous adds a replication copy but can remain below the active bottleneck until link/queue pressure. Not relevant for non-replicated Direct traffic. | Off vs async vs sync with a real standby, fixed link RTT, replication queue and publish window. |
| [Dynamic message routing](https://docs.solace.com/Get-Started/dynamic-message-routing.htm) | Likely near zero incremental data-path work with no remote match; potentially important with remote matches because those add egress/bridge traffic. | No remote subscribers vs one and several remote matching subscriptions. |
| [Cross-protocol delivery](https://docs.solace.com/API/Protocol-Metadata-Payload-Encoding.htm) | Medium or higher depending on ingress/egress protocol and payload conversion. | SMF→SMF vs SMF→AMQP/MQTT/REST with the same payload semantics. |
| [SMF channel compression](https://docs.solace.com/Messaging/Using-Message-Compresssion.htm) | **Signed effect is variable**: CPU cost may reduce msg/s; byte savings may raise a network-limited ceiling. Payload compressibility matters. | Off vs speed/size modes using both compressible and incompressible payloads. |
| [ACL profile rules](https://docs.solace.com/Security/Granting-Clients-Access.htm) | Likely low for a fixed, authorized topic set; could matter with large rule sets or subscription churn. | Simple vs dense ACL rules at fixed topics, then a separate churn case. |
| [Partitioned queue](https://docs.solace.com/Messaging/Guaranteed-Msg/Partitioned-Queue-Messaging.htm) | **Signed effect is variable**: partition bookkeeping adds work, but parallel consumers may remove an egress bottleneck. | Exclusive vs partitioned with 1/4/16 partitions and uniform vs skewed keys. |
| [Non-exclusive queue](https://docs.solace.com/Messaging/Guaranteed-Msg/Queues.htm) | Mostly a consumer distribution choice. A fixed publish-rate penalty is not justified. | Exclusive vs non-exclusive at fixed consumer count and ACK capacity. |
| [Egress selectors](https://docs.solace.com/API/API-Developer-Guide/Using-Selectors.htm) | Potentially dominant once messages are queued or scanned from disk; Solace recommends topic matching for best performance. | No selector vs simple/complex selector under both empty and growing backlog. |
| [Message replay log](https://docs.solace.com/Features/Replay/Message-Replay-Overview.htm) | Medium or high when capturing eligible Guaranteed messages; adds storage/replay-log work. | Replay off vs log capture on; then concurrent replay as a distinct load. |
| [Message expiry / max TTL](https://docs.solace.com/Messaging/Guaranteed-Msg/Configuring-Queues.htm) | Likely low to medium; max TTL adds stored-message state. Expiry/DMQ work rises when backlog actually ages. | No TTL vs max TTL with healthy drain, then stalled consumers and expiration. |
| [Dead-message queue handling](https://docs.solace.com/Messaging/Guaranteed-Msg/Queues.htm) | Likely low when no message becomes dead; potentially significant during mass expiry or redelivery exhaustion. | Eligible but unused DMQ vs measured transfer rate into DMQ. |
| [Message priority](https://docs.solace.com/Messaging/Guaranteed-Msg/Message-Priority.htm) | Likely low unless queue respects priority and mixed-priority backlog exists. | Respect priority off/on with one level, then mixed levels under backlog. |
| [Concurrent queue browsing](https://docs.solace.com/API/API-Developer-Guide-JCSMP/JCSMP-API-Browsing-Guaranteed-Mess.htm) | Likely near zero without an active browser; medium or higher if browsing competes for storage and egress. | Idle browser vs sustained browse scan at controlled rate. |
| [Last-value queue](https://docs.solace.com/Messaging/Guaranteed-Msg/Queues.htm) | **Not a like-for-like throughput multiplier.** A max-spool-usage=0 queue retains only the latest message, changing backlog and loss/retention semantics. | Measure separately as a different workload; compare publish overhead and retained-message behavior, not a universal percent. |
| [Local transactions](https://docs.solace.com/Messaging/Guaranteed-Msg/Transactions.htm) | Potentially important; commit frequency and batch size dominate. Batching can amortize overhead. | Non-transacted vs batches of 1/10/100, measure commit latency and msg/s. |
| [XA transactions](https://docs.solace.com/API/Solace-Jakarta-API/Using-XA-Transactions.htm) | Potentially important at small batches due to two-phase commit; API/transaction-manager specific. | Local vs XA at matched batch size and transaction manager. |
| [Distributed tracing](https://docs.solace.com/Features/Distributed-Tracing/Distributed-Tracing-Best-Practices.htm) | High priority for small messages and high fanout; Solace says impact is considerable at ≤1 KiB and lower at ≥10 KiB. Sampling rate is essential. | 0/1/10/100% traced topics at 256 B and 10 KiB, fanout 1 and 16. |
| [Broker event publishing](https://docs.solace.com/Monitoring/Monitoring-Events.htm) | Likely low for steady message flow; subscription events can affect subscription performance during churn. | Steady subscriptions vs high subscription add/remove rate. |

## Prioritize the next measurements

1. Establish repeatable broker-limited reference boundaries for direct and
   Guaranteed delivery across message size × queue fanout. The observed 5K
   points are stable lower observations, not the ceiling.
2. Run paired tests for replay capture, tracing sampling, egress selectors under
   backlog, local/XA batch size, and **sync/async DR only when a correctly
   provisioned standby is available**. These are the strongest candidates for
   a material, workload-dependent effect.
3. Test TLS, HA, compression, protocol conversion, and partitioning with their
   governing parameters. Treat these as distinct model regimes rather than
   binary global multipliers.
4. Defer LVQ, idle DMQ, idle browsing, unused DMR and event publishing for the
   fast-path ceiling. Test them when the modeled workload actively uses them.

For synchronous DR, a blocking publisher with one outstanding message is
approximately bounded by `1 / inter-site round-trip time` before other work;
with a window of `W`, the link-side bound is approximately `W / RTT`, also
limited by replication link bandwidth and remote persistence. This is a
mechanistic bound, **not** a measured percentage. For DMR, remote matches add
broker-to-broker delivery copies; model them as extra fanout and link bytes.
For LVQ, retain the queue's special semantics instead of fitting a scalar
coefficient against an ordinary queue.
