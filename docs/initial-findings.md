# Initial broker findings

Collected on 2026-10-05 using read-only SEMP access. Scope was restricted to services in the **Solace SEs** organization (`seall`) whose `ownedBy` field exactly matched the uniquely resolved Raphael Caillon user ID (`usfos7cfqge`). No broker configuration or state was changed.

## Fleet discovered

| Broker | Service class | Version | Connections | Queues | Current deterministic limit |
|---|---:|---:|---:|---:|---:|
| fleet-broker | Developer | 10.26 | 2 / 100 | 2 / 100 endpoints | 2.0% connections/endpoints |
| swlb-…-c-data-1k | Enterprise 1K standalone | 10.26 | 4 / 1,000 | 4 / 1,000 endpoints | 0.4% connections/endpoints |
| swlb-…-a-control-data-1k | Enterprise 1K standalone | 10.26 | 8 / 1,000 | 58 / 1,000 endpoints | 5.8% endpoints |
| autoscale-demo-e5dc4c22-01 | Enterprise 250 standalone | 10.25 | 1 / 250 | 12 / 250 endpoints | **~90.9% queue spool** |
| swlb-…-b-data-1k | Enterprise 1K standalone | 10.26 | 3 / 1,000 | 4 / 1,000 endpoints | 0.4% endpoints |

These percentages are a partial deterministic score. Subscription and flow maxima are available, but their current values still need mapped collection endpoints before they can be included honestly.

## Interesting findings

1. **Service-class limits are directly observable through SEMP.** The Enterprise 1K VPNs expose limits of 1,000 connections/endpoints/ingress flows/egress flows, 100,000 subscriptions, 5,000 transactions, and 200,000 MB spool. The Enterprise 250 exposes 250, 50,000, 2,500, and 50,000 MB respectively. The Developer broker exposes 100, 1,000, 500, and 25,000 MB.

2. **Raw SEMP fields cannot be divided blindly.** `msgSpoolUsage` is bytes while `maxMsgSpoolUsage` is MB. For example, 45,476,824 bytes against 50,000 MB is about **0.091%**, not 90,954%. Unit metadata must be first-class in the feature registry.

3. **Per-endpoint limits can dominate the broker-wide score.** The Enterprise 250 VPN uses only about 45.5 MB of its 50,000 MB broker spool allocation (~0.09%), but one queue uses about 45.5 MB of its own 50 MB allocation (~90.9%). A VPN-only model would report 4.8% utilization from endpoint count and miss the first meaningful boundary by a wide margin.

4. **Historical counters are not current pressure.** The Enterprise 250 broker is currently idle, yet it has 952,752 cumulative receive discards. Queue-level counters attribute 947,384 of those to prior queue spool-limit exceedance. A one-shot score must not treat cumulative discards as current overload; the model needs deltas over a sampling window.

5. **Queue topology varies significantly within the same service class.** The three 1K brokers currently have 4, 58, and 4 queues. This supports treating topology and fanout as workload/configuration features rather than inferring them from service class.

6. **All brokers were idle during the snapshot.** Current ingress and egress message rates were zero. This snapshot supports deterministic discovery but cannot establish a performance boundary or performance-utilization score.

7. **Version compatibility matters.** Four services run 10.26 and one runs 10.25. The 10.26 VPN resource exposes at least one field absent in 10.25 (`allowDmqEligibleEndpointOverrideEnabled`), confirming that field discovery and missingness handling are necessary.

8. **Some intuitive collection names are invalid.** `/subscriptions`, `/transactedSessions`, and `/virtualRouters` returned SEMP `INVALID_PATH`; the implementation must derive supported paths from each broker's OpenAPI spec rather than naming endpoints heuristically.

## Immediate next steps

1. Download and cache one monitor/config OpenAPI spec per broker version with bounded streaming.
2. Build the feature registry from schema descriptions and units.
3. Map current subscription, ingress-flow, egress-flow, and delivered-unacknowledged gauges.
4. Collect short time-series samples and calculate counter deltas and slopes.
5. Only then design non-destructive baseline workloads and saturation searches.

The raw sanitized snapshot is in `data/initial_snapshot.json`.
