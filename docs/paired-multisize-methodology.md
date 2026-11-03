# Capacity experiment method: 1K, 5K, 10K

**Status, 3 November 2026:** prepared offline. The 96-slot randomized schedule in
`data/paired-multisize-v1/` is a plan, not completed experiments or permission to
provision or run. The current owned inventory has one dedicated 5K test broker.
The owned 1K services are shared with other projects, and there is no owned 10K
service. Bind isolated targets only after checking current owner, class, broker
build, region, topology and absence of foreign traffic. Do not load-test those
shared 1K services.

## 1. What we estimate

For one frozen broker, build and workload, let \(C\) be the interval of publish
rates sustainable for a **300-second** measurement: accepted ingress tracks the
requested rate, each destination's egress keeps up, unique delivery and CRC
reconcile, and the low-water backlog does not keep rising. State the lower and
upper endpoints of the adaptive search; do not replace an interval with a
spurious exact maximum. A bursty queue that repeatedly drains is recorded
separately from a consistently low-queue path. Yesterday's 5K data reached
32,000 msg/s for five minutes with large transient queue bursts, but did not
establish a broker-limited upper endpoint.

Keep two distinct outputs:

- **Sustainable delivery capacity:** a workload-specific rate interval with
  independent evidence that the broker, rather than SDKPerf, a consumer, a
  configured quota or the network, caused the upper endpoint.
- **Fast-path threshold:** the highest tested rate that also satisfies a
  preregistered low-queue envelope and native broker path evidence. Egress
  shortfall plus queue growth indicates an overload candidate; it does not by
  itself prove a disk or slow-path transition. Missing native path evidence
  leaves this threshold unknown.

The service class is categorical. Solace documents Enterprise 1K on a 1K
software broker, while Enterprise 5K and 10K use the 10K software-broker size
with different service allowances. The 5K-to-10K capacity ratio therefore
must be measured rather than assumed to be two. See the [service-class
limits](https://docs.solace.com/Cloud/service-class-limits.htm).

## 2. First study, then expand

Start with one fixed SMF/TLS Persistent workload: 1,024-byte application
payload, two **distinct queue destinations**, four publisher connections,
one consumer per queue, exclusive queues, publisher ACK window 255, seeded
incompressible payload, and a 255 subscriber ACK window. The first contrast
changes only the subscriber ACK window from 255 (A) to 64 (B), using the
[SDKPerf `-asw` option](https://docs.solace.com/API/SDKPerf/Command-Line-Options.htm).
This is deliberately a flow-control/apparatus sensitivity study, not yet a
broker-feature multiplier. It tests whether the queue bursts seen in the 5K
pilot follow consumer flow control.

For **each** broker class, run **12 matched A/B pairs** in four
sessions of three pairs across at least two dates. Each pair contains two complete capacity-boundary
assessments on the same broker and workload. A fixed seed randomizes AB versus
BA order, balanced six of each per class and with both orders in every session,
then shuffles pair order within each session. A reference check brackets each session. Keep the frozen schedule even
if a result is surprising. One broker at a time within a class; three brokers
may run concurrently only with independent worker fleets and atomic admission
against one shared cost journal. The plan contains **36 pairs, 72 boundary
search slots and 24 short drift checks** across 1K, 5K and 10K. A boundary slot
can contain many SDKPerf rates, so 96 slots are not 96 five-minute trials.

The next workload contexts are 256 B/fanout 1 and 4,096 B/fanout 4. Use a
small, separately labelled interaction screen there; promote any factor with
context dependence to 12 pairs **per context and class** before claiming a
portable coefficient. Payload size and fanout themselves are dose-response
axes, not binary toggles. Later binary contrasts include queue access and TLS
only after their adapters and capabilities are qualified. Partition count,
compression and protocol require dedicated conditional or interaction designs;
do not force them into one global multiplier.

## 3. How one boundary assessment runs

1. Freeze a measurement epoch: exact broker ID/owner/class/build, Message VPN,
   underlying size, region/AZ, queue inventory, topic subscriptions, SEMP schema
   and units, SDKPerf/Java hashes, payload seed, client counts and all command
   arguments. Reconfirm isolation and no pre-existing backlog. Record the
   effective connection, queue, flow and spool caps. A configured quota is a
   separate label.
2. Qualify two or more generator hosts **near the broker**. Confirm offered
   rate tracking, publisher ACKs, consumer CPU and NIC headroom, zero slow-
   subscriber flags, stable counters, exact per-destination message/CRC
   reconciliation and clean drains. Repeat an apparent edge with stronger or
   independently placed clients without changing broker-side connection counts.
   An edge that moves with the apparatus is censored for broker inference.
3. Use 60-second scouts to bracket a rate, then independently started
   300-second lower and upper trials. Narrow the interval to at most 1% for
   a feature-effect decision; a 5% search bracket cannot establish
   equivalence inside a ±5% effect margin. A website label rounded to 100 msg/s
   needs an interval at most 100 msg/s wide; otherwise display the interval.
   Stop on spool guard, unexpected clients,
   telemetry gaps, counter resets, integrity error, budget guard or a
   path-limited result. Do not turn such a stop into an upper broker bound.
4. At each destination, require stable accepted ingress (at least 98% of
   requested, within 5% variation/change), then compare egress with ingress.
   A candidate needs a deficit of at least 2% or 100 msg/s **and** growing
   delivery lag and queue occupancy on the *same* destination. Check
   30-second low-water floors to distinguish one-way backlog growth from
   bursts that return near baseline. Egress decline and a flat egress plateau
   below ingress are separate diagnoses. Confirm a candidate in another
   300-second trial and with independent apparatus controls.
5. Drain only the experiment's own queues and verify every message before the
   next arm. Preserve raw SEMP/SDKPerf time series, broker path gauges,
   connection/TCP diagnostics, worker CPU/network telemetry, all source hashes,
   costs and both the original and any later re-analysis. Never purge another
   workload's data. Use fresh resource names only within a frozen inventory
   epoch; changing that inventory starts a new block.

## 4. A/B analysis and the “one standard deviation” rule

For each valid pair \(i\), estimate the effect on **log capacity**:

\[d_i=\log(C_{B,i}/C_{A,i}).\]

Pairing removes much of the broker/day drift. Pairs in the same session can
still be correlated, so the analyzer takes the wider of a pair-level and a
four-session-level t interval before classifying the effect. The *standard deviation of the
paired differences* helps plan replication; it is **not** a criterion for
calling a feature irrelevant. A mean effect can be smaller than one standard
deviation yet estimated precisely after repeated pairs. Likewise a noisy
non-result does not prove no effect. Use a preregistered **smallest effect of
interest**, initially ±5% of capacity (configurable before execution). For
each class, the analysis tool uses the conservative ratio interval from both
boundary brackets and a t interval across the 12 paired log effects:

- **Equivalent on this reference workload:** the full 90% effect interval
  lies within −5% to +5%. This is an equivalence test, so its coefficient may
  be fixed at 1 **only for this class and tested workload**.
- **Material effect:** the full 95% interval lies beyond +5% or −5%; retain
  a class/workload conditional effect and verify it in a new session.
- **Unresolved:** all other cases, missing broker-attributed upper bounds,
  wide search brackets, invalid arms, an AB/BA order contrast above 5%, or
  missing/failed reference drift checks.
  Add preregistered pairs or improve measurement; never silently set 1.

The planned 12 pairs are a **minimum**, not a guarantee of enough precision.
Decide at 12, and if unresolved plan a new 12-pair confirmation batch rather
than repeatedly peeking at the same accumulating data. Report the estimate,
paired and session SD, both confidence intervals, number of valid/censored pairs and
AB/BA and session contrasts. With only one broker instance per class, all
conclusions remain conditional on that instance and environment. A later
independent broker/build holdout is needed for a general size-class claim.
This follows randomized-block design and equivalence-testing principles
described by [NIST](https://www.itl.nist.gov/div898/handbook/pri/section3/pri332.htm)
and [Lakens](https://doi.org/10.1177/1948550617697177).

The analyzer accepts only a verified boundary artifact for each arm, with a
matching hash, exact target ID, source hash, validated broker attribution and
an interval at most 1% wide. The current 5K pilot artifacts have no validated
broker upper boundary, so they correctly yield **no A/B effect estimate**.

## 5. Time, budget, readiness

Seventy-two boundary slots require **at least 12 broker-hours** if each needs
only one 300-second lower and one 300-second upper trial. Scouts, recovery,
reference checks, repeats and provisioning add time and traffic; this is a
mathematical floor, not an ETA. The new experiment budget returns to the
standing $20/day limit with a $16 operating guard; the one-night $40 approval
on 2 November is not applied to this future plan. Broker subscriptions are
separate. Reprice each route and obtain a forecast before starting; the
controller must checkpoint and terminate workers when budget admission fails.

Readiness work still required before launch:

1. Secure isolated owned 1K and 10K test brokers. Revalidate the dedicated
   5K and match build, region, topology, spool allocation and software family
   as far as supported. Shared 1K demo brokers are not test targets.
   The 5K preflight on 2 November reported broker version 10.26; re-read it
   before execution. Prefer the same exact 10.26 build for 1K and 10K, or
   separate broker-version effects rather than combining unmatched builds.
2. Extend the present live math pilot's 5K/10K allowlist and fixed queue pool
   to a three-class, per-broker binding and namespace. The offline schedule
   deliberately cannot launch the existing runtime as-is.
   Start with one `c7i.xlarge` publisher worker and one `c7i.xlarge` subscriber
   worker near each broker, as in the previous 5K pilot. This is a candidate
   VM profile, not qualified headroom for every class. If CPU, NIC or TCP
   capacity fails, enlarge or split the workers and restart matched arms in
   a new apparatus epoch. Freeze AWS region/AZ, AMI, worker type, SDKPerf binary
   hash and Java version for each block.
3. Export verified boundary artifacts with full provenance into the
   `results-template.csv` schema. The analysis CLI rejects raw spreadsheet
   numbers without those artifacts.
4. Dry-run the first session on the 5K, verify no false saturation from
   consumer placement, estimate actual time and cost per boundary, then
   authorize the rest. Keep the following factors staged rather than
   expanding hundreds of feature combinations.

## Offline commands

```bash
./prepare-paired-study prepare
./prepare-paired-study validate
./prepare-paired-study analyze
```

Outputs: `data/paired-multisize-v1/plan.json`, `schedule.csv`, and
`results-template.csv`. All slots remain `TODO`. The CLI has no provision or
run command and loads no credentials.
