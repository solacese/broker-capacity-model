# Broker Capacity Studio

Capacity explorer at <https://broker-capacity.sol-se-emea.com/> with three views
in one dark, compact HTML page: **Model**, **Connected broker**, and
**Experiments**.
The page links to its public source at
<https://github.com/solacese/broker-capacity-model>.

The presentation follows the structure and vocabulary of the
[Solace SDKPerf documentation](https://docs.solace.com/API/SDKPerf/SDKPerf.htm):
plain headings, field groups, tables, and inline SDKPerf option badges where
the controls map to documented arguments. **Message size** is the UI term; SDKPerf
`-msa` sets generated binary attachment bytes, while the complete wire message
also contains headers and protocol overhead. Every control and feature has a
`?` link to a relevant Solace documentation page. It is
labelled as a research prototype and does not reproduce the documentation site.
The badges cover `-msa`, `-cc`, `-mt` (or MQTT `-mpq/-msq`), `-ptl`, `-stl`,
and applicable feature arguments `-ssl`, `-z`, `-mtl` and `-mdq` when those
features are enabled. Topic-count
controls model counts; SDKPerf `-ptl` and `-stl` require actual topic lists.
MQTT QoS is not identical to the model's Direct/Persistent choice. Controls
without a direct SDKPerf equivalent have no badge.

The Model view has equal-width settings and results columns. The settings column
groups broker configuration, traffic and
routing, connections and queues, feature switches, and model limits. It includes
nine modeled feature switches in a responsive two- or three-column grid plus
numeric workload controls such as published topics, subscriptions, user property bytes, publisher rate skew, other active
queues, and connection churn. Persistent-only switches are disabled for direct
delivery. Each feature row shows its local modeled rate change if enabled; the
results column shows the rounded capacity estimate and interactive 3D surface.
Feature coefficients are illustrative and require broker-specific validation.
DR, HA, partitioning, last-value queues, DMR, compression and other conditional
features have **no fixed coefficient** in the calculator. They display
`varies` until the relevant workload and operating mode are represented.
See [feature impact review](../docs/feature-impact-review.md) for the
documented mechanisms and test priorities.

On the surface, payload and fanout span the floor, modeled throughput is the
vertical axis, and the connection control selects a slice. Drag to rotate,
hover for values, click to select a point, or use the payload/fanout selectors.
Horizontal dragging rotates the surface; its vertical projection is fixed.
The throughput axis and red-to-blue colors use a fixed scale for the selected
service class and reference rate. Changing workload properties therefore moves
the surface on the same scale; changing the reference rate or class resets it.
The selected-point margin of error is shown as not established until repeated,
broker-limited maximum measurements support a numeric interval for the exact
configuration. Finite-horizon stable lower observations do not supply that margin.
The red-to-blue color scale runs from the lowest to highest throughput within
that reference scale. Logarithmic color spacing and slope lighting make the
full range legible; colors are relative to the selected service class and
reference rate.
Protocol and delivery use ordinary native dropdowns; service class uses radios.
The plot uses a 350,000 msg/s reference scale (expanded only for larger
user-entered scenarios) with a square-root height transform, so workload
multipliers visibly change the surface. The model has no measured regional
correction. Service classes sharing an underlying software
broker size can have the same illustrative shape and rate; the selected class
and connection limit still update.

The page separates two things:

1. A **scenario estimate** from an entered reference rate and visible, currently
   uncalibrated workload factors. The initial 70,000 msg/s value is an example,
   not a measured broker limit.
2. **Hard admission and nominal network ceilings**, checked separately from the
   processing estimate. The network-speed field is an ideal line-rate bound,
   not a measured application capacity.

The archived 5K pilot remains in `data/math-pilot-20261102` for research, but
its observations are not displayed on this simplified page and are not used to
set the illustrative baseline.

The calculator is deterministic. Its processing envelope is
`baseline / (protocol cost × delivery cost × payload cost × fanout cost × routing and connection density costs × enabled feature costs)`.
The rate callout, plot hover and neighbor labels, axis ticks, and SDKPerf target round to the nearest 100 msg/s for rates
at least 1,000 msg/s, with finer rounding below that. Each of the nine visible
switches changes the processing envelope when applicable. A configured ingress
or egress ceiling can still hold the total displayed capacity constant; the
per-switch percentage refers to the processing term.
The displayed rate is the minimum of processing, nominal ingress byte rate and
nominal egress byte rate, subject to connection admission. Factor coefficients
are explicitly illustrative until broker-attributed feature contrasts are
measured. The service-class selector supplies an illustrative reference rate for
the documented underlying software broker sizes (1K, 10K for 5K/10K, and 100K
for 50K/100K); these are **not measurements**. The 200K option is provisional
until its service class and underlying broker size are confirmed.
Connected broker metadata does **not** turn that reference into a validated
throughput baseline. A measured reference can be entered under Reference rate
and limits.

## Connect flow

The top-right button sends one service ID and one Solace Cloud API token over
HTTPS to a read-only Python Lambda function. The function calls the fixed Solace
Cloud API host and the service's SEMP Read-Only endpoint. It reports success
only when SEMP config and monitor reads succeed. It returns broker class,
configured limits and current VPN counters; it never returns SEMP credentials
or the token. The token is cleared from the browser input after each request
and is not saved in local storage or AWS logs by application code.
The Lambda URL requires a private CloudFront origin header, limits concurrency to
two, and does not accept arbitrary URLs or SEMP writes.

The Connected broker view is an on-demand snapshot. It shows current ingress
from SEMP divided by the Model view's illustrative publish-rate estimate, then
separate utilization of connection, spool and subscription allowances when SEMP
returns both numerator and denominator. These percentages are independent;
they are not a single measured broker saturation percentage. The workload mix
is selected on the Model view, not inferred from SEMP. Refresh requires a new
token entry because the browser does not retain credentials. SEMP collection
counts and low-frequency polling guidance are documented in
[SEMP Features](https://docs.solace.com/Admin/SEMP/SEMP-Features.htm).

## Publish

Use the AWS SSO session for account `928672620016`:

```sh
aws sso login --profile 928672620016_AdministratorAccess
python3 site/build.py
python3 site/deploy.py
```

The build copies the HTML page, experiment JSON, and CSV to `site/dist`. Deployment
uses a private S3 bucket, CloudFront origin access control, Route 53, an ACM
certificate and the read-only Lambda. Infrastructure identifiers are in
`site/deployment.json`; the origin secret is in ignored `.pilot/` with mode 0600.

Hosting is separate from the experiment's shared $20/day generator/traffic
guard. The site does not start trials or provision brokers.

## Planned calibration campaign

Run `python3 scripts/design_site_model_300.py` to regenerate
`campaigns/site-model-300.csv`. The 300 rows are workload configurations, not
300 fixed publish rates or observed results. Each row includes SDKPerf command
templates where the selected API supports them, required broker-side setup,
and capability gates. Each configuration targets a broker-attributed saturation
bracket no wider than 100 msg/s; if drift or noise prevents that resolution,
the result is inconclusive rather than a rounded capacity point. A controller must substitute broker credentials and a
rate-search target, qualify the generator and consumer path, and record a
broker-attributed bracket before treating a row as a capacity label. AMQP has
no SDKPerf command in the installed package and needs a separate load client;
REST consumers need an RDP; XA, transactions, tracing, active browsing, user
properties, publisher skew, and connection churn need corresponding adapters
or setup. Twenty 200K rows are provisional because the public Solace Cloud
service-class table does not list 200K.

The Experiments page reads `site/dist/experiments.json`, generated from the CSV
by `site/build.py`, and offers a status-enriched CSV for download. It shows TODO,
RUNNING, and DONE independently of experiment readiness. Selecting a table row
shows its purpose, configuration, setup, and commands in a panel to the right
(below the table on narrow screens). With no progress file,
all 300 rows are TODO. The projected completion range assumes one dedicated
broker per service class, one trial per broker, 12–20 minutes per remaining
configuration, and 4–12 hours for setup and qualification. It is a hypothetical
start-now estimate that updates from the remaining class counts, not a schedule
or a claim that adapter and provisional rows are runnable. The estimate is
subject to the existing $20/day generator and traffic cap. A controller can supply
`campaigns/site-model-300-progress.json` with an `experiments` map keyed by ID:

```json
{"updated_at":"2026-11-02T17:00:00Z","experiments":{"W001":{"status":"RUNNING","started_at":"2026-11-02T16:59:00Z","updated_at":"2026-11-02T17:00:00Z"},"W002":{"status":"DONE","result_ref":"data/run-001/W002/result.json","broker_attributed":true,"capacity_interval_msg_s":{"lower":42300,"upper":42400},"updated_at":"2026-11-02T16:45:00Z"}}}
```

The build rejects unknown IDs, requires a result reference for DONE, and only
publishes a capacity point when it is flagged as broker-attributed and its
supplied interval is at most 100 msg/s wide. Rebuild
and publish after progress changes; the site shows the progress snapshot time.
