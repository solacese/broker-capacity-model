# Updating the capacity model

The browser's reference rates, workload costs and feature coefficients come
from [`calibration-v1.json`](calibration-v1.json). `site/build.py` validates
and embeds that file into the single HTML page. A new feature can be added to
the `features` array with a unique ID, Solace terminology, a group, its
applicability (`mode`, `protocol`, or `protocols` when relevant), a documentation
URL and help text, and an illustrative cost factor. The feature then appears
in the grouped controls and the rate calculation after rebuilding the site.

The initial numbers are **illustrative**. Editing them does not make them
measured effects. Keep `status: ILLUSTRATIVE` until an evidence-backed,
workload- and class-scoped calibration schema is reviewed and implemented.
The page labels its coefficients accordingly. A service-class base rate must
come from a broker-attributed boundary, never from a stable lower observation.

## Repeatable evidence path

1. Copy the study spec to a new versioned filename before changing factor,
   workload or statistical thresholds. Never edit the spec behind a frozen
   plan. Run `./prepare-paired-study prepare --spec <spec> --directory <new-data-dir>`
   and record the plan's SHA-256 in the run manifest.
2. Freeze service IDs and exact broker build, SDKPerf/Java binary hashes,
   generator AMI and VM type, region/AZ, resource inventory, command lines,
   payload seed and all costs. Run the randomized schedule without changing
   arm order. Store raw SEMP, SDKPerf and worker time series under each slot.
3. Produce a verified lower/upper boundary artifact for each arm. Mark
   client-limited, quota-limited or missing data inconclusive. Run
   `./prepare-paired-study analyze --spec <spec> --directory <new-data-dir>
   --results <reviewed-results.csv>` and save that report with the input hashes.
4. Compare the 12 paired effects and four session checks. A 90% effect interval
   inside ±5% supports practical equivalence for that class and reference
   workload; a 95% interval wholly outside ±5% supports a material effect.
   Otherwise run a **new** preregistered 12-pair batch or fix the apparatus.
5. Check two other payload/fanout contexts and an independent broker instance
   before proposing a portable feature multiplier. A reviewer records the
   exact scope, effect interval, broker version, result paths and date in a
   new calibration proposal. Preserve the prior version and every raw artifact.
6. Update the JSON coefficients only after review. Run `python3 site/test_build.py`,
   `python3 site/build.py`, and inspect the local page. Deploy the rebuilt page
   with the existing AWS SSO profile when the revision is approved.

The current website schema has global illustrative factors. A measured effect
that changes with payload, fanout, delivery mode or broker class requires a
scoped coefficient implementation and its own tests before it can replace a
global factor. The paired-study analyzer deliberately returns
`global_coefficient_status: NOT_ESTIMATED` even after a class-specific A/B
decision.
