# Terminal grid runner validation — 2026-10-06

The 100-workload live campaign is ready to start with `./run-grid run --directory data/grid-100`. Only validation workloads were run during implementation.

## Completed checks

- 58 repository tests passed: `PYTHONPATH=src python3 -m unittest discover -s tests -q`.
- Python compilation and tracked-file whitespace checks passed.
- A complete synthetic campaign finished 100 configurations and 1,190 rate stages. All synthetic intervals reached the configured 10% width. Resuming completed campaigns created no additional stages. The preserved [synthetic results](../data/grid-runner-simulation-results.json) are explicitly simulated.
- Live Direct and persistent SMF/TLS tests ran against the dedicated 5K, each with four publishers, two destinations, 256-byte messages and 100 aggregate messages/second. Each configuration passed one 30-second probe and two 30-second confirmations. These short checks validate the execution path, not the broker's ceiling or the default 120-second confirmation horizon.
- Every live stage submitted 2,996 messages; broker data ingress matched exactly and egress counted 5,992 deliveries. Each destination observed all 2,996 unique messages. Every persistent publish was acknowledged. [Raw evidence and reports](../data/grid-validation-data-counters/report.md) and the [final classifier replay](../data/grid-validation-data-counters/release-recheck.json) are retained.
- An actual Ctrl-C interrupted a live confirmation, left its completed probe intact, and resumed by retrying only the interrupted stage. Evidence remains in `data/grid-validation/ledger.sqlite`. A concurrent invocation was refused by the campaign lock.

## Findings incorporated

SEMP's total message counters include control traffic. The final adapter reconciles data-message counters instead and brackets measurement with settled snapshots. Receiver lag is calculated from time-aligned publisher/receiver samples; comparing their latest, differently timed samples produced false growth. Missing aligned evidence is now inconclusive. Persistent receipt listeners are installed after publisher startup, as required by the installed SDK.

Earlier `grid-smoke*`, `grid-validation`, `grid-validation-final`, and `grid-validation-settled` directories contain debugging evidence from earlier revisions. They are preserved, but must not be used as capacity-model training labels.

## Limits

These are execution and detector checks. No intrinsic broker saturation ceiling has been established. Live results measure the local generator, network path and broker together. Abrupt controller death with durable backlog has a recovery implementation, but was not validated by deliberately creating and abandoning a live backlog. Shared/demo brokers remain excluded. See the [runner documentation](terminal-grid-runner.md) for the exact grid, guardrails, resume behavior and output format.
