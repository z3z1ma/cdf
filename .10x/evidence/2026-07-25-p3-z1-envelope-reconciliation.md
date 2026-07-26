Status: recorded
Created: 2026-07-25
Updated: 2026-07-25
Relates-To: .10x/tickets/done/2026-07-11-p3-z1-envelope-evidence-reconciliation.md

# P3 Z1 final performance-envelope reconciliation

## Observation

The P3 closeout envelope is now generated from a checked reconciliation manifest that preserves
the host, mode, reference, bias, memory result, durable evidence record, and underlying source
artifacts for every target. It also joins the exact registered destination descriptors to the
current EC2 destination report; invented or drifted destination identities remain rejected.

The published matrix contains six green performance targets, three partial targets, one
user-accepted residual, and one target that remains not demonstrated. The document does not turn
the Parquet physical-package gap, CSV absolute-rate miss, TLC composite miss, 1 TiB unique-byte
device-saturation limit, or absent aggregate correctness-overhead comparator into green claims.
The immutable pre-optimization report remains separately published as
`docs/performance-baseline.md`.

All implementation workstreams, performance-triage tickets, fixed-schema admission, statistics
pruning, and implemented foreign boundaries are terminal. The active ticket inventory after this
reconciliation contains only the P3 aggregate Z2/Z3/parent and P1 release/closeout graph.

## Procedure

The committed freshness and fail-closed checks were:

```text
cargo fmt --all -- --check
DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 \
  cargo test -p cdf-benchmarks --test lab_policy --locked -j 12
DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 \
  cargo clippy -p cdf-benchmarks --all-targets --locked -j 12 -- -D warnings
git diff --check
```

The lab-policy suite passed eight tests. It regenerates both the immutable pre-optimization
baseline and final closeout document exactly, verifies the live first-party destination catalog,
rejects missing source artifacts, requires explicit acceptance authority for accepted residuals,
and retains the existing host/mode/work/reference drift and destination-descriptor falsifiers.
Strict all-target Clippy passed.

The manifest is
`crates/cdf-benchmarks/fixtures/p3-closeout-envelope.json`; the published output is
`docs/performance-envelope.md`; the exact destination report is
`.10x/evidence/.storage/p3-destination-matrix-ec2-current.json`.

## What it supports or challenges

This supports P3 Z1's requirement for one host-labelled, generated, fail-closed performance
authority and removes the stale README claim that the published document is still a
pre-optimization placeholder. It challenges the original P3 parent statement that every
aspirational absolute target is green: the current implementation architecture is complete, but
the final evidence honestly retains non-green cells rather than resetting baselines or combining
incomparable hosts.

## Limits

The reconciliation manifest normalizes heterogeneous evidence records; it does not parse every
historical benchmark format into one fictitious host report. Its source-path and destination joins
are executable, while the normalized prose/numbers remain reviewable claims backed by the linked
raw artifacts. Z2 owns the aggregate demonstration and adversarial workload review; Z3 owns the
program-level acceptance reconciliation and retrospective.
