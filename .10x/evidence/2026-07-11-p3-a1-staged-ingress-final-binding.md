Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md, .10x/specs/streaming-destination-ingress.md, .10x/decisions/destination-staged-ingress-final-package-binding.md

# P3 A1 staged-ingress and verified final-binding evidence

## What was observed

`cdf-runtime` now exposes one destination-neutral pre-finalization state machine. A staged request is keyed by operational `LoadAttemptId` plus immutable destination/target/disposition/schema/plan authority and bounded scheduling context. Durable segment acknowledgements contain no package hash, idempotency token, receipt id, or committed `SegmentAck`. The only public constructor for final authority consumes a `PackageReader` and successful `VerificationReport`, reconstructs the package-hash token, and requires manifest and state-delta segment order to agree.

All existing destinations declare `FinalizedPackageOnly`; the additive default fails closed if staged ingress is requested. Destination implementation remains owned by destination crates.

## Procedure

- `cargo test -p cdf-runtime --locked` — 9 passed.
- `cargo test -p cdf-package --lib --locked` — 34 passed.
- `cargo test -p cdf-project --lib --locked` — 171 passed.
- `cargo test -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres --lib --locked` — DuckDB 21, Parquet 27, Postgres 40 passed.
- `cargo clippy -p cdf-runtime -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres -p cdf-project --all-targets --locked -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.

The runtime conformance specifically exercises exact ordered segment identity, mismatched attempt authority, crash/reattach snapshots, no receipt before final binding, duplicate package receipt reuse, idempotent abort, and finalized-only rejection. The dependency law asserts `cdf-runtime` has no project, engine, concrete-destination, DataFusion, or DuckDB dependency.

## What this supports

This supports every A1 acceptance criterion and establishes the honest pre-commit boundary needed for later destination overlap: attempt identity is operational only, final receipts remain package-hash based, and no provisional package identity leaks into staging.

## Limits

This slice defines and proves the neutral contract. It intentionally does not add Tokio channels, memory accounting, destination-specific staging, bulk encoding, or a throughput claim; those remain owned by the activated WS-A and WS-D children.
