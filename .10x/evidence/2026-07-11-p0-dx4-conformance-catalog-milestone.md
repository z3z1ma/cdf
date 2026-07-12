Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p0-dx4-conformance-extension-law.md, .10x/tickets/2026-07-11-p0-destination-extension-boundary.md

# Destination conformance catalog milestone

## What was observed

All stale conformance calls to the removed `ResolvedProjectDestination::{duckdb,parquet_filesystem,postgres}` constructors were replaced by one catalog-backed `DestinationRegistry`. DuckDB, Parquet, and Postgres are enrolled in one table; every migrated fixture resolves by URI, target, policy, and injected execution services through its driver-owned runtime adapter.

The conformance crate compiles under strict all-target Clippy. Its catalog/static architecture tests pass, and all four golden-package tests pass, including 100 deterministic rebuilds. The live DuckDB golden proceeds past destination resolution and then fails with `compiled declarations are not executable; resolve their typed source driver`, proving that the remaining failure is the active SX1 source migration rather than destination composition.

## Procedure

```text
cargo check -p cdf-conformance --tests --locked
cargo clippy -p cdf-conformance --tests --locked -- -D warnings
cargo test -p cdf-conformance destination_catalog --locked
cargo test -p cdf-conformance golden_package --locked
cargo test -p cdf-conformance live_local_file_duckdb_v1_matches_committed_golden_across_100_runs --locked
```

The first four checks passed. The final command reached source execution and failed at the typed-source resolution guard described above.

## What this supports or challenges

This supports the destination-extension invariant: conformance no longer requires destination-specific constructors in the generic project crate. It also demonstrates that the fail-closed source registry is live.

## Limits

This is not DX4 closure. The fourth-driver full law, complete data-driven assertion catalog, Cargo rebuild timing, and removal of temporary test-only concrete project helpers remain. Cross-destination live/run-matrix/chaos execution awaits SX1 fixture migration.
