Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-v2-validation-graph-integration.md

# Live file fixture resolves through the source registry

## What was observed

The canonical local NDJSON → package → DuckDB fixture now resolves its compiled source plan with `FileSourceDriver` through `SourceRegistry`. Planning and execution consume the returned `QueryableResource`; the generic declaration is never executed.

The first current-path run produced deterministic partition-owned segment identity `p00000000-s00000000` and corresponding current plan, lineage, position, stats, and commit artifacts. The committed golden was updated from the superseded `seg-000001` layout. One hundred clean rebuild/run/package/receipt/checkpoint comparisons then passed.

## Procedure

```text
cargo test -p cdf-conformance live_local_file_duckdb_v1_matches_committed_golden_across_100_runs --locked
cargo clippy -p cdf-conformance --tests --locked -- -D warnings
```

Both commands passed. The 100-run law completed in 51.81 seconds in the debug test profile.

## What this supports or challenges

This supports registry-only file execution, deterministic current-format package identity, and source/destination extension-boundary composition in the live harness. It challenges the old golden as obsolete architecture, not a compatibility contract.

## Limits

The fixture is two NDJSON rows and does not measure throughput or RSS. Its file-only resolver is scoped to conformance; SX1 still owns the complete first-party source fixture catalog, REST/Postgres migrations, static graph law, and performance evidence.
