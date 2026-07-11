Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-g2-type-mismatch-diagnostics.md, .10x/decisions/deep-validation-sampling-warnings-and-type-allowances.md

# P2 G2 deep type diagnostics evidence

## What was observed

Deep validation now resolves file partitions through the production transport facade, probes observed physical schemas without writes, and sends observed/constraint schemas through the shared reconciliation policy. Tier-0 `coerce_types` and `allow_lossy_mapping` values reach discovery, validation-program compilation, preview/plan/run resource execution, and destination planning. Diagnostics include stable codes, severity, resource, safe source location, exact Arrow types, field, and available remedies.

Successfully decoded JSON row-local mismatches remain warning verdicts. Malformed JSON and physical probe failures remain errors. URL query values and fragments are not rendered.

## Procedure

- `cargo test -p cdf-kernel -p cdf-formats -p cdf-declarative -p cdf-project`: 22, 35, 95, and 168 tests passed respectively.
- `cargo test -p cdf-cli`: 262 tests passed and one stale pre-P2 expectation failed; that test asserted NDJSON discovery remained unsupported. It was updated to assert the now-supported pin/package/checkpoint path and passed independently.
- `cargo test -p cdf-cli validate_deep_`: 5 deep-validation regression tests passed, including exact Parquet types, explicit allowance, governed JSON mismatch warning, malformed JSON failure, and no-write checks.
- `cargo test -p cdf-cli run_ndjson_discover_schema_resource_autopins_and_commits`: passed.
- `cargo test -p cdf-cli safe_file_location_removes_every_query_value_and_fragment`: passed.
- `cargo clippy -p cdf-kernel -p cdf-formats -p cdf-declarative -p cdf-project -p cdf-cli --all-targets -- -D warnings`: passed after grouping transport/spool dependencies behind `FileRuntimeDependencies`.
- `git diff --check`: passed.

## What this supports

This supports every G2 acceptance criterion for the supported Parquet and JSON-family surface, including no-write deep validation and policy behavior in actual execution rather than CLI-only reporting.

## Limits

Cloud-specific catalog completeness, Python/WASM sources, and final all-archetype P2 conformance remain owned by the WS-G parent and WS-I.
