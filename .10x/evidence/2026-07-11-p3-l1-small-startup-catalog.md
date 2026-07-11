Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-l1-small-startup-catalog-followup.md

# P3 L1 small/startup catalog follow-up evidence

## What was observed

Catalog schema v2 adds a `benchmark_fixture` recipe that must match the committed fixture catalog version, fixture name, generator-version constant, rows, and batch size. Tiny is eight rows in one batch with a 1 MiB per-format ceiling; medium is 2,048 rows in 512-row batches with an 8 MiB ceiling. Fixture bytes remain generated in temporary directories and are not committed.

`legacy_tiny_startup_e2e` explicitly includes process startup, fixture generation, project compile, pipeline, DuckDB receipt, and checkpoint. `legacy_medium_ndjson_package` explicitly excludes process startup/fixture generation/destination and measures prepared source reads through verified package finalization. Each declares distinct logical and physical byte authorities.

The report fixture now uses `legacy_medium_throughput` / `legacy_medium_ndjson_package`, eliminating its previous orphan identifiers.

## Procedure and results

```text
CARGO_INCREMENTAL=0 cargo test -j1 -p cdf-benchmarks --locked
CARGO_INCREMENTAL=0 cargo clippy -j1 -p cdf-benchmarks --all-targets --locked -- -D warnings
cargo fmt --all -- --check
git diff --check
```

- Twenty tests passed across provider, fixture, comparison/envelope, and runner targets.
- Tiny/medium generation is deterministic across all local formats and every emitted file stays within its declared ceiling.
- Catalog validation rejects fixture recipes that diverge from the actual generator authority.
- Generated envelope freshness remained green after report identity alignment.
- Clippy passed every benchmark target with warnings denied.

Canonical identities intentionally changed:

- catalog v2: `sha256:a795621b04fbbaf27706554c844cd2766abe6e8777f0eb29a7caa631ce2ffa98`;
- aligned report fixture: `sha256:ec1c1216b0c68e6167fc32b6e26804190608f52def93f9f69187eb68d1b345f1`.

## Limits

This follow-up defines deterministic data/workload authority only. It does not execute or claim startup/throughput measurements; L5 owns that baseline observation.
