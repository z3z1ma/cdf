Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-p0-workstream-a-streaming-commit-session.md, .10x/decisions/commit-session-segment-write-api.md, .10x/tickets/2026-07-07-p0-structural-debt-program.md

# Streaming CommitSession API

## What was observed

Workstream A replaced the no-payload destination session write shape with the ratified segment-write API.

Observed source state:

- `crates/cdf-kernel/src/destination.rs` defines `CommitSegment`, `CommitSession::write_segment(&mut self, CommitSegment) -> Result<SegmentAck>`, required `DestinationProtocol::begin`, and trait-level `DestinationProtocol::verify`.
- `crates/cdf-package/src/reader.rs` exposes `PackageReader::read_commit_segments`, preserving requested state segment byte counts separately from package IPC byte counts.
- DuckDB, Parquet, and Postgres sessions accept segments incrementally, reject duplicate/unknown/mismatched segments, and finalize only after all expected segments are accepted.
- DuckDB and Parquet convert accepted commit segments back into their existing package commit data model before finalization, preserving prior receipt/package behavior.
- Postgres stages accepted segments and performs the existing transactional write path after the last expected segment is accepted, preserving its receipt and rollback semantics.
- `crates/cdf-project/src/runtime.rs` feeds package segments into sessions one at a time and calls receipt verification through `DestinationProtocol::verify` for DuckDB, Parquet, and Postgres recovery/checkpoint gates.

The old no-data `CommitSession::write()` shape and the old error-returning default `DestinationProtocol::begin` were not found by:

```text
rg -n "session\\.write\\(|fn write\\(&mut self\\)|default begin|does not support commit sessions" crates -g '*.rs'
```

The command exited with no matches.

## Procedure

Local verification commands observed for this slice:

- `cargo fmt` was run after `cargo fmt --check` reported formatting diffs.
- `cargo fmt --check` passed.
- `cargo check --workspace` passed.
- `cargo clippy -p cdf-kernel -p cdf-package -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres -p cdf-project -p cdf-conformance --all-targets -- -D warnings` passed after two local `collapsible_if` fixes in the Workstream A diff.
- `cargo test -p cdf-kernel -p cdf-package -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres -p cdf-project -p cdf-conformance --no-fail-fast` passed. Observed counts included 9 kernel tests, 28 package tests, 12 DuckDB tests, 20 Parquet tests, 27 Postgres tests, 62 project tests, 40 conformance tests, and doc-tests for the selected crates.
- `git diff --check -- crates/cdf-kernel crates/cdf-package crates/cdf-dest-duckdb crates/cdf-dest-parquet crates/cdf-dest-postgres crates/cdf-project crates/cdf-conformance` passed.
- `jscpd --min-lines 8 --min-tokens 80 --threshold 100 --reporters console` over the touched Rust paths passed. It reported 49 clones, 894 duplicated lines, 6,147 duplicated tokens, 2.87% duplicated lines, and 3.18% duplicated tokens. The remaining notable clones are destination session helper shapes and pre-existing `cdf-project/src/runtime.rs` replay/recovery families; Workstream B owns collapsing the runtime wrapper family.
- `scc` over the touched path set passed and reported 80 Rust files, 31,132 lines, 28,104 code lines, and aggregate complexity 1,367.
- `rust-code-analysis-cli -m -O json -p crates/cdf-kernel/src -p crates/cdf-package/src -p crates/cdf-dest-duckdb/src -p crates/cdf-dest-parquet/src -p crates/cdf-dest-postgres/src -p crates/cdf-project/src -p crates/cdf-conformance/src > /tmp/cdf-workstream-a-rust-code-analysis.json` exited 0. A `jq` summary over 2,008 metric nodes identified the hottest units as `crates/cdf-project/src/runtime.rs` with cyclomatic 546/cognitive 135 and `crates/cdf-dest-postgres/src/source.rs` with cyclomatic 438/cognitive 137. These hotspots are existing structural targets for Workstream B and source/runtime follow-up, not new Workstream A blockers.

CodeQL was intentionally not run. The standing user instruction is to avoid recreating the CodeQL database because it is expensive; this slice did not require a new database to prove the segment-session contract.

Golden fixtures were not regenerated. The evidence above supports that this API refactor preserved package/receipt semantics; no package manifest, receipt format, or golden artifact hash change was intentionally introduced by Workstream A.

## What this supports

This supports closing `.10x/tickets/done/2026-07-07-p0-workstream-a-streaming-commit-session.md` after adversarial review:

- The kernel API decision has an implemented shape.
- `DestinationProtocol::begin` is mandatory for implementors.
- Receipt verification is trait-level and is used by project recovery/checkpoint gates.
- Fully materialized package replay can feed recorded segments through the same session API that future streaming commits will use.
- DuckDB, Parquet, and Postgres preserve existing destination-owned receipt and idempotency behavior under the new API.

## Limits

This evidence does not claim Workstream B is complete. `crates/cdf-project/src/runtime.rs` still contains destination-specialized replay, recovery, and failpoint families.

This evidence does not claim Workstream C is complete. Conformance consumes focused destination and project tests, but the full matrix over source archetype, destination, disposition, replay, recovery, chaos, and property/fuzz targets remains owned by Workstream C.

This evidence does not claim durable per-segment destination settlement. The active decision states that `SegmentAck` means the session accepted the segment; the final `Receipt` remains durable settlement.
