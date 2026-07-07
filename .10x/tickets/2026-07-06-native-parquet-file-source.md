Status: open
Created: 2026-07-06
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md, .10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md

# Replace DuckDB Parquet file source with native Arrow/DataFusion path

## Scope

Replace the current DuckDB-backed Parquet file-source reader in `cdf-formats` with a native Arrow/DataFusion-aligned Parquet implementation using the ratified dependency path from `.10x/decisions/native-arrow-datafusion-parquet-policy.md`.

Expected ownership:

- `crates/cdf-formats/**`
- `Cargo.toml`, `Cargo.lock`, and crate manifests required for the native Parquet dependency
- focused conformance or package replay tests only where the file-source behavior requires updates
- `.10x/` evidence/review/ticket records for this child

The implementation should remove the Arrow 58 DuckDB IPC bridge when it is no longer needed by `cdf-formats`.

## Acceptance criteria

- Parquet file sources still produce deterministic descriptors and batches for supported local files.
- Existing CSV, JSON, and NDJSON behavior is unchanged.
- Tests cover valid Parquet reads, malformed Parquet errors, schema hash population, source positions, and package write/replay compatibility.
- The implementation uses native Arrow/DataFusion Parquet rather than DuckDB's `read_parquet` path.
- The dependency graph contains the ratified native Parquet path and no unratified advisories.
- `cargo deny`, `cargo audit`, OSV, and cargo-vet evidence explicitly distinguish the ratified `RUSTSEC-2024-0436` exception from any other finding.
- No destination writer, package archive writer, native Parquet policy, or `.gitignore` behavior changes are included.

## Evidence expectations

Run focused `cargo fmt --all -- --check`, `git diff --check`, `cargo test -p cdf-formats --locked --no-fail-fast`, `cargo clippy -p cdf-formats --all-targets --locked -- -D warnings`, dependency/advisory scanners, source unsafe scan, and bounded mutation testing over the changed Parquet reader path when feasible.

Before closure, run relevant `QUALITY.md` gates with independent checks parallelized where practical and CodeQL through `tools/codeql-rust-quality.sh`.

## Explicit exclusions

No Parquet destination writer replacement, no package archive writer replacement, no package state/commit artifact changes, no CLI behavior, no native policy broadening beyond `.10x/decisions/native-arrow-datafusion-parquet-policy.md`, and no `.gitignore` edits.

## References

- `.10x/decisions/native-arrow-datafusion-parquet-policy.md`
- `.10x/research/2026-07-06-native-parquet-paste-risk.md`
- `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md`
- `QUALITY.md`

## Progress and notes

- 2026-07-06: Opened after user ratification of native Arrow/DataFusion Parquet. The previous DuckDB-backed implementation remains the behavioral baseline until this ticket closes.
- 2026-07-07: Unblocked by `.10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md`. When this ticket adds native Parquet dependencies, it must prove the actual advisory path and scanner behavior rather than relying only on the dormant policy exception.

## Blockers

None.
