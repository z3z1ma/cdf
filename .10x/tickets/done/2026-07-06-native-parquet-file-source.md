Status: done
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
- 2026-07-07: Activated for implementation. Current `cdf-formats` Parquet reader uses DuckDB `read_parquet(?)` plus an Arrow 58 IPC bridge in `crates/cdf-formats/src/readers.rs`; this ticket replaces that with a native Arrow-aligned Parquet reader while preserving existing CSV/JSON/NDJSON behavior.
- 2026-07-07: Code portion implemented in `cdf-formats`: removed the crate's DuckDB/Arrow 58 Parquet reader bridge, added direct `parquet 59.0.0` native Arrow reader usage, updated malformed Parquet expectations, and kept package replay coverage passing. Focused checks passed: `cargo fmt --all -- --check`, `cargo test -p cdf-formats --locked --no-fail-fast`, `cargo clippy -p cdf-formats --all-targets --locked -- -D warnings`, `git diff --check`, and dependency inverse checks for `paste`/`parquet`.
- 2026-07-07: Closed with evidence `.10x/evidence/2026-07-07-native-parquet-file-source.md` and review `.10x/reviews/2026-07-07-native-parquet-file-source-review.md`. Full workspace check, clippy, test, focused nextest, docs, deny, audit, vet, OSV, Semgrep, gitleaks, dependency graph, and unsafe-scan evidence support closure. CodeQL was skipped per active goal instruction; OSV/cargo-audit report only the ratified `RUSTSEC-2024-0436` `paste` advisory path.

## Blockers

None.
