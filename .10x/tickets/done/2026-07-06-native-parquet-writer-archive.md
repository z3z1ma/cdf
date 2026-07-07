Status: done
Created: 2026-07-06
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md, .10x/tickets/done/2026-07-05-parquet-object-store-destination.md, .10x/tickets/done/2026-07-06-package-archive-persistence-cli.md

# Replace DuckDB Parquet writer and archive path with native Arrow/DataFusion path

## Scope

Replace DuckDB-backed Parquet writing in package archive transcode and the filesystem/object-store Parquet destination with native Arrow/DataFusion-aligned Parquet writing using the ratified dependency path from `.10x/decisions/native-arrow-datafusion-parquet-policy.md`.

Expected ownership:

- `crates/cdf-package/**` for archive/transcode primitives and fidelity reporting
- `crates/cdf-dest-parquet/**` for destination materialization
- `crates/cdf-cli/**` only if `cdf package archive` output needs narrow wording updates
- `Cargo.toml`, `Cargo.lock`, and crate manifests required for the native Parquet dependency
- `.10x/` evidence/review/ticket records for this child

## Acceptance criteria

- `cdf-package` archive transcode writes Parquet through native Arrow/DataFusion-compatible writer APIs, not DuckDB export.
- `cdf-dest-parquet` append/replace materialization writes Parquet through the native writer path while preserving receipt verification, object manifests, replace pointers, and idempotency behavior.
- Existing package identity remains Arrow IPC based; archive Parquet sidecars and archive metadata remain outside identity as currently specified.
- Existing destination conformance coverage for Parquet still passes, with added or updated tests proving native writer output can be read back and receipts still verify.
- Fidelity reports remain honest about Arrow-to-Parquet projection limits.
- The dependency graph contains the ratified native Parquet path and no unratified advisories.
- `cargo deny`, `cargo audit`, OSV, and cargo-vet evidence explicitly distinguish the ratified `RUSTSEC-2024-0436` exception from any other finding.

## Evidence expectations

Run focused `cargo fmt --all -- --check`, `git diff --check`, `cargo test -p cdf-package -p cdf-dest-parquet --locked --no-fail-fast`, `cargo clippy -p cdf-package -p cdf-dest-parquet --all-targets --locked -- -D warnings`, destination conformance tests covering Parquet, dependency/advisory scanners, source unsafe scan, and bounded mutation testing over the changed writer/fidelity modules when feasible.

Before closure, run relevant `QUALITY.md` gates with independent checks parallelized where practical and CodeQL through `tools/codeql-rust-quality.sh`.

## Explicit exclusions

No Parquet file-source reader replacement, no package state/commit artifact changes, no DuckDB destination behavior, no native policy broadening beyond `.10x/decisions/native-arrow-datafusion-parquet-policy.md`, no package identity format change, and no `.gitignore` edits.

## References

- `.10x/decisions/native-arrow-datafusion-parquet-policy.md`
- `.10x/research/2026-07-06-native-parquet-paste-risk.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/tickets/done/2026-07-05-parquet-object-store-destination.md`
- `.10x/tickets/done/2026-07-06-package-archive-persistence-cli.md`
- `QUALITY.md`

## Progress and notes

- 2026-07-06: Opened after user ratification of native Arrow/DataFusion Parquet. The existing DuckDB-backed writer paths remain the behavioral baseline until this ticket closes.
- 2026-07-07: Unblocked by `.10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md`. When this ticket adds native Parquet dependencies, it must prove the actual advisory path and scanner behavior rather than relying only on the dormant policy exception.
- 2026-07-07: Activated after `.10x/tickets/done/2026-07-06-native-parquet-file-source.md` closed. Current slice should remove DuckDB-backed Parquet writing from archive transcode and Parquet destination materialization while preserving IPC package identity, archive sidecar semantics, receipts, and replace-pointer/idempotency behavior.
- 2026-07-07: Bounded worker replaced package archive/destination Parquet writing with the native arrow-rs `ArrowWriter` helper in `cdf-package`, removed the `cdf-dest-parquet` DuckDB writer shim, switched archive/destination Parquet readback tests to arrow-rs `ParquetRecordBatchReaderBuilder`, and updated manifests/lockfile so the scoped crates depend on `parquet` rather than DuckDB. Focused checks passed: `cargo fmt --all -- --check`; `git diff --check`; `cargo test -p cdf-package -p cdf-dest-parquet --locked --no-fail-fast`; `cargo clippy -p cdf-package -p cdf-dest-parquet --all-targets --locked -- -D warnings`; `cargo tree -p cdf-package --edges normal --locked` and `cargo tree -p cdf-dest-parquet --edges normal --locked` showed no DuckDB edge.
- 2026-07-07: Closed with evidence `.10x/evidence/2026-07-07-native-parquet-writer-archive.md` and review `.10x/reviews/2026-07-07-native-parquet-writer-archive-review.md`. Workspace compile, clippy, tests, docs, nextest, cargo-hack, dependency policy scanners, Semgrep, Gitleaks, source unsafe scans, and bounded mutation testing passed or were explicitly limited in the evidence record. CodeQL was intentionally skipped per active goal/user instruction; `cargo geiger` hung and is recorded as a tool limitation.

## Blockers

None.
