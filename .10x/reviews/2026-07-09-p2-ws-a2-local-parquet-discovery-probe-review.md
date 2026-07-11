Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-a2-local-parquet-discovery-probe.md
Verdict: pass

# P2 WS-A2 local Parquet discovery probe review

## Target

Review of the A2 local Parquet discovery implementation and closure records.

## Findings

- Pass: The `cdf-formats` probe uses `ArrowReaderMetadata::load` and not the existing `ParquetRecordBatchReaderBuilder` read path. The acceptance-specific `rg` scan found no `RecordBatch`, `ParquetRecordBatchReader`, or `build(` references in the new probe module.
- Pass: The project helper keeps source identity separate from `SchemaSnapshotArtifact::hash_input`. The test intentionally includes `/tmp/private/orders.parquet` and `sha256:footer` in the identity map and proves neither appears in the snapshot hash input.
- Pass: Invalid input produces a `Data` error that names `Parquet metadata discovery`, which is actionable enough for this API slice.
- Pass: The final implementation avoided adding a direct `cdf-project -> cdf-formats` dependency, so `Cargo.lock` stayed untouched and the worker stayed inside the requested write scope.

## Residual risk

- The local source identity is strong enough for a later probe cache but is not a full file-content hash; it combines size, mtime, row count, row groups, and footer hash. That is intentional for a footer/schema probe that must not read all row data. File-manifest exactness remains owned by WS-D.
- The footer hash uses stable local CDF schema hash input plus Parquet metadata debug text and selected row-group metadata. If future probe-cache semantics need cross-version persistence guarantees for the footer hash itself, that should get a focused decision before external cache compatibility depends on it.
- Parent integration ran CodeQL through `tools/codeql-rust-quality.sh` and the reusable database path. The SARIF result set is the three pre-existing P1 backfill fixture findings in `crates/cdf-cli/src/tests.rs`, owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`; no WS-A2 finding was introduced.
- Semgrep, Gitleaks, cargo-deny, cargo-audit, cargo-vet, clippy, tests, jscpd, and rust-code-analysis passed. OSV reports only the already-ratified `paste` advisory `RUSTSEC-2024-0436`.

## Verdict

Pass. The ticket acceptance criteria are met with the exclusions intact.
