Status: done
Created: 2026-07-08
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md
Depends-On: .10x/decisions/data-onramp-file-source-transport-manifest.md, .10x/specs/data-onramp-file-sources-transports.md

# P2 WS-D1 file glob partition planning

## Scope

Replace the single-file live-run gate with deterministic file partition planning for modest-N local file globs, while keeping manifest incrementality, remote transports, compression, and large-N coalescing for later children.

Owned write scope:

- `crates/cdf-declarative/src/file_runtime.rs`
- `crates/cdf-declarative/src/compiled.rs`
- `crates/cdf-declarative/src/tests.rs`
- `crates/cdf-conformance/**` only for focused preview/run parity or partition planning assertions
- this ticket's evidence/review records

## Acceptance criteria

- A glob matching multiple local files plans deterministic file partitions instead of failing with "narrow the glob to exactly one file".
- Each partition carries file-scoped metadata and scope sufficient for later `FileManifest` incrementality.
- Preview and run use the same deterministic file match ordering and file-resolution front end for local globs.
- Zero-match globs still fail with an actionable data error.
- Existing single-file behavior is preserved.
- Large-N coalescing remains explicitly deferred; modest-N tests must not encode a hidden threshold.

## Evidence expectations

Record focused evidence for:

- `cargo test -p cdf-declarative <new glob partition tests> --locked`
- `cargo test -p cdf-declarative --locked`
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`
- `cargo fmt --all -- --check`
- `git diff --check`
- jscpd scoped to touched Rust files

## Explicit exclusions

This ticket does not implement default manifest incrementality, compression, remote transports, schema variance policy, or no-op reruns.

## Progress and notes

- 2026-07-08: Opened after inspection found `resolve_single_file` and `resolve_preview_file` as divergent paths in `crates/cdf-declarative/src/file_runtime.rs`.
- 2026-07-09: Activated after WS-B1, WS-C1, WS-F1, WS-A1, and WS-C2 closed. Parent inspection confirmed current code still plans one `files` partition, run rejects multi-match globs with "narrow the glob to exactly one file", and preview reads the first sorted match. Work is assigned to a worker with this ticket as the owning executable slice.
- 2026-07-09: Implemented deterministic modest-N local glob partition planning in `cdf-declarative`. Multi-match file resources now plan one partition per resolved file, open/preview consume the selected partition path through the same validation front end, zero matches remain actionable data errors, and single-file partition id compatibility is preserved. Evidence: `.10x/evidence/2026-07-09-p2-ws-d1-file-glob-partition-planning.md`. Review: `.10x/reviews/2026-07-09-p2-ws-d1-file-glob-partition-planning-review.md`.
- 2026-07-09: Parent closure review repaired two issues before commit: multi-file opens now reject the legacy `files` partition id, and partition metadata/scope use root-relative paths rather than dynamic absolute roots. Conformance run-matrix and live local-file DuckDB/Parquet/Postgres goldens were rerun and passed after updating fixture plans and expected hashes for the new `path`/`bytes` partition metadata.
- 2026-07-09: Final parent quality sweep passed: full workspace tests, workspace check/clippy, formatting/diff check, Semgrep after replacing shared temp-dir tests with `tempfile::TempDir`, current-tree Gitleaks source snapshot, cargo deny/audit/vet/OSV with only the ratified `paste` advisory, jscpd full/scoped inventories with no new clones, rust-code-analysis over touched Rust files, and CodeQL through the reusable `target/quality/codeql-db-rust` database.

## Blockers

None. WS-B1/WS-C1/WS-F1 coordination dependency is cleared by done tickets.
