Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-08-p2-ws-a1-schema-source-model-snapshot-foundation.md, .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md

# P2 WS-A1 schema source model and snapshot foundation evidence

## What Was Observed

The implementation separates discover intent from pinned discovered snapshot evidence, adds a project-owned schema snapshot artifact/store, and changes project runtime validation to accept declared schemas and pinned discovered/hints snapshots while failing closed for unpinned discover/hints.

Touched implementation surfaces:

- `crates/cdf-kernel/src/resource.rs`
- `crates/cdf-kernel/src/tests.rs`
- `crates/cdf-project/src/schema_snapshot.rs`
- `crates/cdf-project/src/internal.rs`
- `crates/cdf-project/src/lib.rs`
- `crates/cdf-project/src/lockfile.rs`
- `crates/cdf-project/src/runtime/orchestration.rs`
- `crates/cdf-project/src/runtime/planning.rs`
- `crates/cdf-project/src/runtime/validation.rs`
- `crates/cdf-project/src/tests.rs`
- `crates/cdf-declarative/src/compiled.rs`
- `crates/cdf-declarative/src/rest_runtime.rs`
- `crates/cdf-formats/src/readers.rs`
- `crates/cdf-formats/src/tests.rs`
- `crates/cdf-dest-postgres/src/source.rs`
- `crates/cdf-engine/src/tests.rs`
- `crates/cdf-python/src/internal.rs`
- `crates/cdf-python/src/lib.rs`
- `crates/cdf-benchmarks/src/resource.rs`
- `crates/cdf-conformance/src/resource/mod.rs`
- `crates/cdf-conformance/golden/live-local-file-v1/expected.json`
- `crates/cdf-conformance/golden/live-local-file-parquet-v1/expected.json`
- `crates/cdf-conformance/golden/live-local-file-postgres-v1/expected.json`

`crates/cdf-formats/src/readers.rs` and `crates/cdf-dest-postgres/src/source.rs` were compatibility-only edits required because `cdf-project` depends on those crates and the kernel enum shape changed. They do not implement discovery probes, schema CLI, auto-pin, or lockfile diff rendering.

Parent integration also updated engine, Python, benchmark, conformance, and golden fixture call sites that construct or assert discovered schema descriptors. Those edits keep the same behavioral boundary: they provide pinned snapshot evidence where existing tests/resources already had observed schema hashes; they do not add source discovery probes.

## Procedure

Focused tests:

- `cargo test -p cdf-kernel schema_source_modes_serde_round_trip --locked`
  - Passed: 1 test.
- `cargo test -p cdf-project schema_snapshot_artifact_uses_deterministic_hash_and_project_path --locked`
  - Passed: 1 test after recursive Arrow schema JSON and tamper-detection assertions were added.
- `cargo test -p cdf-project pinned_schema_hash --locked`
  - Passed before the final snapshot-store self-consistency hardening: 2 tests.
  - The final full `cdf-project` run below also re-ran both `pinned_schema_hash_*` tests successfully.

Required package tests:

- `cargo test -p cdf-kernel -p cdf-project --locked`
  - Passed.
  - `cdf-kernel`: 11 unit tests passed; 0 doc tests.
  - `cdf-project`: 88 unit tests passed; 0 doc tests.
  - The `cdf-project` suite exercised local Postgres-backed runtime tests and shut down the temporary servers successfully.

Required lint and formatting:

- `cargo clippy -p cdf-kernel -p cdf-project --all-targets --locked -- -D warnings`
  - Passed.
- `cargo fmt --all -- --check`
  - Passed.
- `git diff --check`
  - Passed.

Parent integration verification after cross-crate compatibility fixes and the schema-snapshot complexity refactor:

- `cargo fmt --all -- --check`
  - Passed.
- `git diff --check`
  - Passed.
- `cargo check --workspace --all-targets --locked`
  - Passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
  - Passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast`
  - Passed.
  - Includes the 100-run DuckDB and Parquet live-run golden tests and the bounded Postgres live-run golden test.
- Focused compatibility reruns:
  - `cargo test -p cdf-cli plan_json_derives_merge_guarantee_per_key --locked`: passed.
  - `cargo test -p cdf-cli plan_unsupported_destination_disposition_fails_closed_without_writes --locked`: passed.
  - `cargo test -p cdf-cli run_discovered_schema_resource_fails_before_writes --locked`: passed.
  - `cargo test -p cdf-conformance live_local_file_duckdb_v1_matches_committed_golden_across_100_runs --locked`: passed.
  - `cargo test -p cdf-conformance live_local_file_parquet_v1_matches_committed_golden_across_100_runs --locked`: passed.
  - `cargo test -p cdf-conformance live_local_file_postgres_v1_matches_committed_golden_across_bounded_repeats --locked`: passed.

Duplication and metrics:

- `jscpd --min-lines 5 --min-tokens 50 --reporters console <14 touched Rust files>`
  - Exited 0.
  - Analyzed 14 Rust files, 8,786 lines, 57,847 tokens.
  - Reported 22 clones, 251 duplicated lines (2.86%), 1,956 duplicated tokens (3.38%).
  - These are reported as residual duplication in large existing test/runtime files; the tool did not fail the command.
- `rust-code-analysis-cli -m -O json -o /tmp/cdf-rust-code-analysis-schema-a1.xja3w9 <14 touched Rust files>`
  - Exited 0.
  - Produced 14 JSON metric files under `/tmp/cdf-rust-code-analysis-schema-a1.xja3w9`, total size about 1.1 MiB.
  - An earlier direct stdout run also exited 0 but the harness truncated the very large metrics output; the later temp-directory JSON run is the retained evidence pointer.

Final integrated quality artifacts:

- `jscpd --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/p2-a1-c2/jscpd --exit-code 0 <touched Rust and golden JSON files>`
  - Exited 0.
  - Reported 20 total clones, 322 duplicated lines, 2.20% duplicated lines, and `newClones = 0`.
- `rust-code-analysis-cli -m -O json -o target/quality/reports/p2-a1-c2/rust-code-analysis <touched crate src dirs>`
  - Exited 0 and produced 137 per-file JSON reports.
  - The initial `SchemaSnapshotDataType::from_arrow` hotspot was cyclomatic 44. It was refactored before commit; final `from_arrow` is cyclomatic 6. The largest new schema snapshot helpers are `primitive_from_arrow` at 15 and `nested_from_arrow` at 14.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/p2-a1-c2/semgrep-rust.json <touched Rust files>`
  - Exited 0 with 0 findings and 0 errors.
- `gitleaks dir --no-banner --redact=100 --report-format json --report-path target/quality/reports/p2-a1-c2/gitleaks-source-mirror.json <temp mirror of tracked source plus untracked P2 files>`
  - Exited 0 with 0 findings.
- `tools/codeql-rust-quality.sh`
  - Exited 0 and reused/refreshed only the repository-standard reusable database path `target/quality/codeql-db-rust`.
  - SARIF contains the three pre-existing `rust/hard-coded-cryptographic-value` findings in `crates/cdf-cli/src/tests.rs` at lines 1252, 1342, and 1398, owned by `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`; no new CodeQL findings were introduced by this slice.
- `cargo deny check`: exited 0.
- `cargo audit --json`: exited 0.
- `cargo vet --locked`: exited 0.
- `osv-scanner scan source -r . --format json`: exited 1 with only `RUSTSEC-2024-0436`, the already-ratified `paste` advisory exception.

Additional parent hardening:

- `SchemaSnapshotStore::artifact_path` now validates referenced snapshot paths before joining them under the project root.
- `crates/cdf-project/src/tests.rs` includes a traversal regression test proving a reference path such as `../outside.json` is rejected with a schema snapshot reference path error.
- `SchemaSnapshotDataType::from_arrow` was split into primitive, decimal, temporal, binary, text, and nested helpers after the complexity pass found the original match too large.

Touched-record scans:

- `gitleaks detect --no-git --source /tmp/cdf-a1-record-scan-final2.Bn7Obs --no-banner --redact --report-format json --report-path /tmp/cdf-a1-record-scan-final2.Bn7Obs/gitleaks.json`
  - Passed with no leaks found.
  - The temp source contained only this ticket's done record, evidence record, review record, and parent workstream record touched for graph coherence.
- `rg` scan for stale pre-move ticket path across the touched records.
  - No matches.
- `rg` scan for placeholder markers and common credential wording across the touched records.
  - No matches.
- `rg` scan for old active discovered-schema wording found one expected historical progress note in the done ticket: `SchemaSource::Discovered { schema_hash: Option<SchemaHash> }`.
  - This is retained as provenance for why the ticket was opened, not active model authority.

## What This Supports

- Kernel models now represent declared schema with a concrete hash, unpinned discover intent, pinned discovered snapshot evidence with metadata, and hints mode.
- The project snapshot artifact serializes a recursive Arrow-derived schema JSON model with field names, data types, nullability, nested child fields, and sorted metadata, plus deterministic hash input.
- Snapshot artifact paths use `.cdf/schemas/<resource>@<hash>.json`.
- The snapshot store validates deterministic hash input, path consistency, and referenced snapshot path shape on write/read.
- `cdf-project` runtime validation accepts pinned discovered and pinned hints snapshots and still rejects unpinned discover/hints before package-producing execution.
- Existing `cdf-kernel` and `cdf-project` package tests pass with locked dependencies.

## Limits

- This evidence does not cover actual discovery probes, first-use auto-pin behavior, schema CLI commands, or lockfile diff rendering; those are explicit exclusions in the ticket.
- `jscpd` still reports duplicated regions across the integrated touched file set, with `newClones = 0`. No additional refactor was made because the remaining duplication is existing test/runtime structure outside this small model foundation.
- `rust-code-analysis-cli` metrics are stored under `target/quality/reports/p2-a1-c2/rust-code-analysis`, not committed into `.10x/evidence/.storage/`, because the output is generated quality-tool detail and not needed as durable source material beyond this recorded result.
