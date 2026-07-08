Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-e2-quarantine-routing-redaction.md, .10x/tickets/done/2026-07-08-p1-contract-depth-program.md

# P1 E2 quarantine routing and redaction evidence

## What was observed

P1 E2 routes live row-level quarantine candidates into identity-participating package artifacts and destination quarantine mirrors where supported.

- `cdf-package` now owns `QuarantineRecord` and `QuarantineObservedValue`, writes `quarantine/part-*.parquet`, reads those artifacts back through manifest-declared identity files, and rejects manifest paths that escape `quarantine/` or are not Parquet files.
- `cdf-engine` now returns accepted rows plus quarantine records from live contract execution. Rejected rows are written to package quarantine artifacts while accepted rows continue through normalization, package writing, destination commit, receipt verification, and checkpoint gating.
- `pii:*` fields use the compiled redaction decision. The focused engine test proves SHA-256 redaction is deterministic and that the raw email fixture is absent from the quarantine Parquet bytes.
- `cdf-project` records an explicit `destination/quarantine-mirror.json` outcome for destinations whose sheets do not support quarantine tables.
- `cdf-dest-postgres` creates and populates `_cdf_quarantine` for supported sheets, with idempotent insert semantics keyed by target, package hash, source row ordinal, rule id, and error code.
- The `live_file_resource` project test fixture now declares its cursor fixture values as `int64` microseconds to match the fixture values under the stricter E1 live type validator. Dedicated timestamp cursor tests continue to own timestamp semantics.

## Procedure and results

Focused behavior checks:

- `cargo test --locked -p cdf-package quarantine_records_round_trip_as_parquet_identity_evidence -- --nocapture` passed. The test round-trips quarantine Parquet records, verifies package identity, proves tamper detection, and rejects a tampered manifest path `quarantine/../escape.parquet`.
- `cargo test --locked -p cdf-package -p cdf-engine --lib -- --nocapture` passed: `cdf-package` 29 tests, `cdf-engine` 21 tests.
- `cargo test --locked -p cdf-project --lib -- --nocapture` passed: 65 tests, including the unsupported quarantine mirror outcome run.
- `cargo test --locked -p cdf-dest-postgres --lib -- --nocapture` passed: 28 tests, including the live `_cdf_quarantine` mirror test.
- `cargo nextest run -p cdf-package -p cdf-engine -p cdf-project -p cdf-dest-postgres --locked -E 'test(quarantine_records_round_trip_as_parquet_identity_evidence) | test(contract_exec_writes_redacted_quarantine_artifact_and_keeps_accepted_rows) | test(project_run_records_non_mirror_outcome_for_unsupported_quarantine_sheet) | test(live_append_populates_quarantine_mirror_when_sheet_supports_it)'` passed: 4 passed, 139 skipped by expression.

Build and lint checks:

- `cargo check -p cdf-package -p cdf-engine -p cdf-project -p cdf-dest-postgres --all-targets --locked` passed.
- `cargo clippy --locked -p cdf-package -p cdf-engine -p cdf-project -p cdf-dest-postgres --all-targets -- -D warnings` passed.
- `cargo fmt --all -- --check` passed.
- `git diff --check` passed.
- `git diff -- Cargo.toml Cargo.lock` produced no diff; this slice added no dependencies or lockfile changes.

Quality and security checks:

- `jscpd --reporters console --no-tips ...` over the 18 touched files passed with exit 0: 18 files, 12,540 lines, 81,362 tokens, 79 clones, 817 duplicated lines (6.52%), and 6,074 duplicated tokens (7.47%).
- `rust-code-analysis-cli` metrics on touched hotspots:
  - `crates/cdf-package/src/quarantine.rs`: SLOC 258, PLOC 238, cognitive max 2, cyclomatic max 14.
  - `crates/cdf-engine/src/execution.rs`: SLOC 417, PLOC 381, cognitive max 20, cyclomatic max 36.
  - `crates/cdf-project/src/runtime/artifacts.rs`: SLOC 561, PLOC 528, cognitive max 8, cyclomatic max 11.
  - `crates/cdf-dest-postgres/src/commit.rs`: SLOC 889, PLOC 829, cognitive max 11, cyclomatic max 18.
- Direct unsafe/FFI scan across the touched files for `unsafe`, `unsafe impl`, `unsafe trait`, `extern "C"`, raw pointer conversion, `transmute`, and `MaybeUninit` produced no matches.
- `cargo machete --with-metadata` scoped to the touched crates passed with no unused dependency findings.
- `gitleaks dir` scoped to the touched crates passed with no leaks.
- `semgrep scan --config p/rust` over the touched files passed: 18 files, 11 rules, 0 findings.
- `cargo vet --locked` passed: vetting succeeded with the existing exempted set.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436` passed.
- `cargo deny check` passed; the existing duplicate dependency warnings remain governed by P0 Workstream D records.
- `osv-scanner --lockfile Cargo.lock --format json` produced only the already-ratified `paste` advisory `RUSTSEC-2024-0436`.
- `tools/codeql-rust-quality.sh` passed. The reusable database path `target/quality/codeql-db-rust` was refreshed because Rust source changed. `target/quality/reports/codeql-rust-current.sarif` has 0 results; metrics: extraction errors 0, extraction warnings 3254, files extracted total 214, files with errors 163, files without errors 51, macro calls resolved 116, macro calls total 4590, macro calls unresolved 4474.

## What this supports

This supports the E2 acceptance criteria:

- Package quarantine artifacts are Parquet and identity-participating.
- Quarantine records include source row ordinal, rule id, error code, source position, and redacted observed value.
- PII redaction is deterministic for SHA-256 and does not persist the raw PII test value in the quarantine artifact bytes.
- Accepted rows keep flowing through the live run path when quarantine is permitted.
- Unsupported destination quarantine mirrors are explicitly recorded rather than silently skipped.
- Postgres sheet-backed quarantine mirrors are populated for supported destinations.
- Package verification includes quarantine artifacts when present.

## Limits

This evidence covers mixed accepted/quarantined batches. It does not claim that all-row-quarantined live runs have a complete checkpoint-state story independent of later contract-depth work. DuckDB and filesystem Parquet quarantine mirrors remain unsupported by sheet and are evidenced through explicit non-mirror outcome artifacts rather than destination tables. Dedup, variant capture, trust promotion/demotion, and drift-quarantine conformance remain owned by later P1 children.
