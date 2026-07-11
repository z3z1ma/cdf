Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-08-p2-ws-b1-declarative-arrow-type-vocabulary.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/decisions/data-onramp-schema-discovery-reconciliation.md

# P2 WS-B1 declarative Arrow type vocabulary evidence

## What Was Observed

The B1 declarative Arrow type vocabulary implementation was verified against focused parser/schema tests, the full `cdf-declarative` crate test suite, clippy with warnings denied, formatting, duplicate detection scoped to touched Rust files, and diff whitespace checks.

Parent review reran the focused and scoped checks in this workspace. The B1-owned `cdf-declarative` files passed tests, clippy, formatting, Semgrep, Gitleaks, supply-chain gates, complexity metrics, and duplication checks with `newClones = 0`. A reusable CodeQL run completed through `tools/codeql-rust-quality.sh`, refreshing `target/quality/codeql-db-rust` only because the Rust input fingerprint changed; CodeQL reported three current-tree findings, all in unrelated `crates/cdf-cli/src/tests.rs` backfill secret fixtures from the earlier P1 WS5C slice, not in the B1 touched files. That residual is owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.

## Procedure

- `cargo test -p cdf-declarative declarative_arrow_type --locked`
  - Result: passed.
  - Coverage: 3 focused tests passed for TOML/YAML Arrow type compilation and invalid type error naming.
- `cargo test -p cdf-declarative json_schema_artifact_exposes_editor_schema_model --locked`
  - Result: passed.
  - Coverage: JSON Schema artifact still exposes the editor schema model and the field `type` property resolves to a string schema without an enum.
- `cargo test -p cdf-declarative --locked`
  - Result: passed.
  - Coverage: 53 unit tests passed, 0 failed; doc tests ran 0 tests and passed.
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`
  - Result: passed.
- `cargo fmt --all -- --check`
  - Result: passed.
- `jscpd --reporters console --format rust crates/cdf-declarative/src/declarations.rs crates/cdf-declarative/src/compiled.rs crates/cdf-declarative/src/tests.rs`
  - Result: command exited 0.
  - Output summary: 3 files analyzed, 44 clone blocks reported, 393 duplicated lines (10.47%), 5187 duplicated tokens (11.39%).
  - Limit: no threshold was configured in the ticket or command. The report includes existing repeated test/runtime patterns in the scoped files; the parser helper scanner duplicate introduced during implementation was refactored before the final run.
- `git diff --check`
  - Result: passed.

Parent rerun after worker completion:

- `cargo test -p cdf-declarative declarative_arrow_type --locked`
  - Result: passed, 3 tests.
- `cargo test -p cdf-declarative json_schema_artifact_exposes_editor_schema_model --locked`
  - Result: passed.
- `cargo test -p cdf-declarative --locked`
  - Result: passed, 53 unit tests and 0 doctests.
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`
  - Result: passed.
- `cargo fmt --all -- --check`
  - Result: passed.
- `jscpd --format rust --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-p2-b1-parent --ignore "**/target/**,**/.git/**,**/reports/**" crates/cdf-declarative/src/declarations.rs crates/cdf-declarative/src/compiled.rs crates/cdf-declarative/src/tests.rs`
  - Result: exited 0; 3 sources, 15 clone blocks, 178 duplicated lines (4.74%), `newClones = 0`, `newDuplicatedLines = 0`.
- `rust-code-analysis-cli -m -O json -p crates/cdf-declarative/src/compiled.rs` and `rust-code-analysis-cli -m -O json -p crates/cdf-declarative/src/declarations.rs`
  - Result: completed; reports under `target/quality/reports/rust-code-analysis-p2-b1-parent/`.
- `scc --format json --output target/quality/reports/scc-p2-b1-parent.json crates/cdf-declarative/src/declarations.rs crates/cdf-declarative/src/compiled.rs crates/cdf-declarative/src/tests.rs`
  - Result: completed.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-p2-b1-parent.json crates/cdf-declarative/src/declarations.rs crates/cdf-declarative/src/compiled.rs crates/cdf-declarative/src/tests.rs`
  - Result: passed, 0 findings.
- `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-p2-b1-cdf-declarative.json crates/cdf-declarative`
  - Result: passed, no leaks found.
- `cargo audit --ignore RUSTSEC-2024-0436 --json > target/quality/reports/cargo-audit-p2-b1-parent.json`
  - Result: passed with the already-ratified `paste` advisory ignored.
- `cargo deny check > target/quality/reports/cargo-deny-p2-b1-parent.txt 2>&1`
  - Result: passed; duplicate Arrow-major warnings remain the already-ratified DuckDB/DataFusion residual.
- `cargo vet --locked > target/quality/reports/cargo-vet-p2-b1-parent.txt 2>&1`
  - Result: passed, `Vetting Succeeded`.
- `tools/codeql-rust-quality.sh`
  - Result: exited 0 after refreshing the reusable database at `target/quality/codeql-db-rust`. Extraction errors: 0; extraction warnings and unresolved macros match the known local Rust extractor limit. `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif` returned `3`; all three results were `rust/hard-coded-cryptographic-value` in unrelated `crates/cdf-cli/src/tests.rs` lines 1252, 1342, and 1398. Follow-up owner: `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.
- Direct first-party unsafe scan over the touched B1 Rust files:
  - Result: no unsafe/FFI/raw-pointer matches in B1 files; one unrelated `PredicateId::new("unsafe")` fixture string appeared in `crates/cdf-declarative/src/tests.rs`.

## What This Supports Or Challenges

This supports that the implementation satisfies the ticket acceptance criteria for mandatory string forms, representative TOML/YAML compilation to Arrow `DataType`, invalid type error naming, JSON Schema string-form acceptance, and the requested local quality gates for the B1-owned declarative crate surface.

## Limits

This evidence does not claim runtime materialization support for every newly accepted Arrow type. Runtime schema discovery, observed-vs-declared reconciliation, widening/coercion policy, destination mapping, and package validation-program serialization are explicitly outside the B1 ticket scope.

The current working tree contains unrelated dirty `crates/cdf-cli/src/tests.rs` work from the P1 live-progress lane. Parent review added the missing helper needed for that dirty test file to compile so CodeQL could finish; that helper remains unstaged and is not part of B1. The three CodeQL SARIF findings are also outside B1 and are separately ticketed.
