Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-08-p2-ws-f1-append-default-merge-key-error.md, .10x/tickets/2026-07-08-p2-ws-f-keys-dispositions.md

# P2 WS-F1 append default and merge-key error evidence

## What Was Observed

F1 changed declarative resource compilation so omitted `write_disposition` compiles as `append`, append resources can compile without `primary_key` or `merge_key`, and `write_disposition = "merge"` requires an explicit non-empty `merge_key` before execution. The previous fallback from missing `merge_key` to `primary_key` was removed. Existing successful merge fixtures were updated to declare `merge_key` explicitly.

The shipped local project scaffold no longer emits `primary_key = ["id"]` for an append resource.

## Procedure

Commands run from the repository root:

- `cargo test -p cdf-declarative disposition_ --locked`
  - Result: passed. 3 disposition tests passed.
- `cargo test -p cdf-declarative --locked`
  - Result: passed. 58 unit tests and doc tests passed.
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`
  - Result: passed.
- `cargo test -p cdf-project local_project_scaffold_writes_valid_project_without_runtime_artifacts --locked`
  - Result: passed.
- `cargo test -p cdf-project declarative_sql_secret_is_collected_for_validation --locked`
  - Result: passed.
- `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`
  - Result: passed.
- `cargo fmt --all -- --check`
  - Result: passed.
- `git diff --check -- crates/cdf-declarative/src/compiled.rs crates/cdf-declarative/src/tests.rs crates/cdf-project/src/scaffold.rs crates/cdf-project/src/tests.rs crates/cdf-project/src/runtime_tests.rs`
  - Result: passed.
- `rustfmt --edition 2024 --check crates/cdf-declarative/src/compiled.rs crates/cdf-declarative/src/tests.rs crates/cdf-project/src/scaffold.rs crates/cdf-project/src/tests.rs crates/cdf-project/src/runtime_tests.rs`
  - Result: passed.
- `npx --yes jscpd@4.0.5 --min-lines 5 --min-tokens 50 --max-lines 10000 --format rust --reporters console --exitCode 0 crates/cdf-declarative/src/compiled.rs crates/cdf-declarative/src/tests.rs crates/cdf-project/src/scaffold.rs crates/cdf-project/src/tests.rs crates/cdf-project/src/runtime_tests.rs`
  - Result: completed. Existing duplication remains in large test files; scoped clone count after tightening F1 tests was 38 clones, 385 duplicated lines, reduced from the initial F1 attempt's 39 clones and 402 duplicated lines.
- `rust-code-analysis-cli --paths crates/cdf-declarative/src/compiled.rs --paths crates/cdf-declarative/src/tests.rs --paths crates/cdf-project/src/scaffold.rs --paths crates/cdf-project/src/tests.rs --paths crates/cdf-project/src/runtime_tests.rs --metrics --language-type Rust --output-format json > /tmp/cdf-p2-f1-quality/rust-code-analysis.json`
  - Result: passed; JSON metrics artifact size was 863,915 bytes.
- `gitleaks dir --no-banner --redact=100 --log-level warn crates/cdf-declarative`
  - Result: passed.
- `gitleaks dir --no-banner --redact=100 --log-level warn crates/cdf-project`
  - Result: passed.
- `gitleaks dir --no-banner --redact=100 --log-level warn .10x/tickets/2026-07-08-p2-ws-f1-append-default-merge-key-error.md`
  - Result: passed before ticket move.
- `gitleaks dir --no-banner --redact=100 --log-level warn .10x/tickets/2026-07-08-p2-ws-f-keys-dispositions.md`
  - Result: passed.
- Banned phrase and legacy-name scan over F1 code and record files.
  - Result: no matches after record cleanup.
- Source scan: `rg -n "write_disposition = \"append\"|write_disposition: append|primary_key = \\[|primary_key: \\[|fake key|composite key|requires.*key|key.*required" crates/cdf-declarative crates/cdf-project/src/scaffold.rs docs .10x/tickets/2026-07-08-p2-ws-f1-append-default-merge-key-error.md .10x/tickets/2026-07-08-p2-ws-f-keys-dispositions.md -g '*.rs' -g '*.toml' -g '*.yaml' -g '*.yml' -g '*.md'`
  - Result: generated scaffold has append without `primary_key`; remaining append/key occurrences are tests or ticket text, not shipped append scaffolds/docs.

## What This Supports

- Omitted `write_disposition` now compiles as append.
- Append can compile without key metadata.
- Merge no longer silently derives `merge_key` from `primary_key`.
- Missing merge identity fails during declarative compilation with both fixes in the remediation text: add `merge_key`, or use append.
- The current local scaffold no longer nudges append users toward a fake key.

## Limits

This evidence does not prove CLI S7 rendering, `cdf add` key suggestions, exact-row dedup options, or full P2 conformance. Those remain owned by later WS-F/WS-I slices.
