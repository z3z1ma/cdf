Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-c2-destination-identifier-policy-adapter.md, .10x/tickets/2026-07-08-p2-ws-c-source-identity-normalization.md, .10x/decisions/data-onramp-source-identity-preview-disposition.md, .10x/specs/types-contracts-normalization.md

# P2 WS-C2 destination identifier policy adapter evidence

## What was observed

`crates/cdf-contract` now exposes a destination-rule adapter through `IdentifierPolicy::from_destination_rules`, `TryFrom<&IdentifierRules>`, and `identifier_policy_from_destination_rules`. The adapter converts current kernel `IdentifierRules` into the contract `IdentifierPolicy` consumed by `normalize_schema`, `normalize_arrow_schema`, and `normalize_identifier`.

The adapter supports:

- DuckDB `namecase-v1` with the current `^[a-z_][a-z0-9_]*$` allowed pattern. DuckDB `max_length = None` is preserved as no max-length limit in the contract policy.
- Postgres `namecase-v1/postgres-quoted-v1` with `max_length = Some(63)`. The current descriptive Postgres allowed-pattern string is recognized; the contract policy preserves the 63-byte length and keeps the existing ASCII lower-snake live-column normalizer subset.

Unsupported rules fail closed. The focused test for Parquet's `object-key-component-v1` asserts the error names that rule and contains `live column normalization for that rule is not implemented by this adapter`.

The focused tests also prove:

- Postgres-derived policy truncates over-length normalized identifiers to 63 bytes and does so stably.
- DuckDB-derived policy does not truncate an 81-byte valid lower-snake name when the destination sheet has no max length.
- DuckDB-derived policy rejects an output such as `123_source_name` that fails the sheet's regex-backed allowed pattern.
- Post-normalization collision behavior remains stable when the policy came from destination rules.
- Deserializing an `IdentifierPolicy` without `max_length` preserves the default 63-byte cap rather than accidentally treating old serialized policies as unbounded.

No `crates/cdf-kernel/src/destination.rs` helper was needed, and no CLI, docs, or live plan/run integration code was changed.

## Procedure

Commands run from the repository root:

- `cargo test -p cdf-contract destination_identifier_policy --locked`: passed, 4 focused tests.
- `cargo test -p cdf-contract --locked`: passed, 28 unit tests plus doctests.
- `cargo clippy -p cdf-contract --all-targets --locked -- -D warnings`: passed after replacing a clippy-reported `field_reassign_with_default` shape in the adapter.
- `cargo fmt -p cdf-contract -- --check`: passed for the ticket-owned package.
- Worker-phase `cargo fmt --all -- --check` initially failed on unrelated concurrent A1 files outside this ticket's write scope. Parent integration later formatted the whole workspace and the final `cargo fmt --all -- --check` passed.
- `jscpd --format rust --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-p2-ws-c2 --exit-code 0 crates/cdf-contract/src/policy.rs crates/cdf-contract/src/normalization.rs crates/cdf-contract/src/tests.rs`: passed with 3 Rust files analyzed, 1616 lines, 10521 tokens, 0 clones, 0 duplicated lines/tokens. JSON report: `target/quality/reports/jscpd-p2-ws-c2/jscpd-report.json`.
- `rust-code-analysis-cli -m -O json -p crates/cdf-contract/src/policy.rs -p crates/cdf-contract/src/normalization.rs -p crates/cdf-contract/src/tests.rs -o target/quality/reports/rust-code-analysis-p2-ws-c2`: passed and wrote JSON reports under `target/quality/reports/rust-code-analysis-p2-ws-c2/`.
- `git diff --check -- crates/cdf-contract/src/policy.rs crates/cdf-contract/src/normalization.rs crates/cdf-contract/src/tests.rs`: passed.
- `git diff --check`: passed across the mixed worktree.
- `git diff --check -- crates/cdf-contract/src/policy.rs crates/cdf-contract/src/normalization.rs crates/cdf-contract/src/tests.rs .10x/evidence/2026-07-09-p2-ws-c2-destination-identifier-policy-adapter.md .10x/reviews/2026-07-09-p2-ws-c2-destination-identifier-policy-adapter-review.md .10x/tickets/done/2026-07-09-p2-ws-c2-destination-identifier-policy-adapter.md`: passed after records were written.
- Banned-phrase/rename scan over the three touched C2 records: exited 1 with no matches.
- `gitleaks dir --no-banner --redact=100 --log-level error --report-format json --report-path target/quality/reports/gitleaks-p2-ws-c2-records.json /tmp/cdf-p2-ws-c2-records-gitleaks`: passed on a temporary mirror of the three touched records. An earlier direct multi-file Gitleaks invocation did not return promptly and was interrupted before producing a scan result.
- `jscpd .10x/evidence/2026-07-09-p2-ws-c2-destination-identifier-policy-adapter.md .10x/reviews/2026-07-09-p2-ws-c2-destination-identifier-policy-adapter-review.md .10x/tickets/done/2026-07-09-p2-ws-c2-destination-identifier-policy-adapter.md --format txt --formats-exts txt:md --min-lines 10 --min-tokens 60 --no-gitignore --reporters console,json --output target/quality/reports/jscpd-p2-ws-c2-records --exit-code 0`: passed with 3 record files analyzed, 148 lines, 2816 tokens, 0 clones, 0 duplicated lines/tokens. JSON report: `target/quality/reports/jscpd-p2-ws-c2-records/jscpd-report.json`.

`rust-code-analysis-cli` top reported function metrics in the touched files were:

- `lower_snake_case`: cyclomatic 9, cognitive 10, SLOC 29. Existing normalizer function.
- `destination_allowed_pattern`: cyclomatic 9, cognitive 5, SLOC 20. New adapter rule table.
- `filter_identifier_charset`: cyclomatic 6, cognitive 11, SLOC 20. Existing normalizer function.

Parent integration verification after the serde-default hardening:

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed.
- `jscpd --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/p2-a1-c2/jscpd --exit-code 0 <touched Rust and golden JSON files>`: exited 0, reported 20 total clones and `newClones = 0`.
- `rust-code-analysis-cli -m -O json -o target/quality/reports/p2-a1-c2/rust-code-analysis <touched crate src dirs>`: exited 0. Final C2-adjacent metrics remained bounded: `destination_allowed_pattern` cyclomatic 9, `lower_snake_case` cyclomatic 10.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/p2-a1-c2/semgrep-rust.json <touched Rust files>`: exited 0 with 0 findings and 0 errors.
- `gitleaks dir --no-banner --redact=100 --report-format json --report-path target/quality/reports/p2-a1-c2/gitleaks-source-mirror.json <temp mirror of tracked source plus untracked P2 files>`: exited 0 with 0 findings.
- `tools/codeql-rust-quality.sh`: exited 0 using the reusable `target/quality/codeql-db-rust` database path. The SARIF still contains only the three pre-existing `rust/hard-coded-cryptographic-value` findings in `crates/cdf-cli/src/tests.rs`, owned by `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.
- `cargo deny check`: exited 0.
- `cargo audit --json`: exited 0.
- `cargo vet --locked`: exited 0.
- `osv-scanner scan source -r . --format json`: exited 1 with only the already-ratified `RUSTSEC-2024-0436` `paste` advisory exception.

## What this supports or challenges

This supports closing the C2 adapter slice: destination sheets can now be adapted into contract identifier policies without hard-coding `IdentifierPolicy::default()` at the later live plan/run integration point. It also preserves C1 behavior because `IdentifierPolicy::default()` remains `namecase-v1` with `max_length = Some(63)` and no destination allowed-pattern constraint.

The worker-phase repo-wide fmt failure is superseded by parent integration evidence: the final repo-wide format and whitespace checks pass.

## Limits

This evidence does not prove live project planning selects destination-derived policies, package manifests record normalizer evidence, `cdf diff schema` uses destination-specific rules, or conformance live runs exercise destination-specific normalization. Those behaviors remain explicitly outside C2 and owned by later WS-C children.

The Postgres allowed-pattern string is descriptive rather than regex-backed. The adapter recognizes the exact current sheet string and preserves the max length, while the current live-column normalizer remains the ASCII lower-snake subset. A broader quoted UTF-8 column normalizer would need a new ratified contract and is not implemented by this adapter.
