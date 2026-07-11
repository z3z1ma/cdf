Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-08-p2-ws-c1-declarative-schema-normalization.md, .10x/tickets/done/2026-07-08-p2-ws-c-source-identity-normalization.md, .10x/specs/data-onramp-source-experience-cli.md, .10x/specs/types-contracts-normalization.md

# P2 WS-C1 declarative schema normalization evidence

## What was observed

The declarative compiler now builds declared Arrow fields with automatic `cdf:source_name` metadata, then runs the schema through `cdf_contract::normalize_arrow_schema` with `IdentifierPolicy::default()` (`namecase-v1`). This makes omitted `source_name` mean "the declared field name is the source-original name"; the compiled Arrow field name becomes the normalized output name. Explicit `source_name` remains preserved as the source-original metadata value.

Focused tests prove:

- `VendorID` compiles to Arrow field name `vendor_id`.
- The compiled field has `cdf:source_name = "VendorID"`.
- A field whose name is already normalized still receives source-name metadata when `source_name` is omitted.
- An explicit `source_name = "VendorIDExplicit"` is preserved in metadata.
- `userName` and `user_name` fail at compile time with a post-normalization collision message and rename hint.

`Cargo.lock` changed only to add the existing workspace path crate `cdf-contract` to `cdf-declarative`'s dependency list. No external dependency was added.

## Procedure

Commands run from the repository root:

- `cargo test -p cdf-declarative declarative_schema_ --locked`: first run failed because `Cargo.lock` needed the new path dependency edge recorded.
- `cargo test -p cdf-declarative declarative_schema_ --offline`: passed, 2 focused tests.
- `cargo test -p cdf-declarative declarative_schema_ --locked`: passed, 2 focused tests.
- `cargo test -p cdf-declarative --locked`: passed, 55 tests plus doctests.
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `jscpd --reporters console --format rust crates/cdf-declarative/src/compiled.rs crates/cdf-declarative/src/tests.rs`: exited 0; reported 48 existing clone blocks across the large compiler/test files, 419 duplicated lines (11.84%) and 5597 duplicated tokens (12.26%).
- `git diff --check`: passed.
- `rust-code-analysis-cli -m -p crates/cdf-declarative/src -O json -o target/quality/reports/rust-code-analysis-p2-c1`: passed and wrote reports under ignored `target/quality`.
- `cargo deny check`: passed with existing duplicate Arrow-major warnings and final status `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo vet`: passed with the existing notice that 455 exemptions remain and some may be pruneable.
- `gitleaks detect --no-git --redact --source crates/cdf-declarative --no-banner`: passed, no leaks found.
- `gitleaks detect --no-git --redact --source .10x --no-banner`: passed, no leaks found.
- Banned-phrase scan over the repository excluding generated directories: returned no matches.
- `jscpd .10x/evidence/2026-07-09-p2-ws-c1-declarative-schema-normalization.md .10x/reviews/2026-07-09-p2-ws-c1-declarative-schema-normalization-review.md .10x/tickets/done/2026-07-08-p2-ws-c1-declarative-schema-normalization.md --format txt --formats-exts txt:md --min-lines 10 --min-tokens 60 --no-gitignore --reporters console --output target/quality/reports/jscpd-p2-c1-records`: passed with 0 clones.

Parent integration rerun after replacing repeated C1 test TOML shells with a focused test helper:

- `cargo test -p cdf-declarative declarative_schema_ --locked`: passed, 2 focused tests.
- `cargo test -p cdf-declarative --locked`: passed, 55 tests plus doctests.
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check` scoped to C1 files and records: passed.
- `jscpd --reporters console --format rust --mode mild --min-lines 5 --min-tokens 40 crates/cdf-declarative/src/compiled.rs crates/cdf-declarative/src/tests.rs`: exited 0; reported 58 existing clone blocks across the large compiler/test files, 492 duplicated lines (13.86%) and 5838 duplicated tokens (12.73%). The helper reduced the parent-observed scoped report from 61 clone blocks and 513 duplicated lines before cleanup.
- `rust-code-analysis-cli -m -p crates/cdf-declarative/src -O json -o target/quality/reports/rust-code-analysis-p2-c1-parent`: passed and wrote reports under ignored `target/quality`.
- `gitleaks dir --no-banner --redact=100 --log-level error crates/cdf-declarative`: passed.
- `gitleaks dir --no-banner --redact=100 --log-level error` on this evidence record and the C1 review record: passed.
- `semgrep scan --config p/rust --error crates/cdf-declarative/src`: passed, 0 findings across 7 tracked files.
- `cargo deny check`: passed with existing duplicate Arrow-major warnings; final status reported advisories, bans, licenses, and sources ok.
- `cargo vet`: passed with 455 existing exemptions and the existing pruneable-exemptions warning.
- `cargo audit`: passed with one allowed warning for `RUSTSEC-2024-0436` on `paste`, matching the already-ratified advisory posture.
- `tools/codeql-rust-quality.sh`: passed. The reusable CodeQL database under `target/quality/codeql-db-rust` was refreshed because Rust source/manifest/lockfile content changed; extraction errors were 0. The SARIF contained 3 findings, all the pre-existing unrelated `rust/hard-coded-cryptographic-value` findings in `crates/cdf-cli/src/tests.rs` already owned by `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`. No CodeQL finding pointed at the C1 files.

## What this supports or challenges

This supports C1 acceptance criteria for automatic declarative schema normalization, automatic source-name metadata, explicit source-name preservation, and compile-time collision detection. It also supports P2 friction rows 6 and 7 for the direct declarative compiled-schema slice.

The result challenges the older behavior where declarative schemas passed field names through unchanged and only recorded `cdf:source_name` when users wrote `source_name` manually.

## Limits

This evidence does not prove destination-specific sheet charset/length variants, schema snapshot recording, package-level normalizer evidence, `cdf diff schema`, or full live destination commit behavior. Those remain explicitly outside C1 and owned by later WS-C tickets.

The scoped `jscpd` run reports existing duplication in the large `compiled.rs` and `tests.rs` files. C1 did not refactor those broader test/helper structures because that would exceed the ticket scope.
