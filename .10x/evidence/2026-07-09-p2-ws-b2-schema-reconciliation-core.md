Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-b2-schema-reconciliation-core.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/types-contracts-normalization.md, .10x/decisions/data-onramp-schema-discovery-reconciliation.md

# P2 WS-B2 schema reconciliation core evidence

## What Was Observed

B2 added a format-independent schema reconciliation API in `cdf-contract` and a shared `cdf:physical_type` metadata helper in `cdf-kernel`.

The reconciliation API accepts observed and constraint Arrow schemas plus the existing `TypePolicy`, returns a reconciled Arrow schema and serializable `SchemaCoercionPlan`, and also exposes an inspectable report for callers that need to render all decisions before failing closed. Focused tests cover preserved fields, widened fields, missing fields, extra observed fields, lossy casts, unsupported mappings, physical provenance metadata, and serialization.

All required scoped verification passed. `jscpd` exited 0 and reported one clone block between the new reconciliation type-definition area and an existing test block; inspection found it to be a token-level false positive rather than removable semantic duplication.

## Procedure

- `cargo test -p cdf-contract schema_reconciliation --locked`
  - Result: passed.
  - Coverage: 6 focused reconciliation tests passed.
- `cargo test -p cdf-contract --locked`
  - Result: passed.
  - Coverage: 24 unit tests passed, 0 failed; doc tests ran 0 tests and passed.
- `cargo test -p cdf-kernel metadata_helpers_round_trip_cdf_annotations --locked`
  - Result: passed.
  - Coverage: focused kernel metadata helper round-trip passed.
- `cargo test -p cdf-kernel --locked`
  - Result: passed.
  - Coverage: 10 unit tests passed, 0 failed; doc tests ran 0 tests and passed.
- `cargo clippy -p cdf-contract --all-targets --locked -- -D warnings`
  - Result: passed.
- `cargo clippy -p cdf-kernel --all-targets --locked -- -D warnings`
  - Result: passed.
- `cargo fmt --all -- --check`
  - Result: passed.
- `jscpd --format rust --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-p2-b2 --ignore "**/target/**,**/.git/**,**/reports/**" crates/cdf-contract/src/reconciliation.rs crates/cdf-contract/src/tests.rs crates/cdf-kernel/src/metadata.rs crates/cdf-kernel/src/tests.rs`
  - Result: exited 0.
  - Output summary: 4 files analyzed; 1 clone block; 36 duplicated lines; 263 duplicated tokens; report at `target/quality/reports/jscpd-p2-b2/jscpd-report.json`.
  - Inspected clone: `crates/cdf-contract/src/reconciliation.rs` lines 30-66 and existing `crates/cdf-contract/src/tests.rs` lines 463-499. This is not a shared implementation pattern and was not refactored.
- `rust-code-analysis-cli -m -O json -p crates/cdf-contract/src/reconciliation.rs > target/quality/reports/rust-code-analysis-p2-b2/reconciliation.json`
  - Result: completed.
- `git diff --check -- crates/cdf-contract/src/lib.rs crates/cdf-contract/src/tests.rs crates/cdf-kernel/src/metadata.rs crates/cdf-kernel/src/tests.rs .10x/tickets/done/2026-07-09-p2-ws-b2-schema-reconciliation-core.md`
  - Result: passed.
- `git diff --no-index --check /dev/null crates/cdf-contract/src/reconciliation.rs`
  - Result: no whitespace errors. The wrapper normalized the expected diff exit code for the untracked new file.
- Per-file Gitleaks scans:
  - `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-p2-b2-reconciliation.json crates/cdf-contract/src/reconciliation.rs`
  - `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-p2-b2-contract-tests.json crates/cdf-contract/src/tests.rs`
  - `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-p2-b2-kernel-metadata.json crates/cdf-kernel/src/metadata.rs`
  - `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-p2-b2-kernel-tests.json crates/cdf-kernel/src/tests.rs`
  - Result: all passed; no leaks found.
- Banned wording and stale-name scan over the B2-touched source and record files.
  - Result: no matches.

## What This Supports Or Challenges

This supports that B2 satisfies its acceptance criteria for a public observed-vs-constraint reconciliation API, source-original field matching, preserved constraint output names/metadata, physical type provenance, automatic lossless width widenings, fail-closed lossy/unsupported mappings, opt-in string parse coercions, error remediation text, and serializable coercion-plan decisions.

## Limits

This evidence does not claim source-format integration. Parquet, NDJSON, REST, SQL, discovery snapshots, source readers, package writing, row execution of coercions, and conformance golden paths remain explicitly outside B2 and must be owned by later WS-B children.

An earlier broad `gitleaks dir` command over two crate paths was interrupted/stale and produced no usable result. The evidence above uses focused per-file scans over the B2 touched files instead.
