Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-e6-drift-quarantine-conformance.md, .10x/tickets/done/2026-07-08-source-decode-type-drift-quarantine-seam.md, .10x/decisions/source-decode-type-drift-quarantine.md

# P1 E6 drift-quarantine partial evidence

## What was observed

The first E6 implementation slice added two useful pieces of the final scenario:

- live package verdict/quarantine summary artifacts: `stats/verdict-summary.json` and `stats/quarantine-summary.json`;
- a conformance scenario that freezes a contract, quarantines a row-rule/domain drift, dedups accepted rows, commits accepted data, verifies the receipt, asserts checkpoint gating, asserts unsupported DuckDB/Parquet quarantine mirror behavior, and asserts supported Postgres `_cdf_quarantine` mirroring.

Parent review found that this is not enough to close E6 because the required literal source type-drift fixture is not yet supported. Changing the drift row from a new string domain value to numeric JSON:

```json
{"id":2,"event_type":42,"name":"drifted-event-type"}
```

failed before package finalization:

```text
Json error: whilst decoding field 'event_type': expected string got 42
```

That failure proves the source decode type-drift quarantine seam is missing.

## Procedure and results

Worker-observed checks after the summary artifact patch:

- `cargo fmt --all` passed.
- `cargo test --locked -p cdf-engine contract_exec_writes_redacted_quarantine_artifact_and_keeps_accepted_rows -- --nocapture` passed after tightening the summary assertion.
- `cargo fmt --all -- --check` passed.
- `cargo check --locked -p cdf-engine -p cdf-conformance --all-targets` passed.
- `git diff --check` passed.
- `cargo clippy --locked -p cdf-engine -p cdf-conformance --all-targets -- -D warnings` passed.

Parent-observed blocker reproduction:

- `cargo test --locked -p cdf-conformance drift_quarantine -- --nocapture` failed when the drift fixture used numeric JSON `event_type: 42`. Both DuckDB and Postgres E6 tests failed with `Json error: whilst decoding field 'event_type': expected string got 42`.

Parent-observed verification after restoring the fixture to the row-rule/domain drift slice and recording the source-decode blocker:

- `cargo fmt --all -- --check` passed.
- `git diff --check` passed.
- `cargo test --locked -p cdf-engine contract_exec_writes_redacted_quarantine_artifact_and_keeps_accepted_rows -- --nocapture` passed.
- `cargo test --locked -p cdf-conformance drift_quarantine -- --nocapture` passed.
- `cargo check --locked -p cdf-engine -p cdf-conformance --all-targets` passed.
- `cargo clippy --locked -p cdf-engine -p cdf-conformance --all-targets -- -D warnings` passed.
- `cargo check --workspace --all-targets --locked` passed.
- `cargo check --workspace --all-targets --all-features --locked` passed.
- `cargo check --workspace --all-targets --no-default-features --locked` passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings` passed.
- `cargo test --workspace --all-targets --locked --no-fail-fast` passed.
- `cargo doc --workspace --all-features --no-deps --locked` passed.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast` passed.
- `cargo metadata --format-version=1 --locked`, `cargo tree --workspace --locked`, and `cargo tree --workspace --locked -d` passed with reports under `target/quality/reports/`.
- `jscpd` on the touched engine/conformance paths completed with 5.89% duplicated lines and reports under `target/quality/reports/jscpd-p1-e6-partial/`.
- `rust-code-analysis-cli`, `scc`, `cargo machete`, and direct unsafe-pattern `rg` scans completed for the touched paths; the unsafe-pattern scan produced an empty report.
- `semgrep scan --config p/rust` and `semgrep scan --config p/security-audit` on the touched paths passed with zero findings.
- `gitleaks detect --no-git --source crates --redact` passed with no leaks.
- `cargo audit --json`, `cargo deny check`, and `cargo vet --locked` passed.
- `osv-scanner scan source --lockfile Cargo.lock --format json` reported only the already-ratified `RUSTSEC-2024-0436` `paste` advisory.
- `tools/codeql-rust-quality.sh` completed using the reusable `target/quality/codeql-db-rust` database; `target/quality/reports/codeql-rust-current.sarif` contained zero results. The Rust extractor reported macro expansion warnings, but no blocking CodeQL findings.
- `cargo geiger` was attempted. The workspace invocation rejected the virtual manifest; per-package invocations required absolute manifest paths and then blocked on Cargo package/build locks until manually stopped. This record therefore does not claim a completed geiger pass. The scoped unsafe-pattern scan over touched files found no unsafe/FFI/raw-pointer patterns.

## What this supports

This supports the E6 partial implementation status:

- package verdict/quarantine summaries now exist for live row-rule quarantine;
- the conformance harness can assert accepted-row continuation, destination receipt verification, checkpoint gating, dedup evidence, package quarantine artifacts, destination mirror support/exclusion, and trust-ring quarantine demotion for a row-rule drift;
- literal source scalar type-drift quarantine remains unimplemented and is now owned by `.10x/tickets/done/2026-07-08-source-decode-type-drift-quarantine-seam.md`.

## Limits

This evidence does not close E6. The passing row-rule/domain drift scenario is a useful harness slice, but it does not satisfy the P0/E6 requirement to drift a fixture type and quarantine the offending row while accepted rows continue.
