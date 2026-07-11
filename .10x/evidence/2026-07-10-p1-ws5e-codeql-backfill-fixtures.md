Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md

# P1 WS5E CodeQL backfill fixture cleanup

## What was observed

The three CodeQL `rust/hard-coded-cryptographic-value` results were caused by fixed password sentinels passed to the SQL backfill test helper. The helper now creates a per-process, per-test dynamic password and each test asserts that the complete resulting DSN is absent from stdout and stderr. Product secret resolution and backfill behavior are unchanged.

## Procedure

The following checks passed:

```text
cargo test -p cdf-cli --locked backfill_execute_ --no-fail-fast
  3 passed; 0 failed
cargo clippy -p cdf-cli --all-targets --locked -- -D warnings
gitleaks detect --no-git --source crates/cdf-cli/src/tests.rs ...
  no leaks found
semgrep --config auto crates/cdf-cli/src/tests.rs ...
  0 findings
tools/codeql-rust-quality.sh
jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif
  0
```

CodeQL first reproduced exactly three findings at current lines 2176, 2266, and 2322. After the fixture change it rebuilt the current-source database and returned zero SARIF results. It scanned 279 current Rust files with zero extraction errors; the known macro-resolution warning profile is governed by `.10x/knowledge/quality-gate-execution.md`.

## What this supports

WS5E is complete and the live-progress parent no longer carries a security-scanner residual.

## Limits

The dynamic value is a test sentinel, not a production credential generator. This evidence does not claim complete semantic CodeQL coverage beyond the documented extractor limits.
