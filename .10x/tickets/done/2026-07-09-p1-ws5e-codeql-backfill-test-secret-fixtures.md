Status: done
Created: 2026-07-09
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-08-p1-product-ws5-live-progress.md
Depends-On: .10x/evidence/2026-07-09-p2-ws-b1-declarative-arrow-type-vocabulary.md

# P1 WS5E CodeQL backfill test secret fixtures

## Scope

Remove or document the current CodeQL `rust/hard-coded-cryptographic-value` findings in `crates/cdf-cli/src/tests.rs` backfill test fixtures without changing product secret handling or backfill semantics.

Current observed locations from `target/quality/reports/codeql-rust-current.sarif`:

- `crates/cdf-cli/src/tests.rs:1252`
- `crates/cdf-cli/src/tests.rs:1342`
- `crates/cdf-cli/src/tests.rs:1398`

## Acceptance criteria

- `tools/codeql-rust-quality.sh` completes through the reusable `target/quality/codeql-db-rust` path.
- `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif` returns `0`, or any remaining result has a narrow, reviewed test-only suppression with an explicit rationale.
- Focused backfill/progress CLI tests still pass.
- Secret redaction tests still prove fixture values do not leak into command output.
- No B1/P2 schema-vocabulary code is modified by this ticket.

## Evidence expectations

Record focused `cdf-cli` backfill/progress tests, source-only Gitleaks over `crates/cdf-cli/src/tests.rs`, Semgrep over the touched CLI test file, reusable-DB CodeQL output, `cargo fmt --all -- --check`, `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`, and `git diff --check`.

## Explicit exclusions

Do not redesign the secret provider, live progress renderer, backfill planner, or SQL source fixtures. This is a scanner-residual cleanup only.

## Progress and notes

- 2026-07-09: Opened from P2 B1 parent review after `tools/codeql-rust-quality.sh` completed with three current-tree SARIF findings in existing P1 backfill test fixtures. The findings are outside the B1 touched files, so B1 remains review-pass with this separate owner.
- 2026-07-10: Replaced fixed password sentinels with a dynamic per-test value, retained resolved-DSN redaction assertions, and reduced current-tree CodeQL SARIF results from three to zero. Evidence: `.10x/evidence/2026-07-10-p1-ws5e-codeql-backfill-fixtures.md`; review: `.10x/reviews/2026-07-10-p1-ws5e-codeql-backfill-fixtures-review.md`.

## Blockers

None. Complete.
