Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-07-cli-status-runtime-ledger-freshness.md
Verdict: pass

# CLI Status Runtime-Ledger Freshness Review

## Target

Review of the `cdf status` runtime-ledger/package-receipt freshness implementation in:

- `crates/cdf-cli/src/status_freshness.rs`
- `crates/cdf-cli/src/tests.rs`
- `.10x/decisions/status-freshness-authority-precedence.md`

## Findings

No blocking findings.

Minor residual: `status_freshness.rs` grew to 873 lines and has two production `jscpd` clones totaling 14 duplicated lines. The duplicated production shapes are small JSON/result population patterns; extracting them now would add indirection to an already localized status evaluator. Keep this visible if status grows another evidence family.

Minor residual: `tests.rs` has high duplication when included in `jscpd` with the production file, but this is dominated by the existing CLI integration harness style. The new status tests use the local helper pattern already present in the file and are explicit enough to audit the JSON contract.

## Assumptions Tested

- Checkpoint-head precedence: tested by intentionally mismatching a fresh committed checkpoint timestamp against a stale package receipt timestamp. The top-level resource remains fresh while the receipt observation reports `corrupt_receipt`.
- Read-only/no-contact behavior: implementation opens SQLite read-only, reads package receipt artifacts by recorded path, and does not call source or destination APIs.
- Missing-state distinctions: missing DB, missing checkpoint table, missing run ledger, missing receipt artifact, fresh receipt, stale receipt, and corrupt receipt evidence are all covered by focused JSON tests.
- Path handling: package receipt lookup tries an absolute/as-recorded path first and falls back to project-root-relative paths for recorded relative paths.

## Verdict

Pass.

The acceptance criteria are covered by focused status tests, full CLI test/nextest runs, clippy/fmt/check gates, duplication and complexity scans, Semgrep, Gitleaks, CodeQL, and supply-chain gates. The only scanner residuals are known project-level limitations: OSV reports the ratified `paste` advisory, CodeQL has the documented Rust extractor macro-warning profile, and geiger stalled while the direct first-party unsafe scan found no touched unsafe surface.

## Residual Risk

Receipt-only freshness is intentionally an observability/recovery hint, not state advancement. Future recovery/status work must continue to keep `CheckpointStore::commit` as the only state-advancement authority.
