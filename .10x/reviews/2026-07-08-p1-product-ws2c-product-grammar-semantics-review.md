Status: recorded
Created: 2026-07-08
Updated: 2026-07-13
Target: .10x/tickets/done/2026-07-08-p1-product-ws2c-product-grammar-semantics.md
Verdict: pass

# P1 product WS2C product grammar semantics review

## Target

Review of the WS2C product grammar semantics implementation in:

- `crates/cdf-cli/src/args.rs`
- `crates/cdf-cli/src/destination_uri.rs`
- `crates/cdf-cli/src/scan_command.rs`
- `crates/cdf-cli/src/run_command.rs`
- `crates/cdf-cli/src/state_command.rs`
- `crates/cdf-cli/src/state_command/recover.rs`
- `crates/cdf-cli/src/resume_command.rs`
- `crates/cdf-cli/src/replay_command.rs`
- `crates/cdf-cli/src/backfill_command.rs`
- `crates/cdf-cli/src/tests.rs`

Governing records:

- `.10x/tickets/done/2026-07-08-p1-product-ws2c-product-grammar-semantics.md`
- `.10x/decisions/superseded/cli-command-grammar-and-parser.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws2b-clap-parser-foundation.md`

Evidence:

- `.10x/evidence/2026-07-08-p1-product-ws2c-product-grammar-semantics.md`

## Assumptions tested

- Short-form command acceptance did not remove legacy flags or change existing JSON envelopes.
- Destination `--to` is used only where the lower destination resolver already supports URI selection.
- Backfill's `--to` remains the upper cursor boundary and does not become a destination alias.
- Bare `resume` does not pretend lower-layer multi-run drain semantics exist.
- No-write commands and rejected paths still avoid packages, destination files, checkpoint rows, and run-ledger events.
- Secret-bearing destination resolution still flows through existing redaction paths.
- Parent quality verification covers formatting, tests, clippy, Semgrep, gitleaks, jscpd, complexity metrics, supply-chain gates, and CodeQL without relying only on the worker report.

## Findings

No blocking findings.

Minor residual limitation: jscpd still reports existing duplication in the large CLI parser/test file pair. The WS2C-specific local duplicate in the new bare-resume tests was factored out, and the final jscpd report records `newClones: 0`. Broad test-suite deduplication is outside this ticket and should not block WS2C closure.

Minor residual limitation: the default product-run pipeline is `cdf-run` because the existing run spine requires a pipeline id and the product grammar omits one. This is covered by tests and paired with state default behavior, but a future product spec could choose to expose or rename that internal default.

## Verdict

Pass. The acceptance criteria are covered by focused tests, the full cdf-cli package suite, and parent-observed quality gates, and unsupported lower-layer behavior fails closed instead of silently inventing multi-run drain semantics.

## Residual risk

The implementation preserves current command envelopes and scoped lower-layer use. Remaining risk is mainly product naming polish for the internal default pipeline and the known pre-existing duplication level in `tests.rs`; neither changes WS2C behavior or safety.
