Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-07-cli-state-migrate-recover.md
Verdict: pass

# CLI state migrate/recover review

## Target

Review of the `cdf state migrate` and `cdf state recover` implementation, including:

- `.10x/decisions/state-migrate-recover-package-receipt.md`
- `crates/cdf-cli/src/args.rs`
- `crates/cdf-cli/src/state_command.rs`
- `crates/cdf-cli/src/state_command/migrate.rs`
- `crates/cdf-cli/src/state_command/recover.rs`
- `crates/cdf-cli/src/replay_command.rs`
- `crates/cdf-cli/src/reports.rs`
- `crates/cdf-state-sqlite/src/migration.rs`
- `crates/cdf-state-sqlite/src/support.rs`
- `crates/cdf-state-sqlite/src/sqlite.rs`
- `crates/cdf-state-sqlite/src/run_ledger.rs`
- focused CLI and SQLite tests.

## Findings

No blocking findings.

The main risk was that `state recover` could become a shortcut around the commit gate. Source inspection found the command selects a package receipt before opening state, proposes a checkpoint only when absent, delegates recovery to `recover_package_from_artifacts`, and abandons only the CLI-created proposal on failure. The lower recovery path validates package replay inputs, validates the destination target, verifies the receipt through the destination runtime, and commits through `CheckpointStore::commit`; reuse is allowed only for an exact committed head with matching delta and receipt.

The second risk was command-module growth. The implementation keeps `state_command.rs` as a dispatcher plus pre-existing show/history/rewind handlers and places new migrate/recover behavior in submodules. `jscpd` still reports existing CLI/test and state-store structural duplication, but the new package context and checkpoint-report helpers removed the new duplicate blocks that would have otherwise landed in this slice.

The third risk was over-claiming recovery semantics. The active decision and CLI JSON evidence limits correctly state that this is package-receipt recovery only. It does not claim arbitrary destination mirror scraping, quarantine-lineage reconstruction, or missing run-ledger reconstruction.

## Assumptions Tested

- SQLite migration is component-version based, with `checkpoint_store` target v1 and `run_ledger` target v2.
- Opening missing SQLite state during `state migrate` may initialize current schema and report that as applied initialization.
- Zero or multiple package receipts without `--receipt` are inconsistent evidence and fail closed.
- Postgres recovery uses the same explicit target and dedup policy rules as package replay.
- Destination rows must not be written during state recovery.
- Existing full-history gitleaks findings and the ratified `paste` advisory are not introduced by this slice and remain owned elsewhere.

## Verdict

Pass. Acceptance criteria are supported by focused tests, nextest, compile/lint gates, supply-chain scans, Semgrep, CodeQL, gitleaks source scans, duplication metrics, and complexity metrics recorded in `.10x/evidence/2026-07-08-cli-state-migrate-recover.md`.

## Residual Risk

Broad destination mirror recovery remains unspecified and intentionally unimplemented. If CDF later needs recovery from destination mirrors without package receipts, the decision must be superseded with destination-specific inventory, precedence, and evidence-limit rules.

The historical Gitleaks findings are triaged under `.10x/tickets/done/2026-07-08-historical-gitleaks-findings-triage.md`; they do not block this source slice because current-tree scans over touched paths passed.

CodeQL's local Rust extractor produced known macro-resolution warning noise. This review relies on CodeQL as one scanner among several, not as complete proof.
