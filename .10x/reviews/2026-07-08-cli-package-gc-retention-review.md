Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-07-cli-package-gc-retention.md
Verdict: pass

# CLI package GC retention review

## Target

Review of the `cdf package gc [DIR]` dry-run retention planner implemented in `crates/cdf-cli/src/package_command.rs`, the read-only committed package-hash helper in `crates/cdf-state-sqlite/src/sqlite.rs`, and focused tests in `crates/cdf-cli/src/tests.rs` and `crates/cdf-state-sqlite/src/tests.rs`.

## Findings

- Pass: The command remains non-destructive. It emits `mode = "dry_run"` and `planned_action = "would_collect"` for collectible artifacts, but no removal/tombstone code path was added to the CLI.
- Pass: Committed checkpoint package hashes are read from SQLite with `SQLITE_OPEN_READ_ONLY`, and missing committed artifacts are reported rather than ignored. This preserves the commit-gate proof boundary.
- Pass: Receipts, tombstones, corrupt manifests, tampered packages, and partial package directories all retain/fail closed. The planner does not turn ambiguous or damaged evidence into deletion authority.
- Pass: JSON output is stable enough for automation and contains the ticket-required package path, hash when readable, retention reason, and planned action.
- Pass: Business logic mostly remains delegated to existing package verification and state-store facts. The CLI classification code is local, but it composes lower package/state APIs rather than bypassing invariants.

## Residual Risk

The classifier is the new complexity hotspot in `package_command.rs` at cyclomatic 14/cognitive 11. That is acceptable for the current finite classification table, but future destructive GC or remote-store support should move the planner into a lower crate before extending it.

Destructive collection remains unimplemented. That is not a defect for this ticket because no deletion flag semantics were ratified and the ticket explicitly required default behavior not to silently delete proof artifacts.

## Verdict

Pass. Evidence in `.10x/evidence/2026-07-08-cli-package-gc-retention.md` supports the acceptance criteria, and no review finding blocks closure.
