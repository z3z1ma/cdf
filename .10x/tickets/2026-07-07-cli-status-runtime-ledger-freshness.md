Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-cli-surface.md
Depends-On: .10x/specs/project-cli-observability-security.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/checkpoint-state-commit-gate.md

# Integrate runtime-ledger freshness into status

## Scope

Extend `cdf status` beyond local committed-head freshness by integrating run-ledger/package receipt timestamps where active specs require them.

Owns:

- `crates/cdf-cli/src/status_command.rs`, `status_freshness.rs`, and focused tests.
- Lower state/run-ledger queries needed to read receipt/run timestamps without source contact.
- Freshness output and exit-code behavior for runtime-ledger/package-receipt cases.

## Acceptance criteria

- `cdf status` evaluates serving-resource freshness from committed checkpoint heads plus run-ledger/package receipt timestamps where those are the authoritative freshness evidence.
- Missing state DB, missing run ledger, missing receipts, stale receipts, and fresh receipts produce distinct stable JSON states.
- Serving-resource freshness breaches exit nonzero; non-evaluable states remain explicit and scheduler-friendly.
- Status does not contact sources or destinations unless a separate active spec requires a health probe.
- Existing local committed-head status behavior remains compatible.

## Evidence expectations

Run focused CLI status tests, fixture tests for fresh/stale/missing runtime-ledger facts, existing status freshness regressions, fmt/clippy/check/diff checks, and applicable quality scans.

## Explicit exclusions

No dashboard, no OTLP, no live destination drift probing beyond existing doctor behavior, no scheduler, and no source reads.

## Blockers

None. If freshness precedence between checkpoint and receipt timestamps is ambiguous in implementation, self-ratify the precedence before source edits.

## Progress and notes

- 2026-07-07: Split from `.10x/tickets/done/2026-07-07-cli-remaining-command-planners.md`. Existing status evaluates local committed-head freshness; runtime-ledger/package receipt timestamp integration remains open.
