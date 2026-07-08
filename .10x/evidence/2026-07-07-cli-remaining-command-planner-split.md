Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-cli-remaining-command-planners.md, .10x/tickets/2026-07-05-cli-surface.md

# CLI remaining command planner split evidence

## What was observed

The remaining non-run-spine CLI command-family gaps were split into direct children of `.10x/tickets/2026-07-05-cli-surface.md`:

- `.10x/tickets/done/2026-07-07-cli-init-scaffold.md`
- `.10x/tickets/done/2026-07-07-cli-plan-explain-ddl-guarantee.md`
- `.10x/tickets/2026-07-07-cli-preview-resource-breadth.md`
- `.10x/tickets/2026-07-07-cli-contract-registry-freeze-test.md`
- `.10x/tickets/2026-07-07-cli-state-migrate-recover.md`
- `.10x/tickets/2026-07-07-cli-backfill-planner.md`
- `.10x/tickets/2026-07-07-cli-package-gc-retention.md`
- `.10x/tickets/2026-07-07-cli-status-runtime-ledger-freshness.md`

The cross-cutting command module architecture child was already closed at `.10x/tickets/done/2026-07-07-cli-command-module-architecture.md`.

## Procedure

Read `.10x/tickets/done/2026-07-07-cli-remaining-command-planners.md`, `.10x/specs/project-cli-observability-security.md`, `.10x/specs/destination-receipts-guarantees.md`, and `.10x/specs/conformance-governance-roadmap.md`.

Inspected current `cdf-cli` modules and command implementations:

- `commands.rs` is a thin dispatcher.
- `project_command.rs` currently leaves `init` unsupported.
- `scan_command.rs` has plan/explain/preview foundations but no destination DDL planning facade and only the currently supported preview paths.
- `contract_command.rs` supports `show` and leaves `freeze`/`test` unsupported.
- `state_command.rs` supports show/history/rewind and leaves migrate/recover unsupported.
- `backfill_command.rs` validates the optional resource id and leaves backfill unsupported.
- `package_command.rs` supports ls/verify/archive and leaves gc unsupported.
- `status_command.rs` delegates to local freshness evaluation; runtime-ledger/package receipt timestamp integration remains open.

No source files were changed for this planning split.

## What this supports

This supports closure of the planning parent: each remaining command family now has a focused implementation owner with scope, acceptance criteria, exclusions, evidence expectations, and governing-record references.

## Limits

This is record-coherence evidence only. It does not implement any command behavior.
