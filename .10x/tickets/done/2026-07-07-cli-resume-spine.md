Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/done/2026-07-07-cli-run-resume-replay-inspect-spine.md
Depends-On: .10x/tickets/done/2026-07-07-cli-run-general-runtime.md, .10x/tickets/done/2026-07-07-cli-replay-package-spine.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# Wire `cdf resume` to run recovery

## Scope

Replace the `resume` not-supported stub with run-id-scoped recovery over run ledger events, package artifacts, destination receipts, and checkpoint rows.

Owns:

- `crates/cdf-cli/src/commands.rs`
- `crates/cdf-cli/src/context.rs`
- CLI-facing `cdf-project` runtime adapters if needed for run recovery assembly.
- Focused CLI tests for crash-window recovery and fail-closed missing artifacts.

## Acceptance criteria

- `cdf resume --run <id>` loads the selected environment ledger and recovers the referenced run according to `.10x/specs/run-orchestration-ledger.md`.
- After package finalization, resume does not contact the source.
- Durable-receipt/uncommitted-checkpoint recovery verifies receipts before checkpoint commit.
- Committed-checkpoint/stale-package-status recovery updates package status only.
- Terminal successful runs are no-op with stable JSON output.
- Missing or inconsistent package/receipt/checkpoint evidence fails closed with recovery guidance.

## Evidence expectations

Run focused CLI resume tests covering at least no-finalized-package, finalized-package/no-receipt, durable-receipt/uncommitted-checkpoint, committed-checkpoint/stale-package-status, and terminal no-op where deterministic; run relevant `cdf-project` recovery tests, clippy for CLI/project, workspace check, and `git diff --check`.

## Explicit exclusions

No no-argument interrupted-run discovery unless already record-backed during implementation, no scheduler, no daemon, no destination introspection as missing semantic input.

## Design notes

- The run ledger is an index, not authority; resume must prefer durable package/receipt/checkpoint facts when they disagree with ledger events.
- No-argument `cdf resume` discovery policy was explicitly left outside the run-ledger store and should not be invented here without a focused decision.

## Blockers

Depends on CLI run and replay package wiring so the command can reuse destination parsing and package replay behavior.

## Progress and notes

- 2026-07-07: Split from the broad CLI spine ticket. This is intentionally sequenced after run/replay wiring.
- 2026-07-07: Activated after CLI run, replay package, and inspect-run children closed. Worker lane owns `resume_command.rs` assembly and focused tests; keep `commands.rs` dispatch-only and reuse existing `cdf-project` recovery/replay primitives instead of creating a specialized recovery path.
- 2026-07-07: Implemented run-id-scoped `cdf resume` over run ledger, package artifacts, receipts, and checkpoint rows. Resume now covers terminal no-op, no finalized package fail-closed guidance, finalized package/no durable receipt replay for DuckDB, filesystem Parquet, and Postgres without source contact, durable-receipt/uncommitted-checkpoint recovery, and committed-checkpoint/stale-package-status repair. Status-only repair is guarded by exact current committed head, exact state delta, and exact durable receipt before mutating package status.
- 2026-07-07: Split resume implementation out of a monolithic file into `resume_command/{attempt,destination,events,model,report}.rs`; `commands.rs` remains dispatch-only and unchanged by this child.
- 2026-07-07: Closure evidence recorded in `.10x/evidence/2026-07-07-cli-resume-spine.md`; closure review recorded in `.10x/reviews/2026-07-07-cli-resume-spine-review.md`.
