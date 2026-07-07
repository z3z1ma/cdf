Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-run-ledger-commit-session-spine-ratification.md, .10x/decisions/run-ledger-commit-session-spine.md, .10x/specs/run-orchestration-ledger.md

# Run ledger and commit-session spine ratification evidence

## What was observed

The run spine semantics are sufficiently book-backed to ratify as architectural authority without implementing code in this turn.

`VISION.md` explicitly requires:

- `CheckpointStore::commit(checkpoint_id, receipt)` as the only path from proposed to committed state.
- A destination protocol with `begin` returning `CommitSession`, and `CommitSession` operations for migrate/write/finalize/abort.
- `finalize` returning either a durable receipt or an error, with no ambiguous third state.
- Recovery in the durable-receipt-before-checkpoint window by verifying the receipt before opening the commit gate.
- Run/resource/partition/package tracing fields.
- `cdf inspect run <id>` assembling plan, verdicts, receipts, and transitions.
- `cdf resume` draining interrupted work per the crash matrix.
- `cdf replay package <pkg> --to <dest>` as idempotent-by-construction package replay.

Active specs already require destination commit sessions and receipt-gated checkpoint commits. Current source has partial ingredients but not the general spine.

## Procedure

Inspected:

- `VISION.md` lines 820-878 for checkpoint schema, commit gate, positions, scopes, and store trait.
- `VISION.md` lines 887-903 for destination and `CommitSession` trait shape.
- `VISION.md` lines 942-958 for receipt verification, idempotency, replay, and guarantee table.
- `VISION.md` lines 986-1007 for retry discipline and run observability.
- `VISION.md` lines 1027-1050 for CLI command surface.
- `VISION.md` lines 1145-1155 for MVP contents and demonstration.
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/tickets/2026-07-05-cli-surface.md`
- `.10x/tickets/2026-07-05-observability-doctor-status-sql.md`
- `.10x/research/2026-07-07-run-spine-gap-map.md`
- `crates/cdf-kernel/src/destination.rs`
- `crates/cdf-project/src/runtime.rs`
- `crates/cdf-kernel/src/checkpoint.rs`
- `crates/cdf-state-sqlite/src/sqlite.rs`
- `crates/cdf-package/src/model.rs`

## What this supports or challenges

This supports creating `.10x/decisions/run-ledger-commit-session-spine.md` and `.10x/specs/run-orchestration-ledger.md` as active authority.

Current source challenges completion of the spine: `DestinationProtocol` still lacks `begin`, the project runtime still hard-codes DuckDB/file functions, and CLI/observability remain blocked on implementation of the run ledger and general runtime.

## Limits

This evidence does not prove implementation. It supports ratification and ticket splitting only. The exact on-disk run-ledger schema, concrete default run-id string format, migration mechanism, and CLI flag spelling remain implementation details constrained by the active decision and spec.
