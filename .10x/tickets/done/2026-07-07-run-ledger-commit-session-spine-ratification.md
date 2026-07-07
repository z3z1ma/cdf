Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md

# Ratify run ledger and commit-session spine

## Scope

Shape and ratify the general run spine that lets CDF compose resources, packages, destinations, checkpoint stores, CLI commands, observability, and crash recovery without hard-coded local-file-to-DuckDB runtime paths.

Owns the decision/specification work for:

- Run identity and run-ledger semantics.
- Run-to-package, run-to-checkpoint, run-to-receipt, and run-to-verdict mapping.
- Transition ordering and recovery story for `run`, `resume`, `replay package`, `inspect run`, and killer-demo crash windows.
- A driver-neutral commit-session abstraction that extends or complements `DestinationProtocol` without weakening destination receipt verification.
- The executable child-ticket split for implementation after ratification.

This is a shaping/ratification ticket, not an executable implementation ticket.

## Acceptance criteria

- An active decision records the run-ledger contract: run id minting, caller-supplied id policy, default id policy, run scope, multi-resource/multi-package boundaries, transition ordering, retry/resume behavior, duplicate behavior, and ownership of run metadata.
- An active specification update records how a general run composes `ResourceStream`, package creation, destination commit sessions, receipt verification, checkpoint commit, package status transitions, and observable run ledger facts.
- The design preserves the existing commit-gate invariant: no checkpoint advances unless a durable destination receipt has been verified.
- The design explicitly maps current specialized DuckDB/file functions to compatibility wrappers or migration targets so existing conformance and golden fixtures do not churn unnecessarily.
- The design identifies the first executable implementation children, including kernel/session API, destination-session consumers for DuckDB/Parquet/Postgres, project runtime general orchestrator, CLI wiring, and inspect-run observability.
- The design records explicit exclusions for the first implementation wave, including scheduling, distributed execution, streaming supervisor, vault-class secret providers, and UI.

## Evidence expectations

Record source inspection and decision review evidence before opening executable implementation tickets. Evidence should cover the current specialized runtime APIs, destination protocol limitations, CLI blockers, observability blockers, and the VISION Chapter 23 killer-demo requirements.

## Explicit exclusions

No source edits, no destination refactor, no CLI run widening, no run-ledger schema migration, no checkpoint-store changes, no conformance fixture updates, and no implementation tickets marked executable until the run semantics above are ratified.

## References

- `VISION.md` D-1, D-4, D-5, D-12, D-20, Chapter 18, Chapter 20, and Chapter 23.
- `.10x/research/2026-07-07-run-spine-gap-map.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/tickets/2026-07-05-cli-surface.md`
- `.10x/tickets/2026-07-05-observability-doctor-status-sql.md`
- `.10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md`
- `.10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md`
- `.10x/tickets/done/2026-07-06-live-local-file-run-golden-conformance.md`

## Decision

- `.10x/decisions/run-ledger-commit-session-spine.md`

## Specification

- `.10x/specs/run-orchestration-ledger.md`

## Evidence

- `.10x/evidence/2026-07-07-run-ledger-commit-session-spine-ratification.md`

## Review

- `.10x/reviews/2026-07-07-run-ledger-commit-session-spine-review.md`

## Progress and notes

- 2026-07-07: Opened after a user-provided architecture audit and parent source inspection both identified the same gap: the runtime has correct specialized DuckDB/file orchestration, but no ratified general run ledger or driver-neutral commit-session spine. The current SQL source execution child remains in progress separately; this ticket owns the next parent-level architecture decision path.
- 2026-07-07: Ratified active decision `.10x/decisions/run-ledger-commit-session-spine.md` and active spec `.10x/specs/run-orchestration-ledger.md`. Updated destination and CLI/observability specs to point at the run spine. Opened implementation parent `.10x/tickets/2026-07-07-run-spine-implementation-program.md` and child tickets for kernel API, DuckDB/Parquet/Postgres session refactors, run ledger store, general project orchestrator, and CLI run/resume/replay/inspect wiring.

## Blockers

None. Implementation is owned by `.10x/tickets/2026-07-07-run-spine-implementation-program.md` and its children.
