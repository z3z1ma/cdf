Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-run-spine-implementation-program.md
Depends-On: .10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md, .10x/specs/resource-authoring-planning-batches.md, .10x/specs/run-orchestration-ledger.md, .10x/decisions/non-file-window-close-checkpoint-semantics.md

# Ratify and implement non-file window-close checkpoint semantics

## Scope

Define and implement project-run checkpoint advancement for non-file resource streams whose source positions cannot be represented by one exact zero-lag cursor position.

Owns:

- Inexact cursor ordering and nonzero cursor lag window-close behavior in project-run state-delta construction.
- Multi-segment cursor aggregation rules for one logical run.
- Page-token or mixed cursor/page-token state-position policy for REST resources.
- Tests proving unsupported or unratified combinations fail closed before checkpoint advancement.

## Acceptance Criteria

- For cursor resources where the active spec is sufficient, committed checkpoint position advances according to window-close semantics instead of the naive maximum.
- Multi-segment non-file runs have deterministic aggregation rules or fail closed with a specific contract error.
- Page-token and mixed cursor/page-token resources either have ratified checkpoint semantics or fail closed before checkpoint mutation.
- Recovery and replay still do not contact sources after package finalization or durable receipt.

## Blockers

None from user for numeric, timestamp, and date cursor window-close semantics. Page-token-only, mixed page-token/cursor, unsupported cursor value kinds, divergent source-position variants, and incompatible cursor fields remain fail-closed behavior under `.10x/decisions/non-file-window-close-checkpoint-semantics.md`.

## Explicit Exclusions

No scheduler/resident streaming, no arbitrary SQL query execution, no live external HTTP credentials, and no CLI presentation work.

## Evidence Expectations

Run focused project-runtime tests for cursor aggregation/window-close cases, REST runtime tests where page-token behavior is involved, workspace check/clippy, relevant security scans, and review mapping each supported state-position variant to ratified semantics.

## Progress and Notes

- 2026-07-07: Opened during closure of `.10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md`. That ticket intentionally supports only exact zero-lag non-file cursors and fail-closes inexact, lagged, missing, or divergent non-file positions.
- 2026-07-07: User ratified `.10x/decisions/non-file-window-close-checkpoint-semantics.md`: implement window-close for numeric/timestamp/date cursors; treat page tokens as pagination transport unless paired with a ratified cursor; fail closed for page-token-only, mixed, unsupported, divergent, or incompatible positions.
