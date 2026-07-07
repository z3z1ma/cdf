Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md
Verdict: pass

# Kernel destination commit-session API review

## Target

Review of the additive kernel destination commit-session API implemented for `.10x/tickets/done/2026-07-07-kernel-destination-commit-session-api.md`.

## Assumptions tested

- Existing destination implementations must not be forced to refactor in this child ticket.
- The new API must expose a session lifecycle without becoming a checkpoint gate.
- `finalize` must have no ambiguous success state.
- A generic caller must not be able to bypass receipt verification or synthesize checkpoint advancement through the session API.

## Findings

No blocking findings.

Minor residual risk: `CommitSession::write` currently takes no package-view argument because the session is begun from a `DestinationCommitRequest` and `CommitPlan`; concrete destination tickets may need to refine how package directories or package readers are captured by destination-specific session structs. This is within the destination-refactor child tickets and does not violate this API slice.

Minor residual risk: the kernel trait does not yet standardize destination receipt verification as a protocol method. That is not required by this child ticket. The API still cannot bypass receipt verification because it exposes no checkpoint-store mutation path and returns only a `Receipt`; generic runtime code must remain responsible for calling destination-owned verification before `CheckpointStore::commit`, as specified in `.10x/specs/run-orchestration-ledger.md`.

## Verdict

Pass. The API is additive, synchronous as allowed by `.10x/specs/destination-receipts-guarantees.md`, and keeps the commit gate separate: sessions can return receipts, but they cannot commit checkpoints. The default `begin` behavior keeps existing concrete destinations compiling while failing closed until their session refactor tickets override it.

## Residual risk

Concrete destinations remained unsupported until `.10x/tickets/done/2026-07-07-duckdb-commit-session-refactor.md`, `.10x/tickets/done/2026-07-07-parquet-commit-session-refactor.md`, and `.10x/tickets/done/2026-07-07-postgres-commit-session-refactor.md` landed.
