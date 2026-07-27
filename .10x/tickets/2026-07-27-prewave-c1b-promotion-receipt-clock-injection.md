Status: open
Created: 2026-07-27
Updated: 2026-07-27
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-c1-receipt-clock-authority.md`

# Bind receipt clock authority in schema promotion

## Scope

Bind the request's `ExecutionServices` into every destination used by direct schema-promotion
settlement and recovery, preserving the C1 host-clock authority when the product path bypasses the
ordinary run wrapper.

## Non-goals

- No receipt schema, timestamp semantics, promotion lifecycle, or destination physical behavior
  change.
- No process-wall-clock fallback.
- No broad destination construction refactor.

## Acceptance criteria

- Direct single- and multi-target schema-promotion settlement binds the injected host clock before
  destination correction execution.
- Every persisted promotion crash boundary resumes with the same receipt-clock authority.
- The two reproducing CLI product tests pass in isolation and in the relevant product gate.
- No destination receipt/correction path gains a direct `SystemTime::now` fallback.

## References

- `.10x/tickets/done/2026-07-26-prewave-c1-receipt-clock-authority.md`
- `.10x/knowledge/destination-receipt-authority.md`
- `.10x/specs/destination-common-services.md`
- `.10x/specs/execution-host-structured-runtime.md`

## Assumptions

- Record-backed: C1 requires destination receipt time to come from injected
  `ExecutionServices`.
- Source-backed: `SchemaPromotionExecutionRequest` already owns those services; the direct
  settlement path fails before its configured failpoint only because the resolved destination
  never receives them.

## Journal

- 2026-07-27: D1's broader CLI gate passed 270 of 272 tests and reproduced
  `DuckDB commit execution requires injected ExecutionServices for receipt time` in
  `schema_promote_execute_recovers_every_persisted_crash_boundary`; the multi-target test failed
  before its expected schema-promotion failpoint for the same direct-settlement path.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
