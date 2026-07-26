Status: open
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

# Make driver concurrency law executable

## Scope

Document the source/destination concurrency canon in authoring surfaces and add compile-time and
runtime conformance that proves driver, runtime, finalized-session, staged-session, execution-host,
and portable-plan responsibilities.

## Non-goals

- No new executor, concurrency knob, ingress mode, or blanket `Send + Sync` bound.
- No requirement that a destination implement both ingress categories.
- No destination-identity branch in conformance or orchestration.

## Acceptance criteria

- Public trait documentation states ownership, thread movement, native-handle confinement,
  injected-host, and fail-closed portability laws.
- Compile assertions prove the intended positive bounds without asserting unintended runtime or
  finalized-session bounds.
- Synthetic staged and finalized destinations pass applicable capability-discovered laws or an
  exact sheet-declared exclusion.
- A synthetic source proves synchronous contact-free compile and explicit portability validation
  before isolated execution.
- Existing destination/source conformance remains data-driven and no generic match arm is added.

## References

- `.10x/decisions/driver-session-concurrency-canon.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/destination-extension-runtime-contract.md`
- `.10x/decisions/destination-ingress-protocol-capability-split.md`

## Assumptions

- Source-backed: current trait bounds already implement the desired law.

## Journal

- 2026-07-26: Shaped as documentation/conformance of existing behavior, not a trait-bound rewrite.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
