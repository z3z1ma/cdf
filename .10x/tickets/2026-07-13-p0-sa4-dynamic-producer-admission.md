Status: open
Created: 2026-07-13
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-13-p0-single-crossing-schema-admission.md
Depends-On: .10x/tickets/2026-07-13-p0-sa1-deferred-admission-plan-ir.md

# P0 SA4: single-invocation dynamic producer admission

## Scope

Apply deferred admission to Python, Lua, and WASM schema handshakes or retained first batches so arbitrary user code executes once per partition absent retry/replay.

## Non-goals

No new language runtime or connector-specific schema semantics.

## Acceptance criteria

- Optional cheap schema handshakes are explicit capabilities.
- Producers without handshakes start once; retained first batches flow downstream.
- Cancellation, retry, quarantine, and replay preserve invocation and package evidence semantics.
- Process/component counters prove no hidden discovery invocation.

## References

- `.10x/specs/single-crossing-schema-admission.md`
- `.10x/tickets/2026-07-11-p3-h2-python-incremental-arrow-boundary.md`
- `.10x/tickets/2026-07-11-p3-h4-wasm-cost-interface-model.md`

## Assumptions

The same SA1 plan operation applies across native and dynamic sources.

## Journal

Pending.

## Blockers

Depends on SA1 and the relevant language runtime tickets.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.

