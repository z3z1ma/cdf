Status: blocked
Created: 2026-07-13
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md
Depends-On: .10x/tickets/done/2026-07-13-p0-sa0-cold-discovery-final-plan-lifecycle.md, .10x/tickets/done/2026-07-13-p0-sa1-compiled-stream-admission-plan.md, .10x/tickets/done/2026-07-11-p3-h1-interop-measurement-copy-proof.md, .10x/tickets/done/2026-07-11-p3-h2-python-incremental-arrow-boundary.md, .10x/tickets/2026-07-11-p3-h4-wasm-cost-interface-model.md, .10x/tickets/2026-07-08-wasm-wit-interface-foundation.md

# P0 SA4: single-invocation dynamic producer admission

## Scope

Apply the cold bootstrap barrier and compiled stream admission to Python, Lua, and WASM schema handshakes or retained first batches so arbitrary user code executes once per partition absent retry/replay.

## Non-goals

No new language runtime or connector-specific schema semantics.

## Acceptance criteria

- Optional cheap schema handshakes are explicit capabilities.
- Producers without handshakes start once; retained first batches flow downstream.
- Cancellation, retry, quarantine, and replay preserve invocation and package evidence semantics.
- Process/component counters prove no hidden discovery invocation.

## References

- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/tickets/done/2026-07-11-p3-h2-python-incremental-arrow-boundary.md`
- `.10x/tickets/2026-07-11-p3-h4-wasm-cost-interface-model.md`

## Assumptions

The same SA1 plan operation applies across native and dynamic sources.

## Journal

- 2026-07-18: Closure graph audit found SA0-SA3 complete but SA4 not executable: upstream dynamic-producer owners had not yet supplied the full incremental boundary needed to count one invocation, retain bootstrap batches, and continue the same producer under the compiled admission program. Marked this ticket blocked on H1/H2/H4/WIT rather than implementing a source-specific workaround.
- 2026-07-18: IX1 closed the neutral foreign-stream contract. SA4 remains blocked because measurement/copy proof, concrete Python migration, WASM cost/interface validation, and WIT recursive composite projection are still upstream of a source-agnostic dynamic producer admission implementation.
- 2026-07-19: H2 closed the concrete Python neutral producer migration. SA4 remains blocked only on H4 and the WIT recursive composite projection; it must reuse H2's neutral lifecycle rather than add Python-specific bootstrap code.

## Blockers

Blocked on H4 and the WIT foundation's ratified recursive composite value projection for foreign-boundary scope/source-position values. H1 and H2 are done. SA4 must not invent dynamic-producer bootstrap semantics ahead of the remaining source-neutral boundary.

## Evidence

- 2026-07-18 ticket/source audit:
  - `.10x/tickets/done/2026-07-11-p0-ix1-neutral-foreign-stream-contract.md` now supplies the neutral producer descriptor/outcome/control/terminal vocabulary.
  - `.10x/tickets/done/2026-07-11-p3-h2-python-incremental-arrow-boundary.md` now supplies the concrete incremental Python producer, cancellation/memory boundary, and runtime-resolved lane behavior.
  - `.10x/tickets/2026-07-11-p3-h4-wasm-cost-interface-model.md` is open and depends on H1 and the WIT foundation.
  - `.10x/tickets/2026-07-08-wasm-wit-interface-foundation.md` is blocked on recursive composite value projection.
  - `crates/cdf-python` now implements the neutral producer lifecycle. H4/WIT still block a language-neutral bootstrap barrier across Python/Lua/WASM, so SA4 remains correctly blocked rather than specializing Python.

## Review

Pass for graph correction. The blocked status prevents premature implementation in the wrong layer and keeps the parent program honest.

## Retrospective

The bootstrap-barrier law is source-neutral, but the concrete enforcement point must be the neutral foreign-stream producer boundary. Implementing it in SA4 before H1/H2/H4 would recreate the exact leaky, source-specific architecture this program exists to remove.
