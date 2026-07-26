Status: open
Created: 2026-07-13
Updated: 2026-07-25
Parent: .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md
Depends-On: .10x/tickets/done/2026-07-13-p0-sa0-cold-discovery-final-plan-lifecycle.md, .10x/tickets/done/2026-07-13-p0-sa1-compiled-stream-admission-plan.md, .10x/tickets/done/2026-07-11-p3-h1-interop-measurement-copy-proof.md, .10x/tickets/done/2026-07-11-p3-h2-python-incremental-arrow-boundary.md

# P0 SA4: single-invocation dynamic producer admission

## Scope

Apply the cold bootstrap barrier and compiled stream admission to the currently registered dynamic-producer boundary, beginning with the existing neutral Python producer, so arbitrary user code executes once per partition absent retry/replay. Future Lua and WASM producers inherit this law when separately activated; they are not prerequisites for proving it.

## Non-goals

No new language runtime, Lua/WASM implementation, or connector-specific schema semantics.

## Acceptance criteria

- Optional cheap schema handshakes are explicit capabilities.
- Producers without handshakes start once; retained first batches flow downstream.
- Cancellation, retry, quarantine, and replay preserve invocation and package evidence semantics.
- Process/component counters prove no hidden discovery invocation.

## References

- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/tickets/done/2026-07-11-p3-h2-python-incremental-arrow-boundary.md`

## Assumptions

The same SA1 plan operation applies across native and dynamic sources.

## Journal

- 2026-07-18: Closure graph audit found SA0-SA3 complete but SA4 not executable: upstream dynamic-producer owners had not yet supplied the full incremental boundary needed to count one invocation, retain bootstrap batches, and continue the same producer under the compiled admission program. Marked this ticket blocked on H1/H2/H4/WIT rather than implementing a source-specific workaround.
- 2026-07-18: IX1 closed the neutral foreign-stream contract. SA4 remains blocked because measurement/copy proof, concrete Python migration, WASM cost/interface validation, and WIT recursive composite projection are still upstream of a source-agnostic dynamic producer admission implementation.
- 2026-07-19: H2 closed the concrete Python neutral producer migration. SA4 remains blocked only on H4 and the WIT recursive composite projection; it must reuse H2's neutral lifecycle rather than add Python-specific bootstrap code.
- 2026-07-25: Backlog grooming removed speculative WASM implementation from P3 closure. The completed neutral producer contract and Python implementation are sufficient to prove the bootstrap-barrier law without waiting for an unimplemented runtime. Future Lua/WASM producers must conform to the same law when activated.

## Blockers

None. H1 and H2 established the neutral producer boundary and its concrete Python implementation.

## Evidence

- 2026-07-18 ticket/source audit:
  - `.10x/tickets/done/2026-07-11-p0-ix1-neutral-foreign-stream-contract.md` now supplies the neutral producer descriptor/outcome/control/terminal vocabulary.
  - `.10x/tickets/done/2026-07-11-p3-h2-python-incremental-arrow-boundary.md` now supplies the concrete incremental Python producer, cancellation/memory boundary, and runtime-resolved lane behavior.
  - `crates/cdf-python` implements the neutral producer lifecycle and supplies the concrete proof surface for the source-neutral bootstrap barrier.
  - Future language runtimes are consumers of this invariant, not prerequisites for defining or testing it.

## Review

Pass for graph correction. The blocked status prevents premature implementation in the wrong layer and keeps the parent program honest.

## Retrospective

The bootstrap-barrier law is source-neutral, but proof should proceed through a concrete registered producer. Waiting for speculative runtimes made the dependency graph circular; future producers should inherit the proven contract instead.
