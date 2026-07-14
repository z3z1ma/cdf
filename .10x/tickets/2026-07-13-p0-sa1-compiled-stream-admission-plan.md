Status: open
Created: 2026-07-13
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md

# P0 SA1: compiled stream-admission plan IR

## Scope

Compile one source/codec-neutral, total stream-admission operation against a fixed output schema and serialize its exact physical-observation/verdict evidence into packages without execution-time reparsing, reoptimization, or policy invention.

## Non-goals

No source I/O, cache, codec loop, or destination behavior.

## Acceptance criteria

- Plan pins baseline/effective schema, codec semantics, normalizer, contract/type allowances, control fields, cache-key shape, and total verdict choices.
- Execution can instantiate the operation from a physical Arrow observation without compiler/source crate imports.
- Plan/package/replay validation rejects missing or mismatched observation/verdict authority.

## References

- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/specs/residual-variant-capture.md`

## Assumptions

Exact semantics are user-ratified in `.10x/decisions/fixed-schema-discovery-and-stream-admission.md`.

## Journal

Pending.

## Blockers

None. FX1's compiled format binding prerequisite is committed and evidenced.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
