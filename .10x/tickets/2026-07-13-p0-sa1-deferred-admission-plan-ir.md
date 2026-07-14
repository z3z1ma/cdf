Status: open
Created: 2026-07-13
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-13-p0-single-crossing-schema-admission.md
Depends-On: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# P0 SA1: deferred schema-admission plan IR

## Scope

Compile one source/codec-neutral, total deferred schema-admission operation and serialize its exact observation/verdict evidence into packages without execution-time reparsing or policy invention.

## Non-goals

No source I/O, cache, codec loop, or destination behavior.

## Acceptance criteria

- Plan pins baseline/effective schema, codec semantics, normalizer, contract/type allowances, control fields, cache-key shape, and total verdict choices.
- Execution can instantiate the operation from a physical Arrow observation without compiler/source crate imports.
- Plan/package/replay validation rejects missing or mismatched observation/verdict authority.

## References

- `.10x/specs/single-crossing-schema-admission.md`
- `.10x/specs/residual-variant-capture.md`

## Assumptions

Exact semantics are user-ratified in `.10x/decisions/single-crossing-expensive-source-boundary.md`.

## Journal

Pending.

## Blockers

Depends on FX1 plan authority.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.

