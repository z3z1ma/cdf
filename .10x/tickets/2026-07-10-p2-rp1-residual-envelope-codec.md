Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/specs/residual-variant-capture.md, .10x/tickets/done/2026-07-08-p1-e4-variant-capture-evolution-event.md

# P2 RP1 canonical residual envelope codec

## Scope

Implement the pure `residual-json-v1` model/codec over Arrow scalar and nested values, canonical JSON Pointer paths, structured Arrow type descriptors, and exact decode back to Arrow. Replace the current ad-hoc variant JSON conversion only at the codec boundary; do not change live verdict selection.

## Acceptance criteria

- Envelope serialization matches `.10x/specs/residual-variant-capture.md` for booleans, all integers/floats including non-finite symbols, decimals, UTF-8, binary, temporal/duration/interval, list/struct/map, and null.
- Integer/decimal/temporal precision and binary bytes round-trip exactly.
- RFC 6901 original-source paths escape deterministically; canonical field ordering and bytes are stable.
- Unsupported Arrow values return a typed residual-encoding error suitable for quarantine, never lossy display strings.
- Existing nested variant fixtures migrate through the new codec without implicit promotions.

## Evidence expectations

Property tests over generated Arrow arrays/scalars, canonical golden vectors, adversarial path/type/value cases, cross-version decode rejection, and no-I/O review.

## Explicit exclusions

No validation compiler, batch routing, package artifact, destination, CLI, or promotion behavior.

## Progress and notes

- 2026-07-10: Opened as the pure foundation lane.

## Blockers

None.
