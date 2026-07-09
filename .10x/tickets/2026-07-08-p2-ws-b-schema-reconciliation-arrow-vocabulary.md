Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p2-data-onramp-program.md
Depends-On: .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/types-contracts-normalization.md

# P2 WS-B schema reconciliation and Arrow vocabulary

## Scope

Implement one schema truth: observed physical schema is fact, declared schemas and hints constrain it, and reconciliation emits a verdict-bearing coercion plan. Expand declarative field types to the full Arrow vocabulary required by `VISION.md` Chapter 7.

Split executable child tickets before code for vocabulary/parser/JSON Schema, reconciliation core, per-format integration, validation-program serialization, and property tests.

## Acceptance criteria

- Declarative schema can express all integer widths, floats, decimal128/256, dates/times/timestamps/durations, binary/utf8 large variants, and nested list/struct/map forms.
- Width widenings are automatic, lossless, and recorded.
- Lossy casts require `allow_lossy_mapping`; string parse-coercions require explicit `coerce_types`.
- Physical type provenance is preserved in field metadata.
- NDJSON, Parquet, REST, and SQL feed the same reconciliation model instead of format-specific truth paths.
- Published declarative JSON Schema is regenerated and freshness-tested.

## Evidence expectations

Unit/property tests for parser round-trips and widening composition/no-loss, package/plan evidence for physical provenance, negative plan tests for lossy mappings, jscpd/complexity checks for the new parser/reconciler, and conformance ownership before closure.

## Explicit exclusions

This ticket does not implement destination-specific type mapping tables except where needed to prove reconciliation handoff.

## Progress and notes

- 2026-07-08: Opened as P2 workstream owner from the directive.
- 2026-07-08: Split first executable child `.10x/tickets/2026-07-08-p2-ws-b1-declarative-arrow-type-vocabulary.md` for the declarative Arrow vocabulary parser/compiler slice. Reconciliation core remains this workstream's next child.

## Blockers

None.
