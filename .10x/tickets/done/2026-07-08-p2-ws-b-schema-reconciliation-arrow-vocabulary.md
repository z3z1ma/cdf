Status: done
Created: 2026-07-08
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-data-onramp-program.md
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

Unit/property tests for parser round-trips and widening composition/no-loss, package/plan evidence for physical provenance, negative plan tests for lossy mappings, the `QUALITY.md` profile selected for parser/reconciler change-set risk, and conformance ownership before closure.

## Explicit exclusions

This ticket does not implement destination-specific type mapping tables except where needed to prove reconciliation handoff.

## Progress and notes

- 2026-07-08: Opened as P2 workstream owner from the directive.
- 2026-07-08: Split first executable child `.10x/tickets/done/2026-07-08-p2-ws-b1-declarative-arrow-type-vocabulary.md` for the declarative Arrow vocabulary parser/compiler slice. Reconciliation core remains this workstream's next child.
- 2026-07-09: B1 closed in `.10x/tickets/done/2026-07-08-p2-ws-b1-declarative-arrow-type-vocabulary.md`. Evidence: `.10x/evidence/2026-07-09-p2-ws-b1-declarative-arrow-type-vocabulary.md`. Review: `.10x/reviews/2026-07-09-p2-ws-b1-declarative-arrow-type-vocabulary-review.md`. Declarative Arrow type expressibility is implemented; observed-vs-declared reconciliation remains this workstream's next executable child.
- 2026-07-09: Split `.10x/tickets/done/2026-07-09-p2-ws-b2-schema-reconciliation-core.md` for the format-independent observed-vs-constraint reconciler and widening/coercion plan core. Per-format integration remains later WS-B work.
- 2026-07-09: B2 closed in `.10x/tickets/done/2026-07-09-p2-ws-b2-schema-reconciliation-core.md`. Evidence: `.10x/evidence/2026-07-09-p2-ws-b2-schema-reconciliation-core.md`. Review: `.10x/reviews/2026-07-09-p2-ws-b2-schema-reconciliation-core-review.md`. The shared reconciler exists; per-format integration and validation-program execution remain later WS-B children.
- 2026-07-09: Split `.10x/tickets/done/2026-07-09-p2-ws-b3-parquet-declared-schema-reconciliation.md` for the first per-format integration: local Parquet declared-schema reconciliation and lossless batch casting.
- 2026-07-09: B3 closed in `.10x/tickets/done/2026-07-09-p2-ws-b3-parquet-declared-schema-reconciliation.md`. Evidence: `.10x/evidence/2026-07-09-p2-ws-b3-parquet-declared-schema-reconciliation.md`. Review: `.10x/reviews/2026-07-09-p2-ws-b3-parquet-declared-schema-reconciliation-review.md`. Local Parquet declared-schema reads now use the shared reconciler and materialize supported Arrow width casts; policy plumbing, validation-program serialization, and conformance golden paths remain later WS-B/WS-I work.
- 2026-07-09: B4 closed in `.10x/tickets/done/2026-07-09-p2-ws-b4-widening-property-conformance.md` with shared evidence `.10x/evidence/2026-07-09-p2-e2-g1-b4-batch.md` and review `.10x/reviews/2026-07-09-p2-e2-g1-b4-batch-review.md`. `cdf-conformance` now owns property tests for signed/unsigned integer widening composition, float32-to-float64 preservation, date32-to-timestamp preservation, and `FieldCoercionDecision::Widened` verdict classification. Remaining WS-B scope: validation-program serialization/in-package evidence for coercion plans, additional per-format integration where required, and final S1-S8 conformance closure.
- 2026-07-09: Split executable child `.10x/tickets/done/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md` for validation-program/package evidence serialization of schema coercion plans.
- 2026-07-09: B5 closed as `.10x/tickets/done/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md` with evidence `.10x/evidence/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md` and review `.10x/reviews/2026-07-09-p2-ws-b5-validation-program-coercion-evidence-review.md`. Validation-program/package evidence now records current schema-coercion plans and physical type provenance. Remaining WS-B scope is additional per-format integration where required and final S1-S8 conformance closure.
- 2026-07-09: Split executable child, now terminal at `.10x/tickets/done/2026-07-09-p2-ws-b6-json-family-observed-reconciliation.md`, for the first remaining non-Parquet live reconciliation gap: local JSON/NDJSON observed-first reconciliation with existing source-decode quarantine preserved.
- 2026-07-09: B6 closed as `.10x/tickets/done/2026-07-09-p2-ws-b6-json-family-observed-reconciliation.md` with `.10x/evidence/2026-07-09-p2-a8-b6-i3-integration.md` and `.10x/reviews/2026-07-09-p2-a8-b6-i3-integration-review.md`. Local JSON/NDJSON now uses observed-first reconciliation with policy-aware localized drift, and coercion evidence fails closed across its dual provenance channels. Remaining formats and final conformance stay open.
- 2026-07-09: Split executable child, now terminal at `.10x/tickets/done/2026-07-09-p2-ws-b7-rest-observed-reconciliation.md`, to bring declarative REST response execution onto the same observed-first JSON reconciliation and evidence-provenance path.
- 2026-07-09: B7 closed with `.10x/evidence/2026-07-09-p2-b7-f2-integration.md` and `.10x/reviews/2026-07-09-p2-b7-f2-integration-review.md`. REST pages now share observed-first reconciliation, localized quarantine, physical provenance, multi-page plan consistency, and dual-channel package evidence without runtime-only coercion authority. Remaining WS-B scope includes a ratified compiled type-policy surface, other source archetypes where applicable, and final S1-S8 conformance.
- 2026-07-10: Workstream closed after Tier-0 type allowances, Hints constraints, Parquet/Arrow IPC/JSON/REST observed-first integration, serialized coercion evidence, full Arrow declarative vocabulary, widening properties, and S1-S8 conformance landed. Aggregate evidence/review are recorded at the P2 parent.

## Blockers

None.
