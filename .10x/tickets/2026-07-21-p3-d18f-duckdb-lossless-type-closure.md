Status: open
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md

# P3 D18F: DuckDB lossless type closure

## Scope

Audit every canonical Arrow type against DuckDB v1.5.4 and close mappings that can be made exact by
a destination-owned vectorized representation transform. At minimum falsify Float16 widening,
run-end expansion, and dense-to-sparse union normalization. Preserve explicit allowance-gated or
unsupported verdicts where DuckDB cannot represent the value and semantics losslessly.

## Non-goals

No implicit Decimal256-to-float, nanosecond-zoned timestamp truncation, stringification presented as
lossless, per-row scalar conversion, or parallel logical type system.

## Acceptance Criteria

- A closed type matrix distinguishes exact native, exact vectorized transform, lossy allowance-
  gated, and fidelity-impossible mappings with field-level remediation.
- Every retained exact transform is compiled before mutation, vectorized, memory-accounted,
  package/replay deterministic, and round-tripped through the sole scanner.
- Float16 values, run-end boundaries/nulls, and dense-union type ids/offsets have adversarial exact-
  value tests if their transforms are retained.
- Decimal precision/scale, timestamp unit/timezone, nested children, dictionary, map/list/struct,
  empty/invalid forms, and unsupported cases remain fail-closed rather than silently coerced.
- Type-matrix throughput shows no ordinary-schema regression beyond 3%; scalar fallbacks are absent.

## References

- `.10x/specs/types-contracts-normalization.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/tickets/done/2026-07-21-p0-temporal-destination-fidelity.md`

## Assumptions

- Record-backed: Arrow remains the canonical type system and lossy destination mappings require an
  explicit allowance.
- User-ratified: minimize unsupported mappings across destinations while preserving exactness and
  performance.

## Journal

None.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
