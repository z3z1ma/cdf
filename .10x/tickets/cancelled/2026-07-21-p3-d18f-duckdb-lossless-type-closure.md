Status: cancelled
Created: 2026-07-21
Updated: 2026-07-22
Parent: .10x/tickets/done/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md

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

- 2026-07-22: Cancelled by explicit user direction as part of the DuckDB closeout. Existing exact
  mappings and fail-closed unsupported/lossy verdicts remain authoritative; no speculative
  representation transform or scalar fallback was added.

## Blockers

None. Cancellation is deliberate, not blocked.

## Evidence

No implementation was attempted. Existing destination type-matrix and round-trip tests remain the
current evidence boundary.

## Review

Cancellation preserves exactness: it neither converts an unsupported mapping into a silent loss nor
adds an unmeasured transform.

## Retrospective

Further type expansion should be driven by a real source/destination need and an exact vectorized
mapping, not by completeness for its own sake.
