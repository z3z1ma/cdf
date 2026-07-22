Status: open
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md
Depends-On: .10x/tickets/done/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md

# P3 D18B: DuckDB sparse-wide projection

## Scope

Use complete, verified package statistics to omit provably all-null nullable user fields from
canonical IPC decode, Arrow-to-DuckDB conversion, and the `INSERT` projection while retaining the
complete destination table schema and exact null values. Implement this within the sole DuckDB
scanner/preparation boundary and use Arrow IPC projection rather than decoding discarded columns.

## Non-goals

No sampled/incomplete-statistics pruning, constant-value synthesis, schema removal, generic-runtime
DuckDB branch, package mutation, or second scanner.

## Acceptance Criteria

- Only complete package-wide `null_count == row_count` evidence bound to the verified package and
  schema may omit a nullable field; missing, incomplete, stale, overflowed, or inconsistent evidence
  retains the field.
- Target tables always contain the complete compiled destination schema and omitted fields read back
  as null for append, replace, and merge.
- `_cdf_package_row_ord`, `_cdf_row_key`, merge keys, correction/provenance fields, and nonnullable
  fields are never omitted.
- Projected IPC readers decode only retained field buffers; scanner schema/type authority remains
  exact and one-path.
- Controlled wide evidence materially improves wall time, CPU, memory, or spill with no more than
  3% TLC regression. Otherwise the implementation is deleted and the ticket closes with the
  measured no-action result.

## References

- `.10x/specs/typed-statistics-evidence.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/decisions/canonical-package-row-ord.md`
- `.10x/tickets/done/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md`

## Assumptions

- Record-backed: complete statistics may prove absence; sampled or incomplete statistics may not.
- User-ratified: optimize sparse-wide ingestion without weakening visible schema or values.

## Journal

None.

## Blockers

Depends on D18A baseline/profile.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
