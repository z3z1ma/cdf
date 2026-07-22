Status: open
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md
Depends-On: .10x/tickets/done/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md

# P3 D18C: DuckDB native write envelope

## Scope

Benchmark DuckDB's native bulk-write controls for narrow and very-wide persistent tables:
`write_buffer_row_group_count`, `write_buffer_row_group_memory_limit`, and database row-group size.
Retain only a destination-owned schema/host-derived policy or explicit tuning surface that improves
the controlled envelope without changing semantics or penalizing ordinary schemas.

## Non-goals

No unconditional row-group setting, hidden hard cap, Parquet-derived tuning rule, or destination-
specific change to canonical package segmentation.

## Acceptance Criteria

- A same-revision matrix records default and candidate settings for the wide package and TLC,
  including wall/CPU/RSS/buffer/temp/spill and downstream read sanity.
- Explicit operator values, if exposed, remain authoritative and are validated before mutation.
- Any automatic policy derives from compiled schema, admitted memory/CPU, and measured DuckDB
  behavior; it has no fixed field-count cutoff.
- A retained default materially improves the wide cell and regresses TLC by no more than 3%; a
  knob-only result must name why no universal default earned promotion.
- Losing configuration/prototype code is absent at closure.

## References

- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/runtime-memory-backpressure.md`
- `.10x/tickets/done/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md`
- `https://duckdb.org/docs/current/configuration/overview.html`
- `https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads.html`

## Assumptions

- Record-backed: DuckDB exposes these controls and documents their memory/performance tradeoffs.
- User-ratified: no hard-coded performance cap; useful constants are derived or explicit knobs and
  defaults require measured no-regression evidence.

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
