Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-a5c-durable-segment-stream.md
Verdict: pass

# A5c adversarial architecture review

## Target

Durable segment publication, verified/accounted package reading, project replay, first-party destination consumption, archive compatibility, and production materialization exclusions.

## Findings

No critical or significant finding remains.

- The memory lease crosses the kernel boundary as opaque ownership; a destination cannot receive an unaccounted verified segment through the production replay path.
- Stream advancement fails before reservation when a prior window is live, preventing accidental collection and same-thread deadlock.
- Destination selection is capability-driven. No project branch names DuckDB, Postgres, Parquet, a file format, or a path id.
- All first-party destinations consume one segment before the next. DuckDB merge and Postgres merge use destination-owned transactional staging rather than CDF-resident package vectors.
- The archive report retains only small per-segment metadata while transcoding one segment at a time.
- The static source gate covers the production surfaces most likely to regress and deliberately excludes test/conformance readers.

## Verdict

Pass. A5c establishes the bounded durable package/destination boundary required by A5e.

## Residual risk

The `MaterializedPackage` capability remains available for a future driver, but generic runtime no longer constructs a package-wide vector. Such a driver could retain sequential segments internally; D1/D5 live capability conformance and the constant-memory law must reject any unbounded implementation. A5e owns that whole-graph enforcement.
