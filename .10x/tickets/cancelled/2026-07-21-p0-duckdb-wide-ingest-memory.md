Status: cancelled
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/done/2026-07-21-p0-iceberg-execution-robustness.md

# P0: DuckDB wide-ingest memory adaptation

## Scope

Make DuckDB's canonical segment-scan ingress adapt its native parallelism and row-group write
buffering to the destination schema and admitted memory envelope, so a valid very-wide package
completes under defaults without reducing the proven fast path for ordinary schemas.

## Non-goals

No generic-runtime DuckDB branch, unbounded memory, unconditional single-thread fallback, hard-coded
column-count cutoff, or global reduction of the measured TLC/default bulk path.

## Acceptance Criteria

- Destination-owned schema/layout evidence derives the safe DuckDB ingest envelope before mutation.
- Narrow schemas retain the admitted host parallelism and normal write buffering.
- Wide schemas reduce only the native resources whose simultaneous row-group footprint cannot fit
  the admitted budget; explicit tuning knobs remain authoritative.
- The finalized 3.5-million-row `flolake.transactions` package replays into DuckDB under the default
  4 GiB CDF memory budget with a verified receipt and checkpoint.
- Focused tests cover auto adaptation and explicit overrides, and the existing DuckDB suite remains
  green.

## References

- `.10x/decisions/duckdb-stream-scan-staged-ingress.md`
- `.10x/evidence/2026-07-14-p3-f2-duckdb-native-resource-envelope.md`
- `.10x/tickets/done/2026-07-18-p3-d14-duckdb-nanoarrow-080-lz4-revalidation.md`
- [DuckDB configuration reference](https://duckdb.org/docs/stable/configuration/overview)

## Assumptions

- User-ratified 2026-07-21: valid data must execute under robust, performant defaults rather than
  requiring operators to guess destination thread or memory settings.
- Record-backed: CDF package ordinals, not DuckDB physical insertion order, are the row-order and
  provenance authority.
- Official DuckDB 1.5 configuration: `write_buffer_row_group_count` controls completed row groups
  retained during bulk ingestion and reducing it lowers memory consumption.

## Journal

- 2026-07-21: The source and Parquet-destination smoke succeeded, then the same fresh package failed
  default DuckDB materialization at 3.3 GiB used. A replay with the existing explicit
  `CDF_DUCKDB_THREADS=1` knob succeeded, proving the package and type mapping are valid, but took
  165.80 seconds and peaked at 4.16 GB RSS. The defect is DuckDB write-envelope selection, not
  source correctness.
- 2026-07-21: Rejected an unconditional `write_buffer_row_group_count=1` change: it still exhausted
  memory at host-wide threads and would penalize narrow schemas. DuckDB ingress now derives the
  default native thread count from actual destination field count and the admitted memory limit,
  applies the smaller row-group buffer only when that adaptation is active, and leaves explicit
  `CDF_DUCKDB_THREADS` authority untouched. The logic remains entirely inside the destination.
- 2026-07-21: Cancelled the proposed field-count coefficient before commit. Adversarial review
  correctly found that the fixed per-field/thread estimate had survival evidence but no TLC/FineWeb
  no-regression evidence and overlapped the active D17 destination-owned wide-ingest work. The
  prototype was deleted rather than retained as a second path. D17 is the sole implementation owner.

## Blockers

Cancelled into `.10x/tickets/cancelled/2026-07-18-p3-d17-duckdb-wide-string-overlap.md`.

## Evidence

- Untuned optimized replay of the finalized 3.5-million-row, 2,052-column package completed with a
  verified receipt/checkpoint in `real 108.06` and maximum RSS 4,842,225,664 bytes. The logical CDF
  budget is not an RSS ceiling for DuckDB/OS allocations; this proves default completion rather than
  final wide-ingest performance.
- Focused native-resource tests prove a 2,054-field/4 GiB layout derives two DuckDB threads and one
  retained completed row group, a 20-field layout retains all 16 admitted threads and the normal
  write buffer, and an explicit 16-thread override remains authoritative.

## Review

Fail for the proposed heuristic: correctness completion alone did not justify a default throughput
change. No heuristic code remains in this P0 tranche.

## Retrospective

Native destination memory is a function of schema width as well as rows and bytes, but a plausible
coefficient is not measured authority. Keep one destination-owned D17 path and require controlled
narrow/wide evidence before changing the default.
