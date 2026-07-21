Status: active
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/done/2026-07-21-p0-iceberg-execution-robustness.md

# P0: DuckDB wide-ingest memory adaptation

## Scope

Make DuckDB's canonical segment-scan ingress adapt its scan/sink parallelism to the destination
schema and admitted memory envelope, so a valid very-wide package completes under defaults without
reducing the proven fast path for ordinary schemas. Separate canonical scan/sink concurrency from
DuckDB's global worker count and retry the same finalized package after a typed DuckDB out-of-memory
result, rolling back each failed transaction and progressively reducing only automatic scan
concurrency.

## Non-goals

No generic-runtime DuckDB branch, unbounded memory, unconditional single-thread fallback, hard-coded
column-count cutoff, or global reduction of the measured TLC/default bulk path.

## Acceptance Criteria

- Destination-owned schema/layout evidence derives the safe DuckDB ingest envelope before mutation.
- Narrow schemas retain the admitted host parallelism.
- Wide schemas reduce only the canonical scan/sink workers whose estimated simultaneous footprint
  cannot fit the admitted budget; explicit tuning knobs remain authoritative.
- DuckDB global threads and canonical scan/sink workers are independently configurable; absent an
  explicit scan override, compiled schema layout and the admitted memory budget derive the initial
  scan concurrency without a hard field-count cutoff.
- A typed DuckDB out-of-memory result before commit rolls back the complete transaction and retries
  the same canonical package with geometrically lower scan concurrency through the same ingress
  path; non-memory errors and explicit scan-concurrency overrides never retry automatically.
- Every retry replays all canonical segments, reapplies disposition DDL inside a fresh transaction,
  and cannot publish rows, mirrors, receipts, or checkpoints from a failed attempt.
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
- User-ratified 2026-07-21: global DuckDB threads and scan/sink concurrency are separate controls;
  ordinary schemas keep full concurrency, wide schemas adapt automatically, typed OOM retries roll
  back and reduce concurrency, and explicit memory/concurrency knobs remain authoritative.
- Record-backed: CDF package ordinals, not DuckDB physical insertion order, are the row-order and
  provenance authority.

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
- 2026-07-21: Reopened by explicit user direction after D17 cancellation left valid 2,052-column
  packages unable to commit under defaults. The successor design removes the rejected fixed
  field-count cutoff: it uses a schema-layout memory estimate for initial admission and a typed,
  rollback-safe same-path OOM retry as the correctness backstop. The ordinary scanner remains the
  sole DuckDB ingress path.
- 2026-07-21: Implemented destination-local `DuckDbIngestEnvelope`. `CDF_DUCKDB_THREADS` remains the
  global DuckDB engine ceiling; `CDF_DUCKDB_SCAN_THREADS` is an independent exact scan/sink override.
  With no scan override, the compiled canonical Arrow schema, prepared batch bounds, DuckDB vector
  size, and admitted memory budget derive the initial table-function concurrency. Ordinary schemas
  keep all admitted threads. No field-count cutoff or alternate ingress exists.
- 2026-07-21: Enabled DuckDB's `errors_as_json` setting and classify the structured
  `exception_type` as destination-local `DuckDbExceptionType`; no diagnostic substring controls
  retry. An automatic OOM explicitly rolls back the transaction before the same durable canonical
  segment set is replayed at half the prior concurrency. The writer lock spans every attempt.

## Blockers

None. The user explicitly rejected the previously accepted wide-table residual.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb --lib --locked -j 12`: 44 passed. Focused
  assertions prove an ordinary schema retains all 16 global workers; a 2,052-UTF8-field schema under
  4 GiB derives two scan workers; an explicit scan override disables automatic retry; a real bounded
  DuckDB runtime emits structured `Out of Memory`; and the retry decision observes that the failed
  transaction's DDL and row are absent before returning the lower concurrency.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-dest-duckdb --all-targets --all-features --locked -j 12
  -- -D warnings`: passed.
- Product-level wide-package replay and narrow performance comparison remain pending before closure.

## Review

Historical field-count heuristic review: fail; that prototype remains deleted. Current
schema-layout admission and typed retry implementation: focused author review passed; independent
product-smoke review remains pending.

## Retrospective

Pending product-level evidence and closure review.
