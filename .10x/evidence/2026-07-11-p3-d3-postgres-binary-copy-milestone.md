Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-d3-postgres-binary-copy.md

# PostgreSQL binary COPY milestone

## What was observed

PostgreSQL ingestion now emits the binary COPY protocol directly from bounded Arrow `RecordBatch` values and advertises only that live path. Production CSV COPY, scalar staging rows, and the unimplemented extended-insert capability were deleted.

The original equal-work release comparison immediately before deletion measured 36,443,900 binary rows/s and 2,054,189 scalar CSV rows/s, a 17.74x encoder speedup across 262,144 rows. After isolating the CSV control from deleted production helpers, the committed test measured 31,841,767 binary rows/s and 13,889,006 rows/s for the simplified scalar CSV control, a 2.29x speedup. The latter is deliberately a stronger baseline and still clears the ticket's 2x encoder threshold.

Thirty non-ignored crate tests passed, including live PostgreSQL append, replace, merge, deduplication, rollback after COPY, duplicate receipt verification, exact Decimal128 NUMERIC round trip, correction, catalog discovery, and source execution. Strict Clippy passed for all crate targets.

## Procedure

From the repository root:

```text
cargo test -p cdf-dest-postgres --all-targets
cargo clippy -p cdf-dest-postgres --all-targets -- -D warnings
cargo test --release -p cdf-dest-postgres binary_copy_encoder_is_at_least_twice_csv -- --ignored --nocapture
```

## What this supports or challenges

This supports correctness of the implemented wire path for the exercised type/transaction matrix, constant-memory Arrow-batch input, removal of text ingestion, and at least 2x encode throughput versus an isolated scalar CSV control.

## Limits

The release comparison isolates encoding rather than a server-inclusive local COPY wall clock. It does not yet measure a remote network-bound profile, allocation counts, Decimal256 against a live PostgreSQL server, or the generic runtime's declared `postgres.sync` final-binding lane. Those remain D3/D1 integration acceptance work.
