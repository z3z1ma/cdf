Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-d3-postgres-binary-copy.md

# PostgreSQL binary COPY milestone

## What was observed

PostgreSQL ingestion now emits the binary COPY protocol directly from bounded Arrow `RecordBatch` values and advertises only that live path. Production CSV COPY, scalar staging rows, and the unimplemented extended-insert capability were deleted.

The original equal-work release comparison immediately before deletion measured 36,443,900 binary rows/s and 2,054,189 scalar CSV rows/s, a 17.74x encoder speedup across 262,144 rows. After isolating the CSV control from deleted production helpers, the committed test measured 31,841,767 binary rows/s and 13,889,006 rows/s for the simplified scalar CSV control, a 2.29x speedup. The latter is deliberately a stronger baseline and still clears the ticket's 2x encoder threshold.

Thirty non-ignored crate tests passed, including live PostgreSQL append, replace, merge, deduplication, rollback after COPY, duplicate receipt verification, exact Decimal128 NUMERIC round trip, correction, catalog discovery, and source execution. Strict Clippy passed for all crate targets.

A subsequent server-inclusive local PostgreSQL benchmark exposed 4 KiB `CopyInWriter` framing and per-cell Arrow downcasts. A bounded 1 MiB aggregate buffer, tied to the declared path minimum, raised the narrow-schema binary path from 3,170,238 to roughly 3.6M rows/s. Compiled typed column views remove repeated downcasts. The final TLC-shaped equal-work workload (17 user fields plus four provenance fields, 524,288 rows, unlogged tables, synchronous commit disabled equally) measured 1,662,005 binary rows/s versus 570,051 rows/s for the exact removed scalar CSV allocation/escaping shape: 2.92x. The narrow three-user-field shape remained PostgreSQL/wire-size bound near 2x because provenance dominates each row; it is retained as an observed limit rather than generalized away.

## Procedure

From the repository root:

```text
cargo test -p cdf-dest-postgres --all-targets
cargo clippy -p cdf-dest-postgres --all-targets -- -D warnings
cargo test --release -p cdf-dest-postgres binary_copy_encoder_is_at_least_twice_csv -- --ignored --nocapture
cargo test --release -p cdf-dest-postgres live_binary_copy_is_at_least_twice_csv -- --ignored --nocapture
```

## What this supports or challenges

This supports correctness of the implemented wire path for the exercised type/transaction matrix, constant-memory Arrow-batch input, removal of text ingestion, at least 2x encode throughput, and at least 2x server-inclusive local COPY throughput on a TLC-shaped schema versus the removed scalar CSV implementation.

## Limits

The local server is loopback and does not establish a remote network-bound profile. Decimal256 is covered through the shared NUMERIC encoder logic but not yet against a live PostgreSQL server. Allocation counts and the generic runtime's declared `postgres.sync` final-binding lane remain D3/D1 integration acceptance work.
