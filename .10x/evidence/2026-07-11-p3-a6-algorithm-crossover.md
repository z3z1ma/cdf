Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-a6-spillable-package-dedup.md

# Dedup fast/external crossover measurement

## What was observed

The release harness compared the simple unaccounted hash reference, the accounted in-memory winner fast path, and the bounded external merge path. Times are internal algorithm intervals and exclude compilation:

| Workload | Rows | Reference | Accounted fast | External merge | Fast spill | External spill |
|---|---:|---:|---:|---:|---:|---:|
| all unique | 250,000 | 25.97 ms | 59.25 ms | 63.48 ms | 1 B | 23.0 MB |
| 50% duplicate | 250,000 | 14.09 ms | 38.34 ms | 58.17 ms | 1 B | 20.5 MB |
| 17-key skew | 250,000 | 4.67 ms | 20.82 ms | 54.59 ms | 1 B | 18.0 MB |
| all identical | 250,000 | 4.43 ms | 21.39 ms | 40.78 ms | 1 B | 18.0 MB |
| 1 KiB wide unique | 100,000 | 81.86 ms | pressure-transitioned 393.58 ms | 374.32 ms | 420.8 MB | 420.8 MB |

The accounted path includes owned key state and canonical decisions that the borrowing reference omits, so the reference is a semantic lower bound rather than an interchangeable implementation. For complete key/decision state at or below 64 MiB, the in-memory mode avoids external key/sort writes and is 1.1–2.6x faster than external merge in measured fitting cases. Above the cap or under shared-pool pressure, it transitions losslessly to external merge; wide keys reduce merge fan-in from their measured width.

## Procedure

`cargo test -p cdf-engine --release dedup_external_merge_crossover_benchmark -- --ignored --nocapture`

The committed ignored release harness generates all-unique, uniform duplicate, high-skew, all-identical, and wide-composite exact keys. It records row count, three wall intervals, ratios, and spill bytes as JSON.

Generated conformance separately ran 2,000 skewed keys through chunk sizes 1, 3, 17, 257, and 2,000 for first and last semantics; every external decision matched the simple reference. A forced shared-memory-pressure test transitioned an in-memory prefix to multiple external runs and returned identical unique decisions with a balanced ledger.

## What this supports

Select the accounted in-memory index only while its complete state remains inside a 64 MiB grant; otherwise use the bounded external merge algorithm. Memory budget, pressure, transition point, chunking, and merge layout remain nonidentity tuning because both modes emit identical ordinal decisions.

## Limits

This is one NVMe laptop host and key-index cost, not full Arrow key encoding, payload IPC, Parquet provenance, or 100 GB RSS stress. The external implementation currently retains obsolete intermediate run files until barrier cleanup, making recorded spill bytes a conservative upper bound; compaction-time deletion can reduce write residency without changing decisions.
