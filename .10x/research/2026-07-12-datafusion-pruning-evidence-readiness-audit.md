Status: done
Created: 2026-07-12
Updated: 2026-07-12

# DataFusion pruning evidence readiness audit

## Question

Can J1 implement sound DataFusion `PruningStatistics` directly over the evidence CDF emits today?

## Sources and methods

Inspected VISION Chapter 10.1, 11.3, and 12.1; `.10x/specs/datafusion-currency-bridges.md`; `cdf-kernel::BatchStats`; package manifest/segment models; engine execution/profile emission; package stats writers/readers; and DataFusion 54's pinned `PruningStatistics` contract and Parquet adapters.

## Findings

- VISION requires vectorized per-column min/max/null/distinct statistics on batches, aggregated per segment into `stats/profile.parquet`, and reused by profiling, package evidence, and destination pruning.
- Current `BatchStats` contains nullable `min_lexical`/`max_lexical` strings but no physical type, completeness, schema-generation, or scalar encoding. No production engine path populates those fields.
- Current package execution writes aggregate rows/bytes/batches as `stats/profile.json`; it does not write per-column/per-segment typed statistics.
- `SegmentEntry` contains row/byte/hash identity only.
- DataFusion's vectorized pruning contract requires Arrow arrays in each field's physical type, plus aligned row/null counts. Lexical comparison or guessed parsing is unsound for numeric widths, decimals, timestamps/timezones, NaN, binary, dictionaries, nested values, schema evolution, and absent/incomplete statistics.
- Therefore a direct J1 adapter would either return no useful statistics or invent an artifact/type contract. The missing typed evidence spine is a prerequisite, not J1 implementation detail.

## Conclusions

Add J0 before J1. J0 owns a CDF-native, DataFusion-free typed statistic model; vectorized hot-path computation; deterministic batch-to-segment aggregation; canonical `stats/profile.parquet`; and read APIs. J1 then only marshals admitted complete scalar statistics into DataFusion arrays and conservatively retains everything else. This preserves the engine boundary and prevents DataFusion serialization from entering package identity.

The existing lexical fields are preproduction vestiges, not a compatibility contract. J0 should replace them rather than add a parallel representation.
