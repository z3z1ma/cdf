Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-d6-compact-provenance-conformance.md
Verdict: pass

# Postgres compact-provenance review

## Findings

No critical or significant finding remains in the Postgres slice. Allocation and range publication occur in the same database transaction as staging, target mutation, mirrors, and receipt creation. PostgreSQL row locking serializes allocator updates across concurrent writers; rollback returns both allocation and mapping. Range end is exclusive and row ordinal resolution is exact. Unique target/package/segment and row-start constraints reject ambiguous mappings.

The adapter owns SQL, physical keys, and range tables. Kernel/runtime contracts continue to carry only logical package/segment/row authority; no generic runtime or conformance branch names Postgres or its physical columns. Corrections and residual readback accept only `RowProvenanceAddress` and join through the range dimension. The physical key remains opaque.

The review also scanned production Postgres source for the removed `_cdf_load`, `_cdf_segment`, and `_cdf_row` payload names. Remaining `record_cdf_load` occurrences name the receipt-mirror operation, not the deleted payload layout; the reserved-prefix test intentionally rejects `_cdf_load` as a user column.

## Residual risk

Long-horizon range metadata grows once per segment, not once per row. D6/F2 own bounded inspection and retention treatment across all destinations; this slice does not invent deletion semantics.
