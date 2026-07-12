Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: DuckDB compact row-range provenance implementation and shared provenance decision
Verdict: pass

# DuckDB compact row-range provenance review

## Findings

- The logical public address remains the kernel-owned package/segment/row tuple. Physical keys are allocated, never hash truncations, and never escape correction/readback diagnostics.
- The allocator and range dimension mutate inside the same single-writer destination transaction as payload rows and receipts. Abort/rollback cannot expose a mapping without rows or rows without a mapping. Gaps are harmless; overlapping committed ranges are prevented by the allocator and primary key.
- Correction planning verifies payload row-key uniqueness before addressed mutation. Exact lookup rejects absent mappings and out-of-range ordinals; an update must affect exactly one row.
- Merge, append, and replace use the same one-column physical provenance representation. The removed long-string, ingress-copy, per-segment transfer, and maintained-index shapes have no live fallback.

## Residual risk

The compact model is not yet homogeneous across Postgres and Parquet. D6 owns those implementations and shared conformance. DuckDB correction uniqueness verification is a scan; correction-path indexing/materialization should be benchmarked when correction workloads enter the envelope, without reintroducing an append-time index tax.
