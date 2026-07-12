Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-d2-duckdb-arrow-bulk.md
Verdict: pass

# DuckDB overlapped staged-ingress review

## Findings

No critical or significant finding remains in this tranche. Generic project/runtime code selects staged ingress and blocking lanes exclusively from destination capabilities. It carries compiled schema and disposition authority without knowing DuckDB table layout or provenance encoding. DuckDB keeps uncommitted rows isolated until exact verified package binding, rolls back on abort, validates canonical segment order/count/schema, and emits the existing receipt shape. Package receipt recording remains generic.

The review rejected treating compact DuckDB row keys as public provenance. The active shared decision keeps `(package hash, segment id, row ordinal)` as the only logical address and requires all adapters to expose it through homogeneous inspection/correction interfaces; physical dictionaries/ranges are adapter-owned optimizations.

## Residual risk

Artifact-only replay constructs default execution services when no host is injected. That pre-existing fallback remains an active runtime-host integration concern and does not affect ordinary injected-host execution. D2 stays open pending its complete envelope and remaining conformance rather than claiming program closure from one workload.
