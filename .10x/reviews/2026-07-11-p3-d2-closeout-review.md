Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-d2-duckdb-arrow-bulk.md
Verdict: pass

# D2 closeout review

## Findings

No critical or significant issue remains. DuckDB-specific ABI, table, provenance, and transaction behavior stays inside `cdf-dest-duckdb`; generic runtime/engine code sees only destination capabilities, bounded durable segments, acknowledgements, and final binding. Adding another destination does not modify this path.

The vtab alternative was correctly rejected because its binding retains passed batches. The isolated Arrow C Stream bridge is narrowly version-pinned, tested for null/length fidelity, and does not expose DuckDB types upstream. Scalar ingestion has been deleted rather than retained as a compatibility path.

The compact physical row key does not redefine public provenance: logical `(package hash, segment id, row ordinal)` remains homogeneous and adapter-neutral. D6 owns cross-destination conformance for that shared contract.

## Verdict

Pass. D2 is complete.

## Residual risk

Pinned Arrow-major skew remains an upgrade-gate concern under the existing decision. Remote/full-year envelope evidence belongs to G4 and the shared destination matrix belongs to D5; neither weakens D2's local adapter acceptance.
