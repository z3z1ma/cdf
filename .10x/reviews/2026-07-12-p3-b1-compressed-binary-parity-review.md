Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-source-files/src/runtime.rs, crates/cdf-project/src/schema_discovery.rs
Verdict: pass

# Compressed binary parity review

## Findings

The initial implementation failed review and runtime testing because it nested synchronous `run_io` inside an I/O worker. Stack sampling localized the wait to `stream_file_match -> spool_transformed_file -> ExecutionServices::run_io`. The code was restructured rather than masked with more runtime threads.

No critical or significant findings remain in this slice. Transport preparation is synchronous and outside the runtime task; transform and format decode are async and ledger-accounted inside it. Multi-file compressed discovery proves each candidate participates rather than sampling the first file accidentally.

## Residual risk

Synchronous transport preparation can still block the caller and remote transformed inputs make two disk passes. Those are explicit P3 G1/G2 performance owners, not hidden compatibility behavior. Full transform-matrix product conformance remains under P3 B1.
