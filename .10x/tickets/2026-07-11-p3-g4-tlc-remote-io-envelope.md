Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-g-remote-io-overlap.md
Depends-On: .10x/tickets/2026-07-11-p3-g3-codec-download-decode-overlap.md, .10x/tickets/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/tickets/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md

# P3 G4: remote/local I/O envelope and TLC closeout

## Scope

Run recorded and live public TLC plus S3/GCS/Azure/local roofline scenarios, publish network/device overlap evidence, tune safe defaults, and make I/O-bound acceptance permanent without making ordinary CI network-dependent.

## Acceptance criteria

- Full-year TLC HTTPS-to-DuckDB meets the 1.5x composite target where environment permits and profile is I/O/destination-bound.
- S3/GCS/Azure/live cells are labeled and recorded; deterministic fixtures gate CI.
- Local sequential/range strategy reaches its measured roofline without unratified unsafe paths.
- Remote controller overhead, retries, waste, cache/spool, memory, and identity are within budgets.

## Evidence expectations

Host/network/provider reports, raw profiles/timelines, live/recorded comparison, jobs/memory/identity conformance, and adversarial weak-validator/throttle review.

## Explicit exclusions

No guarantee about third-party public endpoint uptime/bandwidth.

## Blockers

Depends on G1-G3, DuckDB bulk, and deterministic scaling closeout.

## References

- `.10x/specs/remote-local-io-overlap.md`
- `.10x/decisions/terabyte-scale-performance-envelope.md`
