Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md, .10x/decisions/terabyte-scale-performance-envelope.md, .10x/specs/performance-lab-and-envelope.md

# P3 terabyte-scale activation evidence

## What was observed

`VISION.md` Chapter 6 already specifies distinct I/O/CPU/blocking resources, byte-bounded channels, one memory ledger, recorded adaptive batches, boundedness, and watermark discipline. `.10x/specs/architecture-layering-runtime.md` preserves those requirements. Current benchmark code confirms a private Criterion harness exists with deterministic specs and CDF/native cases, but its README explicitly says it has no hard CI gates or published performance claims.

The open performance backlog contains eight active triage owners for partition parallelism, package hashing, streaming destination commit, DuckDB bulk ingest, Parquet streaming writes, REST/JSON decode, batch sizing, and interop overhead. Each forbids implementation before measurement. P3 references and absorbs these owners rather than opening duplicate technical questions.

The user ratified the P3 target table, roofline doctrine, 15%-then-10% correctness-overhead budget, default 4 GiB memory budget, 100 GB under 2 GiB stress law, and WS-L-first sequencing. The active decision and focused lab specification make those values durable. The P3 parent and nine workstream plans preserve the required separation: WS-L measures current truth; later workstreams cannot activate until the baseline exists.

## Procedure

- Inspected `VISION.md` Chapter 6.
- Inspected `.10x/specs/architecture-layering-runtime.md`, `.10x/knowledge/cdf-product-objective.md`, and the Chapter 6 coverage rows.
- Inspected `crates/cdf-benchmarks` catalog, runners, fixtures, README, and existing baseline gate history.
- Read the performance backlog and all eight open triage children.
- Created one active target decision, one focused measurement specification, the P3 parent, and workstream plans WS-L/A/B/C/D/E/F/G/H.
- Updated the coverage matrix to make P3 ownership and sequencing visible without claiming implementation.

## What this supports

This supports program activation, target ratification, non-duplicative triage absorption, and the WS-L stop-line. It also supports that the architecture is not being derived from a single-file benchmark: the catalog and acceptance envelope include multi-file Parquet, full-year TLC, TPC-H, compressed row formats, remote overlap, a 100 GB stressor, and a 1 TB synthetic run.

## Limits

No P3 performance baseline or optimization was produced by activation. Existing P0 benchmark results are useful foundation but do not satisfy the P3 host-labeled roofline and full-envelope contract.
