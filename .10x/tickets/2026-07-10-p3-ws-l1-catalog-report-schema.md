Status: open
Created: 2026-07-10
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-l-performance-lab.md
Depends-On: .10x/specs/performance-lab-and-envelope.md

# P3 WS-L1: dataset catalog, host fingerprint, and report schema

## Scope

Extend `cdf-benchmarks` with regeneration-grade dataset specifications for TLC, TPC-H SF10/SF100, wide/nested/dirty JSON, and the generated constant-memory stressor; define the versioned machine-readable result/report schema and host fingerprint used by every later P3 measurement.

## Acceptance criteria

- Catalog specs record source/generator version, immutable identity or seed, schema, rows, expected bytes where known, and provenance/license without committing large data.
- Host fingerprint records CPU, logical/physical cores where available, memory, storage/device label, OS, Rust/CDF/dependency versions, and benchmark mode.
- Result schema represents wall time, CPU time where available, rows, logical/physical bytes, throughput, peak RSS, spill, phase metrics, sample distribution, median/dispersion, roofline/reference identity, and bias labels.
- Schema versioning and deterministic serialization tests fail closed on malformed or incomparable records.
- Existing smoke/full/postgres benchmark cases remain loadable or migrate through an explicit compatibility test.
- Dataset recipes, workload timed-region policies, and observations are separate typed records; large generators are bounded and streaming.

## Evidence expectations

Catalog and report fixtures, generation/validation tests, deterministic serialization hashes, and review for sensitive host-data leakage.

## Explicit exclusions

No new runtime telemetry, benchmark execution, reference runners, CI thresholds, or performance claims.

## Blockers

None.
