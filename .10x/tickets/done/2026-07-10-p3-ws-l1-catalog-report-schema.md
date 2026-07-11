Status: done
Created: 2026-07-10
Updated: 2026-07-11
Parent: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md
Depends-On: .10x/specs/performance-lab-and-envelope.md

# P3 WS-L1: dataset catalog, host fingerprint, and report schema

## Scope

Extend `cdf-benchmarks` with regeneration-grade dataset specifications for TLC, TPC-H SF10/SF100, wide/nested/dirty JSON, and the generated constant-memory stressor; define the versioned machine-readable result/report schema and host fingerprint used by every later P3 measurement.

## Acceptance criteria

- Catalog specs record source/generator version, immutable identity or seed, schema, rows, expected bytes where known, and provenance/license without committing large data.
- Host fingerprint records CPU, logical/physical cores where available, memory, storage/device label, OS, Rust/CDF/dependency versions, and benchmark mode.
- Host capability records are typed supported/unavailable/failed values, sanitized of host/user/path/device identity, and distinguish effective container quotas from advertised host capacity.
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

## Progress and notes

- 2026-07-11: Implemented versioned P3 dataset/workload/catalog and host/capability/observation/report types plus committed TLC, TPC-H SF10/SF100, wide/nested/dirty/schema-varying JSON, constant-memory, and observed/unavailable report fixtures. Legacy trend records import only as inconclusive.
- 2026-07-11: Closure evidence is `.10x/evidence/2026-07-11-p3-l1-catalog-report-schema.md`; adversarial review is `.10x/reviews/2026-07-11-p3-l1-catalog-report-schema-review.md` (pass). Seven focused tests, all-target clippy with warnings denied, formatting, and diff checks passed. No large dataset, benchmark result, runtime telemetry, or performance claim was produced.

## References

- `.10x/decisions/performance-lab-host-capability-boundary.md`
- `.10x/research/2026-07-11-performance-host-capability-inventory.md`
