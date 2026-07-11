Status: done
Created: 2026-07-10
Updated: 2026-07-11
Parent: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l1-catalog-report-schema.md, .10x/tickets/done/2026-07-10-p3-ws-l2-phase-telemetry.md

# P3 WS-L3: macro, roofline, and reference runners

## Scope

Build the repeatable macro runner with warm/cold modes and median-of-N sampling; add raw sequential device and memcpy rooflines, raw arrow-rs format references, DuckDB native references, and an externally isolated Polars reference where available. Add profile-command wrappers for flamegraph and `perf stat` without making platform-specific tools mandatory for ordinary tests.

## Acceptance criteria

- Runners emit only the L1 report schema and use deterministic generated/acquired fixtures.
- Warm and cold modes are separate; unsupported cache eviction is labeled rather than simulated.
- Each reference carries explicit omitted/added semantic-work bias labels.
- Timeouts, unavailable tools, changed host fingerprints, and failed cells are reported, never omitted.
- Median and dispersion are derived from raw samples retained in the report.
- Profiling wrappers record exact command/tool versions and place artifacts under ignored output paths.
- Macro cases run with process isolation where required for RSS/CPU/timeout observation; Criterion remains limited to microkernels.
- Every workload records setup inclusion/exclusion and non-ambiguous logical/physical byte counters.
- macOS, Linux/procfs, Linux/cgroup overlay, and portable-fallback providers remain behind one host capability interface; privileged methods are opt-in and missing tools produce unavailable cells.

## Evidence expectations

Fixture-run reports, reference correctness cross-checks, unavailable-tool tests, profiling dry runs, and fairness review.

## Explicit exclusions

No CI regression policy or optimization of measured code.

## Blockers

Depends on L1 report authority and L2 phase telemetry.

## References

- `.10x/decisions/performance-lab-host-capability-boundary.md`
- `.10x/research/2026-07-11-performance-host-capability-inventory.md`

## Progress and notes

- 2026-07-11: Activated after L1/L2 closure. Implementation will keep platform probes, child observation, cache authority, external-tool discovery, and profiling behind the host capability boundary; workload runners consume typed capabilities only.
- 2026-07-11: Implemented schema-driven isolated macro cells, retained raw samples with exact median/MAD derivation, worker-timed regions plus parent CPU/RSS/timeout observation, per-sample cold eviction, untimed warm-up, host-class drift rejection, bounded worker metadata, and credential-safe command specs.
- 2026-07-11: Added portable sequential read/write and memcpy rooflines; raw Arrow Parquet/CSV/NDJSON and DuckDB Parquet references; external Python-Polars discovery/scan commands with typed unavailable behavior; and flamegraph/perf dry-run plans under ignored output paths.
- 2026-07-11: Closure evidence is `.10x/evidence/2026-07-11-p3-l3-macro-roofline-runners.md`; fairness/architecture review is `.10x/reviews/2026-07-11-p3-l3-macro-roofline-runners-review.md` (pass).
