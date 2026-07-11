Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p3-ws-l-performance-lab.md
Depends-On: .10x/tickets/2026-07-10-p3-ws-l1-catalog-report-schema.md, .10x/tickets/2026-07-10-p3-ws-l2-phase-telemetry.md

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

## Evidence expectations

Fixture-run reports, reference correctness cross-checks, unavailable-tool tests, profiling dry runs, and fairness review.

## Explicit exclusions

No CI regression policy or optimization of measured code.

## Blockers

Depends on L1 report authority and L2 phase telemetry.
