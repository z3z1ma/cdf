Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/specs/performance-lab-and-envelope.md, .10x/tickets/done/2026-07-07-p0-workstream-f-benchmark-gate.md

# P3 WS-L: performance lab and baseline

## Scope

Turn `cdf-benchmarks` into the authoritative macro/micro performance lab, add phase byte/duration telemetry, deterministic dataset generation, roofline/reference runners, host-labeled trend reports, profiling procedures, variance-aware CI gates, and the generated envelope document. Record the complete pre-optimization baseline before any later P3 workstream starts.

This workstream is a plan. Split dataset/report schema, macro/reference runners, telemetry, profiling/CI, and first-baseline execution into bounded executable children before implementation.

## Acceptance criteria

- Every dataset and metric required by `.10x/specs/performance-lab-and-envelope.md` is represented or explicitly recorded unavailable.
- Warm/cold modes, same-host rooflines, bias labels, median/variance, peak RSS, and phase bytes/durations are machine-readable.
- The 100 GB stressor is generated rather than committed.
- CI fails comparable regressions over 10% without silently resetting baselines.
- A full host-labeled baseline and envelope document exist before WS-A through WS-H activate.

## Evidence expectations

Committed catalog/report fixtures, deterministic-generation tests, macro/micro output, host inventory, raw profile artifacts, baseline report, and adversarial review of comparison fairness.

## Explicit exclusions

No runtime, decoder, destination, hashing, parallelism, or memory optimization. WS-L measures current behavior, including failures and impossible cells.

## Progress and notes

- 2026-07-10: Opened from P3. Existing P0 benchmark coverage is retained as foundation, not treated as the required P3 baseline.

## Blockers

None for shaping. Implementation begins only after P1 closes and bounded WS-L children exist.
