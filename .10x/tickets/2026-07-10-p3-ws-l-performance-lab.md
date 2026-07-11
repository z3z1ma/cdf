Status: open
Created: 2026-07-10
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/specs/performance-lab-and-envelope.md, .10x/tickets/done/2026-07-07-p0-workstream-f-benchmark-gate.md

# P3 WS-L: performance lab and baseline

## Scope

Turn `cdf-benchmarks` into the authoritative macro/micro performance lab, add phase byte/duration telemetry, deterministic dataset generation, roofline/reference runners, host-labeled trend reports, profiling procedures, variance-aware CI gates, and the generated envelope document. Record the complete pre-optimization baseline before any later P3 workstream starts.

This workstream is a plan. Split dataset/report schema, macro/reference runners, telemetry, profiling/CI, and first-baseline execution into bounded executable children before implementation.

## Child tickets

- `.10x/tickets/done/2026-07-10-p3-ws-l1-catalog-report-schema.md`
- `.10x/tickets/done/2026-07-10-p3-ws-l2-phase-telemetry.md`
- `.10x/tickets/done/2026-07-10-p3-ws-l3-macro-roofline-runners.md`
- `.10x/tickets/done/2026-07-10-p3-ws-l4-ci-envelope-generation.md`
- `.10x/tickets/2026-07-10-p3-ws-l5-preoptimization-baseline.md`
- `.10x/tickets/2026-07-11-p3-l1-small-startup-catalog-followup.md`

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
- 2026-07-10: Decomposed into L1 catalog/report authority, L2 shared phase telemetry, L3 macro/reference/profile runners, L4 CI/envelope generation, and L5 baseline execution. L1 and L2 are independent; L3-L5 form the integration sequence.
- 2026-07-11: Current-state audit recorded at `.10x/research/2026-07-11-performance-lab-current-state-audit.md`. Existing tiny Criterion/trend coverage is retained as a compatibility seed but is not P3 baseline evidence. User reprioritization removed the P1-closure blocker; L1/L2 may execute immediately without optimizing the data plane.
- 2026-07-11: L1 closed at `.10x/tickets/done/2026-07-10-p3-ws-l1-catalog-report-schema.md` with versioned deterministic catalog/workload/host-capability/report authority, explicit legacy incompatibility, fixed fixture hashes, focused evidence, and pass review. L2 remains independently executable; L3 now has its required schema dependency.
- 2026-07-11: L2 closed at `.10x/tickets/done/2026-07-10-p3-ws-l2-phase-telemetry.md` with typed bounded phase metrics, genuinely disabled default collection, package-identity invariance, append-only ledger v5 migration, complete runtime success/failure fixtures, and pass architecture review. L3 now owns provider/runners and live collection.
- 2026-07-11: L3 wiring audit found that L1 omitted the spec-required explicit small/startup catalog cells. Follow-up `.10x/tickets/2026-07-11-p3-l1-small-startup-catalog-followup.md` owns the gap and gates L5, not L3.
- 2026-07-11: L3 closed at `.10x/tickets/done/2026-07-10-p3-ws-l3-macro-roofline-runners.md` with isolated median-of-N execution, host/cache/process/tool capability providers, raw I/O/memcpy/Arrow/DuckDB references, external Polars, profiling plans, and pass fairness review. L4 can now consume authoritative reports.
- 2026-07-11: L4 closed at `.10x/tickets/done/2026-07-10-p3-ws-l4-ci-envelope-generation.md` with strict comparable-authority regression policy, immutable evidence-backed baseline history, generated no-claim envelope freshness, lean dedicated performance CI, and pass reset/fairness review. L5 remains gated by the explicit L1 small/startup follow-up.

## Blockers

None. L1 and L2 are bounded and eligible to execute immediately.
