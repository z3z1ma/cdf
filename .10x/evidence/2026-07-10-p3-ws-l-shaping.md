Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/done/2026-07-10-p3-ws-l1-catalog-report-schema.md, .10x/tickets/2026-07-10-p3-ws-l2-phase-telemetry.md, .10x/tickets/2026-07-10-p3-ws-l3-macro-roofline-runners.md, .10x/tickets/2026-07-10-p3-ws-l4-ci-envelope-generation.md, .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md

# P3 WS-L shaping evidence

## What was observed

The existing benchmark crate already separates deterministic fixture specs from generated data and has native/CDF case labels, Criterion suites, trend JSONL, and Postgres opt-in execution. It lacks the P3 authority layers: versioned host/result schema, full dataset catalog, phase duration/bytes, roofline runners, warm/cold distinction, peak RSS/spill, variance-aware regression comparison, generated envelope, and a complete pre-optimization run.

## Shaped execution graph

- L1 owns catalog, host fingerprint, and machine report schema.
- L2 independently owns additive shared runtime phase telemetry; it may not create benchmark-only runtime channels.
- L3 consumes L1/L2 to run macro, roofline, reference, and profiling cases.
- L4 consumes L3 reports for fair comparison, CI gates, and generated envelope output.
- L5 executes the untouched current data plane and releases the P3 stop-line only after preserving the full before picture.

Each ticket has exclusions preventing optimization from leaking into the lab tranche. The graph keeps host/report authority separate from runtime telemetry and keeps baseline execution after the measurement tools, so later results remain comparable.

## Limits

This is shaping evidence only. No benchmark implementation or baseline claim exists yet.
