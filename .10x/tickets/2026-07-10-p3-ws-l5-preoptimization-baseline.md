Status: active
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p3-ws-l-performance-lab.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l4-ci-envelope-generation.md, .10x/tickets/done/2026-07-11-p3-l1-small-startup-catalog-followup.md

# P3 WS-L5: pre-optimization baseline and stop-line release

## Scope

Run the complete P3 lab against the unoptimized data plane on named host classes, preserve raw reports/profiles, publish the first envelope with failing/unavailable cells intact, reconcile all performance triage hypotheses against measured costs, and release the stop-line for later P3 workstreams.

## Acceptance criteria

- Every ratified envelope row has an observed result, explicit failure, or explicit environment/tool unavailability.
- Raw samples, host fingerprints, phase breakdown, peak RSS, and at least representative profiles are retained as evidence.
- The correctness/evidence overhead is measured, not estimated.
- Each open performance triage is linked to a measured cost center and its corresponding P3 owner or a no-action rationale.
- No source change that optimizes a measured path is included in the baseline commit.
- WS-L closure review confirms the before picture predates WS-A through WS-H implementation.

## Evidence expectations

Complete machine reports, generated envelope, tool/version inventory, raw profile locations, triage reconciliation, and adversarial review focused on benchmark bias and missing embarrassing workloads.

## Blockers

Depends on L1-L4. Later P3 implementation remains blocked until this ticket and WS-L are done.

## Progress and notes

- 2026-07-11: Activated after L1-L4 and the small/startup catalog repair. Baseline execution will use prepared-input isolated workers and real L2 phase telemetry; legacy Criterion labels are compatibility data, not baseline authority. Enterprise rows that cannot run on the current data plane/host will remain explicit failed or unavailable cells.
