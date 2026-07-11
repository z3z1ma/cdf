Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p1-ws9-cli-experience-excellence.md
Depends-On: .10x/tickets/2026-07-11-p1-cx3-live-progress-activity.md, .10x/tickets/2026-07-10-p3-ws-l2-phase-telemetry.md

# P1 CX4: CLI conformance, performance, and demo

## Scope

Build the permanent terminal/channel/width/accessibility/redaction matrix, benchmark million-event and high-partition rendering, enforce the overhead budget, and rerecord canonical plan/run/replay/error sessions.

## Acceptance criteria

- Every conformance case in the active CLI spec is automated.
- Rendering enabled versus disabled stays within 1% end-to-end overhead on the P3 reference workload.
- A slow or blocked terminal cannot backpressure runtime authority.
- Canonical recordings demonstrate normal, verbose, redirected, JSON, and failure experiences.
- Adversarial review finds no high/severe experience, accessibility, leakage, or hot-path issue.

## Blockers

Depends on CX3 and P3 phase telemetry.

## Evidence expectations

Permanent matrix output, million-event and end-to-end benchmark artifacts, canonical terminal recordings, slow-consumer proof, and a severity-focused adversarial review.

## References

- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/tickets/2026-07-10-p3-ws-l-performance-lab.md`
