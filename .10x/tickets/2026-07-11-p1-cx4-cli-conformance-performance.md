Status: active
Created: 2026-07-11
Updated: 2026-07-25
Parent: .10x/tickets/2026-07-11-p1-ws9-cli-experience-excellence.md
Depends-On: .10x/tickets/done/2026-07-11-p1-cx3-live-progress-activity.md, .10x/tickets/done/2026-07-10-p3-ws-l2-phase-telemetry.md

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
- `.10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md`

## Journal

- 2026-07-25: CX2/CX3 landed as one coordinated renderer/progress rewrite. Permanent tests now
  cover the 40/80/160-column TTY/headless ASCII/Unicode/no-color matrix, JSON isolation,
  progressive disclosure, generated artifacts, redaction, slow terminals, bounded state, and
  terminal-event preservation.
- 2026-07-25: Moved the benchmark out of the heavyweight general benchmark crate into the
  dependency-light `cdf-cli-benchmarks` leaf so ordinary CLI checks do not compile unrelated
  engines/destinations. Criterion measured 19.260 million governed events/second for one million
  buffered events and 19.400 million events/second for the 10,000-partition case, with no detected
  regression.
- 2026-07-25: `.10x/evidence/2026-07-25-cli-experience-rewrite.md` records the current conformance,
  benchmark, and real-project smoke. Remaining closure work is the enabled-versus-disabled
  reference-workload cell, canonical hosted recordings, and the final severity-focused review.
- 2026-07-25: Hosted measurement is active on the reusable tuned `c7i.4xlarge` authority. The
  first no-progress FineWeb sample exposed a generic staged-ingress defect before destination
  mutation: a segment already admitted by the managed-memory ledger was rejected because its
  retained Arrow bytes narrowly exceeded the destination's concurrent byte window. The bounded
  repair is owned by `.10x/tickets/2026-07-25-staged-segment-oversized-singleton.md`; CX4 resumes
  from its clean revision rather than hiding the defect with a benchmark-only override.
