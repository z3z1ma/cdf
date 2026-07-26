Status: done
Created: 2026-07-11
Updated: 2026-07-25
Parent: .10x/tickets/done/2026-07-11-p1-ws9-cli-experience-excellence.md
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
  repair is owned by `.10x/tickets/done/2026-07-25-staged-segment-oversized-singleton.md`; CX4 resumes
  from its clean revision rather than hiding the defect with a benchmark-only override.
- 2026-07-25: The repaired clean release completed the exact 2.147 GB FineWeb run with defaults.
  A balanced three-sample-per-mode matrix measured 22.69 seconds median with progress disabled
  and 22.59 seconds with progress enabled (`-0.4407%`, treated as variance), passing the maximum
  `+1%` overhead gate. Canonical 100×40 recordings now cover normal, verbose, redirected, JSON,
  fresh-store second-database replay, and failure behavior.

## Evidence

- `.10x/evidence/2026-07-25-cli-experience-rewrite.md` — permanent terminal/channel/width,
  progressive-disclosure, redaction, slow-terminal, million-event, and real-project matrix.
- `.10x/evidence/2026-07-25-cli-hosted-conformance.md` — hosted overhead samples, exact default
  FineWeb repair proof, canonical recording procedure, hashes, and limits.
- `.10x/evidence/.storage/2026-07-25-cx4-hosted-cli-overhead.json` — machine overhead report.
- `.10x/evidence/.storage/2026-07-25-cx4-hosted-cli-artifacts.tar.gz` — raw timings, channels, JSON,
  and terminal transcripts.

## Review

Pass. Severity-focused review found no critical, high, or significant experience, accessibility,
secret-leakage, channel-isolation, or hot-path issue. One recording-only issue was caught before
closure: the first pseudo-terminal inherited a tiny width and produced unusable wrapping. The
recordings were repeated at an explicit 100×40 geometry. JSON parses with empty stderr; redirected
progress is stderr-only; normal output is outcome-first; verbose adds evidence without changing
the primary decision; parser failure carries a stable code and copyable action; the nonblocking
subscriber and dedicated terminal-event slot remain permanently tested.

Residual risk: a terminal emulator can render ANSI motion differently from the recorded
`script(1)` stream. That is bounded by the automated TTY/headless and 40/80/160-column matrix and
does not affect runtime authority.

## Retrospective

Renderer microbenchmarks are necessary but insufficient: the hosted paired run proved the whole
subscriber/rendering path inside a real CDF command. Canonical recordings also need explicit
terminal geometry; a pseudo-terminal is not automatically a representative terminal. Finally,
replay demonstrations must use the governed second-database/fresh-checkpoint topology so they
measure destination replay rather than intentionally rejected checkpoint-id reuse.
