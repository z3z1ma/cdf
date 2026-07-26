Status: open
Created: 2026-07-11
Updated: 2026-07-25
Parent: .10x/tickets/2026-07-08-p1-product-experience-program.md
Depends-On: .10x/specs/cli-interaction-excellence.md, .10x/decisions/cli-progressive-disclosure-terminal-contract.md

# P1 WS9: CLI experience excellence

## Scope

Turn the P1 renderer foundation into a daily-driver CLI with progressive disclosure, correct terminal/channel adaptation, compact execution progress, excellent help/errors, and measured negligible overhead. This parent is a plan; children are executable units.

## Children

- `.10x/tickets/cancelled/2026-07-11-p1-cx1-terminal-policy-help.md`
- `.10x/tickets/done/2026-07-11-p1-cx2-compact-renderer-errors.md`
- `.10x/tickets/done/2026-07-11-p1-cx3-live-progress-activity.md`
- `.10x/tickets/2026-07-11-p1-cx4-cli-conformance-performance.md`

## Acceptance criteria

- The active CLI interaction spec's scenarios pass across TTY/headless/width/color/Unicode modes.
- Normal run/replay output is compact, nonrepetitive, and outcome-first while verbose/inspect retain evidence depth.
- Help and errors are discoverable and copy-actionable.
- stdout/stderr and JSON isolation obey the contract.
- Rendering/subscriber overhead stays within the P3 budget and cannot backpressure execution.
- The canonical terminal demo is rerecorded through the new experience.

## Blockers

None. CX4 depends on the earlier implementation children and P3 lab telemetry.

## Journal

- 2026-07-25: CX1 moved terminal with its implemented terminal/help foundation retained and only
  the unratified pager remainder cancelled. CX2 is unblocked; WS9 does not require speculative
  paging machinery to deliver the ratified daily-driver experience.
- 2026-07-25: The user required WS-C to execute as a holistic CLI redesign rather than a sequence
  of disconnected cosmetic tickets. CX2-CX4 remain evidence owners, but implementation is one
  coordinated tranche: shared information architecture and primitives, execution summaries and
  errors, nonblocking live progress, then a single terminal/channel/accessibility/performance
  conformance matrix. No child may preserve a conflicting intermediate visual grammar.
- 2026-07-25: CX2 and CX3 are done from the coordinated tranche. Static output, error grammar,
  progressive disclosure, and live progress share one renderer vocabulary; the complete CLI/core
  suite, strict Clippy, local benchmark, and public HTTPS-to-DuckDB smoke are green. CX4 owns only
  the hosted overhead cell, canonical recordings, and aggregate adversarial review.

## Evidence expectations

Aggregate closure maps every spec scenario to snapshots/terminal recordings, parser and channel tests, redaction evidence, benchmark output, generated-artifact freshness, and adversarial review.

## References

- `.10x/research/2026-07-11-rust-cli-experience-study.md`
- `.10x/decisions/cli-design-language-and-renderer.md`
- `.10x/specs/cli-live-progress.md`
- `.10x/specs/cli-error-experience-catalog.md`
