Status: done
Created: 2026-07-11
Updated: 2026-07-25
Parent: .10x/tickets/done/2026-07-08-p1-product-experience-program.md
Depends-On: .10x/specs/cli-interaction-excellence.md, .10x/decisions/cli-progressive-disclosure-terminal-contract.md

# P1 WS9: CLI experience excellence

## Scope

Turn the P1 renderer foundation into a daily-driver CLI with progressive disclosure, correct terminal/channel adaptation, compact execution progress, excellent help/errors, and measured negligible overhead. This parent is a plan; children are executable units.

## Children

- `.10x/tickets/cancelled/2026-07-11-p1-cx1-terminal-policy-help.md`
- `.10x/tickets/done/2026-07-11-p1-cx2-compact-renderer-errors.md`
- `.10x/tickets/done/2026-07-11-p1-cx3-live-progress-activity.md`
- `.10x/tickets/done/2026-07-11-p1-cx4-cli-conformance-performance.md`

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
- 2026-07-25: WS9 is complete. CX4 closed the remaining hosted and recording gates: progress
  enabled measured within noise of disabled (`-0.4407%` median delta against a maximum `+1%`),
  the exact 2.147 GB reference run completed with defaults, and canonical normal, verbose,
  redirected, JSON, replay, and failure transcripts passed severity-focused review.

## Evidence expectations

Aggregate closure maps every spec scenario to snapshots/terminal recordings, parser and channel tests, redaction evidence, benchmark output, generated-artifact freshness, and adversarial review.

## References

- `.10x/research/2026-07-11-rust-cli-experience-study.md`
- `.10x/decisions/cli-design-language-and-renderer.md`
- `.10x/specs/cli-live-progress.md`
- `.10x/specs/cli-error-experience-catalog.md`

## Evidence

- `.10x/evidence/2026-07-25-cli-experience-rewrite.md`
- `.10x/evidence/2026-07-25-cli-hosted-conformance.md`
- CX2, CX3, and CX4 terminal ticket evidence.

## Review

Pass. The coordinated tranche is one renderer grammar rather than three intermediate designs.
Every child is terminal; the active CLI scenarios have automated channel/terminal coverage;
slow-output backpressure cannot enter runtime authority; machine output remains isolated; and the
hosted whole-command overhead gate passes. No high-severity experience or architecture residual
remains.

## Retrospective

CLI quality improved when information architecture, static outcomes, progress, errors, and
conformance were treated as one product surface. Child tickets remained useful as evidence
owners, but a single visual grammar prevented locally polished commands from diverging into
different products.
