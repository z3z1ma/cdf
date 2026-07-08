Status: active
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-experience-program.md
Depends-On: .10x/specs/cli-live-progress.md, .10x/tickets/2026-07-08-p1-product-ws1-runtime-event-spine.md, .10x/tickets/2026-07-08-p1-product-ws3-rendering-system-design-language.md

# P1 product WS5: Live progress

## Scope

Subscribe the renderer to the runtime event spine so run, replay, resume, and backfill show phase-structured live progress in interactive terminals and readable milestones in non-TTY logs.

## Child tickets

- `.10x/tickets/done/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws5b-run-replay-resume-progress.md`
- `.10x/tickets/2026-07-08-p1-product-ws5c-backfill-multi-resource-progress.md`
- `.10x/tickets/2026-07-08-p1-product-ws5d-progress-evidence-gate.md`

## Required outcomes

- Interactive mode shows plan, extract, validate, package, commit, verify, and gate phases.
- Multi-resource runs render one line per resource with a summary footer.
- Progress shows rows, bytes, rates, quarantine counts, retries, and rate-limit notices as events arrive.
- Known totals use bars; unknown totals use indeterminate indicators.
- Non-TTY mode emits timestamped, rate-limited milestone lines suitable for CI logs.
- `-v` raises event verbosity and `-q` reduces output to the final panel.
- Optional `--json --events` NDJSON streaming may be ratified as an additive feature if a child decision proves the contract.

## Acceptance criteria

- Run/replay/resume/backfill consume the WS1 event spine.
- Chaos-path output names the failed phase, preserved artifacts, and exact `cdf resume` invocation.
- TTY session recordings and non-TTY snapshots cover success and failure paths.
- Redaction checks cover progress events and verbose traces.

## Evidence expectations

Attach recorded terminal sessions or equivalent artifacts, non-TTY milestone snapshots, chaos-path output evidence, and redaction adversarial checks.

## Explicit exclusions

No new lifecycle events beyond the WS1 event model without updating WS1. No artifact identity changes. No JSON event stream unless separately ratified.

## Progress and notes

- 2026-07-08: Opened from P1 product directive. This workstream waits for WS1 and WS3 implementation before execution.
- 2026-07-08: Shaped `.10x/specs/cli-live-progress.md` and split execution into WS5A-WS5D child tickets. Shaping evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws5-live-progress-shaping.md`; shaping review recorded in `.10x/reviews/2026-07-08-p1-product-ws5-live-progress-shaping-review.md`.
- 2026-07-08: WS1A/WS1B/WS1C, WS2C, and WS3B/WS3C are done. WS5A is unblocked for progress sink/renderer foundation; replay/resume/backfill progress remains sequenced with WS1D event convergence and WS3D static rendering.
- 2026-07-08: WS3D static recovery/state/backfill rendering is done, so later WS5 progress children no longer depend on that rendering slice.
- 2026-07-08: WS5A progress sink/renderer foundation is done in `.10x/tickets/done/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md`; evidence is `.10x/evidence/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md` and review is `.10x/reviews/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation-review.md`.
- 2026-07-08: WS5B run/replay/resume progress is done in `.10x/tickets/done/2026-07-08-p1-product-ws5b-run-replay-resume-progress.md`; evidence is `.10x/evidence/2026-07-08-p1-product-ws5b-run-replay-resume-progress.md` and review is `.10x/reviews/2026-07-08-p1-product-ws5b-run-replay-resume-progress-review.md`.

## Blockers

None for WS5A. Later progress children still depend on WS1D event convergence where they consume replay/resume/backfill event surfaces.
