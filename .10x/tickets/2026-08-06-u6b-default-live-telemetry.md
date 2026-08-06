Status: active
Created: 2026-08-06
Updated: 2026-08-06
Parent: `.10x/tickets/2026-08-04-resource-first-cli-experience-program.md`
Depends-On: `.10x/tickets/done/2026-08-05-u6-operational-recovery-command-coherence.md`

# U6b useful default live telemetry

## Scope

Make normal live progress continuously and concisely explain measurable execution work for every
`run` input mode and executed backfill. Replace string-filtered event fields with typed phase-local
metric state; retain a monotonic clock for active and completed phases; add bounded TTY ticks and
headless liveness records; surface retry/wait/backoff state at occurrence time; and preserve final
telemetry on success or failure.

Publish existing bounded semantic package-segment, destination-acknowledgement, and source-retry
observations when the work actually occurs rather than replaying them after the underlying phase.
Do not create periodic durable events or make presentation authoritative. Normal telemetry remains
on stderr; verbose mode adds event/scope/retry/physical-I/O internals.

## Non-goals

- a TUI, progress bars requiring fabricated totals, JSON/NDJSON event streaming, OTLP, or a new
  durable telemetry store;
- changing package, receipt, checkpoint, retry, or event identity/ordering semantics;
- high-frequency per-row/per-batch durable run-ledger writes merely to animate the terminal;
- U7's whole-tranche broad suite, current-only cutover sweep, or final red-team review.

## Acceptance Criteria

1. Normal TTY output owns typed `ProgressMetrics` (or an equivalently explicit type) containing
   optional rows, bytes, segments, batches, and quarantine rows. It consumes producer keys such as
   `row_count` and `byte_count` directly; no presentation path searches for incompatible plural
   metric-name strings.
2. Progress state is keyed by run/resource and semantic phase. Package-segment and destination-ack
   counters accumulate only within their phase; package-finalized, receipt, and checkpoint totals
   replace the applicable phase-local cumulative values. Metrics from extraction, packaging,
   destination, and checkpoint phases are never summed together or double-counted.
3. Each active TTY line shows monotonic elapsed time and refreshes at the existing bounded cadence
   (at least 1 Hz, at most 10 Hz) without another event. Known metrics render as concise humanized
   values; unavailable metrics are omitted. Rate uses phase-local rows when present, otherwise
   phase-local bytes, divided by monotonic phase elapsed time.
4. Phase transitions leave stable completed lines with final measurements. A terminal failure
   marks the current phase failed without replacing it with the gate phase, retains its last known
   metrics and elapsed time, and renders before the typed diagnostic. Short phases still emit final
   summaries even when no intermediate tick occurred.
5. Headless live stderr emits plain ANSI-free phase-start and phase-completion records. An active
   phase emits a liveness record at thirty seconds and no more than once per thirty seconds
   thereafter, including elapsed time and known counters. Buffered post-command snapshots remain
   deterministic and do not synthesize historical heartbeat timestamps.
6. Retry, throttle, wait, and backoff observations reach normal live progress when the runtime
   makes the decision. Existing durable source-retry evidence remains bounded and authoritative;
   any display-only clock/status observation is process-local, drop-capable, and cannot block or
   alter execution.
7. `-v` additionally exposes event kind, sequence, scope, retry details, and physical-I/O phase
   metrics. `-q`, `--json`, and `--progress never` retain suppression; live human progress remains
   stderr-only; secret-bearing fields are redacted before both TTY and headless rendering.
8. Focused tests cover clock-only redraw, singular producer payload normalization, cumulative and
   incremental counters, phase isolation, rate calculation, missing metrics, success/failure
   finalization, headless heartbeat cadence, retry/backoff immediacy, quiet/JSON/disabled policy,
   redaction, bounded memory, and nonblocking publication.
9. Formatter, affected-package check, strict Clippy, generated-reference freshness when touched,
   and diff check pass. A bundled release binary runs the FineWeb sandbox long enough to show
   multiple changing elapsed/counter updates and stable completed lines.

## References

- `.10x/specs/cli-live-progress.md`
- `.10x/specs/cli-interaction-excellence.md`
- `.10x/specs/runtime-event-spine.md`
- `.10x/decisions/cli-progressive-disclosure-terminal-contract.md`
- `.10x/knowledge/cli-report-authority.md`
- `.agents/skills/audit-cli-report-authority/SKILL.md`
- `.10x/tickets/done/2026-08-05-u6-operational-recovery-command-coherence.md`

## Assumptions

- User-ratified: normal mode shows useful phase telemetry; verbose mode owns diagnostic internals;
  missing metrics are omitted; TTY refresh is 1-10 Hz; headless heartbeat cadence is thirty
  seconds; periodic rendering never becomes durable run authority.
- Record-backed: current progress producers publish singular keys (`row_count`, `byte_count`,
  `batch_count`, `segment_count`) while the normal renderer filters plural aliases and therefore
  hides real metrics.
- Record-backed: package-segment events carry incremental segment rows/bytes plus index/count;
  package-finalized carries cumulative package totals; destination acknowledgements carry
  incremental rows/bytes; receipt/checkpoint events carry cumulative semantic totals.
- Record-backed: the live subscriber is bounded/drop-capable and uses stderr; JSON, quiet, and
  disabled-progress policy already suppress it.

## Journal

- 2026-08-06: Shaped from the user-provided terminal and headless contract. Source inspection found
  the plural/singular renderer mismatch, milestone-only state with no phase clocks, TTY timed
  redraw only when an event had set `pending_redraw`, no headless heartbeat, `PhaseMeasured` always
  mapped to Package regardless of its typed runtime phase, and source retry/package segment events
  appended only after engine execution completed.
- 2026-08-06: Activated after the user approved execution. Protected unrelated untracked evidence,
  personal Codex configuration, and release artifacts remain outside the ticket diff.
- 2026-08-06: Implemented typed phase-local progress state, monotonic TTY redraws, bounded headless
  heartbeats, stable phase completion/failure lines, runtime-phase-aware physical measurements,
  and a bounded process-local retry-wait observation path. Package-segment events now publish from
  the engine's durable-segment frontier instead of being reconstructed after execution.
- 2026-08-06: Focused progress, engine retry/segment-frontier, project event-order/fanout, affected
  check, formatter, and strict affected-package Clippy checks pass. A broader `cdf-cli` run-module
  probe exposed out-of-scope current-model test drift and persistent fixture-state failures owned
  by U7; the two progress-specific human run cells passed. Release sandbox evidence remains.

## Blockers

None.

## Evidence

Pending execution.

## Review

Deferred to U7's single final tranche review per user instruction.

## Retrospective

Pending execution.
