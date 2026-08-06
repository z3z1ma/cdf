Status: active
Created: 2026-07-08
Updated: 2026-08-06

# CLI live progress

## Purpose and scope

This specification governs how the CDF CLI consumes run events and bounded process-local activity
observations to show live progress for resource, portable-plan, exact-package, interrupted-run, and
executed-backfill modes in interactive terminals and headless logs.

It derives from `VISION.md` Chapters 18, 19, 20, and 23; `.10x/specs/project-cli-observability-security.md`; `.10x/specs/run-orchestration-ledger.md`; `.10x/decisions/cli-design-language-and-renderer.md`; `.10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md`; and `.10x/tickets/done/2026-07-08-p1-product-ws5-live-progress.md`.

## Behavior

Live progress MUST be a subscriber to durable run events, not an authority for state advancement. Dropped progress events MUST NOT fail, stall, retry, or change package artifacts, receipts, checkpoints, run-ledger rows, or golden hashes.

Interactive mode MUST render phase-structured progress for:

- plan,
- extract,
- validate,
- package,
- commit,
- verify,
- gate.

The initial phase mapping is:

| Run event kind | Phase |
|---|---|
| `run_started` | plan |
| `plan_recorded` | plan |
| `package_started` | extract |
| `package_finalized` | package |
| `validation_depth_transition_recorded` | validate |
| `destination_commit_started` | commit |
| `destination_receipt_recorded` | verify |
| `checkpoint_proposed` | gate |
| `checkpoint_committed` | gate |
| `package_status_updated` | gate |
| `run_succeeded` | gate |
| `run_failed` | current failed phase |
| `run_resumed` | gate |
| `replay_recorded` | commit |

When events contain quantitative payloads, progress MUST display rows, bytes, batches, segments, quarantine counts, retries, and rate-limit notices. Missing payloads MUST be absent or marked unknown; the CLI MUST NOT fabricate totals.

Normal progress MUST normalize producer fields such as `row_count`, `byte_count`, `batch_count`,
`segment_count`, and `quarantine_record_count` into typed phase-local presentation metrics. It MUST
NOT discover metrics by matching already-formatted field-name strings. Incremental segment or
acknowledgement observations accumulate explicitly; cumulative final observations replace prior
values. Extraction, validation, packaging, destination, verification, and checkpoint metrics are
independent and MUST NOT be summed across phases.

Every active interactive phase line MUST include monotonic elapsed time and refresh without waiting
for another runtime event. Refresh MUST occur at least once per second and no more than ten times
per second. Rows, bytes, segments or batches, quarantine rows, and a phase-local rate appear only
when known. Completed phases remain visible with final measurements. Failure preserves the active
phase's elapsed time and last known measurements before the diagnostic. Short phases MAY have only
a final line.

Retries, throttling, waiting, and backoff MUST be shown immediately in normal mode when the runtime
observes them. Existing semantic segment/retry events SHOULD be published at their occurrence time
rather than reconstructed after execution. Clock ticks used only for display MUST remain bounded,
process-local, and nonauthoritative; they MUST NOT be appended to the durable run ledger.

Interactive mode SHOULD show one active line per resource or run slice plus a summary footer. Known totals may use bars. Unknown totals use an indeterminate but nonblocking indicator. Headless mode MUST emit line-oriented milestone logs with timestamps, bounded verbosity, no ANSI, no spinners, and no terminal control sequences.

Headless live mode MUST emit phase start and completion records. A phase still active after thirty
seconds MUST emit a liveness record containing monotonic elapsed time and counters that are known;
subsequent liveness records occur no more frequently than every thirty seconds. Buffered snapshots
remain deterministic and do not synthesize wall-clock heartbeats after command completion.

`-v` and `-q` are CLI display controls once ratified by the grammar lane. Verbose mode shows more event detail. Quiet mode suppresses live progress and prints only the final panel and failures. `--json` success/error envelopes remain stable and MUST NOT interleave human progress. An NDJSON event stream is excluded until separately ratified.

Chaos and recovery paths MUST make the failed phase, durable artifacts, mutation status, and exact next command visible. Resume after package finalization MUST continue to make source non-contact clear.

All progress rendering MUST use the WS3 renderer once it exists. Redaction MUST apply before event values leave the progress subsystem.

## Acceptance criteria

- Every `cdf run` input mode and executed `cdf backfill` consume the run event spine where the
  command path emits or appends run events.
- Active TTY lines advance elapsed time without new events; headless lines observe the bounded
  thirty-second heartbeat cadence.
- Current singular quantitative payloads appear in normal mode with correct cumulative versus
  incremental behavior and phase-local rates.
- Interactive and headless snapshots or terminal recordings cover success and failure paths.
- Backfill renders one line per slice or resource and a summary footer.
- Chaos-path output names the failed phase, preserved artifacts, mutation status, and exact
  `cdf run --resume` or `cdf run --package` guidance where applicable.
- Redaction tests prove secret-like values do not appear in live progress, verbose traces, headless logs, or snapshots.
- Progress loss or sink backpressure does not change run success, package identity, ledger completeness, or checkpoint gating.

## Explicit exclusions

This spec does not create a scheduler, dashboard, OTLP exporter, or JSON/NDJSON event stream. It does not change success/error JSON envelopes or artifact identity. It does not require fabricated progress totals where sources or destinations do not report them.
