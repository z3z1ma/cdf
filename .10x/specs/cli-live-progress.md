Status: active
Created: 2026-07-08
Updated: 2026-07-08

# CLI live progress

## Purpose and scope

This specification governs how the CDF CLI consumes run events to show live progress for `run`, `replay package`, `resume`, and `backfill` in interactive terminals and headless logs.

It derives from `VISION.md` Chapters 18, 19, 20, and 23; `.10x/specs/project-cli-observability-security.md`; `.10x/specs/run-orchestration-ledger.md`; `.10x/decisions/cli-design-language-and-renderer.md`; `.10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md`; and `.10x/tickets/2026-07-08-p1-product-ws5-live-progress.md`.

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

Interactive mode SHOULD show one active line per resource or run slice plus a summary footer. Known totals may use bars. Unknown totals use an indeterminate but nonblocking indicator. Headless mode MUST emit line-oriented milestone logs with timestamps, bounded verbosity, no ANSI, no spinners, and no terminal control sequences.

`-v` and `-q` are CLI display controls once ratified by the grammar lane. Verbose mode shows more event detail. Quiet mode suppresses live progress and prints only the final panel and failures. `--json` success/error envelopes remain stable and MUST NOT interleave human progress. An NDJSON event stream is excluded until separately ratified.

Chaos and recovery paths MUST make the failed phase, durable artifacts, mutation status, and exact next command visible. Resume after package finalization MUST continue to make source non-contact clear.

All progress rendering MUST use the WS3 renderer once it exists. Redaction MUST apply before event values leave the progress subsystem.

## Acceptance criteria

- `cdf run`, `cdf replay package`, `cdf resume`, and executed `cdf backfill` consume the run event spine where the command path emits or appends run events.
- Interactive and headless snapshots or terminal recordings cover success and failure paths.
- Backfill renders one line per slice or resource and a summary footer.
- Chaos-path output names the failed phase, preserved artifacts, mutation status, and exact `cdf resume` or replay guidance where applicable.
- Redaction tests prove secret-like values do not appear in live progress, verbose traces, headless logs, or snapshots.
- Progress loss or sink backpressure does not change run success, package identity, ledger completeness, or checkpoint gating.

## Explicit exclusions

This spec does not create a scheduler, dashboard, OTLP exporter, or JSON/NDJSON event stream. It does not change success/error JSON envelopes or artifact identity. It does not require fabricated progress totals where sources or destinations do not report them.
