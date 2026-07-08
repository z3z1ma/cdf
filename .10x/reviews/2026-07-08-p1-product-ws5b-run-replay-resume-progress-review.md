Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws5b-run-replay-resume-progress.md
Verdict: pass

# WS5B closure review

## Target

Review of WS5B changes wiring CLI live progress into `cdf run`, `cdf replay package`, and `cdf resume`, plus the supporting test and output-rendering changes.

## Assumptions tested

- Human progress must be human-mode only and must not alter or interleave `--json` envelopes.
- Replay progress must use existing runtime/event-spine vocabulary rather than adding NDJSON or a second progress protocol.
- Resume recovery progress must include existing durable ledger events and newly appended recovery events without source contact after package finalization.
- Redaction must remain fail-closed for destination URIs, package paths, and event detail values.
- Backfill and runtime behavior are out of scope for this ticket.

## Findings

No blocking findings.

Resolved significant finding: the initial worker implementation attached progress snapshots to successful `CommandOutput` values but not to `CliError` paths, so human replay failures lost the phase context that WS5B requires. Parent repair added optional boxed progress snapshots to `CliError`, config-aware human error rendering, run/replay failure attachment, and `replay_package_failure_human_stderr_includes_progress_context`. The repair was verified by focused failure tests, full `cdf-cli` tests, and Clippy.

Minor residual risk: replay now records multiple durable replay-stage events instead of the prior single `replay_recorded` event. This is intentional for WS5B and covered by updated JSON assertions, but downstream consumers that assumed the first event was `replay_recorded` need to query by event kind. The CLI test `inspect_run_reports_duplicate_replay_status` was updated to follow that safer pattern.

Minor residual risk: the progress snapshot is prepended to the final human document rather than streamed live during test harness execution. This matches the current WS5A sink/snapshot foundation and deterministic test path, but a later interactive renderer could still refine incremental terminal behavior without changing the WS5B command wiring.

## Verdict

Pass. Acceptance criteria are supported by `.10x/evidence/2026-07-08-p1-product-ws5b-run-replay-resume-progress.md`, and the residual risks do not block closure because they are either intentional behavior changes within WS5B scope or follow-on renderer polish outside this ticket.

## Residual risk

Existing quality-scan context remains: `cargo audit` reports an allowed unmaintained `paste` warning, `cargo deny` prints duplicate dependency warnings while passing policy, and `jscpd` reports duplicate test/code patterns. None were introduced as blocking WS5B failures in the observed checks.
