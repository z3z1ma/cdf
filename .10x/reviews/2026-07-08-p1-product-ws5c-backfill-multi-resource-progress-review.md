Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws5c-backfill-multi-resource-progress.md
Verdict: pass

# WS5C closure review

## Target

Review of WS5C implementation for executed backfill and multi-slice progress.

## Findings

- Pass: `backfill_command.rs` now constructs `human_progress_sink(cli.json, cli.no_color)`, passes the resulting non-authoritative `RunEventSink` to every per-slice `run_project` request, and attaches the final snapshot only to human `CommandOutput`. JSON mode remains stable because no progress sink is created when `cli.json` is true.
- Pass: `progress.rs` now tracks seen sequence numbers per run id and resets terminal state when a new run id begins, which fixes the multi-slice case where each slice has its own run ledger sequence starting at 1. Focused coverage proves two run ids with restarted sequences produce four visible milestones rather than duplicate/out-of-order no-ops.
- Pass: headless progress carries `run=` and `scope=window:start..end` fields, so each backfill slice is distinguishable in CI-style logs without ANSI/control sequences.
- Pass: executed human backfill has a summary footer with succeeded slices, rows, and segments, and command tests cover multi-slice success.
- Pass: failure output annotates failed slice, planned package/checkpoint artifacts, mutation status, and recovery availability. The exercised duplicate-package failure correctly reports that no run id exists and no recovery command is available before run-ledger start.
- Resolved significant finding: parent review found that recovery guidance originally used the latest progress run id, which could have belonged to an earlier successful slice if a later slice failed before emitting events. The final diff now uses `ProgressSnapshot::latest_run_id_for_package` and has focused coverage proving package-specific lookup does not borrow unrelated slice run ids.
- Pass: no artifact identity, package hash, checkpoint gating, or durable ledger authority is driven by progress state. The progress sink remains best-effort and bounded.

## Residual risk

No natural post-run-ledger backfill failure fixture was added. The `cdf resume <run_id>` branch is code-inspected and now uses package-matched `ProgressSnapshot::latest_run_id_for_package` plus the same `CliError` progress rendering path verified in WS5B, but this closure does not include a backfill-only chaos hook because scheduler/failure-injection semantics were outside WS5C scope.

Scoped jscpd still reports residual duplication in the large CLI test file: 25 clones, 337 duplicated lines, 3.34%. A small WS5C helper reduced the local duplicate count from the first implementation pass, and the remaining blocks are pre-existing test-family patterns rather than new production duplication.

`cargo audit` retains the ratified `paste` advisory warning and `cargo deny check` retains the ratified duplicate Arrow tuple warnings.

## Verdict

Pass. WS5C acceptance criteria are satisfied with the residual test-fixture limitation recorded above.
