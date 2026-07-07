Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md
Depends-On: .10x/tickets/done/2026-07-07-cli-run-general-runtime.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# Add `cdf inspect run`

## Scope

Add `cdf inspect run <id>` as a redacted, stable assembly of run ledger events, package/checkpoint/receipt pointers, transition state, and recovery guidance.

Owns:

- `crates/cdf-cli/src/args.rs`
- `crates/cdf-cli/src/commands.rs`
- `crates/cdf-cli/src/context.rs` if a run-inspection helper belongs there.
- Focused JSON/human output and redaction tests.

## Acceptance criteria

- Parser accepts `cdf inspect run <id>` and rejects malformed or extra arguments.
- Output includes run id, ordered ledger events, resource/scope/package/checkpoint/receipt/destination pointers when available, package status, checkpoint status, duplicate status when visible, and recovery guidance for incomplete states.
- Output explicitly marks missing package, receipt, or checkpoint artifacts instead of silently omitting them.
- Output redacts resolved secrets and does not expose destination credentials.
- `--json` output is stable for automation-relevant fields.

## Evidence expectations

Run focused inspect-run CLI tests for successful, failed/recoverable, missing-artifact, and redaction cases; run relevant run-ledger/project tests, clippy for CLI/project, workspace check, and `git diff --check`.

## Explicit exclusions

No resume mutation, no replay mutation, no UI, no broad observability dashboard.

## Design notes

- Existing `inspect` supports project/resources/resource/lock/destinations/package but not run.
- Inspect-run can land before full resume if it reports guidance honestly and does not claim mutation support.

## Blockers

None.

## Progress and notes

- 2026-07-07: Split from the broad CLI spine ticket after general orchestrator closure.
- 2026-07-07: Activated for implementation after `cdf run` and `cdf replay package` CLI children closed. Worker lane owns parser/report/test implementation for `cdf inspect run`; resume mutation remains excluded.
- 2026-07-07: Implemented `cdf inspect run <id>` as a read-only run-ledger report with stable JSON, redacted event details, package/receipt/checkpoint availability, duplicate status, and recovery guidance. Added read-only SQLite run-ledger opening so inspection never initializes missing state or mutates schema/data.
- 2026-07-07: Verification recorded in `.10x/evidence/2026-07-07-cli-inspect-run-spine.md`; closure review recorded in `.10x/reviews/2026-07-07-cli-inspect-run-spine-review.md`.
