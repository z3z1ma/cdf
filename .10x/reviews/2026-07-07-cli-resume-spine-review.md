Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-cli-resume-spine.md
Verdict: pass

# CLI resume spine review

## Target

Review of the `cdf resume` implementation and tests under `crates/cdf-cli/src/resume_command*` and `crates/cdf-cli/src/tests.rs`.

## Findings

- Significant: An initial status-only package repair path could have updated package status after seeing any committed checkpoint/receipt evidence. This was corrected before closure. `resume_command/model.rs::prove_status_repair_head` now requires the current head to be committed, marked `is_head`, carry the exact package replay `StateDelta`, and carry the exact selected durable receipt. Negative tests cover different current head and different selected receipt.
- Significant: An initial finalized Postgres package/no durable receipt branch failed closed even though `.10x/specs/run-orchestration-ledger.md` requires resume to drain the crash matrix after package finalization. This was corrected before closure. Postgres resume now derives the target from durable package replay inputs, requires explicit selected-environment policy `merge_dedup = "fail"`, and replays via `replay_postgres_package_from_artifacts` without source contact.
- Minor: The first implementation concentrated resume behavior in one large `resume_command.rs`. This was corrected before closure by splitting the module into `attempt`, `destination`, `events`, `model`, and `report`, while keeping `commands.rs` unchanged.
- Minor: `jscpd` duplicated-line percentage rose from 7.3100% to 7.4062% in the CLI slice, mostly from new crash-window and Postgres live tests. `QUALITY.md` says to penalize meaningful copy/paste and avoid cosmetic abstractions. I did not find duplicated business invariants that should be abstracted in this closure slice; the small increase is accepted with metrics recorded.

## Verdict

Pass. The child acceptance criteria are supported by focused resume tests, full `cdf-cli` tests, workspace check, clippy, format/diff checks, Semgrep, Gitleaks, jscpd, rust-code-analysis, scc, and prior unchanged workspace recovery tests.

## Residual risk

The broader CLI spine parent is not closed by this review. CLI table-backed SQL source execution still needs product-facing CLI success-path evidence, tracked separately under the parent after closure audit.
