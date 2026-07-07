Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/done/2026-07-07-cli-remaining-command-planners.md
Depends-On: .10x/specs/project-cli-observability-security.md

# Split CLI command modules by noun

## Scope

Refactor `cdf-cli` command implementation so `crates/cdf-cli/src/commands.rs` is a thin dispatcher plus shared report/output glue, not the home for every command family.

Candidate module owners include `preview`, `state`, `package`, `doctor`, `status`, `sql`, `contract`, `backfill`, `resume`, and `inspect`, but this ticket must keep the split mechanical and behavior-preserving unless a child command ticket explicitly owns behavior.

## Acceptance criteria

- `commands.rs` no longer contains the dominant implementation bodies for unrelated command families.
- Command-family modules own their parsing adapters, helper functions, and report structs where those helpers are not shared.
- Shared output/reporting utilities remain small and explicitly named.
- Public CLI behavior, JSON field names, exit codes, and failure ordering remain unchanged unless governed by a separate active ticket.
- `rust-code-analysis-cli` and `jscpd` before/after reports are recorded, with no accepted increase in `commands.rs` aggregate complexity or duplicated code without an explicit rationale.

## Evidence expectations

Run `cargo fmt --all -- --check`, `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`, `cargo test -p cdf-cli --locked --no-fail-fast`, `cargo check --workspace --all-targets --locked`, `git diff --check`, `rust-code-analysis-cli`, and `jscpd` over `crates/cdf-cli/src`.

## Explicit exclusions

No new command semantics, no new CLI flags, no JSON schema changes, no dependency changes, no lower-layer runtime work, no command-family behavior implementation beyond code movement required to preserve current behavior.

## Blockers

None.

## Progress and notes

- 2026-07-07: Opened after the `cdf run` extraction reduced `commands.rs` from 2414 SLOC / cyclomatic 461 / cognitive 108 to 2078 SLOC / cyclomatic 370 / cognitive 85, while leaving `state`, replay/reporting, dispatch, and other command families as remaining hotspots. The `cdf run` slice is closed separately under `.10x/tickets/done/2026-07-07-cli-run-general-runtime.md`.
- 2026-07-07: Split command-family implementation into noun-owned modules plus shared `reports.rs`; `commands.rs` is now a dispatcher/output helper surface.
- 2026-07-07: Quality gates passed and metrics recorded in `.10x/evidence/2026-07-07-cli-command-module-architecture.md`; closure review recorded in `.10x/reviews/2026-07-07-cli-command-module-architecture-review.md`.
