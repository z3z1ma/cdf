Status: done
Created: 2026-07-07
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-05-cli-surface.md
Depends-On: .10x/specs/project-cli-observability-security.md

# Implement cdf init scaffold

## Scope

Implement the project scaffold/write path for `cdf init [DIR] [--name NAME] [--force]`.

Owns:

- `crates/cdf-cli/src/project_command.rs` and focused CLI tests.
- `cdf-project` scaffold/write helpers if the CLI needs lower-layer project-format ownership rather than ad hoc file writes.
- Fixture-backed default project shape and validation coverage.

## Acceptance criteria

- `cdf init` creates a minimal typed CDF project at the selected directory and emits stable human and JSON output.
- The scaffolded project validates with `cdf validate` without manual edits.
- Existing files are not overwritten unless `--force` is supplied.
- The scaffold contains only secret references or commented examples, never resolved secret values.
- Any non-obvious default project shape is ratified in an active spec or decision before implementation lands.

## Evidence expectations

Run focused `cdf-cli` init/validate tests, overwrite/no-overwrite tests, JSON output assertions, `cargo fmt --all -- --check`, `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`, `cargo check --workspace --all-targets --locked`, `git diff --check`, and applicable security scans for touched files.

## Explicit exclusions

No remote template fetching, no environment-specific credential discovery, no package or state initialization beyond directories/files required by the ratified scaffold, and no hidden defaults that affect run behavior.

## Blockers

None. If implementation exposes a project-default semantic not already covered by `.10x/specs/project-cli-observability-security.md`, self-ratify it before editing source.

## Progress and notes

- 2026-07-07: Split from `.10x/tickets/done/2026-07-07-cli-remaining-command-planners.md`. Current `project_command::init` returns not-supported because project scaffold semantics are not exposed by `cdf-project` yet.
- 2026-07-08: Worker B implemented the ratified local `cdf init` scaffold in `crates/cdf-cli/src/project_command.rs` with focused CLI coverage in `crates/cdf-cli/src/tests.rs`. Local verification passed: `cargo fmt --all -- --check`; `cargo test -p cdf-cli init_ --locked`; `cargo test -p cdf-cli validate_json_reports_project_shape --locked`; `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`; `git diff --check`. Worker handoff still needed parent evidence, review, and closure.
- 2026-07-08: Parent review moved scaffold semantics into `cdf-project::write_local_project_scaffold`, leaving `cdf-cli` as argument normalization plus output formatting. Closure evidence is recorded in `.10x/evidence/2026-07-08-cli-init-scaffold.md`; adversarial review is recorded in `.10x/reviews/2026-07-08-cli-init-scaffold-review.md`. Acceptance is met and the ticket is closed.
