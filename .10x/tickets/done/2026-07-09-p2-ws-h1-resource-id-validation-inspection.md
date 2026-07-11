Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-h-scaffolding-id-model-two-minute-path.md
Depends-On: .10x/decisions/data-onramp-source-identity-preview-disposition.md, .10x/specs/data-onramp-source-experience-cli.md

# P2 WS-H1 resource id validation and inspection

## Scope

Make the compiled resource-id model legible before `cdf add` and ad-hoc mode: compiled ids are `<source>.<resource>`, project mapping patterns validate against those compiled ids, and operators can inspect what ids were produced.

Owned write scope:

- `crates/cdf-declarative/src/**` for compiled-id and mapping-pattern validation if that is the current ownership boundary;
- `crates/cdf-project/src/**` only if project configuration owns mapping-pattern validation;
- `crates/cdf-cli/src/**` only for `cdf inspect resources` output or tests directly required by this ticket;
- this ticket's evidence and review records.

## Acceptance criteria

- Tests prove the compiled id for `[source.tlc]` plus `[resource.yellow]` is `tlc.yellow`.
- Project/environment mapping patterns are checked against compiled ids; a pattern that matches zero compiled resources fails validation before run/plan with a diagnostic naming:
  - the unmatched pattern;
  - the compiled resource ids that did exist;
  - the likely source/resource id mismatch fix.
- `cdf inspect resources` or the closest current inspection command renders each compiled id, source name, resource name, source file when available, and mapping status.
- Existing resource-id compatibility behavior is preserved only if current tests prove it is needed; otherwise new behavior follows the P2 id rule.
- The diagnostic must not mention the wrong command name; if it appears during `cdf validate` or `cdf plan`, the wording must match that command.

## Evidence expectations

Record focused evidence for:

- targeted declarative/project/CLI tests by name;
- full crate tests for every crate touched;
- clippy for every crate touched;
- `cargo fmt --all -- --check`;
- scoped `jscpd` and `rust-code-analysis-cli` on touched Rust files;
- `git diff --check`;
- gitleaks and banned-phrase/rename scans on touched records.

## Explicit exclusions

This ticket does not implement `cdf add`, schema discovery, ad-hoc mode, docs quickstart rewrites, or TLC live runs. It only makes the existing resource-id model visible and validates zero-match mappings.

## Progress and notes

- 2026-07-09: Opened from WS-H after P2 friction row 10 identified the confusing `[source.<name>]` vs mapping-pattern behavior as a first-run blocker.
- 2026-07-09: Implemented project mapping validation against compiled resource ids, source-file/mapping origin metadata, command-aware validate/plan diagnostics, and `cdf inspect resources` origin rendering. Focused and full crate evidence recorded in `.10x/evidence/2026-07-09-p2-ws-h1-resource-id-validation-inspection.md`; closure review recorded in `.10x/reviews/2026-07-09-p2-ws-h1-resource-id-validation-inspection-review.md`.
- 2026-07-09: Preserved explicit `id` compatibility because existing `cdf-project` SQL runtime tests prove `postgres.orders` is still required. New declarative resources still default to `<source>.<resource>`.
- 2026-07-09: Parent integration reran focused tests, full touched-crate tests, workspace check, clippy, fmt, diff check, Semgrep, source-only Gitleaks, cargo deny/audit/vet, OSV, CodeQL through the reusable database path, jscpd, rust-code-analysis, banned-phrase scans, and direct `unsafe` search. CodeQL's remaining current-tree findings are pre-existing P1 backfill-test fake-secret fixtures owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.

## Blockers

None.
