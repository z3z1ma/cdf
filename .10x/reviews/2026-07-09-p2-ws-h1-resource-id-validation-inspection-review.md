Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-h1-resource-id-validation-inspection.md
Verdict: pass

# P2 WS-H1 Resource Id Validation And Inspection Review

## Target

Implementation for `.10x/tickets/done/2026-07-09-p2-ws-h1-resource-id-validation-inspection.md`, primarily:

- `crates/cdf-declarative/src/compiled.rs`
- `crates/cdf-project/src/lockfile.rs`
- `crates/cdf-cli/src/context.rs`
- `crates/cdf-cli/src/inspect_command.rs`
- focused tests in `crates/cdf-declarative/src/tests.rs`, `crates/cdf-project/src/tests.rs`, and `crates/cdf-cli/src/tests.rs`

## Assumptions Tested

- Project `[resources."pattern"]` entries must match compiled resource ids, not raw `[resource.<name>]` names.
- A zero-match declarative mapping must fail before validate/plan runtime work.
- Command-facing diagnostics must name the active command.
- Inspection must show enough origin metadata for an operator to understand why the compiled id exists.
- Explicit `id` behavior must be preserved when current tests prove compatibility is still needed.

## Findings

Pass: Canonical default ids are still generated as `<source>.<resource>`. The new `source_and_resource_names_form_canonical_compiled_id` test proves `[source.tlc]` plus `[resource.yellow]` compiles to `tlc.yellow`.

Pass: Mapping validation is performed in the project compile path used by CLI project loading and `validate_project`. A bad `[resources."yellow"]` mapping fails with the unmatched pattern, the compiled id `tlc.yellow`, and the concrete `[resources."tlc.yellow"]` fix.

Pass: `cdf validate` and `cdf plan` use command-aware project loading for mapping errors. The CLI regression test proves the validate message names `cdf validate`, the plan message names `cdf plan`, and the plan message does not contain `cdf validate cannot load project`.

Pass: `cdf inspect resources` now renders compiled id, source name, resource name, source file, and mapping status. The single-resource inspect view also includes the same origin fields while retaining trust/cursor detail.

Pass: Explicit id compatibility was not broken. An attempted stricter implementation failed existing `cdf-project` SQL runtime tests that use `postgres.orders`; the final implementation preserves explicit ids and adds a compatibility test documenting that path.

Pass: The implementation stays within the ticket's owned surface. No `cdf add`, ad-hoc mode, docs quickstart, or unrelated P1/WASM files were changed by this work.

Parent review repaired two non-blocking scope issues before commit: command-aware project-load wrapping is now limited to the new mapping-pattern diagnostic, and project mapping patterns do not add an unratified `?` wildcard beyond the existing `*` style used by project mappings.

## Verdict

Pass. The acceptance criteria are supported by focused tests and full crate tests for the touched crates.

## Residual Risk

No closure-blocking risk remains for this ticket. Future migration away from explicit id overrides remains a separate product decision because current tests still prove compatibility is required.

Parent integration CodeQL produced three pre-existing fake-secret fixture findings in `crates/cdf-cli/src/tests.rs` backfill tests. They do not implicate the H1 implementation and are already owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.
