Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-cli-sql-run-success.md
Verdict: pass

# CLI SQL run success review

## Target

Review of the CLI SQL run success slice, including the unchanged `crates/cdf-cli/src/run_command.rs` SQL resource assembly and the new `crates/cdf-cli/src/tests.rs` live CLI success test.

## Findings

No blocking findings.

The review specifically checked:

- `run_command.rs` already supplies the project secret provider to `SqlRuntimeDependencies`, so adding production code would have been unnecessary.
- The new test uses a live Postgres source table, not a mocked source or lower-layer runtime shortcut.
- The resource fixture declares an exact zero-lag `updated_at` ordered cursor, matching the currently ratified non-file checkpoint advancement contract.
- The CLI invocation goes through `run_valid_run_resource_target`, so the test exercises the public `cdf run` command surface.
- The JSON assertions cover resource id, target, destination kind, row count, destination receipt row count, committed checkpoint status, and terminal `run_succeeded` ledger event.
- The destination table is queried directly from DuckDB to prove the run wrote the expected rows.
- The SQLite checkpoint head is inspected directly to prove the committed output position is the expected cursor maximum.
- The resolved source DSN and marker secret are absent from CLI stdout/stderr.
- The existing SQL fail-closed tests for missing secrets and missing ordered cursor remain in the focused `run_sql_resource` filter and passed with the new success case.

## Verdict

Pass. The ticket acceptance criteria are satisfied by a focused test-only change, with no production runtime code changes required.

## Residual risk

The live Postgres harness still skips on machines without `TEST_DATABASE_URL` or local Postgres binaries; this is the established local Postgres test pattern and does not weaken this slice relative to the governing lower-layer tickets.

`crates/cdf-cli/src/tests.rs` has pre-existing duplication reported by Jscpd. This review records a no-action rationale for this slice: the new code follows existing fixture and assertion style, and a test-abstraction refactor would be broader than the product-facing SQL CLI proof.
