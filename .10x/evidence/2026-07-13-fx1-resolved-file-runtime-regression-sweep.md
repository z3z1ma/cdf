Status: recorded
Created: 2026-07-13
Updated: 2026-07-13

# FX1 resolved file-runtime regression sweep

## Observation

Project tests no longer execute file declarations through the deleted dependency-free `CompiledResource` shortcut. They resolve the same transport/format/transform runtime and compiler-bound physical observations used by production. Format-agnostic discovery helpers require explicit file dependencies, newly appearing files require a fresh compiled observation, rule changes rebind the recorded expression plan, and destination tests plan against the selected sheet.

## Procedure

1. Ran `CARGO_BUILD_JOBS=12 cargo test -p cdf-project --lib -j12 -- --test-threads=12` after the first migration. Result: 161 passed, 17 failed; the remaining failures named stale fixture assumptions.
2. Deleted `prepare_local_parquet_discover_resource` and the dependency-free `prepare_discover_resource`, migrated discovery tests to explicit file dependencies, and updated runtime fixtures to current compiler/destination/receipt contracts.
3. Re-ran the 177-test project library suite. Result: 174 passed and three precise assertion residuals remained.
4. Ran each corrected residual individually:
   - `exhaustive_local_parquet_discovery_budget_and_incompatibility_fail_without_artifacts` — passed.
   - `generic_schema_discovery_dispatch_fails_closed_for_non_postgres_sql_dialect` — passed.
   - `postgres_artifact_replay_after_source_loss_without_receipt_commits_checkpoint` — passed.
5. Ran `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-project -p cdf-format-parquet --all-targets --no-deps -j12 -- -D warnings` — passed.
6. Ran formatting and diff checks — passed.

## What it supports or challenges

The union of the second suite run and the three focused residual runs covers all 177 current project tests without restoring a compatibility path. The sweep also confirms that compiler-bound per-file evidence is refreshed when the file set changes and that expression and destination policy artifacts remain exactly bound.

## Limits

This is project/runtime regression evidence, not FX1 aggregate closure. Descriptor-driven format detection, executable codec-plan binding, monolithic `cdf-formats` deletion, and the remote external-codec project law remain open.
