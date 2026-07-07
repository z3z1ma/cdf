Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-general-run-postgres-destination.md
Verdict: pass

# General run Postgres destination review

## Target

Review of the implementation and evidence for `.10x/tickets/done/2026-07-07-general-run-postgres-destination.md`.

## Findings

- Significant, resolved: the first implementation duplicated Postgres Arrow type mapping inside `cdf-project` by matching formatted `DataType` strings. That would have made the project runtime a second owner of destination schema semantics. The fix exposes `postgres_columns_for_schema` from `cdf-dest-postgres` and has `cdf-project` ask the destination crate to derive columns from package or resource schemas.
- Minor, accepted limit: generic `CommitSession::finalize` returns only `Receipt`, so the project-run Postgres report cannot expose duplicate/no-op detail the way the DuckDB compatibility path does. The implementation records `DestinationCommitReceiptOnly` and leaves duplicate behavior covered by `cdf-dest-postgres` tests.
- Minor, accepted limit: `ResourceDescriptor.merge_key` is already compiler-normalized from declarative metadata, including the current primary-key fallback. This slice consumes descriptor metadata and does not introspect the destination. If future product behavior needs to distinguish user-authored merge keys from compiler-derived defaults, that provenance needs a separate descriptor/compiler owner.
- No finding: unsupported Postgres schema/type combinations are rejected during request validation before package/state/destination table mutation, and the focused test asserts no package directory, state DB, target table, or `_cdf_loads` table is created.
- No finding: durable-receipt recovery has a source-contact regression test that deletes the source file before recovery and verifies checkpoint/package completion from artifacts.

## Verdict

Pass. The resolved destination-ownership issue reduces semantic drift risk, and the remaining limits are accurately reflected in the evidence.

## Residual Risk

This review does not close the parent general-run orchestrator ticket. Parent closure still needs a separate evidence mapping across DuckDB, Parquet, Postgres, non-file resources, crash windows, and run-ledger acceptance.
