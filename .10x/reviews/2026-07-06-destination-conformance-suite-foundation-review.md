Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-destination-conformance-suite-foundation.md
Verdict: pass

# Destination conformance suite foundation review

## Target

Review of the reusable destination conformance foundation implemented in `crates/firn-conformance/src/destination/mod.rs`, plus DuckDB/Parquet consumer test integration.

## Assumptions tested

- The common public destination contract is limited to `DestinationProtocol::sheet` and `DestinationProtocol::plan_commit`; this slice must not invent a generic finalize/receipt trait.
- Destination-specific physical commit and receipt verification tests must remain in DuckDB and Parquet.
- The harness must be reusable through public kernel types and must not depend on private destination internals.
- The crate organization preference against monolithic `lib.rs` files applies.

## Findings

No unresolved findings.

During review and verification, two issues were found and repaired before closure:

- The first migration-support assertion was too vacuous because it compared the planned migrations to the same expected migrations before checking unsupported migration semantics. The harness now explicitly rejects expected or planned migrations when a sheet declares migration support as unsupported.
- Clippy reported an `if_same_then_else` branch in the faulty destination self-test helper. The duplicate branches were collapsed into `should_plan_migration`.

## Verdict

Pass. The implementation stays within the ticket boundary, keeps `crates/firn-conformance/src/lib.rs` thin, reuses public destination interfaces, retains destination-specific physical receipt coverage, and has mutation evidence showing the new conformance assertions are meaningful.

## Residual risk

The suite foundation covers sheet truth and dry-run planning for DuckDB/Parquet. It does not yet cover resource conformance, chaos killpoints, golden-package fixtures, MVP demo execution, or live Postgres destination conformance; those remain owned by `.10x/tickets/2026-07-05-conformance-chaos-golden.md` and `.10x/tickets/done/2026-07-05-postgres-destination.md`.
