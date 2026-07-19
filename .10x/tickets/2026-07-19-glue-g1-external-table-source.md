Status: blocked
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/2026-07-19-iceberg-f1-neutral-object-access.md

# Glue G1: conventional external-table source

## Scope

Implement Glue catalog object classification and conventional object-store external-table compilation/execution through neutral object access and format registries, including descriptors, schema hints, partition predicate pushdown/pagination, per-partition overrides, exact routing errors, object-manifest incrementality, product hooks, and mocked protocol conformance.

## Non-goals

No Iceberg duplication, Delta/Hudi/view/federated/JDBC/stream execution, Glue jobs/crawlers/catalog mutation, or Lake Formation governed access beyond exact detection/failure.

## Acceptance Criteria

- Classification routes every supported/unsupported table family exactly before payload work.
- Supported Parquet and row-format external tables honor table/partition descriptors and registered format semantics.
- Partition planning is bounded/spill-backed, predicate-classified, cancellable, retriable, and deterministic.
- Table/partition/object identity supports correct no-op/new/changed planning without treating Glue schema as physical truth.
- Ordinary add/discovery/deep-validation/preview/run/replay/inspect/doctor and source conformance hooks pass without generic branches.

## References

- `.10x/specs/aws-glue-external-table-source.md`
- `.10x/specs/source-extension-runtime-contract.md`

## Assumptions

- User-ratified 2026-07-19: conventional Glue external tables are a separate source; Iceberg routes to the Iceberg driver.

## Journal

None yet.

## Blockers

Neutral object access F1.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
