Status: active
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/done/2026-07-19-glue-g1-external-table-source.md, .10x/tickets/done/2026-07-19-iceberg-i1-catalog-discovery.md

# Glue G2: Lake Formation authority and live conformance

## Scope

Implement Glue/Lake Formation metadata authorization, table/partition credential vending and renewal, requested-column audit context, exact permission-mode handling, worker-local secret resolution, and authorized FQ12 live Glue external/Iceberg catalog conformance with cleanup and performance evidence.

## Non-goals

No silent ambient-S3 fallback, unsupported cell-filter approximation, catalog mutation beyond disposable fixture setup, or retained cloud infrastructure.

## Acceptance Criteria

- Full-table and supported column-scoped reads use vended least-authority credentials and renew safely during long runs.
- Unsupported cell/nested filters fail closed before S3 access with Athena/Trino remediation.
- Credentials never enter plans/tasks/packages/logs/evidence; workers resolve references locally.
- FQ12 fixtures cover Glue Iceberg catalog plus conventional external tables, many partitions, expiry/retry, denial/redaction, no-op incrementality, performance, and teardown.

## References

- `.10x/specs/aws-glue-external-table-source.md`
- `.10x/specs/iceberg-source.md`
- `.10x/specs/portable-partition-task-protocol.md`

## Assumptions

- User-ratified 2026-07-19: FQ12 is the live integration environment; concrete cloud mutation remains separately confirmed.

## Journal

- 2026-07-20: Activated after Glue G1 and the Iceberg Glue catalog binding closed. The implementation must extend the shared AWS control authority and Glue adapter without serializing credentials into plans/tasks or adding a Glue/Lake Formation branch to generic runtime code.

## Blockers

No implementation blocker. Disposable external resource creation still requires confirmation; read-only inspection and existing-resource conformance are authorized.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
