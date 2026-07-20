Status: cancelled
Created: 2026-07-20
Updated: 2026-07-20
Parent: .10x/tickets/done/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/done/2026-07-19-glue-g2-lake-formation-live-conformance.md

# Glue G3: governed FQ12 fixture conformance

## Scope

Provision a disposable Lake Formation-governed conventional external table in FQ12, grant the CDF application-integration role the minimum metadata/data-access permissions, execute the G2 full-table and column-scoped paths through credential expiry, and tear down every created resource.

## Non-goals

No product implementation, persistent fixture, broad administrator grant, Glue crawler/job, or mutation outside the disposable fixture namespace.

## Acceptance Criteria

- A governed Parquet or row-format table exercises unfiltered table/partition metadata and table/partition credential vending through CDF.
- Full-table, column-scoped, expiry refresh, denial, redaction, many-partition, no-op, and performance cases have reproducible evidence.
- Created Glue, Lake Formation, IAM, and S3 state is completely removed.

## References

- `.10x/specs/aws-glue-external-table-source.md`
- `.10x/decisions/iceberg-glue-source-boundaries.md`
- `.10x/tickets/done/2026-07-19-glue-g2-lake-formation-live-conformance.md`

## Assumptions

- Blocked: AWS resource/IAM mutations require explicit user authorization at execution time.

## Journal

- 2026-07-20: Split from G2 after read-only inspection proved FQ12 has no governed conventional external-table fixture. The active role can inventory Glue/Lake Formation but receives `AccessDeniedException` for `GetUnfilteredTableMetadata`; no mutation was attempted.
- 2026-07-20: Cancelled because the remaining work is external fixture/IAM setup, not product implementation, and the required mutation authority was not granted. Reactivate only when the user explicitly authorizes a disposable fixture plus narrowly scoped Lake Formation/IAM grants and teardown.

## Blockers

Explicit AWS fixture and IAM mutation authority is absent.

## Evidence

- Read-only STS identified account `617739438897` in `us-west-2`.
- Glue `bronze.transactions` is an ungoverned external Iceberg table.
- Lake Formation lists only an S3 Tables wildcard registration; no conventional governed table exists.
- A correctly formed unfiltered-metadata probe reached `AccessDeniedException`, proving endpoint/protocol reachability but not credential vending.

## Review

Cancelled correctly. Simulating a success locally would not prove AWS integration, while provisioning without explicit authority would violate the program's own assumption and external-mutation boundary.

## Retrospective

Live conformance matrices should separate product implementation from fixture authority at shaping time. An unavailable external environment is not a reason to keep complete product work active indefinitely or to overstate read-only evidence.
