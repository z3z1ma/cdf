Status: active
Created: 2026-07-19
Updated: 2026-07-19

# AWS Glue external-table source

## Purpose and scope

This specification governs AWS Glue Data Catalog classification and conventional object-store external-table reads. It does not govern Iceberg table semantics, Glue ETL jobs/workflows, or arbitrary execution of every object Glue can describe.

## Classification and ownership

The driver MUST load and classify the selected Glue catalog object before compiling an executable source plan.

- A conventional S3/object-store external table with a recognized exact format/SerDe mapping MAY compile through the shared format and object-access registries.
- An Iceberg table MUST route at authoring time to the Iceberg source with the Glue catalog binding; runtime MUST NOT duplicate Iceberg semantics.
- Delta, Hudi, views, federated/JDBC tables, streams, and unknown/custom SerDes MUST fail with the exact owning future source or query-engine route.
- Glue ETL jobs and workflows are orchestration and MUST NOT be represented as row sources.

Runtime source delegation is forbidden. `cdf add` MAY classify a Glue URI and emit the authoritative source kind/configuration, but a recorded plan is owned by exactly one driver.

## Schema and partitions

Glue table/partition columns and Schema Registry references are declared hints. Physical file observations remain facts reconciled through the ordinary CDF schema/contract program.

The source MUST interpret table and partition StorageDescriptor location, input format, SerDe, compression, parameters, and per-partition overrides exactly. Unsupported or conflicting metadata fails planning with the relevant field and supported mappings.

Compatible partition predicates MUST push into Glue `GetPartitions` expressions with recorded exact/inexact fidelity. Pagination and service-side segmented enumeration MUST remain bounded, cancellable, retriable, and canonically merged. Provider segment limits are not CDF jobs caps. Partition/task metadata MUST be streamed or spilled rather than collected without account.

Selected data objects MUST open through the neutral CDF object-access authority and registered format drivers. The Glue source MUST NOT duplicate file readers, transports, decompression, schema reconciliation, or retry controllers.

## Identity and incrementality

The compiled plan MUST bind Glue catalog/account/region, database/table, table version/update identity, query-as-of or transaction authority where available, selected partition inventory identity, format mapping, and an externalized object task/manifest reference.

Glue table version alone is not object-content identity. Append incrementality MUST use an explicit partition predicate/cursor and/or exact new/changed object identities. Repeated unchanged input produces a visible no-op. Table/partition/object mutation during planning or retry cannot silently mix metadata or generations.

## Lake Formation

Lake Formation governed data MUST use Lake Formation authorization and table/partition credential vending where required. Ambient S3 credentials MUST NOT bypass catalog governance.

Credentials resolve and refresh locally, are scope-down to selected table/partition locations, and never enter plans, tasks, packages, logs, or evidence. Requested columns and audit context MUST reflect the compiled projection. The driver MUST declare which permission modes it can enforce. Unsupported cell/nested filters MUST fail closed with an Athena/Trino remediation until CDF has an exact governed filter implementation.

## Product, conformance, and performance

The driver MUST implement the ordinary source schema/add/discovery/deep-validation/preview/plan/run/replay/inspect/doctor hooks without generic Glue-id branches. Doctor distinguishes Glue metadata, Lake Formation, credential vending, object access, region/account, egress, and format mapping.

Local mocked AWS protocol fixtures cover classification, pagination, partition predicates, descriptors, schema hints, retries, redaction, and authorization failures. Authorized FQ12 live conformance covers S3 Parquet and row-format tables, many partitions, cross-account/role behavior where provisioned, Lake Formation full/column access, credential expiry, no-op incrementality, and cleanup.

Planning and execution MUST obey the constant-memory and deterministic scheduler laws and scale to the provider/network/format roofline without private pools or hard concurrency caps.

## Explicit exclusions

No Glue ETL orchestration, crawler management, catalog mutation, view SQL evaluation, federated connector execution, Delta/Hudi semantics, or silent fallback to ambient S3 access is included.
