Status: active
Created: 2026-07-19
Updated: 2026-07-19

# Iceberg and AWS Glue source boundaries

## Context

CDF's first post-backlog source tranche is Iceberg, with AWS Glue catalog integration shaped by its actual protocol role. Iceberg is a table format whose identity, schema, partition evolution, deletes, and incrementality are governed by catalog-selected snapshots. AWS Glue is a catalog and authorization plane that may describe Iceberg tables, conventional external files, views, federated tables, and other systems; it is not one uniform row protocol.

The existing source-extension contract requires source implementations to remain dependency-isolated and receive transport, memory, secret, egress, and execution authority through neutral injected services. Today reusable local/HTTP/object-store access is physically housed in `cdf-source-files`, which a second source cannot depend on without violating that contract. Current typed positions also have no table-snapshot authority, while `ScanPlan` materializes partitions inline and therefore cannot represent million-file Iceberg tables under the constant-memory law.

## Decision

CDF will implement one first-class `iceberg` source. Iceberg REST is the neutral catalog protocol; AWS Glue, and later Polaris or Unity, are catalog bindings of that source rather than separate Iceberg execution paths. Catalog implementations and Iceberg protocol types remain inside the Iceberg source boundary.

CDF will separately implement a Glue external-table source for conventional cataloged object-store tables. It will classify Glue objects before execution. Iceberg tables route to the Iceberg source; Delta, Hudi, views, federated tables, JDBC tables, streams, and unknown/custom SerDes fail with an exact owning-source or query-engine route rather than entering a partial generic path. Glue ETL jobs and workflows are orchestration, not data sources.

Reusable local/HTTP/S3/GCS/Azure object access will move from `cdf-source-files` into one neutral injected object-access crate. The extraction preserves one credential, egress, retry, cancellation, telemetry, client-pool, generation, memory, and spool authority. File-source glob, discovery, compression, and `FileManifest` semantics remain in `cdf-source-files`.

Iceberg checkpoint authority will use a new source-neutral typed table-snapshot position. It will not overload `FileManifest`, `LogPosition`, or `ForeignState`. Individual data/delete file identities remain bound into canonical scan-task artifacts, while successful multi-partition aggregation commits the one selected table snapshot.

High-cardinality planned tasks will be canonical, content-addressed, bounded or spill-backed artifacts consumed as streams and suitable for portable partition capsules. Upstream Iceberg scan-task structs are not serialized into CDF artifacts. CDF-owned tasks exclude secret values and include all partition, schema, name-mapping, delete, generation, and predicate semantics required for isolated reconstruction.

CDF will use the official Apache Iceberg Rust implementation only on the same Arrow/Parquet major as CDF. A permanent Arrow-major conversion bridge is rejected. `iceberg-datafusion` will not enter the source hot path; CDF's ordinary `QueryableResource`/DataFusion boundary remains the sole engine integration.

## Alternatives Considered

### Treat Glue as one polymorphic source that dynamically delegates at runtime

Rejected. It would either depend on sibling sources or move format-specific semantics into generic runtime code. Classification may be an authoring convenience, but the compiled runtime owner remains the actual table/format source.

### Reuse `cdf-source-files` directly from the Iceberg source

Rejected. It violates source dependency isolation and makes transport ownership follow the first consumer rather than the neutral capability.

### Use `FileManifest`, `LogPosition`, a composite workaround, or `ForeignState` for Iceberg snapshots

Rejected. Each obscures table snapshot semantics or stores the wrong checkpoint authority.

### Serialize `iceberg-rust::FileScanTask`

Rejected. Released tasks cannot serialize all required partition/spec/name-mapping fields, and newer task shapes may contain key metadata that cannot cross plan or worker boundaries as plaintext.

### Use Iceberg Rust on an older Arrow major and convert batches

Rejected. It adds a permanent copy/compatibility path to the primary data plane and conflicts with the active Arrow tuple policy.

## Consequences

The first implementation tranche requires a neutral object-access extraction, Arrow-aligned Iceberg dependency, typed snapshot position, and externalized task-set authority before full scans can close. Those are general capabilities, not Iceberg-name branches.

Adding a future Iceberg catalog binding changes only the Iceberg catalog registry/composition. Adding a conventional Glue physical format joins a registered CDF format mapping. Generic project, engine, package, destination, receipt, and checkpoint orchestration remain source-identity-free.

Local filesystem Iceberg and local REST-catalog fixtures provide deterministic conformance. AWS Glue/S3/Lake Formation evidence uses the FQ12 environment only after explicit external-mutation confirmation and must clean up provisioned resources.
