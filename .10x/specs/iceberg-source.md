Status: active
Created: 2026-07-19
Updated: 2026-07-19

# Iceberg source

## Purpose and scope

This specification governs first-class Iceberg source configuration, catalog binding, discovery, snapshot identity, scan planning, task artifacts, execution, incrementality, security, product hooks, conformance, and performance. It refines the source-extension, deterministic-scheduler, constant-memory, portable-task, schema-discovery, and checkpoint contracts.

The first complete support level is Iceberg v1/v2 over Parquet with Iceberg REST and AWS Glue catalogs, including schema and partition evolution, position and equality deletes, branches/tags/time travel, fixed-snapshot reads, and append-only snapshot incrementality. ORC/Avro data, Iceberg v3/deletion vectors, modular encryption, changelog output, and resident tailing remain explicitly capability-gated until their focused owners close; unsupported use MUST fail during planning before payload or destination mutation.

## Configuration and catalog authority

An Iceberg source MUST identify one catalog binding and one or more table resources. Source options MUST contain secret references rather than credential values and MUST validate catalog URI, warehouse/catalog ID, region, endpoints, egress policy, and catalog-specific fields before source contact. Resource options MUST identify namespace, table, and optional branch, tag, snapshot ID, or timestamp without encoding credentials in URIs.

Iceberg REST is the neutral catalog protocol. Glue is a catalog binding with identical downstream table semantics. Catalog implementations MUST remain behind an Iceberg-local registry and MUST NOT expose Iceberg or AWS types through kernel, runtime, project, engine, or generic CLI APIs.

Given a catalog ref that changes while a command runs, when the final plan has selected a snapshot, then execution and replay MUST continue against that exact recorded snapshot or fail its generation precondition; it MUST NOT reinterpret `current`.

## Discovery and schema

Discovery MUST load catalog/table metadata and the exact selected snapshot without sampling data files. It MUST convert the selected Iceberg schema to Arrow while preserving Iceberg field IDs, schema ID, documentation, defaults, source-name provenance, nullability, and logical/physical type metadata.

Discovery, deep validation, planning, and execution within one command MUST reuse generation-bound metadata observations rather than reloading the catalog pointer, table metadata, manifest lists, or manifests without need. Evidence MUST report actual metadata bytes and objects read.

An Iceberg schema snapshot participates in normal CDF pin/hash/package identity. Runtime physical observations reconcile against the fixed effective schema and cannot silently evolve it during a run.

## Compiled snapshot and task authority

The compiled physical plan MUST pin catalog binding/version, catalog identity, table UUID and identifier, selected ref, snapshot ID and sequence, parent snapshot where present, metadata location/generation, format version, schema ID/hash, partition-spec IDs, sort order, projection, predicate/fidelity, delete requirements, execution capabilities, and a canonical scan-task-set reference/hash.

Every planned task MUST bind a contiguous canonical ordinal, table/snapshot identity, data path/format/size/range/generation, record count where known, schema ID/hash, projected field IDs, partition spec and typed partition values, name mapping, case policy, delete-file descriptors, predicate program, and required reader/capability versions. Tasks MUST contain secret/key references only; credentials, bearer tokens, plaintext encryption keys, open handles, callbacks, and coordinator paths are forbidden.

Task sets MUST be append/spill-backed and content-addressed. Parallel catalog/manifest planning MUST canonicalize output independently of completion order without a fixed item-count ceiling or single-thread fallback. Metadata allocation participates in the memory ledger.

## Object access and execution

Catalog metadata, manifest lists, manifests, delete files, and data files MUST use the injected neutral CDF object-access authority. No Iceberg component may create an independent credential chain, object-store pool, retry semaphore, Tokio runtime, jobs pool, memory cache, or unaccounted spool.

The Iceberg storage bridge MUST preserve CDF generation preconditions, egress authorization, cancellation, typed retries, telemetry, memory leases, and disk budgets. Ranged payload returned to Iceberg MUST retain its CDF lease until all zero-copy byte owners release it.

Execution MUST emit preaccounted Arrow batches through the ordinary `QueryableResource` pipeline. CDF reconciliation, validation, normalization, quarantine/residual handling, segmentation, package identity, destination ingress, receipts, and checkpoint gate remain unchanged.

The reader MUST correctly apply field-ID projection, schema/partition evolution, partition constants, defaults, position deletes, and equality deletes. Predicate pruning MAY be exact or inexact only when classified by the ordinary fidelity contract; residual evaluation remains authoritative where required.

Concurrency and prefetch sizes MUST derive from recorded knobs and admitted CPU/network/memory resources. There MUST be no hard performance cap masquerading as policy. Jobs 1/N MUST yield identical rows, positions, verdicts, segments, and package hashes.

## Source position and incrementality

Every partition MUST attest the selected typed table-snapshot position. Identical partition attestations aggregate to one terminal snapshot position only after every selected task has completed or reached an allowed terminal quarantine outcome.

`snapshot` mode reads one fixed current or historical snapshot. `append_snapshots` mode MUST prove that the committed snapshot is an ancestor of the new snapshot and that every intervening operation is append-only before selecting added data files. No-change ancestry produces a visible fast no-op. Overwrite, rewrite, replace, delete, missing history, or divergent ancestry MUST fail planning with the exact unsupported operation and the remedies: use fixed-snapshot/replace semantics or a changelog-capable disposition.

Changelog and resident tail modes MUST NOT be approximated with append-only semantics. They remain unavailable until their CDC/stream epoch contracts can represent removals and row operations exactly.

## Security and failure behavior

Secrets resolve locally through the existing provider chain. Egress policy covers catalog, object-store, credential, and KMS endpoints. Errors and artifacts MUST redact credential-bearing configuration and signed URLs.

Encrypted data MUST fail closed unless the selected capability can resolve keys locally without serializing plaintext key metadata into plans, task capsules, packages, logs, or evidence.

Catalog/table/snapshot/schema/spec/generation drift, missing delete files, unsupported format/version, malformed metadata, credential expiry, cancellation, and object mutation MUST produce typed errors before admitting inconsistent results. Retries may not mix generations or snapshots.

## Product hooks

The driver MUST implement generated config schema, reference compilation/add, bounded discovery/pin/diff, deep validation, preview/run parity, plan/explain, inspect, doctor, replay, and capability rendering through the ordinary registry hooks. Generic command modules MUST NOT match Iceberg identifiers.

Doctor MUST distinguish catalog authentication, table visibility, metadata access, data-object access, Lake Formation/KMS authorization where applicable, egress denial, and unsupported table capabilities without scanning payload data.

## Conformance and performance

Permanent local conformance MUST cover filesystem tables and a local REST catalog/object store. The matrix MUST include empty/unpartitioned/partitioned tables, schema and partition evolution, missing field IDs/name mappings, position/equality deletes, branches/tags/time travel, append ancestry/no-op/rewrite rejection, stale/tampered metadata, retries/cancellation, jobs invariance, replay after catalog advancement, and budget/spill failure.

High-cardinality tests MUST prove bounded metadata memory with at least million-task synthetic plans. Performance evidence MUST separately measure catalog/manifest planning, pruning, object I/O, Parquet decode/delete application, and downstream CDF overhead against the aligned raw Iceberg reader and CDF Parquet roofline. The source MUST scale until the actual CPU, network, object-store, memory, or destination roofline; CDF overhead remains within the active P3 budget.

AWS Glue/S3/Lake Formation live conformance MUST run on an explicitly authorized FQ12 fixture, record region/instance/dataset/permissions, and tear down provisioned resources.

## Explicit exclusions

This specification does not make Iceberg a package format or destination, implement catalog writes/table commits, introduce a new query engine, permit Arrow-major conversion, define Glue external-table behavior, or give workers package/receipt/checkpoint authority.
