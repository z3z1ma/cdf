Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Catalog-task source commons

## Purpose and scope

This specification governs reusable implementation machinery shared by sources that resolve a
catalog/table identity into an immutable snapshot or generation, compile a potentially large
external task set, and execute those tasks through ordinary CDF partitions. Iceberg and Glue
prove the archetype. It does not erase their distinct catalog, authorization, delete, schema, or
snapshot semantics.

## Shared lifecycle

A catalog-task source MUST implement the following visible lifecycle through the existing
`SourceDriver` contract:

```text
typed catalog/table selection
→ bounded catalog discovery
→ immutable table/snapshot or generation authority
→ streamed task planning under metadata/spill budgets
→ content-addressed external task-set authority
→ bounded typed task decoding
→ ordinary executable partitions
→ source-specific read/admission
```

The fixed schema, compiled scan intent, source position, task-set hash, task content hash, and
generation attestation remain typed identity authorities. Generic machinery MUST never substitute
one hash for another or infer a source's snapshot semantics.

## Required shared implementations

`cdf-task-store` (or a focused lower leaf if dependency evidence requires it) MUST own the
implementation that is already structurally duplicated:

- typed authority retention under accounted encoded and parse memory;
- canonical task ordinal/content verification while streaming an external task set;
- typed task decode/validation hooks;
- retained executable payload construction with exact retained-byte accounting;
- spill-backed task-set builder/workspace finalization and cancellation cleanup;
- common diagnostics for malformed authority, ordinal mismatch, content mismatch, and budget
  exhaustion.

The shared API MUST use closed typed inputs and callbacks/generics at task-set boundaries. It MUST
NOT expose source-specific JSON values or introduce a universal `CatalogTableSource` trait whose
methods merely mirror `SourceDriver`.

Iceberg continues to own snapshot ancestry, manifest semantics, equality/position deletes,
field-id projection, generation attestation, and Iceberg task execution. Glue continues to own
Glue/Lake Formation authorization, partition expression semantics, SerDe classification,
credential vending, and row-format execution. A shared Parquet/file reader is used only when
both sources produce the same `FormatRegistry`/`ByteSource` contract; source-specific correctness
logic remains in the adapter.

## Planning-index boundary

Both sources MUST use the same lower lifecycle for budgeted spill workspace, canonical ordinal
emission, content hashing, final task-set publication, and cleanup. Their index record types and
planning algorithms remain source-owned. The common layer must not assume one row per file,
Iceberg manifests, Glue partitions, or a particular object format.

## Acceptance scenarios

- Given equivalent task authority and task records, Iceberg and Glue use one shared typed reader
  to reject wrong task-set type, authority hash, task ordinal, task content, and parse-memory
  overflow.
- Given task cardinality above resident metadata capacity, both planners spill through the same
  accounted workspace lifecycle and publish deterministic task-set identities.
- Given cancellation or a failed task decode, memory leases and temporary task artifacts are
  released exactly once.
- Given Iceberg delete semantics or governed Glue authorization, migration to the common layer
  changes no source-specific behavior, package identity, positions, or retry guarantees.
- Given a third synthetic catalog-task source, it can reuse task-set planning/reading without
  copying the retained-authority and spill lifecycle or editing generic runtime code.

## Explicit exclusions

No catalog client abstraction, universal table metadata model, shared SQL dialect, DataFusion
`TableProvider`, dynamic plugin ABI, or common snapshot semantics is introduced. This work does
not combine Iceberg and Glue crates or replace their first-class native readers.
