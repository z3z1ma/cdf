Status: superseded
Created: 2026-08-02
Updated: 2026-08-08
Superseded-By: `.10x/specs/mongodb-collection-source.md`

# MongoDB collection source with recursive shape discovery

## Historical contract

This specification governed the first-party finite MongoDB collection source before resource-
scoped discovery depth was ratified. The source targeted MongoDB 7.0 or later, used the official
asynchronous raw-BSON cursor, kept credentials in secret references, compiled without contact, and
provided bounded collection metadata and raw-document discovery.

Its material difference from the current contract was schema inference: BSON arrays were inferred
as Arrow lists and BSON documents as Arrow structs/maps throughout the bounded recursive shape.
The implementation recursively merged keys observed across sampled nested documents, rejected
heterogeneous arrays and shapes without an explicit drift disposition, and capped retained nested
shape at 4,096 schema fields, 32 levels, and the connector's structural memory limits.

The remainder of the contract required typed BSON filters and cursor ranges, stable `_id` cursor
tie-breaking, reusable official client/pool execution, exact primitive/ObjectId/Decimal128/DateTime
mapping, residual policy for drift, bounded memory and cancellation, live connector conformance,
and a 0.90 raw-driver source roofline.

## Reason for supersession

Recursive inference is unsafe as the default for schemaless document stores. A document used as a
map can contain one key per entity, UUID, or event; sampling then turns data values into columns,
causes unbounded-looking schema churn, and makes ordinary document evolution appear relational.
The user superseded this behavior on 2026-08-08. The current contract defaults to top-level-only
discovery and represents values at the configured depth boundary as opaque Extended JSON.

