Status: active
Created: 2026-08-02
Updated: 2026-08-04
Parent: .10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md
Depends-On: .10x/tickets/done/2026-08-02-clickhouse-destination-connector.md

# MongoDB source connector

## Scope

Implement and ship `cdf-source-mongodb` for MongoDB 8.0+ finite collection reads using the official
asynchronous driver's raw BSON cursor. Add bounded discovery/schema evidence, exact pushdown and
cursor semantics, BSON-to-Arrow mapping, catalog enrollment, live fixtures, documentation, and a
direct raw-BSON source roofline cell.

## Non-goals

Change streams, resume tokens, CDC operations, ObjectId checkpoints, arbitrary aggregation
pipelines, map-reduce, or implicit Extended JSON coercion.

## Acceptance Criteria

- Configuration, bounded discovery, freeze/drift, mapping, projection/filter fidelity,
  numeric/date/timestamp cursor ordering, compile/portability, health, and execution implement
  `.10x/specs/mongodb-collection-source.md`.
- Raw BSON mapping and variant/quarantine behavior cover heterogeneous documents, missing/null,
  nested arrays/documents, ObjectId, Decimal128, DateTime, duplicate keys, and unsupported types.
- One reusable client/pool streams byte-accounted batches under injected async host, memory,
  cancellation, retry, and egress authorities without a private runtime or unbounded queue.
- Built-in catalog integrity, generic source matrix, jobs invariance, package/replay/checkpoint
  laws, and `tools/certify-connector.py --kind source --id mongodb --core-impact` pass against a
  digest-pinned MongoDB 8.0+ fixture.
- The source macro benchmark reaches the 0.90 direct raw-BSON driver roofline with pool/batch
  settings recorded.
- Independent review passes after closure repair.

## References

- `.10x/specs/mongodb-collection-source.md`
- `.10x/decisions/exact-value-text-fallbacks.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/research/2026-08-02-sqlite-clickhouse-mongodb-connector-shaping.md`
- `docs/connector-authoring.md`

## Assumptions

- Finite source semantics, deferred change streams, MongoDB 8.0+, and the 90% roofline are
  user-ratified.
- Collection-only authoring is the smallest complete finite document-source surface.

## Journal

- 2026-08-02: Ticket opened; execution waits for complete ClickHouse tranche closure.
- 2026-08-03: Exact-value audit corrected schemaless BSON Decimal128 to tagged canonical text;
  schema-proven fixed domains remain Arrow Decimal128 and special values never become floats.
- 2026-08-04: Execution resumed after the ClickHouse source/destination implementations and their
  independent reviews stabilized. Their remaining non-terminal gate is the connector parent's
  deliberately final six-cell certificate, so treating it as a prerequisite to connector five
  would create a cycle. Read the complete ticket and governing source, roofline, exact-value,
  extension, checkpoint, and error-ownership authorities. Selected official `mongodb` 3.8.0 with
  BSON 3 and the raw-batch cursor API. The error audit scope is the new source leaf; catalog and
  conformance enrollment remain supporting boundaries and do not own SDK error classification.
- 2026-08-04: Implemented the `cdf-source-mongodb` leaf with contact-free compilation, typed
  configuration and secret references, one reusable official async client/pool, bounded discovery,
  raw BSON batch decoding, sampled effective-schema authority, exact ObjectId/Decimal128/DateTime
  mappings, typed filters/projection/cursors, cancellation, egress, and byte-accounted memory.
  Added the canonical MongoDB semantic definitions to the built-in registry and enrolled the
  source in the built-in catalog and generic source matrix.
- 2026-08-04: The digest-pinned MongoDB 8.0.13 live shard passed 15 executed append/replace/merge
  cells and three sheet-governed exclusions across ClickHouse, DuckDB, Parquet, Postgres, SQLite,
  and Quasar. Every executed cell proved package verification, receipt-gated checkpointing,
  duplicate no-op replay, and fresh-artifact replay. The live path exposed and repaired three
  shared current-contract defects without compatibility behavior: ClickHouse's sheet now publishes
  its real namecase/type rows, its conformance fixture creates disposition-truthful table engines,
  and packages persist the source schema-admission program separately from the destination-
  normalized validation program.
- 2026-08-04: Froze the nine-file source error scope and 146 construction-bearing lines under
  `.10x/evidence/.storage/2026-08-04-mongodb-source-error-files.nul` and
  `.10x/evidence/.storage/2026-08-04-mongodb-source-error-sites.tsv`. All 12 Internal-bearing lines
  are CDF/official-driver invariant sites. Direct and nested typed errors preserve kind, message,
  and retry delay before raw I/O or SDK fallback classification.

## Blockers

None.

## Evidence

- Eight focused source unit tests pass, including exact BSON mapping, drift, duplicate-key,
  injection, portability/redaction, cursor, and error-wrapper behavior.
- The selected live generic source shard passes all required cells against MongoDB 8.0.13,
  PostgreSQL 17, and the clean digest-pinned ClickHouse fixture.
- Error inventory and classification are frozen at the paths recorded in the journal.
- Roofline, final connector certificate, and closure review remain pending.

## Review

Pending independent red-team review.

## Retrospective

Pending executor handback.
