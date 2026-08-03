Status: open
Created: 2026-08-02
Updated: 2026-08-02
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

## Blockers

None beyond the declared dependency.

## Evidence

Pending.

## Review

Pending independent red-team review.

## Retrospective

Pending executor handback.
