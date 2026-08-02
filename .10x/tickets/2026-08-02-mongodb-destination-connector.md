Status: open
Created: 2026-08-02
Updated: 2026-08-02
Parent: .10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md
Depends-On: .10x/tickets/2026-08-02-mongodb-source-connector.md

# MongoDB destination connector

## Scope

Implement and ship `cdf-dest-mongodb` for MongoDB 8.0+ transactional deployments with append,
atomic replace, merge, deterministic `_id`, package-token idempotency, independently verifiable
receipts, catalog/live/chaos coverage, documentation, and a direct-driver destination roofline cell.

## Non-goals

Standalone or pre-8.0 MongoDB, random ObjectIds, CDC application, patch-style merge, arbitrary
aggregation writes, unbounded transactions/concurrency, or Extended JSON fallback.

## Acceptance Criteria

- Sheet, mapping, deterministic identity, merge dedup, transaction/session retry, bulk path,
  provenance/mirrors, append/replace/merge, and verification implement
  `.10x/specs/mongodb-destination.md`.
- Live tests cover replica-set/sharded preflight as available, standalone rejection, nested type
  round trips, reserved `_id`, duplicate packages, zero rows, merge-key/shard compatibility, and
  crash/ambiguous commit at every lifecycle boundary.
- The official async client/pool and bulk APIs operate under injected memory, cancellation, retry,
  and execution bounds; unsupported parallel session operations are absent.
- Built-in catalog integrity, generic destination/product/chaos/jobs laws, and
  `tools/certify-connector.py --kind destination --id mongodb --core-impact` pass.
- The destination macro benchmark reaches the 0.90 same-semantics official-driver roofline and
  records transaction, write concern, index, pool, and batch settings.
- Independent review passes after closure repair.

## References

- `.10x/specs/mongodb-destination.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `docs/connector-authoring.md`

## Assumptions

- Append/replace/merge, deterministic `_id`, required merge keys, MongoDB 8.0+, transaction-capable
  deployment, and the 90% roofline are user-ratified or required by the active receipt contract.

## Journal

- 2026-08-02: Ticket opened; execution waits for MongoDB source closure.

## Blockers

None beyond the declared dependency.

## Evidence

Pending.

## Review

Pending independent red-team review.

## Retrospective

Pending executor handback.
