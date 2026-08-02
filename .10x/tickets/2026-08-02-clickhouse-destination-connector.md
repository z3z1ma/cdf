Status: open
Created: 2026-08-02
Updated: 2026-08-02
Parent: .10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md
Depends-On: .10x/tickets/2026-08-02-clickhouse-source-connector.md

# ClickHouse destination connector

## Scope

Implement and ship `cdf-dest-clickhouse` with verified append and capability-proven atomic replace
through the official ArrowStream client path. Add deterministic insert-token settlement, recovery,
type/engine inspection, live crash coverage, built-in enrollment, documentation, and a direct
official-client destination roofline cell.

## Non-goals

Merge, `ReplacingMergeTree` upsert claims, eventual dedup as verification, unacknowledged async
inserts, generic/multi-table transaction claims, engine replacement, or private wire protocols.

## Acceptance Criteria

- Sheet, mapping, bulk preparation, append tokens, async/sync acknowledgement, recoverable mirror
  settlement, replace capability proof, atomic exchange, zero-row marker, and receipt verification
  implement `.10x/specs/clickhouse-destination.md`.
- Live tests cover supported engine/topology matrices, materialized-view dedup capability, duplicate
  segments/packages, crashes between target and mirror settlement, complete atomic replace, and
  unsupported merge/targets.
- ArrowStream batches, compression, client reuse, writers, and in-flight bytes are injected,
  bounded, observable, and fully joined before settlement.
- Built-in catalog integrity, generic destination/product/chaos/jobs laws, and
  `tools/certify-connector.py --kind destination --id clickhouse --core-impact` pass.
- The destination macro benchmark reaches the 0.90 direct ArrowStream roofline with identical
  acknowledgement/deduplication settings.
- Independent review passes after closure repair.

## References

- `.10x/specs/clickhouse-destination.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `docs/connector-authoring.md`

## Assumptions

- Append plus proven atomic replace, explicit merge exclusion, and the 90% roofline are
  user-ratified.
- A target that cannot prove deterministic insert deduplication or atomic replace fails the
  applicable prepared path rather than weakening its guarantee.

## Journal

- 2026-08-02: Ticket opened; execution waits for ClickHouse source closure.

## Blockers

None beyond the declared dependency.

## Evidence

Pending.

## Review

Pending independent red-team review.

## Retrospective

Pending executor handback.
