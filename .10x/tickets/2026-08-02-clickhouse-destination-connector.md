Status: open
Created: 2026-08-02
Updated: 2026-08-02
Parent: .10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md
Depends-On: .10x/tickets/2026-08-02-clickhouse-source-connector.md

# ClickHouse destination connector

## Scope

Implement and ship `cdf-dest-clickhouse` with verified append, capability-proven atomic replace,
default native ReplacingMergeTree merge, and opt-in atomic copy-on-write merge through the official
ArrowStream client path. Add deterministic insert-token settlement, recovery, type/engine/key
inspection, merge-mode policy, live crash coverage, built-in enrollment, documentation, and a
direct official-client destination roofline cell.

## Non-goals

Versioned ReplacingMergeTree guessing, silent engine replacement, cross-partition replacement
without proof, eventual compaction as a receipt, unacknowledged async inserts, generic/multi-table
transaction claims, mutations, or private wire protocols.

## Acceptance Criteria

- Sheet, mapping, bulk preparation, append tokens, synchronous acknowledgement, recoverable mirror
  settlement, replace capability proof, atomic exchange, zero-row marker, and receipt verification
  implement `.10x/specs/clickhouse-destination.md`.
- Merge defaults to direct ReplacingMergeTree ArrowStream insertion with exact sorting/partition
  proof, logical `FINAL` verification, deterministic recovery, and explicit eventual physical
  uniqueness; environment policy selects atomic copy-on-write when immediate uniqueness is needed.
- Live tests cover supported engine/topology matrices, materialized-view dedup capability, duplicate
  segments/packages, crashes between target and mirror settlement, complete atomic replace, and
  native/atomic merge, merge capability rejection, and unsupported targets.
- ArrowStream batches, compression, client reuse, writers, and in-flight bytes are injected,
  bounded, observable, and fully joined before settlement.
- Built-in catalog integrity, generic destination/product/chaos/jobs laws, and
  `tools/certify-connector.py --kind destination --id clickhouse --core-impact` pass.
- The append and default native-merge macro cells reach the 0.90 direct ArrowStream roofline with
  identical acknowledgement/deduplication settings.
- Independent review passes after closure repair.

## References

- `.10x/specs/clickhouse-destination.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/spillable-package-dedup.md`
- `.10x/decisions/clickhouse-merge-modes.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `docs/connector-authoring.md`

## Assumptions

- Append, proven atomic replace, default ReplacingMergeTree merge, opt-in atomic copy-on-write
  merge, and the 90% roofline are user-ratified.
- The default merge mode explicitly guarantees logical `FINAL` uniqueness rather than immediate
  physical uniqueness; the atomic mode guarantees immediate publication of a unique target.
- A target that cannot prove deterministic insert deduplication or atomic replace fails the
  applicable prepared path rather than weakening its guarantee.

## Journal

- 2026-08-02: Ticket opened; execution waits for ClickHouse source closure.
- 2026-08-02: Execution started from reviewed and pushed ClickHouse source commit `afe7bab4`. The
  source child retains only parent-owned final integration gates; its official client, bounded
  ArrowStream, transport lease, type parser, identifier, error, and pinned live-server boundaries
  are stable enough for the declared destination dependency. Read the complete destination ticket
  and every direct spec, invariant, research, and authoring authority before implementation.
- 2026-08-02: User expanded the destination to support merge, superseding the active ClickHouse
  spec's explicit merge exclusion. Source authority establishes keyed, deterministic, effectively
  once merge semantics; ClickHouse leaves one consequential choice between immediate atomic
  copy-on-write publication and an eventually deduplicated ReplacingMergeTree representation.
  Paused implementation before encoding either semantic model.
- 2026-08-03: User ratified native ReplacingMergeTree as the throughput-first default and atomic
  copy-on-write as the opt-in immediate-uniqueness mode. Superseded the former ClickHouse spec,
  recorded `.10x/decisions/clickhouse-merge-modes.md`, and reshaped this ticket without resuming
  implementation in the same shaping turn.

## Blockers

None. The merge-mode visibility tradeoff and configuration boundary are user-ratified and governed
by the active spec and decision record.

## Evidence

Pending.

## Review

Pending independent red-team review.

## Retrospective

Pending executor handback.
