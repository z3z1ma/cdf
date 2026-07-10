Status: active
Created: 2026-07-05
Updated: 2026-07-10

# Destinations, receipts, and delivery guarantees

## Purpose and scope

This specification governs destination commit protocols, destination sheets, dispositions, receipts, idempotency, replay, guarantee derivation, and first destinations. It derives from book Chapter 13 and decisions D-5, D-16, D-27, and D-28. General run composition is governed by `.10x/specs/run-orchestration-ledger.md`.

Destination row-provenance, correction/readback capability sheets, and residual-promotion strategies are further governed by `.10x/specs/schema-promotion-corrections.md`.

## Destination protocol

A destination MUST be a commit protocol, not an unverified sink. It MUST expose a destination sheet, dry-runnable commit planning, commit sessions with migration, segment-write, finalize, and abort operations, and trait-level receipt verification.

`plan_commit` MUST be plan-time/dry-runnable and MUST surface migration DDL, target, disposition, and idempotency behavior before data moves.

Package-embedded commit-plan evidence MUST follow `.10x/decisions/package-state-commit-preimage-artifacts.md`. When package-token idempotency uses the finalized package hash, the identity-participating artifact records `idempotency_token_source = "package_hash"` rather than a concrete token value. The concrete destination commit request uses the finalized package hash as the token after package identity is known.

`DestinationProtocol::begin` MUST be implemented by every destination protocol implementation. Session support is not optional and MUST NOT be hidden behind an error-returning default implementation. If a destination category cannot support sessions, that category must be modeled explicitly in the destination sheet and active specs before implementation.

`CommitSession` MUST accept package segments incrementally, either as a segment stream or as per-segment writes returning `SegmentAck`. Fully materialized package replay MUST feed recorded package segments through the same API shape as future streaming package-to-destination commits. A session MAY be synchronous for MVP, but the API shape MUST NOT require callers to preload a whole package into the destination session.

`finalize` MUST return a durable receipt over every segment accepted by the session or an error. There is no accepted ambiguous third state.

A commit session MUST NOT synthesize checkpoint commits. It returns receipts; only the checkpoint store opens the commit gate. Destination-specific verification remains owned by the destination driver, even when called by a generic runtime.

Destination receipt verification MUST be exposed through the kernel destination protocol or an equivalent trait-level interface. Recovery and replay paths MUST verify receipts through that protocol rather than destination-specific free functions.

Async and restartable sessions remain later implementation choices unless a focused runtime/performance ticket ratifies them.

## Destination sheets

A destination sheet MUST declare supported dispositions, transaction support, idempotency mechanism, bulk paths, Arrow-to-destination type mappings with fidelity, identifier rules, migration support, quarantine table support, and concurrency constraints.

Destination sheets MUST be consumed by the planner, falsified by conformance suites, and snapshotted into `cdf.lock`.

## Dispositions

MVP dispositions are `append`, `replace`, and `merge`.

`replace` MUST be atomic where supported and MUST NOT degrade into delete-then-insert without explicit unsupported/error behavior.

`merge` MUST use primary or merge keys and deterministic batch deduplication before commit.

`cdc_apply` arrives with log CDC and MUST apply `_cdf_op` operations ordered by source position.

`scd2` and `snapshot` are excluded from loader dispositions.

## Receipts

A receipt MUST include receipt id, destination, target, package hash, segment acks, disposition, idempotency token, transaction or object-store commit metadata where applicable, counts, schema hash, migrations, commit time, and an independently executable verify clause.

Crash recovery in the committed-before-checkpointed window MUST verify the receipt against the destination before committing the checkpoint.

## Idempotency and guarantees

Every commit MUST carry package hash as idempotency token. Destinations with package-token support MUST make re-driving the same package a no-op with duplicate indication.

The planner MUST mechanically derive and print the observed delivery guarantee:

- At-least-once extraction plus `merge` with primary key gives effectively-once per key.
- At-least-once extraction plus `append` plus package-token idempotency gives effectively-once per package.
- At-least-once extraction plus `append` without idempotency remains at-least-once with duplicate risk.
- At-least-once extraction plus atomic `replace` gives effectively-once per target.
- At-least-once extraction plus ordered `cdc_apply` plus package token gives effectively-once per position.

Unqualified "exactly-once" MUST NOT appear in product claims.

## First destinations

DuckDB MUST be the first local-loop destination. Its sheet MUST declare single-writer file constraints and ICU/timezone limitations; `cdf doctor` MUST check ICU availability.

Parquet/object-store MUST support manifest receipts with key, etag, and sha256 details and act as the seam for Iceberg/Delta destinations.

Postgres MUST be the transactional reference with DDL migration, `ON CONFLICT` merge, xid-bearing receipts, and source-side exercise.

Iceberg and Delta are destinations, not package formats. Their transaction metadata belongs inside cdf receipts.

## Acceptance criteria

- Every destination commit can be planned without writing and finalized into a verifiable receipt.
- Replaying the same package against package-token destinations returns duplicate/no-op behavior.
- Guarantee output matches the table above for representative capability combinations.
- Conformance falsifies incorrect destination sheet claims.

## Explicit exclusions

This spec does not define resource extraction, package hashing, project-file syntax, or conformance harness implementation details.
