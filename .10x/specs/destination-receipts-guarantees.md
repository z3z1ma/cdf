Status: active
Created: 2026-07-05
Updated: 2026-07-05

# Destinations, receipts, and delivery guarantees

## Purpose and scope

This specification governs destination commit protocols, destination sheets, dispositions, receipts, idempotency, replay, guarantee derivation, and first destinations. It derives from book Chapter 13 and decisions D-5, D-16, D-27, and D-28.

## Destination protocol

A destination MUST be a commit protocol, not an unverified sink. It MUST expose a destination sheet, dry-runnable commit planning, and commit sessions with migration, write, finalize, and abort operations.

`plan_commit` MUST be plan-time/dry-runnable and MUST surface migration DDL, target, disposition, and idempotency behavior before data moves.

Package-embedded commit-plan evidence MUST follow `.10x/decisions/package-state-commit-preimage-artifacts.md`. When package-token idempotency uses the finalized package hash, the identity-participating artifact records `idempotency_token_source = "package_hash"` rather than a concrete token value. The concrete destination commit request uses the finalized package hash as the token after package identity is known.

`finalize` MUST return a durable receipt or an error. There is no accepted ambiguous third state.

## Destination sheets

A destination sheet MUST declare supported dispositions, transaction support, idempotency mechanism, bulk paths, Arrow-to-destination type mappings with fidelity, identifier rules, migration support, quarantine table support, and concurrency constraints.

Destination sheets MUST be consumed by the planner, falsified by conformance suites, and snapshotted into `firn.lock`.

## Dispositions

MVP dispositions are `append`, `replace`, and `merge`.

`replace` MUST be atomic where supported and MUST NOT degrade into delete-then-insert without explicit unsupported/error behavior.

`merge` MUST use primary or merge keys and deterministic batch deduplication before commit.

`cdc_apply` arrives with log CDC and MUST apply `_firn_op` operations ordered by source position.

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

DuckDB MUST be the first local-loop destination. Its sheet MUST declare single-writer file constraints and ICU/timezone limitations; `firn doctor` MUST check ICU availability.

Parquet/object-store MUST support manifest receipts with key, etag, and sha256 details and act as the seam for Iceberg/Delta destinations.

Postgres MUST be the transactional reference with DDL migration, `ON CONFLICT` merge, xid-bearing receipts, and source-side exercise.

Iceberg and Delta are destinations, not package formats. Their transaction metadata belongs inside firn receipts.

## Acceptance criteria

- Every destination commit can be planned without writing and finalized into a verifiable receipt.
- Replaying the same package against package-token destinations returns duplicate/no-op behavior.
- Guarantee output matches the table above for representative capability combinations.
- Conformance falsifies incorrect destination sheet claims.

## Explicit exclusions

This spec does not define resource extraction, package hashing, project-file syntax, or conformance harness implementation details.
