Status: active
Created: 2026-07-05
Updated: 2026-07-10

# Checkpoints, state, and the commit gate

## Purpose and scope

This specification governs cdf's checkpoint ledger, typed positions, state scopes, store trait, receipt-gated commit invariant, rewind, and destination-mirrored state. It derives from book Chapter 12 and decisions D-5, D-8, D-16, D-18, and D-19.

## State model

State MUST be an append-only ledger of typed transitions. The current cursor/head is a view over latest committed transitions, not a mutable cell.

The SQLite store MUST use WAL mode and enforce exactly one committed head per `(pipeline_id, resource_id, scope)` through a database constraint or equivalent transactionally maintained invariant.

Checkpoint rows MUST include checkpoint id, pipeline id, resource id, scope, state version, parent id, input position, output position, package hash, schema hash, receipt id, status, head marker, created time, and committed time.

Statuses MUST include `proposed`, `committed`, `abandoned`, and `rewound`.

## Commit-gate invariant

The only path from proposed to committed MUST be `CheckpointStore::commit(checkpoint_id, receipt)` or its moral equivalent. Commit MUST structurally verify that the receipt covers the package hash and every segment represented by the state delta.

There MUST be no public API that writes a committed checkpoint directly. A source cursor may advance only after all data represented by that cursor has been durably committed to destination and the destination receipt has been recorded in the checkpoint ledger.

## Positions and scopes

Positions MUST be typed and versioned. Required position shapes include `CursorPosition`, `LogPosition`, `FileManifest`, `PageToken`, `Composite`, and `ForeignState`.

`ForeignState` MUST preserve protocol, opaque blob, and blob hash for Singer/Airbyte compatibility without pretending to understand foreign semantics.

Scopes MUST support partition, window, file, stream, schema-contract, and destination-load keys. Checkpoint granularity MUST be selected by resource archetype and MUST NOT be forced into one cursor string.

`state_version` MUST gate deserialization. Migrations MUST be explicit, fixture-backed, and reachable through `cdf state migrate`.

## Store trait and operations

The store trait MUST support head lookup, propose, commit, abandon, history, and rewind. SQLite and in-memory stores ship at MVP. Future stores MUST pass the same conformance contract.

The public checkpoint store trait MUST be `Send + Sync` and use shared receivers so a runtime can hold one store handle across workers. Store implementations MUST hide mutation behind implementation-owned synchronization or transactional storage and MUST NOT expose raw write handles that bypass the commit-gate invariant.

Schema promotion additionally requires the executor-neutral fenced `ScopeKey::SchemaContract` lease specified by `.10x/specs/schema-promotion-corrections.md`. That focused lease does not authorize a scheduler or weaken receipt-gated checkpoint commit.

Rewind MUST append history or markers and move the head without deleting old transitions. Rewind output MUST report committed packages that are now ahead of state.

## Destination mirrors

Destinations capable of tables SHOULD mirror `_cdf_loads` and `_cdf_state`. Recovery from destination mirrors MAY reconstruct ledger heads but MUST warn that quarantine and lineage evidence is not reconstructible.

`cdf doctor` MUST be able to compare ledger heads to destination mirrors and report drift.

## Acceptance criteria

- Tests prove no checkpoint commit succeeds without a receipt covering package hash and segments.
- SQLite enforces a single committed head per scope under transaction.
- Tests prove the MVP stores satisfy the shared thread-safe store contract without exposing a committed-write bypass.
- Rewind never deletes checkpoint history and reports ahead-of-state packages.
- Destination mirror recovery reconstructs heads from receipts and marks evidence limits.

## Explicit exclusions

This spec does not define the destination write protocol itself, package file layout, or CLI command UX beyond required operations.
