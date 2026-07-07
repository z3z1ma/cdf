Status: active
Created: 2026-07-07
Updated: 2026-07-07

# Run orchestration and ledger

## Purpose and scope

This specification governs the general run runtime, run ledger, run recovery, replay, inspect-run assembly, and destination commit-session composition.

It derives from `VISION.md` Chapters 4, 13, 14, 15, 16, 18, and 23, `.10x/decisions/run-ledger-commit-session-spine.md`, `.10x/specs/checkpoint-state-commit-gate.md`, `.10x/specs/destination-receipts-guarantees.md`, and `.10x/specs/package-lifecycle-determinism.md`.

## Run identity

Every general orchestration attempt MUST have exactly one `RunId`.

`RunId` MUST be opaque to callers and serialized artifacts. CDF MAY change the concrete string format in a future artifact migration, but it MUST NOT require consumers to parse the identifier.

When a caller omits a run id, the project runtime MUST mint a unique run id before planning or extraction writes begin. Caller-supplied run ids MUST fail closed on collision in the selected environment ledger.

Run ids MUST appear in tracing spans, run ledger events, package/run association metadata, and inspect-run output.

## Run ledger

The run ledger MUST be append-only. It records facts about orchestration and recovery; it MUST NOT be a state-advancement authority.

The first ledger backend SHOULD live beside the existing SQLite checkpoint store for a selected environment. It MAY share a database file with checkpoint tables, but run tables MUST NOT expose a way to write committed checkpoints directly.

Each event MUST include run id, sequence number, timestamp, event kind, redacted structured details, and any relevant resource id, scope, partition id, package id/hash/path, checkpoint id, receipt id, and destination id.

Required event kinds:

- `run_started`
- `plan_recorded`
- `package_started`
- `package_finalized`
- `destination_commit_started`
- `destination_receipt_recorded`
- `checkpoint_proposed`
- `checkpoint_committed`
- `package_status_updated`
- `run_succeeded`
- `run_failed`
- `run_resumed`
- `replay_recorded`

Event details MUST contain secret references only. Resolved secret values MUST NOT be serialized into run ledger events.

## General run flow

Given a selected environment, resource set, destination, checkpoint store, package root, and run id:

1. The runtime MUST append `run_started`.
2. The runtime MUST plan resource scans and destination commits before source writes where the needed lower-layer planning APIs exist, then append `plan_recorded`.
3. For each resource/scope transition, the runtime MUST read the current checkpoint head and package input checkpoint artifact.
4. The runtime MUST execute `ResourceStream` partitions into a package with state-delta and destination-commit preimage artifacts before finalizing package identity.
5. The runtime MUST append `package_finalized` with package path and hash after package finalization.
6. The runtime MUST propose the checkpoint before destination mutation when a state delta is present and append `checkpoint_proposed`.
7. The runtime MUST commit the package through a destination `CommitSession` and append `destination_receipt_recorded` only after a durable receipt exists.
8. The runtime MUST verify or structurally accept the destination receipt according to the destination contract before calling `CheckpointStore::commit`.
9. The runtime MUST call `CheckpointStore::commit(checkpoint_id, receipt)` as the only state advancement path and append `checkpoint_committed` after it succeeds.
10. The runtime MUST update package status to checkpointed after checkpoint commit and append `package_status_updated`.
11. The runtime MUST append `run_succeeded` only after every selected transition reaches its terminal success condition.

If a run fails before success, the runtime MUST append `run_failed` when possible. Failure to append `run_failed` MUST NOT destroy recoverability from package, receipt, and checkpoint evidence.

## Destination commit sessions

Destination drivers MUST expose a commit session API equivalent to:

- Begin from a dry-runnable commit plan.
- Apply migrations when the plan requires them.
- Write package segments or a package view.
- Finalize to either a durable receipt or an error.
- Abort when possible.

The session API MUST preserve destination-specific receipt verification. A generic runtime MUST NOT synthesize receipts or bypass a destination verify clause.

Destination sessions MUST support duplicate package-token behavior when declared by the destination sheet. Duplicate receipts MUST be recorded and inspected like non-duplicate receipts.

## Resume

`cdf resume` MUST recover by inspecting run ledger events, package artifacts, destination receipts, and checkpoint rows.

Recovery MUST follow the crash matrix:

- No finalized package and no durable receipt: re-run extraction from the last committed checkpoint head.
- Finalized package and no durable receipt: replay the package into the destination without contacting the source.
- Durable receipt and uncommitted checkpoint: verify the receipt against the destination and commit the checkpoint without contacting the source.
- Committed checkpoint and stale package status: update package status only.
- Terminal successful run: no-op.

Resume MUST prefer durable package/receipt/checkpoint facts over run ledger events when they disagree. The run ledger is an index, not authority.

## Replay package

`cdf replay package <pkg> --to <dest>` MUST create a new run id and append `replay_recorded`.

Replay MUST use package replay inputs and MUST NOT re-run extraction, re-plan batching, or re-evaluate contracts. It MAY create a new checkpoint in a selected checkpoint store only when explicit target/state inputs are present and the receipt covers the package state delta.

Duplicate package-token receipts MUST be visible in the run ledger and inspect-run output.

## Inspect run

`cdf inspect run <id>` MUST assemble the run story from run ledger events, package manifests, package receipts, checkpoint rows, destination receipt verification status where available, and package-owned verdict summaries.

Inspect output MUST redact secrets. It MUST show missing or unavailable artifacts explicitly rather than silently omitting them.

Inspect output MUST distinguish:

- planned but not started work
- package finalized but not loaded work
- destination receipt recorded but checkpoint not committed work
- checkpoint committed but package status not checkpointed work
- duplicate replay receipt work
- failed work with recoverability guidance

## Acceptance criteria

- A general runtime test proves one source/destination run records run events, finalizes a package, records a receipt, commits a checkpoint, and leaves inspectable pointers.
- Crash/recovery tests prove the four current lifecycle windows can be recovered through the general runtime without source contact after package finalization.
- Destination session conformance proves DuckDB, Parquet/object-store, and Postgres sessions return verifiable receipts and preserve duplicate behavior.
- CLI tests prove `run`, `resume`, `replay package`, and `inspect run` route through the general runtime and do not bypass the commit gate.
- Redaction tests prove `inspect run` and run ledger serialization do not expose resolved secrets.

## Explicit exclusions

This spec does not authorize distributed scheduling, resident streaming, non-SQLite run ledger backends, vault-class secret providers, warehouse destinations, arbitrary SQL query execution, CDC, UI, or public performance claims.
