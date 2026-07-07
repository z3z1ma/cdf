Status: active
Created: 2026-07-07
Updated: 2026-07-07

# Run ledger and commit-session spine

## Context

`VISION.md` makes the commit gate the central invariant: a source cursor may advance only after destination durability is proven by a receipt recorded in the checkpoint ledger. It also requires `cdf run`, `cdf resume`, `cdf replay package`, `cdf inspect run`, run/resource/partition/package tracing fields, and a destination protocol with `begin -> CommitSession -> write/finalize/abort`.

Current source proves the invariant for specialized slices but not for the general system. `crates/cdf-project/src/runtime.rs` hard-codes local-file-to-DuckDB/package replay functions, and `crates/cdf-kernel/src/destination.rs` exposes `DestinationProtocol::sheet` and `plan_commit` but no driver-neutral session. CLI and observability records are blocked on run identity, run-ledger ownership, run-to-package/checkpoint/receipt mapping, transition ordering, and resume semantics.

The active product objective identifies the run model as the first decision queue item for P0, the spine.

## Decision

CDF will add a general run spine made of two cooperating contracts:

- A project-owned append-only run ledger records attempted orchestration, run events, and pointers to plans, packages, receipts, checkpoints, verdict summaries, and recovery actions.
- A kernel-level destination commit-session protocol makes every destination commit follow the same lifecycle: dry-run plan, begin session, apply migrations/write package segments, finalize to a durable receipt or error, abort when possible.

The run ledger is an operational index, not the commit gate. Checkpoint heads remain authoritative only through `CheckpointStore::commit(checkpoint_id, receipt)`. A run ledger event may say what happened or what was attempted; it cannot advance state.

Run identity:

- `RunId` is an opaque, non-empty identifier. Consumers MUST NOT parse semantics from its string form.
- The project runtime MUST mint a unique `RunId` at run start when the caller does not supply one.
- Caller-supplied run ids are allowed only when they do not collide with an existing run in the selected environment ledger.
- Package ids, checkpoint ids, and receipt ids remain distinct identities. A run may own many packages, checkpoints, and receipts.

Run scope and mapping:

- A run is one orchestration attempt over one selected environment and one requested resource set: one resource, `--all`, replay of one package, resume of interrupted work, or a bounded backfill slice.
- Multi-resource runs are represented as one run with per-resource and per-scope transition attempts. The commit gate remains per package/checkpoint/scope.
- Multi-package runs are normal. Each package records its package id/hash and links back to the run ledger; the run ledger records the package path/hash and subsequent receipt/checkpoint facts.
- Verdict summaries are package-owned facts. The run ledger stores summaries and pointers for inspection, but package artifacts remain the source of truth.

Transition ordering:

- Run facts are append-only events with monotonic sequence numbers and timestamps inside the environment run ledger.
- Required event families are `run_started`, `plan_recorded`, `package_started`, `package_finalized`, `destination_commit_started`, `destination_receipt_recorded`, `checkpoint_proposed`, `checkpoint_committed`, `package_status_updated`, `run_succeeded`, `run_failed`, `run_resumed`, and `replay_recorded`.
- Event append failure after destination settlement MUST NOT block recovery. Durable package artifacts, destination receipts, and checkpoint rows remain enough to recover according to the crash matrix.

Resume and replay:

- `cdf resume` drains interrupted work by inspecting run ledger events, package artifacts, receipts, and checkpoint rows. It follows the Chapter 4 crash matrix: no finalized package means re-run extraction; finalized package without receipt means replay package without source contact; durable receipt without checkpoint means verify the receipt and commit checkpoint; committed checkpoint without package-status update means update status only.
- `cdf replay package <pkg> --to <dest>` is a new run that reads package replay inputs and destination target inputs, commits through the destination session, and records duplicate receipts as first-class facts.
- Duplicate destination receipts do not weaken the gate. If a checkpoint still needs committing, the duplicate receipt is eligible only if it verifies and covers the state delta.

Commit-session protocol:

- `DestinationProtocol` will grow a driver-neutral `begin` operation returning a `CommitSession`.
- A `CommitSession` MUST expose migration/write/finalize/abort phases or equivalent methods that preserve the same lifecycle and evidence. `finalize` MUST return a durable receipt or an error; no ambiguous success state is allowed.
- Destination-specific receipt verification remains destination-owned. The general orchestrator may call destination verification, but it must not reinterpret destination internals or bypass destination-specific guarantees.
- Existing DuckDB, Parquet/object-store, and Postgres commit implementations will be refactored onto the session protocol without changing their receipt semantics.

Compatibility:

- Existing specialized DuckDB/file runtime functions remain as thin wrappers or compatibility facades until the CLI/conformance callers migrate.
- Existing golden fixtures should not churn solely because the general runtime exists. Fixture changes require a package artifact/schema reason.

First implementation wave exclusions:

- No distributed scheduler, resident streaming supervisor, non-SQLite ledger backend, vault-class secret provider, UI, warehouse destination, arbitrary SQL query execution, or CDC semantics are included in the run-spine implementation wave.

## Alternatives considered

- Keep specialized runtimes and wire CLI commands one by one. Rejected because it repeats the current blocker: every source/destination slice dodges composition and leaves `resume`, `inspect run`, and multi-destination replay without one spine.
- Put run state inside the checkpoint ledger only. Rejected because run attempts include non-state-advancing facts such as plan records, failed packages, duplicate replay receipts, and inspection summaries. The checkpoint ledger must stay narrowly authoritative for committed heads.
- Make packages the only run ledger. Rejected because interrupted runs can exist before package finalization, and `inspect run` needs transition ordering across packages/checkpoints/receipts.
- Add a run ledger without commit sessions. Rejected because orchestration would still need destination-specific commit paths and could not generalize across DuckDB, Parquet, and Postgres.

## Consequences

- The next implementation work can proceed without inventing run semantics in code.
- CLI `run`, `resume`, `replay package`, and `inspect run` gain one shared lower-layer owner.
- The commit gate remains small and auditable: run ledger events are observable facts, while `CheckpointStore::commit` remains the only state advancement path.
- The run ledger becomes another serialized artifact surface and will need versioning/migration fixtures before external package compatibility claims.
- The destination API change is public Rust API surface and must be guarded with semver checks before closure.
