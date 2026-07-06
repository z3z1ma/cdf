Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md

# Checkpoint store SQLite implementation evidence

## What was observed

The `firn-kernel` crate now exposes the synchronous runtime-neutral `CheckpointStore` contract and serde-backed checkpoint artifact values: `CHECKPOINT_STATE_VERSION`, `CheckpointStatus`, `Checkpoint`, `RewindRequest`, and `RewindReport`. The `firn-state-sqlite` crate keeps the in-memory implementation and SQLite implementation initialized with WAL mode.

The SQLite schema records checkpoint identity, pipeline/resource/scope, state version, parent, input/output positions, package hash, schema hash, receipt id, status, head marker, timestamps, serialized state delta, serialized receipt, and rewind target marker. A partial unique index enforces at most one committed head for each `(pipeline_id, resource_id, scope_json)` where `is_head = 1`.

Commits are accepted only through `CheckpointStore::commit(checkpoint_id, receipt)`. Commit rejects receipts that do not cover the state delta package hash, segment ids, and segment row/byte counts. Proposed and abandoned checkpoints never become heads. Rewind appends a `rewound` marker row, moves the head back to the committed target checkpoint, preserves later committed checkpoints in history, and reports package hashes that are now ahead of state.

State serialization is gated to checkpoint state version `1` and round-trips SQLite JSON storage for all kernel `SourcePosition` shapes: cursor, log, file manifest, page token, composite, and foreign state. The same test also covers resource, partition, window, file, stream, schema-contract, destination-load, and composite scope JSON round-trips.

## Procedure

All commands were run from `/Users/alexanderbut/code_projects/personal/firn`.

Dependency refresh and targeted loop:

```text
cargo check -p firn-state-sqlite --all-targets
cargo test -p firn-state-sqlite --locked --no-fail-fast
cargo tree -p firn-state-sqlite --locked
```

The first command refreshed `Cargo.lock` for the newly declared state-crate dependencies. The final dependency tree for `firn-state-sqlite` uses `firn-kernel`, `rusqlite` with default features disabled, `serde`, `serde_json`, and dev-only `tempfile`.

Required worker verification:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test -p firn-state-sqlite --locked --no-fail-fast
```

All required commands passed. The package test run executed 6 unit tests and 0 doctests with no failures:

```text
commit_requires_receipt_covering_package_and_segments ... ok
abandon_keeps_proposed_checkpoint_out_of_head ... ok
sqlite_uses_wal_and_single_committed_head_index ... ok
sqlite_head_move_remains_transactionally_unique_across_connections ... ok
rewind_appends_marker_and_reports_packages_ahead ... ok
sqlite_round_trips_position_scope_and_state_json ... ok
```

## What this supports or challenges

This supports the ticket acceptance criteria for receipt-gated commits, transactional SQLite head uniqueness, append-only rewind behavior with ahead-of-state package reporting, and position/scope/state serialization round-trips.

This also supports the architecture-layering constraint: the runtime-neutral checkpoint contract now lives in `firn-kernel`, and `firn-state-sqlite` depends downward on it for the in-memory and SQLite implementations.

Parent review later found that the runtime-neutral checkpoint contract belonged in `firn-kernel` and that `SqliteCheckpointStore::connection(&self) -> &Connection` exposed a public raw SQL bypass. The repair moved the contract to `firn-kernel`, added a kernel serde round-trip test for checkpoint contract values, and removed the public raw connection accessor. SQLite tests now inspect the private `conn` field from the module test submodule only.

Repair verification on 2026-07-06:

```text
cargo fmt --all -- --check
```

Passed with exit code 0.

```text
cargo check -p firn-kernel --all-targets --locked
```

Passed with exit code 0.

```text
cargo check -p firn-state-sqlite --all-targets --locked
```

Passed with exit code 0.

```text
cargo clippy -p firn-kernel --all-targets --locked -- -D warnings
```

Passed with exit code 0.

```text
cargo clippy -p firn-state-sqlite --all-targets --locked -- -D warnings
```

Passed with exit code 0.

```text
cargo test -p firn-kernel --locked --no-fail-fast
```

Passed with exit code 0: 8 unit tests passed, 0 failed; 0 doctests.

```text
cargo test -p firn-state-sqlite --locked --no-fail-fast
```

Passed with exit code 0: 6 unit tests passed, 0 failed; 0 doctests.

```text
git diff --check
```

Passed with exit code 0.

The ratified book section 12.5 later established that `CheckpointStore` must be a shared thread-safe handle: `trait CheckpointStore: Send + Sync` with shared `&self` receivers. The repair changed the kernel trait accordingly, changed `propose`, `commit`, `abandon`, and `rewind` to shared receivers, and hid store mutation behind standard-library `Mutex` synchronization in `InMemoryCheckpointStore` and `SqliteCheckpointStore`. No public raw SQLite handle was reintroduced. A compile-time test now asserts both stores satisfy `CheckpointStore + Send + Sync`.

Thread-safe shared-store repair verification on 2026-07-06:

```text
cargo fmt --all -- --check
```

Passed with exit code 0.

```text
cargo check -p firn-kernel --all-targets --locked
```

Passed with exit code 0.

```text
cargo check -p firn-state-sqlite --all-targets --locked
```

Passed with exit code 0.

```text
cargo clippy -p firn-kernel --all-targets --locked -- -D warnings
```

Passed with exit code 0.

```text
cargo clippy -p firn-state-sqlite --all-targets --locked -- -D warnings
```

Passed with exit code 0.

```text
cargo test -p firn-kernel --locked --no-fail-fast
```

Passed with exit code 0: 8 unit tests passed, 0 failed; 0 doctests.

```text
cargo test -p firn-state-sqlite --locked --no-fail-fast
```

Passed with exit code 0: 7 unit tests passed, 0 failed; 0 doctests.

```text
git diff --check
```

Passed with exit code 0.

Parent mutation testing then found real test gaps in the checkpoint-store slice. The parent command was:

```text
cargo mutants -p firn-state-sqlite --test-tool nextest --timeout 60 --minimum-test-timeout 5 -j 4 -o reports/ai-quality/mutants-checkpoint --cargo-arg=--locked
```

Parent reported 111 mutants, 31 missed, 43 caught, and 37 unviable.

Mutation-hardening changes on 2026-07-06 added in-memory/SQLite parity coverage for the core store contract, tuple isolation for head/history/rewind behavior, rewind validation for non-committed targets, wrong tuples, missing targets, and no-head states, private SQLite row-corruption reads for scalar-vs-`delta_json` mismatches and `receipt_id`-vs-`receipt_json` mismatches, timestamp sanity assertions, and branch-lineage ahead-of-state reporting.

The first local mutation rerun after the broader test hardening used report output outside the repository:

```text
cargo mutants -p firn-state-sqlite --test-tool nextest --timeout 60 --minimum-test-timeout 5 -j 4 -o /tmp/firn-mutants-checkpoint-rerun --cargo-arg=--locked
```

That rerun reduced missed mutants to 2: 111 mutants tested, 2 missed, 72 caught, 37 unviable. The two remaining missed mutants were the SQLite read mapping of `is_head` and the in-memory-visible `validate_state_version` branch.

Final mutation-hardening verification on 2026-07-06:

```text
cargo fmt --all -- --check
```

Passed with exit code 0.

```text
cargo test -p firn-state-sqlite --locked --no-fail-fast
```

Passed with exit code 0: 14 unit tests passed, 0 failed; 0 doctests.

```text
cargo clippy -p firn-state-sqlite --all-targets --locked -- -D warnings
```

Passed with exit code 0.

```text
git diff --check
```

Passed with exit code 0.

The final local mutation rerun wrote reports outside the repository:

```text
cargo mutants -p firn-state-sqlite --test-tool nextest --timeout 60 --minimum-test-timeout 5 -j 4 -o /tmp/firn-mutants-checkpoint-rerun-final --cargo-arg=--locked
```

Passed with exit code 0: 111 mutants tested, 74 caught, 37 unviable, 0 missed.

## Limits

Full parent quality gates and closure review are recorded separately in `.10x/evidence/2026-07-06-checkpoint-quality-gates.md` and `.10x/reviews/2026-07-06-checkpoint-store-sqlite-review.md`. No destination mirror recovery or CLI migration command was implemented because both are outside this child ticket's explicit scope.
