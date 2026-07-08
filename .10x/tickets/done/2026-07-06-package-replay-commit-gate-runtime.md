Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-package-builder-reader.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md, .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/tickets/done/2026-07-05-duckdb-destination.md

# Implement prepared package commit-gate runtime foundation

## Scope

Implement the first lower-layer runtime primitive that can take an already replayable load package, an explicit caller-supplied `StateDelta`, a DuckDB destination path/handle, and a `CheckpointStore`, then drive the commit-gate sequence without contacting the source.

Own `crates/cdf-project/**` for the prepared-package orchestration surface and focused tests. The expected home is a focused `runtime` module exported by the thin `cdf-project` crate root. The worker may add scoped `cdf-project` dependencies on `cdf-package`, `cdf-state-sqlite`, and `cdf-dest-duckdb` if needed to compose existing lower-crate APIs. The worker may make the smallest supporting edits in `crates/cdf-kernel/**`, `crates/cdf-package/**`, `crates/cdf-dest-duckdb/**`, or `crates/cdf-state-sqlite/**` only if existing public types cannot express the invariant; any such edit must be justified in progress notes.

The primitive is intentionally concrete for DuckDB and SQLite-backed local state because DuckDB is the MVP local-loop destination and exposes the concrete package commit and receipt verification API today. Do not invent a generic destination commit trait in this ticket; `DestinationProtocol` currently plans commits but does not finalize them. `cdf-project` must not depend on `cdf-cli`, and this ticket must not add CLI command plumbing.

## Acceptance criteria

- `cdf-project` exposes a reusable prepared-package runtime API for DuckDB package replay/checkpoint commit over public `PackageReader`, `StateDelta`, `DestinationCommitRequest`, `Receipt`, and `CheckpointStore` concepts.
- The API rejects non-replayable packages and packages whose manifest hash or segment set cannot cover the supplied `StateDelta`.
- The API accepts explicit target, disposition, merge keys, schema hash, package directory, DuckDB destination, checkpoint store, and complete `StateDelta`; it must not infer `StateDelta`, schema hash, target, disposition, merge keys, scope, or output position from package filenames or package ids.
- The API constructs `DestinationCommitRequest` with `idempotency_token = package_hash` and segment data matching the supplied `StateDelta`.
- The API proposes the supplied `StateDelta` before committing new destination work, and the only successful path to a committed checkpoint is `CheckpointStore::commit(checkpoint_id, receipt)`.
- The DuckDB commit path requires a durable `Receipt` that covers the package hash, schema hash, and every state segment before committing the checkpoint, then verifies that receipt against DuckDB before crossing the commit gate.
- The committed-before-checkpointed recovery path can take an already durable DuckDB/package receipt from the package/destination window, verify it, and commit the checkpoint without opening the source.
- If receipt verification fails, receipt identity does not match the delta, or the receipt lacks required segment acknowledgements, no checkpoint is committed.
- If destination commit fails before a durable receipt is returned, the proposed checkpoint is abandoned or otherwise left non-head/non-committed; the behavior must be explicit and tested.
- If checkpoint commit fails after a durable receipt exists, the receipt remains durable for resume/recovery and the API returns an error/report that does not pretend state advanced.
- The API includes a narrow test hook after destination receipt durability/verification and before checkpoint commit so a later chaos harness can exercise the MVP crash window without adding process-kill infrastructure here.
- Package lifecycle status updates use existing `PackageStatus` values to distinguish at least the loading and checkpointed states; exact `loaded` versus `committed` use must follow inspected source/spec authority and be recorded if implementation chooses one.
- Tests cover success, duplicate/idempotent destination replay, committed-before-checkpointed recovery, receipt verification failure, bad package hash, missing segment ack, destination failure before receipt, and checkpoint failure after receipt.

## Evidence expectations

Record focused `cargo fmt --all -- --check`, `cargo test -p cdf-project --locked --no-fail-fast`, `cargo test -p cdf-dest-duckdb -p cdf-state-sqlite --locked --no-fail-fast`, `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`, `cargo nextest run -p cdf-project --locked`, `cargo check --workspace --all-targets --locked`, and `git diff --check`.

Because this runtime surface is a commit-gate invariant, run a bounded mutation test over the new runtime module when feasible, for example `cargo mutants --package cdf-project --file crates/cdf-project/src/runtime.rs --no-shuffle --jobs 4 --timeout 120 -- --locked`. If mutation tooling is too slow or structurally blocked, record the exact limit and add negative tests for the missed invariant.

Significant closure must follow `QUALITY.md`. Use the reusable CodeQL database path from `.10x/knowledge/quality-gate-execution.md` and parallelize independent checks where practical.

## Explicit exclusions

No live source extraction, no declarative `CompiledResource::open` implementation, no full `cdf run` CLI support, no `cdf resume` command plumbing, no `cdf replay package` CLI plumbing unless the runtime API proves trivially sufficient without semantic invention, no generic destination abstraction, no Parquet/Postgres replay orchestration, no project-file defaults for pipeline/resource/target inference, no chaos harness, no golden-package fixture suite, no package GC retention policy, and no distributed execution.

This ticket intentionally provides the lower-layer primitive needed by later CLI, chaos, and MVP-demo work; those consumers remain separate tickets unless explicitly split and authorized.

## References

- `VISION.md` Chapter 11 lifecycle/crash matrix, Chapter 12 checkpoint commit-gate invariant, Chapter 13 receipt verification and replay idempotency, Chapter 17 CLI replay/resume surface, Chapter 19 conformance/chaos/golden gates, and Chapter 22 MVP acceptance demo.
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/tickets/done/2026-07-05-cli-surface.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`

## Progress and notes

- 2026-07-06: Split after parent inspection found several blocked surfaces share the same missing lower-layer API: CLI `run`/`resume`/`replay package`, chaos recovery, golden-package replay gates, and the MVP acceptance demo all need a receipt-verified package-to-checkpoint runtime primitive. Existing source already provides `execute_to_package`, `PackageReader`, DuckDB package commit/verify methods, and receipt-gated `CheckpointStore::commit`.
- 2026-07-06: Read-only explorer Popper recommended narrowing this child from a generic `cdf-engine` abstraction to a prepared-package DuckDB/SQLite orchestrator in `cdf-project`. Rationale: `cdf-project` is the project/orchestration boundary, DuckDB is the MVP local-loop destination with concrete commit/verify APIs, and the public `DestinationProtocol` currently supports planning only, not finalization. The ticket was revised before implementation.
- 2026-07-06: Do not implement in the ticket-creation turn. Assign to a worker in a later turn with the references above and a write boundary of `crates/cdf-project/**` plus justified minimal supporting edits.
- 2026-07-06: Parent activated the ticket and assigned implementation to a worker. Worker owns the prepared-package runtime API, focused tests, and focused verification inside the scoped write boundary. Parent owns integration review, broader `QUALITY.md` closure evidence, final review record, parent-ticket updates, and commit.
- 2026-07-06: Worker implemented the prepared-package DuckDB/SQLite commit-gate runtime in `crates/cdf-project/src/runtime.rs`, exported it through the thin crate root, and added focused tests in `crates/cdf-project/src/runtime_tests.rs`. Public API shape: `replay_prepared_duckdb_package`, `recover_prepared_duckdb_package`, request/report structs, `PreparedReceiptSource`, and a narrow `after_receipt_verified` hook. Added scoped `cdf-project` dependencies on `cdf-package` and `cdf-dest-duckdb`, plus test-only `cdf-state-sqlite`, Arrow, and tempfile dependencies; `Cargo.lock` changed only to record those `cdf-project` dependency edges. No lower-crate source edits were needed.
- 2026-07-06: Runtime behavior: verifies the package before mutation; rejects non-replayable status, package hash mismatch, schema hash mismatch, duplicate/missing/mismatched segments, bad receipt identity, missing acks, wrong ack counts, and failed DuckDB receipt verification; constructs `DestinationCommitRequest` from the explicit target/disposition/delta with `idempotency_token = package_hash`; proposes the explicit `StateDelta` before destination work; abandons the proposed checkpoint when DuckDB fails before returning a durable receipt; verifies a durable DuckDB receipt before `CheckpointStore::commit`; leaves checkpoint state unadvanced when the post-receipt hook or checkpoint commit fails; and supports recovery by verifying a supplied durable receipt and committing the already proposed checkpoint without source contact. Lifecycle uses `Loading` before destination work and `Checkpointed` only after checkpoint commit; `Loaded`/`Committed` were deliberately not used because current source authority does not expose a ratified distinction for those intermediate package states beyond durable receipt evidence in package receipts/DuckDB mirrors.
- 2026-07-06: Verification passed: `cargo fmt --all -- --check`; `cargo test -p cdf-project --locked --no-fail-fast` (24 passed); `cargo test -p cdf-dest-duckdb -p cdf-state-sqlite --locked --no-fail-fast` (9 DuckDB + 16 SQLite passed); `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`; `cargo nextest run -p cdf-project --locked` (24 passed); `cargo check --workspace --all-targets --locked`; `git diff --check`; and bounded mutation `cargo mutants --package cdf-project --file crates/cdf-project/src/runtime.rs --no-shuffle --jobs 4 --timeout 120 -- --locked` (27 mutants tested: 23 caught, 4 unviable, 0 survivors).
- 2026-07-06: Parent closure ran the full relevant `QUALITY.md` suite, installed missing user-level quality tools requested by the user, refreshed the reusable CodeQL DB only because source/manifest/lockfile content changed, and recorded evidence in `.10x/evidence/2026-07-06-package-replay-commit-gate-runtime.md`. Closure review passed in `.10x/reviews/2026-07-06-package-replay-commit-gate-runtime-review.md`. Remaining CLI plumbing, chaos/golden harnesses, live source extraction, and broader lifecycle edges remain with their existing parent tickets.

## Blockers

None for the prepared-package DuckDB/SQLite commit-gate runtime foundation.
