Status: open
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-package-builder-reader.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md, .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/tickets/done/2026-07-05-duckdb-destination.md

# Implement prepared package firn-line runtime foundation

## Scope

Implement the first lower-layer runtime primitive that can take an already replayable load package, an explicit caller-supplied `StateDelta`, a DuckDB destination path/handle, and a `CheckpointStore`, then drive the firn-line sequence without contacting the source.

Own `crates/firn-project/**` for the prepared-package orchestration surface and focused tests. The expected home is a focused `runtime` module exported by the thin `firn-project` crate root. The worker may add scoped `firn-project` dependencies on `firn-package`, `firn-state-sqlite`, and `firn-dest-duckdb` if needed to compose existing lower-crate APIs. The worker may make the smallest supporting edits in `crates/firn-kernel/**`, `crates/firn-package/**`, `crates/firn-dest-duckdb/**`, or `crates/firn-state-sqlite/**` only if existing public types cannot express the invariant; any such edit must be justified in progress notes.

The primitive is intentionally concrete for DuckDB and SQLite-backed local state because DuckDB is the MVP local-loop destination and exposes the concrete package commit and receipt verification API today. Do not invent a generic destination commit trait in this ticket; `DestinationProtocol` currently plans commits but does not finalize them. `firn-project` must not depend on `firn-cli`, and this ticket must not add CLI command plumbing.

## Acceptance criteria

- `firn-project` exposes a reusable prepared-package runtime API for DuckDB package replay/checkpoint commit over public `PackageReader`, `StateDelta`, `DestinationCommitRequest`, `Receipt`, and `CheckpointStore` concepts.
- The API rejects non-replayable packages and packages whose manifest hash or segment set cannot cover the supplied `StateDelta`.
- The API accepts explicit target, disposition, merge keys, schema hash, package directory, DuckDB destination, checkpoint store, and complete `StateDelta`; it must not infer `StateDelta`, schema hash, target, disposition, merge keys, scope, or output position from package filenames or package ids.
- The API constructs `DestinationCommitRequest` with `idempotency_token = package_hash` and segment data matching the supplied `StateDelta`.
- The API proposes the supplied `StateDelta` before committing new destination work, and the only successful path to a committed checkpoint is `CheckpointStore::commit(checkpoint_id, receipt)`.
- The DuckDB commit path requires a durable `Receipt` that covers the package hash, schema hash, and every state segment before committing the checkpoint, then verifies that receipt against DuckDB before crossing the firn line.
- The committed-before-checkpointed recovery path can take an already durable DuckDB/package receipt from the package/destination window, verify it, and commit the checkpoint without opening the source.
- If receipt verification fails, receipt identity does not match the delta, or the receipt lacks required segment acknowledgements, no checkpoint is committed.
- If destination commit fails before a durable receipt is returned, the proposed checkpoint is abandoned or otherwise left non-head/non-committed; the behavior must be explicit and tested.
- If checkpoint commit fails after a durable receipt exists, the receipt remains durable for resume/recovery and the API returns an error/report that does not pretend state advanced.
- The API includes a narrow test hook after destination receipt durability/verification and before checkpoint commit so a later chaos harness can exercise the MVP crash window without adding process-kill infrastructure here.
- Package lifecycle status updates use existing `PackageStatus` values to distinguish at least the loading and checkpointed states; exact `loaded` versus `committed` use must follow inspected source/spec authority and be recorded if implementation chooses one.
- Tests cover success, duplicate/idempotent destination replay, committed-before-checkpointed recovery, receipt verification failure, bad package hash, missing segment ack, destination failure before receipt, and checkpoint failure after receipt.

## Evidence expectations

Record focused `cargo fmt --all -- --check`, `cargo test -p firn-project --locked --no-fail-fast`, `cargo test -p firn-dest-duckdb -p firn-state-sqlite --locked --no-fail-fast`, `cargo clippy -p firn-project --all-targets --locked -- -D warnings`, `cargo nextest run -p firn-project --locked`, `cargo check --workspace --all-targets --locked`, and `git diff --check`.

Because this runtime surface is a firn-line invariant, run a bounded mutation test over the new runtime module when feasible, for example `cargo mutants --package firn-project --file crates/firn-project/src/runtime.rs --no-shuffle --jobs 4 --timeout 120 -- --locked`. If mutation tooling is too slow or structurally blocked, record the exact limit and add negative tests for the missed invariant.

Significant closure must follow `QUALITY.md`. Use the reusable CodeQL database path from `.10x/knowledge/quality-gate-execution.md` and parallelize independent checks where practical.

## Explicit exclusions

No live source extraction, no declarative `CompiledResource::open` implementation, no full `firn run` CLI support, no `firn resume` command plumbing, no `firn replay package` CLI plumbing unless the runtime API proves trivially sufficient without semantic invention, no generic destination abstraction, no Parquet/Postgres replay orchestration, no project-file defaults for pipeline/resource/target inference, no chaos harness, no golden-package fixture suite, no package GC retention policy, and no distributed execution.

This ticket intentionally provides the lower-layer primitive needed by later CLI, chaos, and MVP-demo work; those consumers remain separate tickets unless explicitly split and authorized.

## References

- `firn-the-book-of-the-system.md` Chapter 11 lifecycle/crash matrix, Chapter 12 checkpoint firn-line invariant, Chapter 13 receipt verification and replay idempotency, Chapter 17 CLI replay/resume surface, Chapter 19 conformance/chaos/golden gates, and Chapter 22 MVP killer demo.
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/checkpoint-state-firn-line.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/tickets/2026-07-05-cli-surface.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`

## Progress and notes

- 2026-07-06: Split after parent inspection found several blocked surfaces share the same missing lower-layer API: CLI `run`/`resume`/`replay package`, chaos recovery, golden-package replay gates, and the MVP killer demo all need a receipt-verified package-to-checkpoint runtime primitive. Existing source already provides `execute_to_package`, `PackageReader`, DuckDB package commit/verify methods, and receipt-gated `CheckpointStore::commit`.
- 2026-07-06: Read-only explorer Popper recommended narrowing this child from a generic `firn-engine` abstraction to a prepared-package DuckDB/SQLite orchestrator in `firn-project`. Rationale: `firn-project` is the project/orchestration boundary, DuckDB is the MVP local-loop destination with concrete commit/verify APIs, and the public `DestinationProtocol` currently supports planning only, not finalization. The ticket was revised before implementation.
- 2026-07-06: Do not implement in the ticket-creation turn. Assign to a worker in a later turn with the references above and a write boundary of `crates/firn-project/**` plus justified minimal supporting edits.

## Blockers

None for the prepared-package DuckDB/SQLite firn-line runtime foundation.
