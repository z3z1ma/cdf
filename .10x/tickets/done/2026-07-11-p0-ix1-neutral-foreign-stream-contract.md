Status: done
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/specs/foreign-stream-interop.md

# P0 IX1: neutral foreign stream contract

## Scope

Add executor-neutral foreign producer descriptors, incremental outcome/control/terminal stream contract, transfer/copy/lane/memory/cancellation capabilities, conformance mocks, and adapters into the shared source/runtime graph without changing Python/subprocess behavior yet.

## Acceptance criteria

- Contract crate exposes no PyO3/Tokio/process/Wasmtime/DataFusion/CLI types.
- Mock C-data, IPC, and row producers traverse ordinary schema/runtime/package paths incrementally.
- Architecture gates prevent concrete-tier branching and eager collection APIs in generic production runtime.
- Memory/cancellation/copy semantics are structurally declared and conformance-readable.

## Evidence expectations

Dependency graph, API/static checks, mock conformance, compile/test matrix, migration/adaptation notes, and adversarial extension review.

## Explicit exclusions

No Python/subprocess migration, Wasmtime host, or performance claim.

## Blockers

None. SX1 and DX1 are done; this ticket is executable.

## References

- `.10x/decisions/neutral-foreign-stream-boundary.md`
- `.10x/specs/foreign-stream-interop.md`

## Journal

- 2026-07-18 — Activated IX1 after SA4 was blocked on the absence of this neutral interop seam.
- 2026-07-18 — Added `crates/cdf-foreign-stream` as the executor-neutral contract crate. It depends only on `cdf-kernel`, `futures-core`/`futures-util`, and `serde`; it deliberately does not depend on `cdf-runtime`, Python, subprocess, Wasmtime, DataFusion, or CLI crates.
- 2026-07-18 — Modeled `ForeignProducerDescriptor`, transfer modes (`arrow_c_data`, `arrow_ipc_stream`, `row_compat`), lane/backpressure/startup declarations, memory/cancellation/state/security contracts, copy classification, outcome/control/terminal events, a neutral cancellation token, and `batch_stream_from_foreign_events` for feeding ordinary `BatchStream` consumers incrementally.
- 2026-07-18 — Re-exported the contract under `cdf_runtime::foreign` so adapters can consume the neutral boundary through the shared runtime surface without creating concrete-tier branches.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-foreign-stream --lib --locked -j 12` — passed. Covers descriptor validation, C Data/IPC/row mock events traversing as incremental batches, package segment writing from the mock stream without whole-stream collection, no eager collection before first output, exactly-one-terminal enforcement, and static guards against concrete runtime dependencies/eager batch collection APIs inside the contract crate.
- `CARGO_BUILD_JOBS=12 cargo check -p cdf-runtime -p cdf-python -p cdf-subprocess --locked -j 12` — passed. Confirms the runtime re-export and existing Python/subprocess crates compile with the new neutral contract available.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-foreign-stream -p cdf-runtime -p cdf-python -p cdf-subprocess --all-targets --locked -j 12 -- -D warnings` — passed. Confirms the focused cone remains warning-clean; the only large enum concern is intentionally documented to avoid adding a heap allocation per foreign batch.
- `CARGO_BUILD_JOBS=12 cargo fmt --all -- --check` and `git diff --check` — passed.

## Review

Pass. Adversarial read checked for the failure modes IX1 was intended to prevent: no production dependency from the neutral contract to Python/subprocess/runtime/Wasmtime/DataFusion/CLI crates; no concrete-host branch in `cdf-runtime`; no public `Vec<Batch>`/`Vec<RecordBatch>` boundary in the new contract; and no per-batch boxing added to satisfy clippy. Residual risk is migration risk only: Python and subprocess still have eager paths, but H2/H3 own replacing those paths and IX1 explicitly excluded adapter migration.

## Retrospective

The important design move was keeping the contract crate below `cdf-runtime` rather than stuffing foreign descriptors into Python or subprocess. That avoided a source-specific workaround for SA4 and gives H1/H2/H3/H4 one shared vocabulary for transfer mode, copy proof, cancellation, state, and memory. The package-path proof needed a dev-only `cdf-package` dependency; keeping it out of production preserves the clean dependency graph while still proving the stream can reach durable package evidence.
