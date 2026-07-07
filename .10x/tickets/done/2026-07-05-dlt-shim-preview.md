Status: done
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-python-sdk-bridge.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md

# Implement dlt shim preview

## Scope

Implement preview support for running feasible `@dlt.resource` and `@dlt.source` functions through cdf's Python bridge, mapping dlt hints and state to cdf descriptors, contracts, and ledger views. Owns dlt-specific modules under Python bridge/SDK areas.

## Acceptance criteria

- dlt primary key, merge key, incremental, write disposition, and contract-mode hints map to cdf descriptors/contracts where feasible.
- `dlt.current.state` maps to a scoped ledger-backed state view.
- Divergences from dlt behavior are documented as migration-table data or generated docs.
- Shim output is planned, packaged, and checkpointed like native cdf resources.

## Evidence expectations

Record integration tests with representative dlt resources and mapping snapshots.

## Explicit exclusions

Bug-for-bug dlt emulation and dlt destination delegation are excluded.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-06: Assigned to dlt shim worker. Worker owns dlt-specific modules under `crates/cdf-python/**` and `python/cdf_sdk/**`, its own evidence/review records, and may update `Cargo.lock` only if a minimal dlt-shim dependency is truly required. Do not touch `.gitignore`, CLI, destination crates, parent ticket, or unrelated records.
- 2026-07-06: Implemented the scoped preview shim in `crates/cdf-python/src/dlt.rs`, wired `PythonResourceBridge::batches_from_dlt_resource` / `batches_from_dlt_source`, added typed `python/cdf_sdk/dlt.py`, and added representative Rust fixture tests for descriptor hint mapping, source expansion, migration-table divergence data, and committed-head state views. Python checks and scoped formatting checks pass. Integrated Rust gates are blocked before reaching `cdf-python` by parallel out-of-scope split work. Evidence recorded in `.10x/evidence/2026-07-06-dlt-shim-preview.md`.
- 2026-07-06: Rechecked after the parallel workspace split blockers cleared. Fixed the dlt bridge lifetime and made Rust dlt fixtures self-contained instead of importing `cdf_sdk` from embedded Python. `cargo fmt --all -- --check`, `cargo check --workspace --all-targets --locked`, `cargo test -p cdf-python --locked --no-fail-fast`, `cargo clippy -p cdf-python --all-targets --locked -- -D warnings`, `python3 -m compileall -q python/cdf_sdk python/examples`, and `uvx pyright python/cdf_sdk python/examples` pass. Evidence updated in `.10x/evidence/2026-07-06-dlt-shim-preview.md`; closure review recorded in `.10x/reviews/2026-07-06-dlt-shim-preview-review.md`.

## Blockers

None.
