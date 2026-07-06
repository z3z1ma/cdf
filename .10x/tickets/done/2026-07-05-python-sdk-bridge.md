Status: done
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/done/2026-07-05-http-toolkit.md, .10x/tickets/done/2026-07-05-contract-compiler-normalization.md

# Implement Python SDK and bridge

## Scope

Implement `firn-python` PyO3 bridge, Arrow PyCapsule/C Data Interface ingestion, dict batching, typed `firn-sdk` stubs, Python context APIs for HTTP/secrets/cursor/logger, interpreter resolution checks, GIL/free-threaded execution semantics, watchdogs, and byte-bounded boundary channels. Owns `crates/firn-python/**` and Python SDK files.

## Acceptance criteria

- Python resources can yield dicts or Arrow PyCapsule-speaking objects into kernel batches.
- `firn-sdk` is typed, marked `py.typed`, and example resources are pyright-clean.
- GIL and free-threaded runs over deterministic fixtures produce identical output hashes.
- Free-threaded Python can parallelize allowed resources; GIL builds remain correct.
- Secrets and logs use firn redaction/HTTP tooling rather than raw leakage.

## Evidence expectations

Record Rust/Python integration tests, pyright output, deterministic hash comparisons, and concurrency tests where the local interpreter supports them.

## Explicit exclusions

dlt shim preview is owned by `.10x/tickets/done/2026-07-05-dlt-shim-preview.md`.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-06: Assigned to Python bridge worker. Worker owns `crates/firn-python/**`, Python SDK files if required, its own evidence/review records, and this ticket. Do not touch `.gitignore`, parent ticket, destination crates, or unrelated records.
- 2026-07-06: Implemented `firn-python` dict batching, PyCapsule/C Data Interface boundary import through `pyo3-arrow`, interpreter/free-threaded semantics, watchdogs, byte-bounded boundary queue, deterministic fixture hashes, and redaction-aware context APIs. Added typed `python/firn_sdk` with `py.typed` and a pyright-clean example resource. Initial `arrow-pyarrow`/PyO3 0.28 dependency failed advisory scans; replaced it with `pyo3-arrow`/PyO3 0.29 and scanners passed. Evidence recorded in `.10x/evidence/2026-07-06-python-sdk-bridge.md`; closure review recorded in `.10x/reviews/2026-07-06-python-sdk-bridge-review.md`.
- 2026-07-06: Split the large `crates/firn-python/src/lib.rs` into focused files under `crates/firn-python/src/` while preserving the crate-root API. Organization evidence recorded in `.10x/evidence/2026-07-06-rust-crate-organization-refactor.md`.
- 2026-07-06: Replaced the intermediate `include!` split with ordinary Rust modules under `.10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md`. Final parent quality gates recorded in `.10x/evidence/2026-07-06-project-python-destinations-quality-gates.md`.

## Blockers

None.
