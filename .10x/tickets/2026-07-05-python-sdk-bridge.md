Status: open
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

dlt shim preview is owned by `.10x/tickets/2026-07-05-dlt-shim-preview.md`.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.
