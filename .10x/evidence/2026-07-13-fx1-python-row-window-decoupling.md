Status: recorded
Created: 2026-07-13
Updated: 2026-07-13

# FX1 Python row-window decoupling

## Observation

`cdf-python` no longer depends on `cdf-formats`, its closed `FormatRead`, or its NDJSON/Arrow IPC convenience readers. Dict fallback rows now flush at the configured row window while iterating, enforce the configured boundary-byte ceiling before allocating the NDJSON conversion buffer, decode through Python's owned Arrow JSON dependency, and enter the existing Arrow batch path. The prior implementation accumulated the entire Python iterable before its first flush.

The Arrow IPC round-trip test now uses Arrow's stream reader directly and no longer keeps a production compatibility reader alive solely for tests.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo check --locked -j 12 -p cdf-python --tests`
- `CARGO_BUILD_JOBS=12 cargo test --locked -j 12 -p cdf-python --lib` — 21 passed.
- Source inventory found no `cdf_formats`, `cdf-formats`, `FormatRead`, `JsonOptions`, or `read_ndjson_bytes` in `cdf-python`.
- `cargo fmt --all -- --check`
- `git diff --check`

## What it supports or challenges

This removes another production parser-facade consumer and eliminates whole-iterable dict accumulation. It preserves batch/schema identity and Arrow capsule behavior while making row-fallback buffering explicitly bounded by both rows and bytes.

## Limits

This does not close P3 H2: `PythonBatchRead` and Arrow C stream import still collect batches, row conversion is not yet ledger-accounted, and real zero-copy/lane/cancellation evidence remains open under `.10x/tickets/2026-07-11-p3-h2-python-incremental-arrow-boundary.md`.
