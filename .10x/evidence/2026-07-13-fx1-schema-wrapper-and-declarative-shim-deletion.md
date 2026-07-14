Status: recorded
Created: 2026-07-13
Updated: 2026-07-13

# FX1 schema wrapper and declarative shim deletion

## Observation

The monolithic format crate no longer owns duplicate canonical Arrow hashing or validation-program compilation. Callers consume `cdf-kernel::canonical_arrow_schema_hash` and `cdf-contract::compile_validation_program` directly. The declarative crate's unused local Arrow IPC discovery wrappers were deleted rather than redirected, and both `cdf-declarative` and `cdf-source-files` dropped their `cdf-formats` dependency edges.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo check --locked -j 12 -p cdf-formats -p cdf-declarative -p cdf-source-files -p cdf-python -p cdf-benchmarks --tests`
- `CARGO_BUILD_JOBS=12 cargo test --locked -j 12 -p cdf-formats --lib` — 31 passed.
- Source inventory found no `cdf_formats::schema_hash`, `compile_observed_schema`, `SCHEMA_HASH_PREFIX`, or declarative local-IPC discovery wrapper.
- `cargo fmt --all -- --check`
- `git diff --check`

## What it supports or challenges

This removes competing authority and two build-graph edges while preserving the current codec regression corpus. Canonical hashing remains kernel-owned, contract compilation remains contract-owned, and driver-backed discovery remains the only live file-discovery architecture.

## Limits

`cdf-formats` still owns eager parser functions consumed by benchmarks, REST, Python, subprocess, and property tests. Those consumers must move to their current leaf/runtime boundaries before the crate can be deleted and FX1 can close.
