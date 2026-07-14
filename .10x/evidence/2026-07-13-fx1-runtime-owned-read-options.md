Status: recorded
Created: 2026-07-13
Updated: 2026-07-13

# FX1 runtime-owned format read options

## Observation

The shared resource/partition/batch identity and batch-row configuration used by format adapters now lives in `cdf-runtime`, the neutral format execution boundary. Production sources, Python/subprocess adapters, conformance, and benchmarks no longer import this runtime contract from the legacy parser aggregation crate. `cdf-formats` consumes the neutral type while its remaining parser surfaces are migrated and deleted.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo check -p cdf-runtime -p cdf-formats -p cdf-source-files -p cdf-source-rest -p cdf-subprocess -p cdf-python -p cdf-conformance -p cdf-benchmarks -j12`
- `CARGO_BUILD_JOBS=12 cargo test --locked -j 12 -p cdf-runtime -p cdf-formats -p cdf-source-files -p cdf-source-rest -p cdf-subprocess --lib`
- `CARGO_BUILD_JOBS=12 cargo test --locked -j 12 -p cdf-runtime read_options_derive_stable_batch_identity_and_reject_invalid_overrides`
- `cargo fmt --all -- --check`
- `git diff --check`

## What it supports or challenges

This supports FX1's parser-local build-domain criterion by removing one cross-cutting execution contract from `cdf-formats`. The focused suites preserve default 64-K-row batching, stable sanitized batch identity, override validation, source behavior, and protocol adapters.

## Limits

This is a bounded dependency move, not deletion of `cdf-formats`. Its eager `FormatRead`, closed `FileFormat`, schema wrapper, and parser functions remain closure blockers and are the next migration slices.
