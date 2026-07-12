Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/2026-07-11-p3-b2-parquet-codec.md

# Parquet driver extraction through the neutral byte-source boundary

## What was observed

The first native codec now exists in the dependency-isolated `cdf-format-parquet` crate. Its `FormatDriver` implementation performs PAR1 detection, footer-only physical schema discovery, deterministic row-group unit planning, exact projection pushdown, bounded-concurrency asynchronous range fetch, and incremental physical Arrow decode without importing filesystem, HTTP, object-store, project, CLI, source-driver, or destination types.

Parquet's async reader receives `bytes::Bytes` whose owner is CDF's `AccountedBytes`. The source memory lease therefore remains live across parser-owned clones and is released only when the parser releases the underlying range buffer. Decoded Arrow batches reserve the shared decode ledger before polling the parser and cross the driver boundary as `AccountedPhysicalBatch`.

## Procedure

- `cargo test -p cdf-format-parquet --lib`
- `cargo clippy -p cdf-format-parquet --all-targets -- -D warnings`
- `cargo test -p cdf-runtime format::tests --lib`
- `cargo clippy -p cdf-runtime --all-targets -- -D warnings`

All commands passed on 2026-07-11. The codec test writes a real Parquet fixture, exposes it only through a mock neutral `ByteSource`, then detects, discovers, plans row groups, decodes, validates row identity, and observes zero residual ledger bytes after release.

## What this supports or challenges

This proves that a parser-local crate can exploit asynchronous ranges and Arrow-native decoding without leaking transport or orchestration types. It also proves that parser-owned zero-copy buffers can retain CDF memory authority through a standard owner-backed `Bytes` value.

## Limits

The production file source has not yet been switched to the registry driver, so the old monolithic Parquet implementation remains temporarily live under the open FX1 migration. Predicate/page-index pushdown, adaptive byte-target feedback, and measured row-group scaling remain B2 work after production composition.
