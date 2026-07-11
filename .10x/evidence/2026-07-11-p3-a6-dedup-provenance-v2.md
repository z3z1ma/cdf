Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-a6-spillable-package-dedup.md, .10x/decisions/spillable-package-order-dedup.md

# Dedup provenance artifact v2 migration

## What was observed

New package writers emit dedup summary version 2. The summary contains bounded counts, rule/key/keep authority, `parquet` provenance format/version, the fixed 65,536-row shard target, and deterministic shard identities. It no longer embeds dropped-row cardinality in JSON. Dropped/kept `uint64` ordinal pairs are written in strict dropped-row order to `stats/dedup-dropped/part-<ordinal>.parquet`, and every shard participates in manifest identity.

The package reader accepts both legacy inline v1 summaries and v2 shards. V2 reads reject paths outside the provenance directory, shards missing from package identity, wrong/missing UInt64 columns, null ordinals, or noncanonical cross-shard order.

## Procedure

- `cargo test -p cdf-package --lib dedup -- --nocapture` — passed legacy inline-v1 read/identity/tamper behavior.
- `cargo test -p cdf-engine --lib -- --nocapture` — 70 passed, including first/last/fail, exact-row, rechunking identity, reference-versus-spill package hash, and v2 shard assertions.
- `cargo clippy -p cdf-package -p cdf-engine --all-targets -- -D warnings` — passed.

## What this supports

Detailed dedup evidence no longer grows resident memory or one JSON value with package cardinality. The migration implements the active v2 artifact decision while preserving inspection/replay compatibility for existing v1 packages.

## Limits

The v2 reader is an inspection compatibility helper and materializes requested provenance. Production replay never reevaluates or needs dropped-row provenance. A6 still requires generated type/equality properties, accounted sort memory, algorithm/crossover measurement, and large forced-spill stress.
