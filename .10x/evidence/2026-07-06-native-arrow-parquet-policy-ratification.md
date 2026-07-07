Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md, .10x/decisions/native-arrow-datafusion-parquet-policy.md

# Native Arrow/DataFusion Parquet policy ratification evidence

## What was observed

The user explicitly ratified `.10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md` on 2026-07-06 after asking why Firn should keep a DuckDB-backed Parquet workaround instead of using native DataFusion/Arrow Parquet or accepting the specific `RUSTSEC-2024-0436` advisory temporarily.

The ratified decision is recorded in `.10x/decisions/native-arrow-datafusion-parquet-policy.md`.

## Procedure

Inspected:

- `.10x/research/2026-07-06-native-parquet-paste-risk.md`
- `.10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md`
- `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`
- `deny.toml`
- current `Cargo.lock` and crate manifests via prior `rg`/`cargo tree` checks showing current `parquet` and `paste` packages are absent from the locked graph.

The current policy state before implementation remains:

- `deny.toml` has `[advisories] ignore = []`.
- `crates/firn-engine/Cargo.toml` keeps `datafusion = { version = "54.0.0", default-features = false }`.
- `crates/firn-formats`, `crates/firn-package`, and `crates/firn-dest-parquet` currently use DuckDB-backed Parquet paths where Parquet support exists.

## What this supports

This supports closing the shaping ticket `.10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md` and opening implementation tickets for the scoped advisory exception and native Parquet replacement surfaces.

## Limits

This is ratification and ticket-graph evidence only. It does not prove the implementation is complete, does not modify dependency policy files, does not add the native `parquet` crate, and does not replace any DuckDB-backed Parquet source or writer behavior.
