Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-a4-injected-execution-host.md

# Parquet destination uses injected host I/O

## What was observed

The Parquet destination contains no Tokio dependency, runtime construction, or `block_on` call. Its object-store operations execute through injected `ExecutionServices`; production `cdf run` and executable backfill place those services in destination resolution context.

## Procedure

- `cargo check -p cdf-dest-parquet --tests`
- `cargo check -p cdf-project --all-targets`
- `cargo check -p cdf-cli --all-targets`
- `cargo test -p cdf-dest-parquet --lib` — 27 passed
- `cargo test -p cdf-cli --lib tests::run_parquet_destination_writes_filesystem_root -- --exact` — passed
- `cargo clippy -p cdf-runtime -p cdf-engine -p cdf-dest-parquet -p cdf-project -p cdf-cli --all-targets -- -D warnings` — passed
- `rg -n "tokio|RuntimeBuilder|\\.block_on\\(" crates/cdf-dest-parquet --glob '*.rs' --glob 'Cargo.toml'` — no matches

## What this supports

The generic destination extension boundary can perform async object-store work without importing Tokio or editing scheduler code. Parquet filesystem/object-store commit and receipt verification retain their tested semantics after removing the private runtime.

## Limits

This does not yet remove the private runtime in declarative file transport, migrate replay/resume composition, or prove the complete A4 static gate.
