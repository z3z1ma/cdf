Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md

# File transport uses injected host I/O

## What was observed

`FileTransportFacade` no longer owns a global `OnceLock<Tokio Runtime>` or any runtime dependency. Object-store list, metadata, and exact ranged reads use injected neutral `ExecutionServices`; production run, preview, plan/explain, and executable backfill propagate the composition-root services through discovery and file-resource construction.

## Procedure

- `cargo check -p cdf-declarative --all-targets`
- `cargo check -p cdf-cli --all-targets`
- focused object-store facade list/head/range test — passed
- focused recursive object-store glob partition test — passed
- focused CLI Parquet destination run — passed
- `cargo clippy -p cdf-runtime -p cdf-engine -p cdf-declarative -p cdf-project -p cdf-cli --all-targets -- -D warnings` — passed
- static search for Tokio runtime builders, runtime singletons, and `block_on` in the production file-transport module — no production matches

## What this supports

Source and destination object-store extensions now share one host-neutral execution mechanism. Adding another transport does not require a private runtime or scheduler branch, and embedding controls the actual executor.

## Limits

Add/schema/deep-validate/doctor composition still needs explicit service propagation for object-store operations. Test-only `futures_executor` setup remains outside the production static gate.
