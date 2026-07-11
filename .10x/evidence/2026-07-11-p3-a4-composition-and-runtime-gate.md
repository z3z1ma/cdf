Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-a4-injected-execution-host.md

# Composition-root propagation and runtime ownership gate

## What was observed

CLI composition now supplies execution services to add, validate/deep validation, plan/explain, run, preview, schema operations/promotion, state recovery, resume, replay, executable backfill, and doctor. Remote file discovery/diagnostics and Parquet recovery therefore use the same host as ordinary runs.

The one non-I/O production `futures_executor` use—waiting for a discovery memory lease on a dedicated worker—was replaced by `reserve_blocking`, which parks the worker and uses the coordinator's waker contract without creating or entering an async runtime.

A permanent source-architecture test scans production Rust modules and rejects Tokio runtime builders, private `block_on`, futures-executor bridges, and runtime singletons outside the standalone host.

## Procedure

- `cargo check -p cdf-cli --all-targets` — passed
- `cargo clippy -p cdf-memory -p cdf-project -p cdf-cli --all-targets -- -D warnings` — passed
- blocking memory reservation wake/release test — passed
- focused add, schema discover, deep validate, doctor remote transport, and replay rendering tests — passed
- `standalone_host::tests::production_runtime_ownership_is_centralized` — passed

## What this supports

Runtime ownership is a composition concern rather than source/destination/command implementation detail. Memory backpressure on worker threads does not require an executor. Future private-runtime regressions have a permanent fast test owner.

## Limits

Blocking/FFI destination operations are not yet submitted through declared host lanes, and the source/destination requirement join is not yet complete. Those remain A4 acceptance work.
