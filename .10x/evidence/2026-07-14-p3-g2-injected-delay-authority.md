Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 G2 injected delay authority

## Observation

`ExecutionHost` now owns cancellable asynchronous delays. `ExecutionServices::delay` delegates to that host authority, and the standalone host schedules the timer on its single owned Tokio I/O runtime. A retry controller therefore does not need a transport-local runtime, a blocking sleep, or an ambient Tokio context.

This slice deliberately adds no new retry count, deadline, or backoff default. It supplies the missing execution primitive needed by the already-governed exact-range retry and throttling work.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo check -p cdf-runtime -p cdf-engine -p cdf-source-files -p cdf-source-rest --all-targets --locked`
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine execution_delay_uses_the_io_runtime_and_honors_cancellation --locked`
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime --lib --locked`

All commands passed. The engine regression proves a normal timer completes when its future is polled outside Tokio and a pre-cancelled 60-second delay returns immediately. The runtime suite passed 48 tests with one explicit performance benchmark ignored.

## What this supports or challenges

Supports `.10x/specs/remote-local-io-overlap.md`: provider backoff can honor retry headers through an injected timer and cancellation can stop a pending delay promptly. It also exposes the existing REST loop's ignored delay as legacy behavior to remove when retry policy authority is migrated.

## Limits

No retry policy or retry execution was added. Delay duration, attempt limits, wall deadline, jitter, retry telemetry, and source/run budget composition remain open.
