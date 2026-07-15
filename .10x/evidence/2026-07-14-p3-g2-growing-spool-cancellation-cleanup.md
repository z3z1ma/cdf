Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# Growing-spool cancellation releases every bounded resource

## Observation

Cancellation now interrupts a spool producer even while its upstream provider future is pending and does not cooperatively poll CDF's cancellation token. The same cancellation wakes a decoder waiting for an unpublished local range. After both futures terminate and the retained source/session owners are dropped, the source-memory ledger, complete finite-object disk reservation, and temporary spool file are all released.

The same cancellation-aware wait now protects the finalized spool path. This is runtime behavior, not HTTP, Parquet, or destination-specific orchestration.

## Procedure

The permanent `growing_spool_cancellation_releases_blocked_io_memory_disk_and_file` chaos test uses a strong 96-byte source that publishes one 32-byte chunk and then blocks forever on a provider semaphore without observing cancellation. It:

1. admits the entire 96-byte object against the shared spill budget and proves the temporary file exists;
2. reads a published prefix and starts a decoder read against an unpublished middle range;
3. proves both producer and reader are pending;
4. cancels the run and requires both futures to return the cancellation error;
5. proves the reservation remains owned while the session still retains the spool; and
6. drops the source and retention authorities, then requires the file to be absent and both ledgers to report zero current bytes.

Commands and results:

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files growing_spool --locked`: 3 passed, including the cancellation chaos law.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime --locked`: 49 passed, 1 ignored performance benchmark.
- `CARGO_BUILD_JOBS=12 cargo check -p cdf-source-files --all-targets --locked`: passed.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime -p cdf-source-files --all-targets --no-deps --locked -- -D warnings`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.

## What this supports or challenges

This supports `.10x/specs/remote-local-io-overlap.md`'s bounded spool, cleanup, and cancellation requirements. It also challenges the prior assumption that checking cancellation between stream items was sufficient: a pending provider future could otherwise retain the full disk admission indefinitely. `RunCancellation::await_or_cancel` is now the neutral runtime boundary that drops such a future promptly.

## Limits

This deterministic test covers cancellation during a stalled provider body and a blocked growing-spool reader. It does not establish live HTTP/cloud socket teardown latency, cancellation while the OS is blocked in a file write, partial-run source-I/O telemetry, progressive prefix eviction, or rolling-spool checkpoint eviction. Those remain open in G2/G1 conformance.
