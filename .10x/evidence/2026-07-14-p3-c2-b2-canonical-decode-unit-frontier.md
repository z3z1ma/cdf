Status: recorded
Created: 2026-07-14
Updated: 2026-07-14
Relates-To: .10x/tickets/done/2026-07-11-p3-c2-parallel-frontier-execution.md, .10x/tickets/2026-07-11-p3-b2-parquet-codec.md

# Canonical decode-unit frontier evidence

## Observation

Registered format sessions can now execute independent decode units concurrently without moving Parquet identity or transport policy into generic orchestration. A runtime-owned canonical frontier admits a bounded number of lazy child streams, polls each active ordinal at most once ahead of the canonical head, releases batches only in unit/item order, stops admission on a later observed error, and drops all outstanding scoped streams on terminal failure or cancellation.

Decode-unit admission joins the format working-set estimate, two bounded child handoff batches, available managed memory, source useful range concurrency, logical CPU slots, I/O workers, and unit count. A one-unit or one-admitted-job session stays on the direct streaming path and does not pay nested task/channel overhead.

## Procedure and results

The production file-source fixture writes 150,000 rows as three explicit Parquet row groups and executes them through the registered driver and the multi-unit frontier. The generic stalled-head test proves that a two-slot frontier opens only ordinals zero and one, polls the ready later stream exactly once while ordinal zero is blocked, then emits `[0, 10, 11, 20]` and admits ordinal two only after the head completes. A separate later-error test proves ordinal one's error does not overtake ordinal zero and prevents any later opener from running.

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime -p cdf-source-files --lib --locked -j 12
CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime -p cdf-source-files -p cdf-format-parquet --lib --no-deps --locked -j 12 -- -D warnings
cargo fmt --all -- --check
git diff --check
```

The focused suite passed 45 runtime tests plus one ignored benchmark and 46 file-source tests. Strict Clippy, formatting, and diff checks passed.

A release build then ran the cached 2,147,509,487-byte FineWeb Parquet fixture (1,059 row groups, 1,058,640 rows) from local storage through the same project configuration. The preceding prepared-session/range-controller build completed the local file-to-package-to-DuckDB path in 7.27 seconds. The canonical unit frontier completed it in 5.37 seconds (`user 12.63`, `sys 2.70`), a 26.1% wall-time reduction with actual multicore work. It produced 115 canonical segments and a verified DuckDB receipt/checkpoint.

The full DuckDB process reached 1,859,420,160 bytes maximum RSS and 1,862,125,560 bytes peak footprint. To distinguish source/frontier retention from overlapping DuckDB native memory, the same build and source were run to the native Parquet destination. That run completed in 10.36 seconds (`user 14.06`, `sys 3.08`) at 900,513,792 bytes maximum RSS and 943,523,184 bytes peak footprint, materially unchanged from the approximately 909 MiB source/package profile before unit parallelism. The additional DuckDB run footprint is therefore destination-native overlap exposed by a faster upstream, not unbounded row-group retention; it remains below the current 4 GiB process envelope and is still owned by the destination/memory closeout tickets.

The DuckDB run's package-execution wall was 5.008 seconds. Its phase metrics recorded 0.314 seconds of consumer-visible decode blocking, 8.725 cumulative seconds of segment encoding, and 3.100 cumulative seconds of persistence/hash. Those phase durations overlap under the frontier. The decode value is not total decoder CPU: child tasks can decode while the consumer encodes the prior batch. Per-operator wall/CPU attribution under concurrent execution remains owned by P3 J5; this evidence uses process CPU plus end-to-end wall for the scaling claim and does not misstate the blocking metric as a raw-codec roofline.

## What this supports

- The prepared format-session unit boundary is executable, generic, bounded, and lifecycle-owning.
- A stalled canonical head does not permit unbounded later polling or retention.
- Later failures remain canonically ordered and stop speculative admission.
- Production Parquet row groups use the frontier and materially improve the measured wide-file workload.
- The source/package path remains bounded under the measured multi-unit load.

## Limits

This is a C2/B2 milestone, not closure. Unit tasks currently run on injected I/O workers because a decode invocation can interleave asynchronous byte reads and CPU decode; a dedicated I/O/CPU handoff is still required to use every logical core without blocking transport progress. The existing `--jobs` ceiling applies to partition admission but is not yet one shared nested admission authority across partition and format-unit frontiers. Retry/reattest, exact global-limit behavior, jobs=1/N manifest goldens, cancellation chaos, predicate/page-index pruning, and final roofline/envelope evidence remain open.
