Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# Parquet row groups publish complete byte envelopes

## Observation

Prepared Parquet decode units now publish the conservative byte interval containing every column chunk in that row group. The neutral runtime can derive a monotone no-lookback frontier after each canonical unit: the minimum start of all unfinished unit envelopes, or the maximum terminal end after the last unit. If any unit lacks a complete envelope, the proof returns `None` rather than guessing.

This creates no source- or Parquet-specific branch in orchestration and changes no decoded bytes, ordering, package identity, or spool policy. It establishes the codec proof required before a bounded progressively evicting spool can reclaim earlier source regions.

## Procedure

The eight-row-group Parquet fixture clears its source range trace after the one prepared footer load. For each decode unit it then requires at least one request and proves every request is contained by that unit's declared byte envelope. It also derives all eight no-lookback frontiers and requires monotonicity. The runtime unit test covers overlapping and physically separated envelopes, a missing-envelope fail-closed result, and rejection of noncanonical ordinals.

The row-group calculation does not call Arrow's asserting `ColumnChunkMetaData::byte_range()`: corrupt negative page offsets or compressed sizes are converted with checked fallible operations and return a data error.

Commands and results:

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime --locked`: 49 passed, 1 ignored performance benchmark.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-format-parquet --locked`: 3 passed.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime -p cdf-format-parquet --all-targets --no-deps --locked -- -D warnings`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.

## What this supports or challenges

This supports `.10x/specs/remote-local-io-overlap.md`'s requirement that progressive eviction be authorized by a codec-session monotone no-lookback frontier. It also makes `DecodeUnitPlan.extent` an enforceable complete-read-envelope contract rather than an advisory locality hint.

## Limits

No spool region is evicted in this slice. The current growing spool still reserves and retains a complete admitted finite object. Parquet predicate/page-index pushdown is unsupported, so future index reads must expand the declared unit envelope or suppress the eviction proof. External format drivers remain responsible for conformance to the same complete-envelope contract.
