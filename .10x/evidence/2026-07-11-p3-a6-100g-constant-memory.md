Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md, .10x/specs/spillable-package-dedup.md

# P3 A6 100 GiB constant-memory stress

## What was observed

The release A6 stress streamed 100 GiB of 8 MiB final-output Arrow binary payload batches through the real `DedupPayloadSpool`, forced the exact-key index onto external runs, finalized first-winner decisions, then read the entire payload and decision streams back in ordinal lockstep.

Results on Apple M5 Pro, arm64 macOS, Rust 1.96.1:

- logical payload: 100 GiB;
- batches/rows: 12,800;
- payload bytes observed through Arrow allocation accounting: 214,753,177,600;
- shared spill peak: 107,380,057,738 bytes;
- managed accounting ceiling/peak: 134,217,728 bytes;
- OS maximum resident set size: 37,240,832 bytes;
- elapsed: 26.16 seconds (`26,048,420,458` ns inside the stress timed region);
- decisions: all 12,800 ordinals retained exactly, no trailing/missing decision;
- page faults/swaps: zero/zero.

## Procedure

1. Built the `cdf-engine` test target in release mode.
2. Selected the already-built test executable directly to exclude Cargo compilation from the RSS measurement.
3. Ran `CDF_A6_STRESS_GIB=100 /usr/bin/time -l <cdf_engine-test> dedup_spill::tests::dedup_payload_constant_memory_stress --ignored --nocapture`.
4. Ran the full post-change suites: `cdf-contract` 77 passed; `cdf-engine` 80 passed, three ignored performance/stress cells; strict clippy passed for both crates.

## What this supports or challenges

This supports constant process memory independent of dedup payload size, shared spill-budget enforcement, external exact-winner operation, full payload replay, ordinal decision joining, and cleanup at a 100 GiB scale. The observed RSS is below both the test's 128 MiB managed ceiling and the program's 2 GiB slow-tier ceiling.

## Limits

The stress uses one wide binary column and all-unique eight-byte keys to force full payload retention/replay without duplicate elision. It is a local NVMe/cache-path result, not a remote filesystem or multi-column decoder benchmark. A5 owns upstream accounted-envelope transfer; this test begins at A6's final-output barrier.
