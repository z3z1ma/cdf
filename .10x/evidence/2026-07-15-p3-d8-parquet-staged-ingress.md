Status: recorded
Created: 2026-07-15
Updated: 2026-07-15

# P3 D8 bounded staged Parquet ingress

## Observation

The exact four-partition FineWeb control that previously completed in 40.67 seconds with 33.069 seconds of serialized finalized-package Parquet work now completes in 19.04 seconds. This is a 53.2% end-to-end wall reduction. A jobs=8 falsification run completed in 19.29 seconds, so raising the shared job ceiling above the four source partitions did not improve this cell.

The run processed 8,590,037,948 source bytes and 4,234,560 rows into 460 canonical package segments, wrote an 8,820,812,024-byte package and 14,371,507,332 bytes of final Parquet objects, verified the receipt, and committed the checkpoint. Final binding plus receipt publication took 0.243 seconds rather than the old 33.069-second serialized transcode/verification phase. The 17.906-second destination-ingress interval overlaps the 18.471-second package-execution wall instead of following it.

Output-only destination throughput was 765.4 MiB/s: 0.404x the contemporaneous 1,896.6 MiB/s durable raw-write roofline and 0.481x the 1,591.8 MiB/s direct Arrow-to-Parquet writer roofline. A separate median-of-three staged replay benchmark, which includes IPC segment reopen, four 32 MiB object encodes, durable publication, manifest/receipt binding, and cleanup but excludes package construction, measured 642.0 MiB/s against 1,727.2 MiB/s durable raw write (0.372x). The direct single-object writer measured 0.839x raw write. These output-only ratios supersede the rejected mixed-path 0.639 claim; source/package/destination byte sums are not a valid destination roofline.

Peak RSS was 1,281,064,960 bytes, below the 4 GiB managed authority and the earlier jobs=4 control's 1,534,377,984 bytes. No attempt-scoped staging objects remained after success.

## Procedure

1. Built the release CLI from the D8 staged-ingress worktree with `CARGO_BUILD_JOBS=12 cargo build --release -p cdf-cli --locked -j12`.
2. Used the existing pinned project `/Users/alexanderbut/code_projects/tmp/cdf-c4-scale`, whose four paths are hard links to the 2,147,509,487-byte FineWeb Parquet fixture. The destination remained `parquet://.cdf/destination`.
3. Removed only generated state, package, and destination output from the preceding benchmark cell.
4. Ran `/usr/bin/time -lp target/release/cdf run fineweb.documents --jobs 4 --progress never --color never` and inspected run `run-00145d9006862f6e398981e4993a4c5c` plus its durable phase metrics, destination manifest, receipt, checkpoint, and staging namespace.
5. Repeated the exact cell at jobs=8 as run `run-bea2028b4832add79b26836a6fc1fe6d` to test whether the jobs=4 ceiling was the remaining limit.
6. Ran the release-only median benchmarks `local_streaming_parquet_reaches_sixty_percent_of_write_roofline` and `local_staged_parquet_ingress_reports_isolated_write_roofline` on the same host and filesystem.

The exact confirmation recorded:

| Metric | Observation |
|---|---:|
| wall / user / system | 19.04 / 48.84 / 30.86 s |
| peak RSS | 1,281,064,960 B |
| package execution | 18.471 s |
| destination ingress, overlapped | 17.906 s |
| final binding + receipt | 0.243 s |
| source/package/destination bytes | 8.590 / 8.821 / 14.372 GB |
| destination output / ingress wall | 765.4 MiB/s |
| destination / direct Parquet writer | 0.481x |
| isolated staged replay / raw write | 0.372x |
| direct Parquet writer / raw write | 0.839x |

The implementation was falsified before this successful cell. Immediate failure originally hid the destination error behind a closed worker channel; the generic background staging boundary now preserves the exact worker error. A nonblocking writer reservation then failed under legitimate transient pressure; it now waits on the shared memory coordinator. Retaining every local staged output until final binding exhausted the 8 GiB spill budget, so local and remote writers now install each completed object into isolated destination-owned attempt staging and release CDF spill immediately. Local final binding promotes with create-only hard links and one batched directory durability barrier; object stores use create-only server-side copy. A redundant post-commit reread and SHA-256 of all 14.37 GB was replaced by generic commit-bound receipt verification over hash-while-write evidence, create-only publication, durability barriers, and the exact manifest. Duplicate and recovered commits still take the independent verification path.

Two tempting tunings were measured and removed. Four concurrent Parquet writers completed in 21.18 seconds versus 20.63 seconds with two, so the declared useful writer bound remains two. Snappy output compression completed in 30.78 seconds and consumed materially more CPU, so the destination remains uncompressed until a workload-aware, plan-recorded codec policy has evidence. Raising the complete-run ceiling from jobs=4 to jobs=8 also failed to improve wall (19.04 versus 19.29 seconds). None of the rejected tunings remains in source.

The selected `arrow_ipc_to_parquet@2` physical plan now records its exact two-writer and row/byte batch settings in attempt metadata before staging mutation. Segment staging and final object keys are deterministic applications of its versioned key policy; final package-token keys are derived only after verified final binding rather than replanned. Successful/aborted attempts clean immediately, active attempts heartbeat at most once per minute, and attempt prefixes older than seven days are collected without touching an in-process active sibling.

## What it supports or challenges

- It supports D8's architectural claim: Parquet is a generic staged destination, the expensive phase overlaps package production, final binding is subsecond, memory remains bounded, and the old serialized 33.069-second post-package phase is gone.
- It falsifies the earlier throughput closure claim. The complete staged destination path does not reach 60% of either raw durable write or the direct Arrow writer. The direct writer itself is healthy at 0.839x raw write; the remaining fixed cost is in the many-object staged composition, not an Arrow codec or device ceiling. The ticket must not close on throughput unless fresh review accepts a narrower external-limit interpretation or the object/durability composition is improved.
- Adapter tests prove new commits carry exact commit-bound verification while duplicates require independent verification. Local and in-memory object-store abort tests prove attempt staging and unpublished final objects are removed; successful tests cover duplicate replay, deterministic multi-segment order, replace, tamper detection, and correction behavior.
- Full `cdf-project` and `cdf-runtime` library suites preserve checkpoint-gate and recovery semantics, including independent receipt verification after source loss.
- It challenges compression-as-an-automatic-speedup for wide text: Snappy reduced bytes but was CPU-negative on this host and workload.

## Limits

The source files are APFS hard links with warm-cache bias. The 642 MiB/s staged replay cell reopens an already-built package and therefore includes IPC decode that the live overlap path may avoid; conversely, its four 32 MiB objects underrepresent the FineWeb run's 460-object durability frequency. The direct writer's one 128 MiB object does not measure per-object publication overhead. The object-store tests use the in-memory implementation; live multipart/provider failure injection remains broader destination-conformance work unless fresh review finds a D8-specific correctness gap. The benchmark proves bounded memory and spill for this 8.59 GB input, not the program's separate 100 GB/1 TB constant-memory law.
