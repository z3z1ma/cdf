Status: recorded
Created: 2026-07-15
Updated: 2026-07-15

# P3 D8 bounded staged Parquet ingress

## Observation

The exact four-partition FineWeb control that previously completed in 40.67 seconds with 33.069 seconds of serialized finalized-package Parquet work now completes in 21.72 seconds. This is a 46.6% end-to-end wall reduction. A same-source immediately preceding repetition completed in 20.63 seconds; 21.72 seconds is retained as the conservative exact-source confirmation.

The run processed 8,590,037,948 source bytes and 4,234,560 rows into 460 canonical package segments, wrote an 8,820,812,024-byte package and 14,371,507,332 bytes of final Parquet objects, verified the receipt, and committed the checkpoint. Final binding plus receipt publication took 0.251 seconds rather than the old 33.069-second serialized transcode/verification phase. The 19.758-second destination-ingress interval overlaps the 20.207-second package-execution wall instead of following it.

The combined local data path moved 31,782,357,304 bytes in 21.72 seconds, or 1.463 GB/s. Against the same host's recorded 2.291 GB/s warm sequential-write roofline, this mixed read/two-write path reaches 0.639x. Peak RSS was 1,123,647,488 bytes, below both the 4 GiB managed authority and the earlier jobs=4 control's 1,534,377,984 bytes. No attempt-scoped staging objects remained after success.

## Procedure

1. Built the release CLI from the D8 staged-ingress worktree with `CARGO_BUILD_JOBS=12 cargo build --release -p cdf-cli --locked -j12`.
2. Used the existing pinned project `/Users/alexanderbut/code_projects/tmp/cdf-c4-scale`, whose four paths are hard links to the 2,147,509,487-byte FineWeb Parquet fixture. The destination remained `parquet://.cdf/destination`.
3. Removed only generated state, package, and destination output from the preceding benchmark cell.
4. Ran `/usr/bin/time -lp target/release/cdf run fineweb.documents --jobs 4 --quiet --progress never --color never`.
5. Inspected run `run-f1d5588bc9dde66b9f6ed52c139e6c9d`, its durable phase metrics, destination manifest, receipt, checkpoint, and staging namespace.

The exact confirmation recorded:

| Metric | Observation |
|---|---:|
| wall / user / system | 21.72 / 50.47 / 33.59 s |
| peak RSS | 1,123,647,488 B |
| package execution | 20.207 s |
| destination ingress, overlapped | 19.758 s |
| final binding + receipt | 0.251 s |
| source/package/destination bytes | 8.590 / 8.821 / 14.372 GB |
| aggregate bytes / wall | 1.463 GB/s |
| aggregate / recorded device roofline | 0.639x |

The implementation was falsified before this successful cell. Immediate failure originally hid the destination error behind a closed worker channel; the generic background staging boundary now preserves the exact worker error. A nonblocking writer reservation then failed under legitimate transient pressure; it now waits on the shared memory coordinator. Retaining every local staged output until final binding exhausted the 8 GiB spill budget, so local and remote writers now install each completed object into isolated destination-owned attempt staging and release CDF spill immediately. Local final binding promotes with create-only hard links and one batched directory durability barrier; object stores use create-only server-side copy. A redundant post-commit reread and SHA-256 of all 14.37 GB was replaced by generic commit-bound receipt verification over hash-while-write evidence, create-only publication, durability barriers, and the exact manifest. Duplicate and recovered commits still take the independent verification path.

Two tempting tunings were measured and removed. Four concurrent Parquet writers completed in 21.18 seconds versus 20.63 seconds with two, so the declared useful writer bound remains two. Snappy output compression completed in 30.78 seconds and consumed materially more CPU, so the destination remains uncompressed until a workload-aware, plan-recorded codec policy has evidence. Neither rejected path remains in source.

## What it supports or challenges

- It supports D8's core claim: Parquet is a generic staged destination, the expensive phase overlaps package production, final binding is subsecond, memory remains bounded, and the complete path exceeds 60% of the named local device roofline under the same aggregate accounting used by the C4 control.
- Adapter tests prove new commits carry exact commit-bound verification while duplicates require independent verification. Local and in-memory object-store abort tests prove attempt staging and unpublished final objects are removed; successful tests cover duplicate replay, deterministic multi-segment order, replace, tamper detection, and correction behavior.
- Full `cdf-project` and `cdf-runtime` library suites preserve checkpoint-gate and recovery semantics, including independent receipt verification after source loss.
- It challenges compression-as-an-automatic-speedup for wide text: Snappy reduced bytes but was CPU-negative on this host and workload.

## Limits

The 0.639 roofline ratio is deliberately labeled as a mixed-path aggregate: source reads, package writes, and destination writes are counted against a sequential-write reference. It is useful for before/after continuity with C4 but is not an isolated destination-codec roofline. The source files are APFS hard links with warm-cache bias. The object-store tests use the in-memory implementation; live multipart/provider failure injection remains broader destination-conformance work unless fresh review finds a D8-specific correctness gap. The benchmark proves bounded memory and spill for this 8.59 GB input, not the program's separate 100 GB/1 TB constant-memory law.
