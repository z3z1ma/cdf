Status: recorded
Created: 2026-07-14
Updated: 2026-07-14
Relates-To: .10x/tickets/done/2026-07-11-p3-f2-materialization-closure-audit.md, .10x/specs/constant-memory-proof.md, .10x/specs/destination-bulk-path-runtime.md

# DuckDB native resource envelope on wide FineWeb Parquet

## Observation

The same local 2,147,509,487-byte FineWeb Parquet object (1,058,640 rows, 1,059 row groups, 115 canonical CDF segments) isolated DuckDB's package-long native transaction as the dominant unaccounted allocation owner. The pre-change CDF-to-DuckDB run completed in 6.24 seconds but reached 3,248,835,536 bytes of peak footprint. The identical source/package path to the streaming Parquet destination reached 962,921,888 bytes, so roughly 2.29 GiB of the excess belonged to DuckDB rather than source decode, canonical package segments, or the growing remote spool.

DuckDB connections now use an adapter-owned native envelope: one native thread, `preserve_insertion_order=false`, a memory limit derived as one quarter of the execution host's managed pool clamped to 256 MiB–1 GiB, and at most 1 GiB of temporary-directory scratch. Generic project resolution supplies the execution services; the adapter reserves its entire scratch ceiling from the shared spill coordinator before source work and shares that reservation across destination clones until the runtime is dropped. The output database and WAL remain destination storage and are not misclassified as temporary spill.

With the ordinary default host, the derived memory cap was approximately 870 MiB. A fresh local CDF-to-DuckDB run completed in 7.27 seconds with 1,385,006,712 bytes of peak footprint and a verified receipt/checkpoint. That is a 57.4% footprint reduction versus the uncapped run while retaining 85.8% of its wall throughput. The immediately preceding equivalent final implementation run measured 7.10 seconds and 1,422,673,576 bytes, demonstrating normal host variance rather than input-size-dependent growth.

The remote end-to-end control completed the same HTTPS FineWeb object in 18.54 seconds with 1,623,410,728 bytes of peak footprint. A sequential `curl` transfer immediately afterward took 15.31 seconds, so the fully governed source-to-package-to-DuckDB run was 1.21x the contemporaneous network floor. The earlier uncapped remote run took 16.21 seconds while reaching approximately 3.09 GB RSS. The bounded path therefore remains network-dominated while cutting the native footprint roughly in half.

## Procedure

- Raw Arrow reference decode on the local object measured 3.33 seconds/57,524,224 bytes RSS at 1,024-row batches, 3.28 seconds/1,194,033,152 bytes at 8,192 rows, and 3.17 seconds/2,492,039,168 bytes at 65,536 rows. This falsified a throughput justification for retaining very wide 64k text batches.
- A DuckDB CLI control with `memory_limit='256MiB'`, `threads=1`, and bounded temp storage completed in 7.22 seconds and 175,260,464 bytes peak footprint. The same 256 MiB cap under DuckDB's 18-thread default failed at 257.5 MiB. A 512 MiB/four-thread CDF trial was both slower and larger than the retained single-thread policy.
- Fresh CDF runs deleted only the dedicated benchmark environment's database, state, and package directories before execution. The retained run id was `run-36cc1123c65262bffa390b09d93b85ba`; the remote run id was `run-1cd5ffe5dfb9ddb763c892ebb96ae140`.
- `cargo test -p cdf-dest-duckdb --lib --locked -j 12` passed 27 tests with the one explicit performance benchmark ignored. Focused tests prove live DuckDB settings, shared scratch reservation lifetime across adapter clones, release on final drop, and clean failure before use when scratch capacity is unavailable.
- The destination catalog fixture now truthfully declares external staging and the `p3-f2-2026-07-14-v2` measured path evidence.

## What this supports

This closes the measured DuckDB native-allocation defect without adding a destination branch to generic orchestration. The destination adapter owns its native memory/thread/temp policy; the common resolution context supplies host and spill authority; the existing capability sheet tells orchestration that the path uses external staging. Receipt, compact provenance, transaction, package identity, and checkpoint-gate semantics remain unchanged.

## Limits

This evidence is one finite wide-text object on macOS. `time -l` peak footprint and RSS include different mapped/file-backed effects and are reported separately from the managed-memory ledger. F1 still owns exact process/native-headroom authority and cross-host calibration. F2 still owns the full production allocation matrix, including direct non-registry construction, metadata cardinality, remaining destination/format native allocations, and a geometric constant-memory proof. The 1 GiB DuckDB scratch ceiling is a reserved upper bound, not observed bytes written; filesystem exhaustion still returns the underlying clean destination error.
