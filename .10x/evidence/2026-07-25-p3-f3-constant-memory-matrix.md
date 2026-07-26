Status: recorded
Created: 2026-07-25
Updated: 2026-07-25

# P3 F3 constant-memory matrix

## Observation

An optimized `cdf` build at revision `45a1a84851a2fd99536a155b01fb4761d8c34a98`
completed deterministic 5, 20, and 100 GiB governed Parquet-to-package-to-Parquet workloads under
an enforced 2 GiB cgroup without an OOM event. Peak process RSS ranged from 1,657,630,720 to
1,701,265,408 bytes and did not increase with input size. Managed peak remained within 100 bytes of
1,610,598,707 bytes in every case.

Two independent 5 GiB executions peaked at 1,701,265,408 and 1,674,674,176 bytes. Their 26,591,232
byte difference is 1.6% of the lower observation and provides no evidence of repeated-run allocator
or handle drift. The 20 and 100 GiB executions processed 100 and 500 canonical segments while
peaking at 1,670,701,056 and 1,657,630,720 bytes respectively.

An explicit 64 MiB product budget failed with exit code 5 and `kind = "data"` before creating a
`.cdf` directory. The error stated the requested budget, 64 MiB minimum working set, 512 MiB native
headroom, and both corrective actions: raise the budget or reduce the working set.

Focused laws separately proved forced-spill semantic equivalence and cleanup, exhausted-spill
cleanup, source backpressure under a blocked consumer, bounded remote metadata listing, streaming
gzip NDJSON cancellation before full download, and staged-writer progress with no unreserved
memory. The existing F1 release-host evidence remains authoritative for foreign-child cgroup
containment.

## Procedure

The retained EC2 benchmark host was a `c7i.4xlarge` with 16 logical CPUs, 32 GiB RAM, and a tuned
2 TiB gp3 root volume. `tools/p3-ec2-benchmark-host.sh` synchronized clean revision
`45a1a84851a2fd99536a155b01fb4761d8c34a98` and built the fat-LTO release products. Each workload
ran through `tools/run-constant-memory-stress.sh` inside:

```text
systemd-run --user --wait --collect \
  --property=MemoryMax=2G \
  --property=MemorySwapMax=0
```

The generator created one bounded Parquet base object and hard-linked the requested deterministic
file cardinality outside the timed region. The timed product path included metadata inventory,
source decode, validation/normalization, canonical persistence and hashing, Parquet destination
commit, receipt verification, and checkpoint commit. The runner independently verified source and
receipt row counts, package integrity, checkpoint commitment, managed peak, and `/usr/bin/time`
process RSS.

Raw summaries:

- `.10x/evidence/.storage/2026-07-25-p3-f3-ec2-5g-a-summary.json`
- `.10x/evidence/.storage/2026-07-25-p3-f3-ec2-5g-b-summary.json`
- `.10x/evidence/.storage/2026-07-25-p3-f3-ec2-20g-summary.json`
- `.10x/evidence/.storage/2026-07-25-p3-f3-ec2-100g-summary.json`
- `.10x/evidence/.storage/2026-07-25-p3-f3-ec2-too-small.json`

Focused commands:

```text
cargo test -p cdf-engine --locked -j 12 disk_exhaustion_is_clean_and_scratch_cleanup_is_idempotent
cargo test -p cdf-engine --locked -j 12 in_memory_pressure_transitions_losslessly_to_external_runs
cargo test -p cdf-engine --locked -j 12 append_exact_row_dedup_compiles_and_drops_only_complete_duplicates
cargo test -p cdf-source-files --locked -j 12 blocked_decode_publication_releases_shared_run_work
cargo test -p cdf-source-files --locked -j 12 object_store_gzip_ndjson_streams_without_spill_and_preserves_remote_position
cargo test -p cdf-source-files --locked -j 12 remote_listing_filters_without_materializing_all_metadata
cargo test -p cdf-project --locked -j 12 http_gzip_ndjson_backpressures_and_cancels_before_download_completion
cargo test -p cdf-memory --locked -j 12 budget_resolution_preserves_native_headroom_and_rejects_unsafe_shape
cargo test -p cdf-dest-parquet --locked -j 12 staged_writer_window_is_reserved_before_input_and_not_charged_again
```

All selected tests ran one matching test and passed.

## What it supports or challenges

This supports the F3 law that process memory and open resources are bounded by admitted topology,
not total input bytes, files, or segments. It supports clean failure for an impossible process
budget and clean spill-budget exhaustion. It also supports the active specification's exact rule:
spill must be observed when the selected algorithm plans spill; the ordinary streaming Parquet
workload correctly completed without spill and should not be forced to perform unnecessary disk
I/O merely to increment a counter.

The 5/20/100 GiB workload reuses identical file content by hard link. CDF still decodes and commits
every logical partition, but content-addressed destination objects may be reused. The result is a
memory-slope and lifecycle proof, not a unique-byte storage-capacity or compression-ratio claim.

## Limits

This matrix does not claim the 1 TiB acceptance law, device-saturation scaling, or a permanent CI
schedule; F4 owns those. It does not multiply every semantic rule by every input size. Focused
package and adapter suites remain the stronger authority for deduplication, quarantine, remote
streaming, foreign-child containment, and destination publication semantics.
