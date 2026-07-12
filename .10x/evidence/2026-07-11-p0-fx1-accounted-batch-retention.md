Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/specs/native-format-codec-runtime.md

# Accounted physical batches retain ownership across the kernel stream

## What was observed

The kernel now has one transport-agnostic `PayloadRetention` primitive used by both in-memory source batches and verified destination commit windows. `AccountedPhysicalBatch::into_batch` moves its `MemoryLease` into that opaque retention owner; no memory implementation type enters `cdf-kernel`, and no parallel source-only retention abstraction was added.

The prior destination-specific `CommitSegmentRetention` type was deleted and package replay now uses the shared primitive.

## Procedure

- `cargo test -p cdf-runtime physical_batch_retains_its_memory_lease_after_entering_kernel_stream --lib`
- `cargo test -p cdf-kernel --lib`
- `cargo test -p cdf-engine effective_schema_reuses_observation_across_partitions_and_attests_only_attempted_inputs --lib`
- `cargo clippy -p cdf-kernel -p cdf-runtime -p cdf-package -p cdf-format-parquet -p cdf-engine --all-targets -- -D warnings`

All listed checks passed. The focused runtime law observes nonzero coordinator usage after converting the native-driver envelope into a kernel batch and zero usage only after dropping that batch.

The full `cdf-package` suite separately exposed two archive force-replacement failures. They reproduce outside this retention path and are owned by `.10x/tickets/2026-07-11-package-archive-force-replacement-regression.md`.

## What this supports

The neutral format stream can enter the existing resource/engine boundary without losing its decode accounting and without introducing a kernel-to-memory dependency cycle. The same opaque lifetime concept now serves source and destination boundaries.

## Limits

Production file-resource registry composition is not yet connected. Transform allocations remain separately reserved by the engine working-set lease rather than reusing the source decode lease.
