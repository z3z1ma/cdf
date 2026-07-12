Status: active
Created: 2026-07-12
Updated: 2026-07-12

# Typed statistics evidence

## Purpose and scope

This specification defines the CDF-native statistics currency computed once on accepted Arrow batches, aggregated deterministically at segment/package/file grains, serialized as package evidence, and later marshaled into pruning/query/destination interfaces. It completes VISION §§10.1, 11.3, and 12.1 without introducing DataFusion types below the engine boundary.

## Model and authority

Each statistic row MUST bind artifact version, grain, container identity and ordinal, schema hash, normalized field path, canonical Arrow data-type declaration, row count, null count, optional distinct estimate with algorithm/error metadata, optional minimum and maximum typed scalars, completeness, and evidence generation.

Typed scalars MUST use a closed CDF-owned representation covering only Arrow values with a sound total ordering under CDF semantics. Signed/unsigned integers, finite floats with explicit NaN handling, decimal precision/scale, dates, times, timestamps with unit/timezone, durations, UTF-8, and binary are eligible. Nested, map/list/struct, union, run-end, extension, incomparable dictionary, lossy cast, mixed-generation, or unsupported values MUST record unavailable/incomplete statistics rather than fabricate bounds.

The representation MUST round-trip without lexical comparison, floating coercion, timezone loss, precision loss, or dependence on DataFusion/Rust debug serialization. Kernel and package public APIs MUST remain DataFusion-free.

## Computation and aggregation

Statistics MUST be computed with vectorized Arrow kernels or equivalent type-specialized scans on accepted post-normalization batches, in the fused profile stage, without per-row dynamic dispatch on supported primitive columns. Computation MUST share the batch's memory lifetime and use the unified memory authority for additional buffers.

Segment aggregation MUST be associative and deterministic: row/null counts sum with checked arithmetic; minimum/maximum combine only for identical admitted type/schema semantics; completeness is monotone toward incomplete; distinct estimates merge only when algorithm/version/parameters match. Canonical segment membership—not scheduling or jobs count—determines output rows and ordering.

Package/file aggregation MAY be derived from segment evidence and MUST never open payloads when complete lower-grain evidence suffices.

## Artifact

Package-producing runs MUST write canonical `stats/profile.parquet`, ordered by grain, container ordinal, and field path. It replaces the current aggregate-only `stats/profile.json` and lexical `BatchStats` vestiges; no compatibility reader or dual writer is required before production.

The artifact is identity-bearing evidence. Its Parquet writer settings, field order, scalar encoding, null behavior, and artifact version MUST be deterministic and golden-tested. Readers MUST reject corrupt, duplicate, out-of-order, type-inconsistent, unknown-required-version, or container/manifest-mismatched rows. Missing artifacts and unsupported/incomplete fields are conservative absence, never permission to prune.

## Pruning boundary

Statistics themselves do not decide skips. An engine adapter MAY marshal complete compatible scalars into DataFusion `PruningStatistics`; all missing, stale, incomplete, unsupported, NaN-ambiguous, schema-mismatched, or cast-dependent facts MUST become null/unknown so pruning retains the container.

## Performance and memory

The fused statistics stage MUST preserve P3's constant-memory law and fit within the total ≤10% framework overhead budget. Benchmarking MUST separate supported primitive hot columns, wide schemas, null density, strings/binary, decimals/timestamps, unsupported nested columns, and distinct-estimate cost. Exact distinct sets are forbidden on unbounded cardinality; sketches require explicit algorithm/version/error evidence.

## Acceptance scenarios

- Given the same canonical segments under jobs 1 and N, `stats/profile.parquet` bytes are identical.
- Given signed integers including negative values, typed bounds preserve numeric rather than lexical order.
- Given decimal or timestamp fields, bounds round-trip precision, scale, unit, and timezone exactly.
- Given NaN, nested, drifted, corrupt, missing, or incomplete evidence, downstream pruning retains the affected container.
- Given a package manifest and profile artifact with conflicting container rows/counts/schema hashes, verification fails before pruning.
- Given a workload larger than memory, statistics memory remains bounded by active batches/segments and no whole-package map is required.

## Explicit exclusions

This specification does not define pruning predicates, DataFusion adapters, user-facing profile policy, anomaly detection, or destination-specific merge behavior. It does not authorize sampled/incomplete statistics to prove absence; sampling may inform suggestions but not sound skips.
