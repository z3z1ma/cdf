Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Spillable package-order dedup

## Context

Keyed merge dedup and explicit append exact-row dedup are package semantics, but the implementation retains all payloads, key groups, retained masks, and dropped provenance. It cannot process terabyte packages under a fixed memory budget. Exact-row execution also precedes residual materialization/normalization despite its active decision defining identity over the complete normalized output row.

## Decision

Package dedup is an ordered stateful barrier after contract acceptance, residual/variant materialization, normalization, and output schema conformance, and before canonical segment assembly. Keyed rules resolve their declared fields through preserved source-name/normalized-field authority. Exact-row identity covers the complete final package output row.

Rows receive monotonic canonical package row ordinals from `(plan partition ordinal, local accepted-row order)`. `first`, `last`, and `fail` preserve current canonical package-order meaning. Equality follows versioned `cdf-dedup-key-v1` in `.10x/specs/spillable-package-dedup.md`; hashes are acceleration only and every collision is resolved by exact encoded-key comparison. Scheduling, memory budget, spill timing, hash partition count, and jobs cannot alter results.

The barrier has two execution modes under the same semantics:

- an accounted in-memory fast path while complete payload/key/decision state fits its ledger grant;
- a spill path with canonical Arrow payload spools, typed key+ordinal records, a bounded external winner computation, and an ordinal-sorted decision stream joined sequentially back to payload.

Transition from memory to spill is lossless and may happen at any admitted boundary. The selected external winner algorithm is chosen by L5/A6 measurements between bounded collision-safe partitioned hash/radix and external merge approaches; algorithm choice remains internal telemetry, not package identity.

No dedup output segment may become durable or enter destination staging until the barrier has completed successfully. This preserves `keep=fail`'s pre-mutation guarantee and gives all modes one crash shape. Dedup scratch is not package evidence and follows typed spill lifecycle/cleanup rules.

Dedup evidence artifact version 2 stores bounded aggregates in `stats/dedup-summary.json`. Dropped-row provenance moves to deterministic Parquet shards under `stats/dedup-dropped/`, with `package_row_ordinal` and `kept_package_row_ordinal` as unsigned 64-bit columns. Shard boundaries are plan-versioned and independent of memory pressure/jobs. Readers support legacy inline v1 and sharded v2; writers emit v2 after an explicit package artifact-version migration.

Scratch directories/files use owner-only permissions and opaque names, are disk-budgeted before growth, and are removed idempotently on success/error/cancellation or handed to explicit crash cleanup authority. Secure deletion is not claimed.

## Alternatives considered

- Increase the in-memory hash map budget: rejected because memory still scales with package cardinality.
- Delegate dedup to each destination: rejected because package identity/replay would depend on destination behavior.
- Bloom-filter-only dedup: rejected because false positives would lose data.
- Hash-only identity: rejected because collisions would silently lose data.
- Always external sort all payload rows: rejected as the default because low-cardinality/small packages deserve an in-memory fast path and hash/radix may be materially faster.
- Release `first` rows online: rejected because modes would gain different staging/failure shapes and exact `fail` cannot certify uniqueness early.
- Keep unbounded inline JSON provenance: rejected because evidence metadata would violate constant memory and impose avoidable serialization cost.

## Consequences

Exact-row behavior changes only where current execution contradicted the active complete-output-row decision, notably residual/variant differences. Package evidence receives a versioned migration. Destination-level duplicate guards remain safety rails but never replace the package barrier. The lab must measure spill write amplification, key encoding, winner computation, ordinal join, provenance encoding, skew, and fast-path crossover.
