Status: active
Created: 2026-07-12
Updated: 2026-07-12

# DataFusion currency bridges

## Purpose and scope

This specification governs how CDF exposes statistics, expressions, catalogs, memory/object stores, and physical plans through DataFusion while preserving native package identity, deterministic replay, extension boundaries, and the pinned dependency tuple.

## Identity and layering law

DataFusion MUST NOT produce any decoded batch, verdict, segment, manifest, quarantine artifact, or other byte sequence that enters CDF package identity. Native CDF operators MAY implement DataFusion planning/execution interfaces, but their outputs remain governed by CDF artifact specifications and golden fixtures.

DataFusion types MUST remain inside `cdf-engine` or focused engine-adapter crates. Kernel, neutral runtime/memory, source, format, destination, and package contracts MUST remain DataFusion-free. Adding a source, format, or destination MUST NOT require DataFusion-specific wiring in its implementation crate.

Any DataFusion analysis result that changes execution MUST be canonicalized into plan authority before source contact and reused for run/replay. Re-optimization that could change semantics or identity after plan finalization is forbidden.

## Statistics and pruning

CDF MUST implement DataFusion pruning statistics over its own typed file, segment, package, and profile evidence without opening data payloads. Missing, stale, incompatible, or lossy statistics MUST cause conservative retention, never an unsound skip.

Replay filters, partial backfills, package SQL, and destination merge planning MAY prune only from the recorded predicate and evidence generation. Every supported predicate/type combination MUST satisfy: executing with pruning returns exactly the same rows, verdicts, and committed result as executing without pruning. NULL, NaN, timezone, decimal, cast, nested, missing-statistic, and schema-evolution cases are mandatory adversaries.

## Memory and object-store sessions

Every DataFusion session used by CDF MUST install the same finite pool already adapted to `cdf-memory`; a second independent memory budget is forbidden. DataFusion object-store registration MUST be composed from CDF's secret-resolved, egress-checked provider authority. DataFusion MUST NOT resolve credentials independently or persist secret values in plans, registries, metrics, or errors.

## Expression representation and lowering

Tier-0 derive, filter, and contract expressions SHOULD use a canonical CDF-owned serialized expression form that round-trips to the pinned DataFusion `Expr` model. User-visible semantics MUST be versioned independently of DataFusion's Rust serialization.

Plan-time DataFusion simplification, constant folding, interval reasoning, and function resolution MAY lint and optimize expressions. The canonical optimized result, function/version dependencies, fidelity classification, and residual requirements MUST be recorded in the plan.

Identity-bearing execution MUST lower the recorded expression to CDF's fused native kernels. A DataFusion `PhysicalExpr` MAY evaluate nonidentity query, lint, preview-analysis, or doctor work, but MUST NOT supply values or verdicts to a package-producing path. A declarative function without an admitted native lowering fails identity-bearing planning with an exact capability diagnostic. Unsupported or version-ambiguous functions fail planning rather than changing behavior at runtime.

Substrait export MAY provide externally verifiable expression/plan evidence, but it is an interchange view, not CDF artifact authority. Round-trip loss or unsupported semantics MUST be explicit.

## Evidence catalog

CDF MAY expose ledgers, loads, checkpoints, receipts, lineage, quarantine, packages, and resource-at-checkpoint views through DataFusion `CatalogProvider`, `SchemaProvider`, and table functions. Providers MUST be read-only by default, preserve redaction and authorization, stream bounded batches through the shared memory pool, and report evidence generation/content identity.

ADBC, datafusion-python, notebook, BI, or Ballista consumers are interoperability clients of this catalog. They receive no implicit mutation, checkpoint, receipt, or package-finalization authority.

## Physical-plan marshaling and metrics

CDF native operators MAY be represented as DataFusion `ExecutionPlan` nodes for scheduling, repartition reasoning, metrics, and explain-analyze. Plan nodes MUST reference canonical CDF operator/task authority rather than serialize credentials, callbacks, borrowed objects, or host paths.

Portable serialization MUST compose with `.10x/specs/portable-partition-task-protocol.md`; DataFusion protobuf or Ballista formats may be an envelope/translation, never the sole task authority. Jobs 1/N and direct/capsule execution MUST retain identical CDF identity outputs.

## Selective adoption and exclusions

Primary native codecs MUST NOT be replaced by DataFusion datasource implementations. FX1 MAY host a DataFusion `FileFormat` adapter for exotic formats only when registry, accounting, cancellation, attestation, determinism, and performance laws pass.

CDF's schema coercion plan remains authority. A parity audit with DataFusion's physical expression adapter MUST classify overlaps and differences before reuse or upstreaming. Native validation, dedup, and statistics kernels remain primary; individual DataFusion aggregate/function implementations require differential correctness and before/after measurement.

## Acceptance scenarios

- Given any supported pruning predicate and fixture, pruned and unpruned execution produce identical selected rows, verdicts, package identity where the operation is identity-bearing, and commit outcome.
- Given concurrent DataFusion and native CDF allocations, both compete under one finite pool and neither can exceed the configured managed budget through a private authority.
- Given a secret-backed object store, DataFusion and native scans use the same authorized provider without exposing secret values.
- Given a derive/filter/contract expression, validation records the optimized expression and replay does not re-optimize it into different behavior.
- Given a catalog query over large package history, result memory remains budget-bounded and missing/corrupt evidence fails closed.
- Given the same planned partition graph at jobs 1 and N or through a serialized worker capsule, native operator nodes produce identical CDF identity outputs.

## Explicit exclusions

This specification does not authorize DataFusion to write CDF packages, replace primary codecs, own schema reconciliation, resolve secrets, commit destinations/checkpoints, define CDF's serialized artifact format, or make Ballista the distributed scheduler.
