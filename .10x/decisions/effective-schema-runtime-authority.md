Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Effective schema runtime authority is plan-level and source-neutral

## Context

An immutable discovered baseline is insufficient to execute a changing multi-input resource. Runtime planning needs a separately identified effective schema, the discovery manifest that justified it, and exact coercion evidence for each observed physical schema. Encoding that authority in every partition would duplicate resource-level state at O(partitions), assume one partition per file, and make package identity depend on executor partitioning. Binding it through a metadata key named `path` would also force SQL shards, stream splits, REST page families, and custom parser inputs to impersonate files.

`EnginePlan` and `ResourceSchemaDiscoveryArtifacts` were public exhaustive structs. Adding the required typed authority field directly is a Rust struct-literal compatibility break, while avoiding a field by hiding evidence in metadata strings or the validation program would create a permanent untyped or semantically misplaced extension path.

## Decision

The immutable pinned snapshot remains baseline authority. A bounded compiler-stage observation derives an effective schema without rewriting the baseline snapshot or `cdf.lock`. The plan carries one typed `EffectiveSchemaPlanEvidence` aggregate containing the verified baseline snapshot reference, effective snapshot-schema hash, independently recomputed structural Arrow-schema hash, discovery-manifest reference, and exact coercion plan per schema observation. Packages serialize this authority and the attempted-observation coercion evidence as identity artifacts.

Kernel and engine bind partitions through the source-neutral `cdf:schema_observation_id` key. A source adapter maps its natural identity into that key; the file adapter uses canonical file location. Multiple partitions may share one observation. Physical Arrow schemas are held once in a hash-keyed runtime catalog, and the planner recomputes each structural hash before compiling coercions through the existing contract reconciliation lattice. Generic CLI, planner, executor, and destination orchestration MUST NOT branch on Parquet, Arrow IPC, file paths, or destination names for this behavior.

The canonical Arrow fingerprint uses a length-prefixed recursive structural encoding over every Arrow data type, schema/field metadata, nested child name, nullability, and type parameters. Map ordering is canonical. `DataType::to_string()` and delimiter concatenation are not identity authority.

`EnginePlan` and `ResourceSchemaDiscoveryArtifacts` receive a one-time pre-1.0 `#[non_exhaustive]` migration. The artifact type has a constructor; new kernel/engine authority types are born non-exhaustive with validated constructors/accessors and separate intrinsic versus resource-attachment validation. Future evidence families extend these seams rather than reopening public struct literals or partition metadata.

## Alternatives considered

- Copy baseline/effective/manifest evidence into every partition: rejected because it is O(partitions), couples identity to partition coalescing, and cannot represent repeated partitions per observation cleanly.
- Store one physical Arrow schema per file: rejected because large homogeneous sets would multiply metadata memory; schemas are catalogued once by structural hash.
- Reuse `path` as the generic binding key: rejected because it leaks the file source model into kernel/engine orchestration.
- Put per-input coercion decisions in kernel strings or invent a second widening lattice: rejected because `cdf-contract::SchemaCoercionPlan` is the existing typed verdict authority.
- Preserve exhaustive public structs by hiding evidence in metadata or global runtime state: rejected because serialized plan/package authority and replay determinism would be weakened.
- Hash `DataType::to_string()` and delimiter-joined metadata: rejected because nested child metadata is omitted and delimiter-bearing values are not an unambiguous structural encoding.

## Consequences

- Immutable baseline pins and evolving runtime schemas are separately legible and independently verified.
- Plan/package identity is invariant to partition coalescing while execution can attest only observations actually attempted under a limit.
- Adding a source requires mapping its natural observation identity and exposing bounded compiler evidence; generic execution does not acquire source-specific branches.
- Homogeneous large file sets store O(unique physical schemas) schema metadata rather than O(files).
- `cargo semver-checks` reports exactly one intentional major-class finding in `cdf-engine` and one in `cdf-project`. This is a bounded pre-1.0 construction migration, not a general compatibility waiver.
- The structural fingerprint algorithm is now authority-bearing; changes require an explicit version/migration decision rather than an incidental formatting change.
