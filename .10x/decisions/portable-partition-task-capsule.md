Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Portable partition task capsule

## Context

P3 local execution must remain the foundation for future distributed/container embedding. Serializable plans are insufficient while execution depends on borrowed resources, local paths, stores, secrets, and composition objects.

## Decision

CDF defines a versioned, canonical, content-addressed **partition task capsule** as operational protocol, not a new package/evidence artifact. It binds one planned partition/epoch to:

- CDF/artifact/normalizer/Arrow/DataFusion compatibility tuple;
- project/pipeline/resource/plan and canonical partition/unit/segment authority;
- source driver id/version/schema/options/physical-plan hashes and secret references only;
- scan/pushdown/schema/coercion/validation/normalization/execution-extent policy hashes or embedded canonical values;
- input checkpoint head/scope, lease/fence requirement, source generation/preconditions, budgets, and required host capabilities;
- typed input/output artifact references and redacted policy.

The capsule hash excludes run attempt id, lease token, host placement, timing, and credentials. Dispatch wraps it in an attempt envelope containing fenced lease/attempt/expiry and authorized sink endpoints. A worker verifies hashes/compatibility, resolves drivers/secrets/services through injected registries, executes the ordinary graph, and returns a canonical partition result with artifact writer receipts, processed-position/schema attestation, bounded verdict/evidence references, metrics, and terminal status.

Workers cannot finalize a multi-partition package, bind a destination, issue a destination receipt, or commit checkpoints unless a later topology explicitly delegates one of those existing authorities. The coordinator verifies results, canonically assembles/finalizes the package, binds destinations, and passes the existing commit gate. Fence/generation checks protect all worker writes and stale results are rejected.

P3 implements neutral types and a local isolated-worker serialization round-trip law only. No RPC or remote scheduler ships. Future container/Spark/Flink/Ballista hosts implement placement and transport around unchanged capsules/results.

## Alternatives considered

- Serialize `EnginePlan`: rejected because it omits source/runtime/artifact/state authority and leaks engine ownership.
- Send closures/trait objects: rejected as nonportable and unauditable.
- Translate into Spark/Flink semantics: rejected because correctness and package identity would fork.
- Let workers commit independently: rejected because it creates settlement/state races.
- Defer the seam: rejected because P3 APIs would harden around local-only objects.

## Consequences

WX1 owns the neutral protocol. C5 proves local direct versus serialized isolated-worker equivalence. The later distributed ticket owns transport, remote stores/leases, placement, failure detection, framework adapters, and substrate selection.
