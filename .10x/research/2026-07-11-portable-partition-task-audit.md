Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Portable partition task audit

## Question

Do P3's local partition/runtime artifacts form a real executor-neutral seam for later container, Spark, Flink, or Ballista workers, and what must be proven now to avoid a later runtime rewrite?

## Sources and methods

Inspected VISION D-11/8.6/25.2, distributed/parallel/runtime/source/destination/package/state records, kernel scan/partition/attestation types, engine plan serialization, declarative compiled resources, and project runtime request/resource ownership.

## Findings

`ScanPlan`, `PartitionPlan`, most engine plan values, typed positions, and capability facts are serializable. The live runtime nevertheless accepts borrowed `dyn QueryableResource`, store references, local `PathBuf` package/state/destination paths, and composition-owned secret providers. A serialized engine plan alone cannot reconstruct one partition on another host or prove it used the same driver/options/artifact tuple.

The source-extension plan already requires driver id/version/schema hash, canonical redacted options/hash, physical plan payload/hash, secret references, and discovery/schema authority. The scheduler requires canonical partition/unit ordinals and scope leases. The missing join is a versioned operational task capsule binding these authorities for exactly one partition/epoch and resolved against injected registries/services on any conforming host.

Secrets cannot be embedded; only references and egress/capability policy travel. Local filesystem paths are not portable identities. Inputs/outputs use typed artifact/object references with generation/content preconditions. Worker attempts and lease tokens are operational fencing, not package identity.

A worker executes compiled validation/normalization/segmentation, writes hash-addressed partition artifacts through an authorized sink, and returns a hashed attempt result/attestation. Final package assembly, destination binding, receipt verification, and checkpoint commit remain coordinator-authorized. This preserves packages as the shuffle-free handoff and avoids distributed 2PC.

P3 need not ship a remote scheduler. It should prove the local scheduler can round-trip every task through canonical serialization and an isolated worker harness with no borrowed resource/path/store object, yielding the same partition segments as direct local execution.

## Conclusion

Define a kernel/neutral operational task/result protocol and local isolated-worker conformance. Keep lease/store/transport implementations and scheduler protocols for the distributed program. Require later Spark/Flink/Ballista adapters to host the same capsule rather than translate CDF semantics into a second plan model.

## Limits

This audit does not select RPC, scheduler, remote state store, signing transport, object-store layout, Ballista, Spark, or Flink APIs.
