Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Portable partition task protocol

## Purpose and scope

This specification governs canonical task/attempt/result values, compatibility and authority binding, worker resolution/execution, artifact references, fencing, security, equivalence conformance, and the boundary with later distributed scheduling.

## Task and attempt protocol

The canonical task MUST be versioned, deterministically serialized/hashed, engine-host-neutral, and sufficient for a conforming isolated host to execute one partition/epoch without coordinator borrowed objects. It MUST contain or hash-bind every semantic input named by the decision. Unknown required versions/capabilities fail before source contact.

The task MUST NOT contain secret values, bearer tokens, open handles, callbacks, trait objects, absolute coordinator paths, SQLite connections, destination sessions, runtime handles, or host placement. Secret references and redacted egress/capability policy are allowed. Artifact locations use typed URI/reference plus generation/content preconditions; `file://` is a local provider, not the protocol type.

Attempt envelopes carry nonidentity dispatch/fence/lease/deadline/retry/trace authority. Every side effect to staging/artifact storage MUST verify the attempt fence and source/object generation. An expired/stale attempt may finish computation but its result/write cannot be admitted.

## Worker execution and result

Workers resolve source/format/foreign drivers through explicit registries, resolve secret references locally through authorized providers, and receive an injected execution host/memory/artifact sink. They MUST execute the same compiled graph, validation program, reconciliation, canonical segmentation, and evidence rules as direct local execution.

The result MUST bind task hash, attempt/fence, terminal status, exact processed source/schema attestation, artifact writer receipts, quarantine/residual/verdict/lineage draft references, row/byte counts, and nonidentity telemetry. It contains no destination receipt or committed checkpoint. Coordinator verification rejects missing artifacts, hash/count mismatch, position beyond plan authority, stale fences, version drift, or duplicate/conflicting segment authority.

## Security and operational bounds

Workers receive least-authority source reads and artifact writes. Destination/checkpoint credentials are absent by default. Logs/errors/events use existing redaction. Task/result/control metadata is bounded or externalized; data never flows through scheduler control messages. Cancellation/deadline follows structured runtime rules and leaves only fenced recoverable staging.

## P3 equivalence law

P3 MUST provide an isolated local worker harness that serializes a task, drops all coordinator objects, reconstructs from registries/providers, executes, serializes/verifies the result, and assembles through the ordinary coordinator path. For fixed input/plan at jobs 1/N, direct-local and capsule-local execution MUST produce identical partition segment bytes/hashes, processed attestations, final manifest/package hash, receipt/checkpoint semantics, and verdict evidence; only allowed attempt/host telemetry may differ.

Conformance covers source/format families through mocks, schema drift, late data/drain epoch, retries, stale lease/generation, tampered tasks/results, missing driver/version/secret/capability, cancellation, memory/spill, redaction, high partition cardinality, and package recovery.

## Explicit exclusions

No RPC/wire transport, remote placement/autoscaling, remote state-store implementation, framework adapter, consensus, distributed destination commit, or Ballista/Spark/Flink selection is specified here.
