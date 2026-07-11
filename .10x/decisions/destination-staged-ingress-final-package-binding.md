Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Destination staged ingress and final package binding

## Context

P3 requires durable package segments to flow toward destinations while extraction continues. Current commit sessions begin only after package finalization because `DestinationCommitRequest` requires the final package hash as both identity and idempotency token. The hash cannot be known early without a circular or false claim. A provisional package hash hidden inside adapters would leak semantics, weaken replay, and make destination behavior inconsistent.

## Decision

CDF distinguishes **staged ingress** from **destination commit**.

A destination capability declares one of:

- `finalized_package_only`: destination mutation begins only after the verified package and concrete package-hash token exist;
- `staged_ingress`: the destination may receive durable segment payloads before package finalization under an opaque `LoadAttemptId`, while keeping them invisible to the target and reversible/garbage-collectable.

`LoadAttemptId` is unique and stable for one resumable run attempt. It is operational identity only: it MUST NOT participate in plan/package identity, state deltas, package-hash derivation, or delivery-guarantee calculation. It MAY appear in run-ledger events, staging metadata, and final receipt transaction metadata.

Each staged write is bound to segment id, segment SHA-256, durable byte count, row count, schema hash, and deterministic ordinal. A staging acknowledgement proves only acceptance in the declared staging scope. The capability declares whether staging is resumable after process loss or transaction-ephemeral with rollback/redrive. The acknowledgement is not a `SegmentAck` for a committed package, is not a receipt, and cannot cross the checkpoint gate.

After package construction finishes, the runtime verifies the final manifest and sends a final binding containing the real package hash, package-hash idempotency token, ordered manifest segment identities, target, disposition, schema, and finalized commit plan. The destination MUST verify exact equality with staged identities before any final target publication. It then performs its atomic transaction/pointer publication, returns the ordinary package receipt, and remains subject to ordinary trait-level verification and checkpoint gating.

For transactional databases, staged ingress may be a resumable staging table or an uncommitted destination transaction when crash behavior guarantees invisibility/rollback and declares redrive on loss. For object stores it is a temporary attempt prefix or multipart upload; the target manifest/pointer is published only after final binding. Best-effort abort plus lifecycle-based staging garbage collection is required, but cleanup success is not treated as commit evidence.

Crash recovery remains within existing rows:

- before package finalization, partial local package and staged ingress are abandoned/reattached by attempt id and never advance target/checkpoint authority;
- a finalized package redrives by package hash, reusing compatible staging only after exact identity verification;
- a durable receipt before checkpoint commit follows existing receipt verification and checkpoint commit.

The runtime chooses capability modes generically. No source/destination name branch is permitted. Drivers declare staging visibility, rollback, maximum useful in-flight bytes/segments, and whether finalization requires exclusive writer ownership.

## Alternatives considered

- Use a provisional package hash in `DestinationCommitRequest`: rejected because it makes receipts/idempotency false and cannot be reconciled with package identity.
- Put `LoadAttemptId` into package identity: rejected because scheduling/run identity would break deterministic packages and jobs invariance.
- Delay all destination I/O until final hash: retained as a capability fallback but rejected as the universal architecture because it prevents network/destination overlap.
- Make every destination implement exactly the same staging primitive: rejected because transactions, staging tables, multipart uploads, and finalized-only sinks have different honest capabilities.

## Consequences

The kernel/runtime session contract needs an explicit pre-commit staged-ingress state, recovery mode, and final package-binding operation. Existing `begin(DestinationCommitRequest)` remains the finalized-only compatibility path until migration completes. P3 measurements must separate staging throughput from final publication latency.

Receipts and delivery guarantees remain package-hash based. The commit gate and package identity are unchanged. Destination conformance gains staged-ack-not-receipt, mismatched-final-binding, crash/reattach/cleanup, duplicate package, and finalized-only fallback laws.
