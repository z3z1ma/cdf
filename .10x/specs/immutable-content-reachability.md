Status: active
Created: 2026-07-18
Updated: 2026-07-19

# Immutable content reachability and reclamation

## Purpose and scope

This specification governs destination-neutral lifetime authority for immutable, content-addressed objects that may outlive one run attempt before they become reachable from a committed destination manifest. It exists so Parquet, object-store multipart destinations, distributed workers, and future content-addressed adapters can share one reclamation protocol instead of destination-owned heartbeats, modification-time guesses, or full-store scans.

It applies to immutable content objects written by destination staging/publishing paths. Package identity, receipt verification, commit-gate semantics, and committed historical-manifest retention remain governed by their existing specs and are not changed here.

## Terms

- **Content object**: one immutable byte object in a destination-owned store. It has an opaque store namespace, provider object key, byte count, content digest, and provider generation evidence where available.
- **Publication claim**: a fenced assertion that one live attempt owns or may soon own a specific content object identity. A claim is protected by a runtime `StagingLease` generation.
- **Committed root**: destination-neutral evidence that a committed manifest or settlement references a set of immutable content identities.
- **Reclamation candidate**: an exact content object generation that cleanup may consider deleting only after proving no committed root and no live claim can reference it.
- **Reachability authority**: the runtime/storage protocol that creates claims, records committed roots, proves expiration, and admits deletion candidates.

## Records and boundaries

The kernel/runtime boundary MUST expose versioned, serializable records for:

- `ImmutableContentIdentity`: opaque store namespace, object key, byte count, content digest algorithm/value, provider generation evidence, and optional logical grouping metadata. The type MUST NOT import a concrete destination crate or format crate.
- `ContentPublicationClaim`: destination id, target, attempt id, staging-lease identity/generation, content identity, claim id, claim generation, and claim state.
- `CommittedContentRoot`: destination id, target, committed manifest/root id, root generation, retained policy horizon, and a deterministic set or shard reference for reachable content identities.
- `ContentReclamationCandidate`: content identity, observed provider generation, candidate source, and the exact claim/root index positions consulted.

Concrete destinations MAY store additional private metadata, but generic cleanup eligibility MUST be derivable from these records plus provider CAS/readback behavior.

## Publication protocol

Before a destination publishes an immutable content object or makes an existing object eligible for shared reuse, it MUST hold a current `StagingLease` and install a `ContentPublicationClaim` whose lease identity and fencing token match the attempt. The claim install MUST be create-only or compare-and-swap protected by provider generation evidence.

Writers of identical content MAY converge on one physical object. Convergence MUST NOT transfer cleanup authority from one attempt to another. Each live attempt that may rely on the object MUST have its own claim or MUST be covered by a committed root.

Publishing object bytes MUST be create-only or create-or-verify:

- If the object is absent, the publisher writes the exact bytes, records the resulting provider generation, and verifies byte count and digest before acknowledging the claim as published.
- If the object already exists, the publisher MUST verify byte count, digest, and immutable generation before accepting it as the claimed object.
- If a same key object exists with different bytes or unverifiable identity, publication MUST fail closed.

Claims MUST be renewed by the generic staging-lease supervisor, not by destination-specific heartbeat threads. A destination adapter may request a lease, attach claimed content to it, and observe renewal/cancellation outcomes; it MUST NOT implement independent liveness rules.

## Commit settlement

Final destination binding MUST monotonically convert every still-needed claim into committed reachability evidence before releasing the claim. The committed root write MUST be fenced by destination commit authority and provider CAS/readback where the store permits it.

Because an object store cannot atomically publish a destination manifest and update CDF's reachability index, settlement MUST use a prepared root intent. Preparing the root and its reverse membership index MUST be atomic and MUST occur before manifest publication. A prepared root protects its exact content identities from reclamation but is not committed evidence. After the destination manifest is durably published and verified, committing the prepared root and settling its claims MUST be one atomic reachability-store transition. Recovery MUST inspect the exact destination manifest named by the attempt record and either commit the matching prepared root or abort it; an unresolved prepared root fails safe by retaining content.

After settlement:

- The committed root, not the original attempt claim, protects retained content.
- Releasing or expiring the attempt lease MUST NOT make committed content reclaimable.
- Duplicate commit/replay MUST either observe the same committed root or create an identical root generation that references the same content set; it MUST NOT repoint historical committed roots.

If a process crashes after object publication but before root settlement, recovery MAY finish settlement using the exact live claim if the lease is current, or cleanup MAY reclaim only after proving the lease expired and no committed root references the content.

## Reclamation protocol

Cleanup MUST operate from bounded indexes of claims, roots, and explicitly reported candidates. Normal runs MUST NOT scan all destination objects or all historical manifests on the hot path.

A content object generation may be deleted only when all of the following hold:

1. The candidate's object identity and provider generation are exact.
2. Every claim that can reference that exact identity is either absent or has a lease generation proven expired by the runtime staging-lease authority.
3. No committed root within retention policy references that identity.
4. The provider delete is conditional on the same object generation that was checked.
5. A racing new claim or root write fails the cleanup CAS/readback proof or makes deletion ineligible.

After proving eligibility, cleanup MUST atomically install a durable reclamation reservation for the exact content address and consulted claim/root generations. Claim installation and root preparation MUST reject an active reservation. Cleanup MUST remove the reservation only after an exact-generation delete is confirmed or after recording an inconclusive/failed delete; a crash with a reservation in place retains the object and blocks reuse until exact provider-state recovery resolves the reservation.

Cleanup MUST prefer false negatives over false positives. If a provider cannot prove the checked generation at delete time, cleanup MUST retain the object and record an inconclusive candidate.

Cleanup memory usage MUST be bounded by the configured cleanup batch and index shard sizes, not by the total retained dataset size.

## Retention and history

Committed roots are retained according to destination retention policy. Historical roots protect their referenced content until the policy explicitly releases them. Retention policy changes are separate committed-root events and MUST NOT retroactively delete content without a bounded cleanup pass that rechecks claims and provider generations.

## Conformance scenarios

1. Given two concurrent attempts publish identical content, when one attempt crashes and cleanup runs, then the object is retained while the other attempt's claim is live.
2. Given an attempt publishes content and crashes before manifest settlement, when its lease is renewed, then cleanup cannot delete the object.
3. Given an attempt publishes content and crashes before manifest settlement, when its lease is proven expired and no root references the object, then cleanup may delete only the exact checked generation.
4. Given a committed root references content, when the original attempt lease expires, then cleanup retains the object.
5. Given a historical committed root is still inside retention, when a newer root no longer references the object, then cleanup retains the object.
6. Given a provider object changes generation between candidate scan and delete, when cleanup attempts deletion, then deletion fails closed or retains the object.
7. Given a destination adapter enrolls in content reachability, when reviewed from generic runtime code, then no cleanup branch names the concrete destination or format.

## Explicit exclusions

This spec does not authorize destination-specific heartbeat threads, deletion by object age alone, full-store scans on normal runs, package artifact garbage collection, weakening object-store egress/secret policy, changing immutable object bytes, changing receipt verification, or changing manifest retention policy.
