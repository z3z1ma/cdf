Status: active
Created: 2026-07-11
Updated: 2026-07-15

# Streaming destination ingress

## Purpose and scope

This specification governs pre-finalization destination staging, final package binding, capability selection, crash behavior, evidence, and memory/backpressure integration. Ordinary receipts, idempotency, package identity, and checkpoint gating remain governed by their existing specifications.

## Behavioral contract

Only a durable package segment whose bytes and SHA-256 are complete MAY enter staged ingress. The runtime MUST preserve deterministic manifest order independently of arrival/scheduling order.

A staged-ingress request MUST carry a `LoadAttemptId`, destination/target identity, planned disposition/schema/plan authority, the complete compiled output Arrow schema, disposition inputs such as merge keys, and bounded scheduling context. It MUST NOT claim a package hash or package-token idempotency guarantee. Driver-specific schema preparation remains inside the destination adapter; the generic runtime MUST NOT branch on destination identity or physical provenance encoding.

A staged-segment request MUST carry segment id, SHA-256, row/byte counts, schema hash, ordinal, and a bounded Arrow batch stream or segment reader. The reader retains the source memory accounting until the destination finishes or transfers it into equally authoritative destination accounting. A destination MAY hold multiple unacknowledged requests only within the request's declared segment/byte scheduling bounds. It MUST acknowledge each exact identity accepted into its declared staging scope or fail; completion and acknowledgement MAY arrive out of order, but snapshots and final binding MUST restore canonical ordinal order. Partial acknowledgement is forbidden. The capability MUST declare `resumable` or `rollback_redrive` recovery and MUST NOT imply persistence across process loss for ephemeral transactions.

Before final binding, staged data MUST be invisible to target reads that represent committed destination state. No package receipt, `_cdf_loads` committed row, final object manifest/pointer, checkpoint, or delivery guarantee may be emitted.

Final binding MUST occur only after local package finalization and verification. It MUST provide the verified package hash/token and complete ordered segment identities. Any missing, duplicate, extra, reordered-without-declared-order-independence, or mismatched staged identity MUST fail before target publication.

Staged writes and final binding MUST execute through the blocking lanes declared by destination capabilities. The generic host owns scheduling and admission; adapters own native connection and transaction behavior.

Successful final binding MUST produce the existing receipt shape over the final package hash and committed segment acknowledgements. When final binding itself has exactly verified every published mutation from hash-while-write evidence, create-only publication, durability barriers, and the completed manifest, it MAY return commit-bound receipt verification for that exact receipt id. The generic gate MUST still validate receipt/state structure and exact receipt-id binding before accepting that evidence. Duplicate, recovered, or otherwise pre-existing commits MUST use independent runtime verification. This is an optimization of when verification occurs, never permission to omit it; checkpoint commit still follows only verified receipt evidence.

Abort MUST be idempotent. Staging garbage collection MUST be bounded by an explicit lifecycle/retention policy and MUST never infer committed state from attempt identity. Recovery MAY reattach only after destination-reported staging identities exactly match the run ledger and final package where available.

All staging buffers, queues, transactions, temporary files, and multipart parts under CDF control MUST register with the P3 memory ledger or declare external durable storage. Backpressure propagates from staged ingress to package persistence and upstream operators through byte permits.

## Capability and conformance

Destination inspection/sheets MUST declare finalized-only versus staged ingress, visibility isolation, rollback/cleanup support, writer concurrency, and useful in-flight byte/segment bounds. Planner/runtime selection MUST be capability-driven.

Conformance MUST cover successful overlap, bounded multi-segment ownership, out-of-order acknowledgement with canonical final order, staged-write failure, crash after every staged segment, resumable reattach, rollback/redrive, abort repetition, final-binding mismatch, duplicate finalized package, no receipt before binding, no checkpoint before verified receipt, staging cleanup, and finalized-only fallback. Jobs 1/N MUST yield identical package hashes and receipt package/segment identities; destination transaction metadata may truthfully differ.

## Explicit exclusions

This spec does not create distributed worker leases, make staging a package artifact, change SHA-256, permit direct unverified source-to-destination writes, or require every destination to overlap ingestion.
