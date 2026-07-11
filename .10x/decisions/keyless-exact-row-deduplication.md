Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Keyless exact-row deduplication

## Context

P2 WS-F requires deduplication for append/event data without inventing a business key. CDF already compiles and executes deterministic package-order dedup rules, but execution is incorrectly restricted to `merge` and its evaluator treats every selected field as a non-null key.

## Decision

Tier-0 exposes `deduplicate = "exact_row"`. It is valid for `append` only. The compiler records an exact-row deduplication semantic on the kernel resource descriptor and binds it into the ordinary serialized validation program as a dedup rule over every normalized output field.

Identity is the complete typed Arrow row, including nulls and nested values. The first occurrence in deterministic package order is retained. Deduplication is package-scoped: it does not query or rewrite previously committed destination rows and does not claim an effectively-once key guarantee. The package records the ordinary dedup summary and dropped-row provenance.

## Alternatives considered

- Reuse merge keys. Rejected because it pressures append users to invent business identity and does not express complete-row equality.
- Implement separately in each destination. Rejected because package identity and replay would then depend on destination behavior.
- Make dedup implicit for append. Rejected because dropping repeated events changes data semantics and must be explicit.
- Retain last. Rejected because identical data values make last semantically unnecessary while first minimizes retained provenance movement and matches forward package order.

## Consequences

Exact-row dedup is portable across sources and destinations and participates in package identity. Its current package-order evaluator retains package-scale identity state; P3's memory-ledger/spill work must account for that state without changing semantics.
