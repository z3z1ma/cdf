Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Destination common services

## Purpose and scope

This specification governs shared receipt assembly, commit timestamps, SQL mirror lifecycle, and
identifier handling across destination adapters. Physical DDL/DML, wire protocols, type mapping,
transactions, object publication, and destination-specific verification remain adapter-owned.

## Receipt assembly

`cdf-package-contract` MUST provide one validated receipt-draft/finalization path for ordinary and
correction receipts. The common path owns required-field completeness and the invariant mapping
for destination, target, package, segment acknowledgements, disposition, idempotency token,
counts, schema hash, migrations, committed time, transaction metadata, and verify clause.

Destinations supply only their physical transaction metadata, counts, receipt-id derivation,
verify clause, and any destination-specific segment acknowledgements. A destination MUST NOT
reconstruct common receipt fields indirectly from a verify-parameter string map when typed plan
authority is available.

## Runtime time authority

Commit timestamps MUST come from the injected execution host's Unix clock. Destination crates
MUST NOT call `SystemTime::now` for receipt, mirror, staging, or correction identity/evidence.
Profiling may use monotonic host time. The kernel does not gain a convenience wall-clock helper;
the existing execution-host authority is reused so tests can control time and replay can
distinguish recorded evidence from recomputation.

## SQL mirror lifecycle

A focused `cdf-dest-sql` leaf MUST own the backend-neutral mirror lifecycle for `_cdf_loads`,
`_cdf_state`, `_cdf_segments`, and `_cdf_quarantine`:

- typed mirror mutations and readback rows;
- ordering, idempotency, and state-upsert preconditions;
- sequencing relative to payload commit and receipt verification;
- drift/doctor query intent;
- conversion between typed receipt/state evidence and mirror inputs.

The shared manager runs against a typed `TransactionalMirrorBackend` contract. It does not accept
arbitrary SQL strings as its semantic API. DuckDB and Postgres implement physical SQL,
placeholders, JSON types, transaction handles, and row decoding behind that contract. Warehouse
destinations may reuse it only when their guarantee sheet can implement the same transactional
semantics; otherwise they declare a different verified mirror choreography.

## Identifier handling

The common layer validates normalized identifiers against the destination sheet's
`IdentifierRules` and carries a typed identifier into adapter code. SQL quoting/escaping remains
a dialect operation because identifier delimiters and folding differ. No caller may interpolate
an unvalidated source string into SQL.

## Acceptance scenarios

- DuckDB and Postgres ordinary/correction receipts pass the same common completeness and
  correction-evidence laws while retaining their exact transaction metadata and verify clauses.
- A deterministic test execution host supplies commit time; no destination production path reads
  the process wall clock directly.
- DuckDB and Postgres execute the same typed load/state/quarantine mirror sequence inside their
  native transaction and pass crash/idempotency/readback conformance.
- A mirror failure rolls back payload/mirror state according to the destination guarantee; the
  shared manager cannot commit independently.
- Quoted, folded, reserved, and invalid identifiers follow each destination sheet and never cross
  the shared API as unchecked SQL fragments.
- Parquet reuses common receipt assembly and host time without acquiring SQL dependencies.

## Explicit exclusions

This specification does not create a universal SQL executor, common SQL text generator, ORM,
cross-destination transaction, lowest-common-denominator type system, or generic warehouse
adapter. It changes no receipt/package artifact version or destination guarantee.
