Status: open
Created: 2026-08-03
Updated: 2026-08-03
Parent: .10x/tickets/2026-08-03-postgres-binary-and-exact-value-program.md
Depends-On: .10x/tickets/done/2026-08-03-postgres-source-binary-copy.md

# PostgreSQL destination exact-value text

## Scope

Teach the PostgreSQL destination to reconstruct source-owned tagged canonical JSON, JSONB, and
NUMERIC text through its existing binary COPY path, while ordinary and foreign-tagged `Utf8`
continues to map to TEXT.

## Non-goals

Inferring types from field names or physical provenance, arbitrary string casting, cross-database
numeric coercion, lexical JSONB preservation, or other destination behavior changes.

## Acceptance Criteria

- Exact PostgreSQL semantic tags select native JSON, JSONB, or NUMERIC; untagged,
  physical-type-only, and foreign-tagged `Utf8` remain TEXT.
- Planning validates the complete tagged field and resolved target declaration before mutation.
- Finite and supported special NUMERIC values encode directly without floating-point conversion;
  JSON/JSONB use PostgreSQL native parsing semantics.
- Invalid tagged text, target incompatibility, range failure, append/replace/merge, replay, schema
  inspection, and receipt atomicity have focused unit and live round-trip coverage.
- Existing PostgreSQL destination mapping and throughput paths remain unchanged for ordinary Arrow
  fields.

## References

- `.10x/specs/postgres-destination-exact-value-text.md`
- `.10x/decisions/exact-value-text-fallbacks.md`
- `.10x/research/2026-08-03-exact-value-adapter-audit.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/destination-receipts-guarantees.md`

## Assumptions

- Same-native reconstruction by exact versioned semantic tag is user-ratified and record-backed.

## Journal

- 2026-08-03: Ticket opened after specification ratification; depends on the source establishing
  the exact tag and canonical text contracts.

## Blockers

None beyond the declared dependency.

## Evidence

Pending execution.

## Review

Pending parent final red-team review.

## Retrospective

Pending executor handback.
