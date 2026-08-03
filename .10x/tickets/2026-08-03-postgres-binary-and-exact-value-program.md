Status: open
Created: 2026-08-03
Updated: 2026-08-03

# PostgreSQL binary source and exact-value program

This is a parent planning ticket and is not executable implementation scope.

## Scope

Complete the user-ratified PostgreSQL throughput and exact-value tranche in this order:

1. `.10x/tickets/done/2026-08-03-postgres-source-binary-copy.md`
2. `.10x/tickets/done/2026-08-03-postgres-destination-exact-value-text.md`

The source child owns canonical binary COPY OUT, JSON/JSONB, NUMERIC discovery and decoding, and
the direct-client roofline. The destination child owns exact-tag native reconstruction. The parent
owns one final focused source/destination integration check and one final red-team review; it does
not own a repeated workspace-wide suite.

## Non-goals

Cross-connection source partitioning, arbitrary SQL resources, CDC/logical replication, unrelated
adapter refactors, and MySQL or MongoDB implementation.

## Acceptance Criteria

- Both child tickets close with their focused evidence and retrospectives.
- PostgreSQL source-to-destination round trips prove JSON, JSONB, constrained Decimal128/256, and
  tagged unconstrained/wide NUMERIC without float conversion or accidental ordinary-text casts.
- The source reaches the ratified 0.90 same-semantics direct binary COPY OUT roofline.
- The cross-adapter findings in
  `.10x/research/2026-08-03-exact-value-adapter-audit.md` remain resolved.
- One final independent red-team review passes after closure repair, without an iterative review
  loop.

## References

- `.10x/decisions/exact-value-text-fallbacks.md`
- `.10x/specs/postgres-source-binary-copy.md`
- `.10x/specs/postgres-destination-exact-value-text.md`
- `.10x/research/2026-08-03-exact-value-adapter-audit.md`
- `.10x/specs/database-connector-roofline.md`

## Assumptions

- The binary COPY path, exact JSON/JSONB text boundary, hybrid numeric mapping, 0.90 roofline, and
  cross-adapter consistency policy were explicitly user-ratified on 2026-08-03.
- Focused validation throughout plus one parent integration check is the user-ratified economical
  validation cadence.

## Journal

- 2026-08-03: Program opened after the exact-value policy and PostgreSQL behavior were ratified.
- 2026-08-03: Source child closed with canonical bounded binary COPY OUT, exact tagged
  JSON/JSONB/NUMERIC mappings, Decimal128/256 decoding, focused live/project evidence, and passing
  1.028x fixed-width plus 1.192x mixed-schema official-client roofline cells. Destination tagged
  text reconstruction is the next executable child.
- 2026-08-03: Destination child closed with field-aware exact-tag planning, direct native binary
  JSON/JSONB/NUMERIC encoding, append/replace/merge/replay and rejection atomicity on PostgreSQL
  17, strict focused lint, and preserved ordinary Decimal/binary COPY behavior. The parent
  integration gate is next.

## Blockers

None.

## Evidence

- Source child: `.10x/tickets/done/2026-08-03-postgres-source-binary-copy.md`.
- Source evidence: `.10x/evidence/2026-08-03-postgres-source-binary-copy.md`.
- Destination child: `.10x/tickets/done/2026-08-03-postgres-destination-exact-value-text.md`.
- Destination evidence: `.10x/evidence/2026-08-03-postgres-destination-exact-value-text.md`.
- Final integration evidence remains pending.

## Review

Pending one final independent red-team review.

## Retrospective

Pending.
