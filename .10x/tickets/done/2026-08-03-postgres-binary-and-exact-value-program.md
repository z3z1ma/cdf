Status: done
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
- One final independent red-team review completes after integration; all material findings are
  repaired once with focused evidence, without an iterative review loop.

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
- 2026-08-03: Added one production-path conformance law spanning live PostgreSQL discovery,
  registered source compilation/resolution, binary COPY, generic engine/package publication,
  native destination commit, package/receipt verification, target declaration inspection, and
  exact source/target comparison. It covers JSON, JSONB, Decimal128/256, tagged wide/unbounded
  NUMERIC, and numeric-looking ordinary TEXT.
- 2026-08-03: The final independent red-team found no integration, destination, or abstraction
  defect and returned four concrete source findings: UInt64 lexical ordering, eager allocation
  outside memory authority, flattened PostgreSQL/I/O error ownership, and unchecked NUMERIC
  display scale. All four were repaired in one closure pass. Focused unit/live/integration/strict
  lint passed, and the final release roofline remained green at 0.956x narrow and 1.228x mixed.

## Blockers

None.

## Evidence

- Source child: `.10x/tickets/done/2026-08-03-postgres-source-binary-copy.md`.
- Source evidence: `.10x/evidence/2026-08-03-postgres-source-binary-copy.md`.
- Destination child: `.10x/tickets/done/2026-08-03-postgres-destination-exact-value-text.md`.
- Destination evidence: `.10x/evidence/2026-08-03-postgres-destination-exact-value-text.md`.
- Final integration and closure repair:
  `.10x/evidence/2026-08-03-postgres-source-destination-integration.md`.

## Review

The one independent final red-team review completed after integration. It found no critical issue
and no integration, destination reconstruction, or abstraction-boundary defect. Its three
significant source findings and one minor source finding were all repaired once and mapped to
focused unit, PostgreSQL 17 live, strict Clippy, integration, and two-cell release roofline
evidence. Closure judgment is pass; the user-requested no-iteration boundary is preserved.

Residual risk is limited to the recorded PostgreSQL 17 loopback performance environment and the
absence of an induced remote transport fault. Typed/nested I/O, SQLSTATE ownership, and safe
transport redaction are covered deterministically.

## Retrospective

The useful final gate was composition through compiler-owned source and partition-schedule
authority, not a direct adapter-to-adapter shortcut. That law caught fixture omissions before any
data mutation and now proves metadata and values survive every ordinary runtime boundary.

The final memory finding also clarified that a retained-batch ceiling is not by itself an
allocation ceiling. Decoder structure now has schema-sized authority, payload builders start at
zero capacity under the batch lease, and conservative logical admission leaves capacity/container
headroom without sacrificing the measured roofline. Centralizing source PostgreSQL error ownership
keeps catalog, transaction setup, COPY start, and nested stream reads consistent.
