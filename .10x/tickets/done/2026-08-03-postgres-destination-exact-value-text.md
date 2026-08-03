Status: done
Created: 2026-08-03
Updated: 2026-08-03
Parent: .10x/tickets/done/2026-08-03-postgres-binary-and-exact-value-program.md
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
- 2026-08-03: Execution started after the source dependency closed and was pushed. Validation will
  remain limited to focused destination mapping/encoder/live tests plus the parent integration
  gate.
- 2026-08-03: Added field-aware mapping that reconstructs only the three owned exact-value tags,
  validates their physical declaration, and serializes the tag plus resolved PostgreSQL type in
  the compiled column plan. Untagged, physical-only, and foreign-tagged strings remain TEXT.
- 2026-08-03: Extended the existing buffered binary COPY encoder with direct JSON, JSONB, and
  base-10000 NUMERIC wire values, including PostgreSQL NaN and infinity signs. Replaced the prior
  front-removal numeric grouping with one linear grouping/trim pass; ordinary field dispatch still
  performs only one metadata decision per batch column, not per row.
- 2026-08-03: PostgreSQL SQLSTATE class 22 COPY rejections now remain package-owned Data with the
  server's safe field/location diagnostic. Nested typed I/O errors retain their kind and retry
  delay; other remote COPY failures remain Destination.
- 2026-08-03: Focused unit, plan, strict lint, and PostgreSQL 17 live tests passed. The live case
  proves append, duplicate replay, replace, merge, schema inspection, wide/special NUMERIC, native
  JSON parsing, invalid JSON/range rejection, and rollback with no target or receipt residue. The
  established Decimal128 live regression also passed.

## Blockers

None.

## Evidence

- Exact mapping/preflight: five focused `rows::tests` passed; the exact plan serialization and
  incompatible existing-TEXT target test passed.
- Encoder/provenance: eight binary encoder/provenance tests passed with only the release benchmark
  ignored.
- Live PostgreSQL 17: `live_exact_value_text_round_trips_all_dispositions_and_rolls_back_rejections`
  and `live_decimal128_values_preserve_exact_numeric_text` each passed.
- Compilation/lint: focused test compilation and strict destination test-target clippy passed;
  final formatting and diff checks passed.
- Durable detail: `.10x/evidence/2026-08-03-postgres-destination-exact-value-text.md`.

## Review

The parent ticket owns the single final independent red-team review after its source/destination
integration gate, avoiding a separate iterative child review cycle.

## Retrospective

The most important boundary was to keep native reconstruction field-aware rather than teach the
ordinary `Utf8` mapping to cast. PostgreSQL's binary JSON/JSONB receivers provide the exact native
parser semantics without staging or row-wise SQL, while NUMERIC's compact wire format avoids both
float conversion and server-side text casting. The live fixture also exposed that independent
targets share the same state-mirror lineage in one schema, so the combined test correctly links
its checkpoints instead of bypassing that invariant. The existing DuckDB link flag remains
necessary for this crate's test binary even though the exercised adapter is PostgreSQL.
