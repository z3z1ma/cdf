Status: done
Created: 2026-08-03
Updated: 2026-08-03
Parent: .10x/tickets/done/2026-08-03-postgres-binary-and-exact-value-program.md

# PostgreSQL source binary COPY

## Scope

Replace the PostgreSQL table source's row-oriented portal extraction with bounded canonical binary
COPY OUT. Add exact discovery and execution for JSON, JSONB, constrained Arrow Decimal128/256, and
tagged canonical-text NUMERIC domains that Arrow cannot represent, while preserving all existing
source semantics and the 32 MiB emitted-batch ceiling.

## Non-goals

Arbitrary SQL resources, CDC/logical replication, cross-connection partitioning, private runtimes,
or speculative generic SQL-source abstraction.

## Acceptance Criteria

- `COPY (SELECT ...) TO STDOUT (FORMAT BINARY)` reuses the existing canonical projection casts and
  retains repeatable-read snapshot, pushdown/order/limit, cursor, cancellation, retry, join, source
  position, and batch-memory behavior.
- A pre-stream descriptor check proves output count, order, names, OIDs, and Arrow mapping; native
  table types are never guessed from COPY payloads.
- The bounded decoder validates header, flags, extension, tuples, lengths, fixed-width values,
  UTF-8, numeric structure, trailer, EOF, fragmentation, and allocation bounds, and builds Arrow
  columns without PostgreSQL `Row` or full-batch per-cell owned intermediates.
- Discovery and live execution prove UUID-to-text, JSON/JSONB tagged stored-value text,
  Decimal128/256, tagged unconstrained/wide/out-of-scale NUMERIC text, NULL, `NaN`, infinities,
  negative scale, overflow, and explicit user-declared `Utf8` compatibility.
- Decimal values never pass through floating point; incompatible special values or values outside a
  pinned Arrow domain fail Data before publishing a partial batch with the `Utf8` remedy.
- Focused unit, source conformance, live PostgreSQL, and malformed-stream tests pass.
- A release benchmark with identical query, casts, acknowledgement/EOF work, and value verification
  reaches at least 0.90 of the official-client direct binary COPY OUT roofline.

## References

- `.10x/specs/postgres-source-binary-copy.md`
- `.10x/decisions/exact-value-text-fallbacks.md`
- `.10x/research/2026-08-03-exact-value-adapter-audit.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/specs/source-extension-runtime-contract.md`

## Assumptions

- Scope and all semantic defaults are user-ratified and record-backed by the referenced active
  specification and decision.
- One binary stream is the default until the required roofline evidence proves it insufficient.

## Journal

- 2026-08-03: Ticket opened after specification ratification. Current code rejects JSON/JSONB and
  NUMERIC discovery and streams portal rows in 8,192-row fetch groups.
- 2026-08-03: Execution started. Validation is intentionally limited to focused source unit/live
  tests and the PostgreSQL source roofline until the parent integration gate.
- 2026-08-03: Replaced portal-row extraction with one read-only repeatable-read binary COPY stream.
  The canonical SELECT now casts before COPY, is descriptor-checked for names/OIDs, and is
  catalog-checked for nullability plus complete decimal-domain compatibility before payload bytes
  are exposed.
- 2026-08-03: Added a bounded direct-to-Arrow decoder for fixed values, fragmented UTF-8, and exact
  PostgreSQL base-10000 NUMERIC. Contiguous client buffers are borrowed directly on the hot path;
  the bounded scratch fallback retains arbitrary fragmentation support. Decimal scale reduction
  occurs before accumulation so valid 76-digit values cannot overflow a padded intermediate.
- 2026-08-03: Discovery now maps JSON/JSONB to tagged value text, constrained NUMERIC to
  Decimal128/256, and Arrow-inexpressible NUMERIC domains to tagged canonical text while retaining
  exact physical declarations, including signed negative scale from PostgreSQL typmod.
- 2026-08-03: Added the official-client binary COPY OUT roofline with identical queries, Arrow
  construction, EOF work, and per-value verification. The final PostgreSQL 17 release run passed
  at 1.028x for the fixed-width cell and 1.192x for mixed text/Decimal128/Decimal256.
- 2026-08-03: The parent final red-team found UInt64 lexical ordering, eager builder allocation,
  flattened PostgreSQL/I/O provenance, and unchecked NUMERIC display scale. The authorized closure
  repair orders UInt64 numerically before text transfer, splits the 32 MiB memory authority into a
  schema-sized decoder lease and allocation-safe batch lease, centralizes typed error ownership,
  and validates canonical finite/special display scale including negative-scale columns.

## Blockers

None.

## Evidence

- `.10x/evidence/2026-08-03-postgres-source-binary-copy.md` records the reproducible focused
  validation and limitations.
- `.10x/evidence/.storage/2026-08-03-postgres-source-roofline.json` records raw release samples,
  medians, dispersion, CPU/RSS, batch bounds, exact content checksums, host/comparability identity,
  and the passing 1.028x/1.192x cells.
- `cargo test -p cdf-source-postgres --lib -j 12` passed 20 tests with only the explicitly live test
  ignored. Malformed framing, fragmentation, EOF, allocation bounds, special NUMERIC, discarded
  digits, nullability preflight, and the 20-group Decimal256 boundary are covered.
- The focused PostgreSQL 17 exact-value live test passed for UUID, JSON, JSONB, Decimal128,
  Decimal256, negative scale, scale above precision, precision above 76, unconstrained NUMERIC,
  NULL, `NaN`, infinities, and explicit `Utf8` compatibility.
- The existing focused project test passed four jobs settings through package publication,
  checkpoint commit, exact cursor position, and jobs-invariant identity.
- Focused source/roofline Clippy with `-D warnings`, formatting, and `git diff --check` passed.
- Parent closure evidence and the final 0.956x/1.228x roofline are recorded in
  `.10x/evidence/2026-08-03-postgres-source-destination-integration.md`.

## Review

The child closure review passed its original scope. The program-level final red-team later found
four concrete source defects; all were repaired once under the parent and validated with focused
unit/live/integration/roofline evidence. No second iterative review cycle was opened.

## Retrospective

- `information_schema.numeric_scale` exposed PostgreSQL's encoded negative-scale representation;
  decoding `pg_attribute.atttypmod` was the stable authority for exact signed precision/scale.
- Per-base-10000-digit reader calls were the mixed-schema bottleneck. Borrowing contiguous
  `BufRead` fields and retaining one bounded fragmented fallback moved the mixed cell from 0.727x
  to above the required 0.90 roofline without changing semantics.
- A fair direct source control must construct the same Arrow output. Omitting Arrow construction
  measured a different product and understated the legitimate direct-library roofline work.
- Base-10000 padding can temporarily exceed 76 decimal digits. Exact scale reduction must happen
  before i256 accumulation, with discarded digits proved zero, rather than after an overflowing
  intermediate is built.
