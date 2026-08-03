Status: open
Created: 2026-08-03
Updated: 2026-08-03
Parent: .10x/tickets/2026-08-03-postgres-binary-and-exact-value-program.md

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

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending parent final red-team review.

## Retrospective

Pending executor handback.
