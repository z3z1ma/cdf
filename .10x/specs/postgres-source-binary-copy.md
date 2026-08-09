Status: active
Created: 2026-08-03
Updated: 2026-08-08

# PostgreSQL source binary COPY

This specification is governed by `.10x/decisions/exact-value-text-fallbacks.md`.

## Contract

The PostgreSQL table source MUST retain its existing read-only repeatable-read snapshot, compiled
projection/filter/order/limit authority, cancellation/join ownership, exact source positions, and
32 MiB emitted-batch ceiling while replacing row-oriented portal extraction with bounded binary
`COPY (SELECT ...) TO STDOUT` extraction.

The `SELECT` inside `COPY` MUST remain the canonical type boundary. It MUST cast every projected
column to the exact PostgreSQL output type owned by the compiled Arrow field before data transfer.
The binary decoder MUST NOT infer or decode arbitrary native table types. A pre-stream descriptor
check MUST prove the canonical output column count, order, names, OIDs, nullability contract, and
Arrow mapping before the first payload byte can become visible to package execution.

The decoder MUST validate the binary COPY signature, flags, extension length, tuple field count,
signed field lengths, fixed-width lengths, text UTF-8, numeric structure, stream trailer, and EOF.
It MUST reject malformed or over-bound input before an allocation exceeds the admitted source
window. It MUST build Arrow columns directly without PostgreSQL `Row` objects or per-cell owned
intermediate vectors. Batches SHOULD target the largest row prefix that remains within the existing
32 MiB retained-output ceiling.

The source roofline MUST compare the CDF path to a direct official-client binary `COPY OUT` path
performing the same canonical casts, acknowledgement/EOF work, and exact row/value verification.
The release median MUST reach at least 0.90 of that direct roofline. Cross-connection range
partitioning is excluded until a semantically equivalent single-stream cell demonstrates that one
server connection cannot saturate the relevant CPU or network envelope.

## JSON and JSONB

PostgreSQL `json` and `jsonb` MUST discover as Arrow `Utf8` with their exact physical type retained
in field metadata. The canonical projection MUST use `column::text`; binary COPY therefore carries
the text type's binary bytes rather than requiring a CDF-owned native JSONB decoder.

For PostgreSQL `json`, this preserves the stored input text. For PostgreSQL `jsonb`, this is
lossless with respect to the stored PostgreSQL JSON value: parsing the emitted text back to `jsonb`
MUST compare equal to the source datum. It does not and cannot preserve whitespace, object-key
order, duplicate keys, or escape spelling that PostgreSQL discarded when the value entered `jsonb`.
Documentation and metadata MUST identify JSON as
`cdf:semantic=postgres.json_text@1` and JSONB as
`cdf:semantic=postgres.jsonb_text@1`, not claim lexical JSON preservation.

## NUMERIC and DECIMAL

PostgreSQL `numeric` and `decimal` are exact arbitrary-precision types whose domain exceeds Arrow
Decimal256 and includes `NaN`; unconstrained numeric additionally admits positive and negative
infinity. CDF MUST NOT map them to floating point.

The user-ratified discovery policy is:

- A constrained finite `NUMERIC(p,s)` whose precision and scale fit Arrow Decimal128 discovers as
  `Decimal128(p,s)`.
- A constrained finite `NUMERIC(p,s)` outside Decimal128 but within Arrow Decimal256 discovers as
  `Decimal256(p,s)`.
- Unconstrained numeric, precision above 76, or scale outside Arrow Decimal256 discovers as `Utf8`
  containing PostgreSQL's canonical exact numeric text, with the physical numeric declaration and
  `cdf:semantic=postgres.numeric_text@1` retained in metadata.
- `NaN` in a field compiled as Arrow Decimal is a typed Data failure with a remediation to declare
  that field as `Utf8`; `Infinity` and `-Infinity` are accepted by the unconstrained-text mapping.
- A user-declared `Utf8` numeric field always uses `column::text` and preserves finite and special
  values. A user-declared Decimal field MUST prove compatible precision/scale before streaming and
  MUST reject, never round, values outside its exact Arrow domain.

Catalog discovery MUST read numeric precision and scale rather than treating the bare
`information_schema.data_type` string as sufficient authority. The binary numeric decoder MUST
decode PostgreSQL's base-10000 representation directly into the selected Arrow decimal integer and
MUST reject nonzero discarded digits, overflow, scale mismatch, and special values not representable
by that Arrow field.

## Scenarios

Given a UUID, JSON, or JSONB source column, when the compiled field is `Utf8`, then PostgreSQL casts
it to text before binary COPY and CDF emits exact UTF-8 bytes under the field's physical metadata.

Given a `NUMERIC(38,9)` finite value, when discovery compiles the field, then CDF emits an exact
`Decimal128(38,9)` value without a floating-point or text round trip.

Given a `NUMERIC(60,18)` finite value, when discovery compiles the field, then CDF emits an exact
`Decimal256(60,18)` value.

Given unconstrained numeric, precision above 76, an out-of-range scale, or numeric infinity, when
discovery compiles the field, then CDF emits PostgreSQL canonical numeric text and retains the native
numeric authority in metadata.

Given a constrained numeric `NaN` compiled as Decimal, when extraction encounters the value, then
the source fails Data before publishing a partial batch and names the `Utf8` declaration remedy.

Given any native source type whose declared CDF field is `Utf8` and which PostgreSQL can cast to
text, when extraction begins, then the canonical text result type is descriptor-checked before COPY
and the binary path preserves the existing declared-schema compatibility surface.

## Exclusions

This tranche does not add CDC/logical replication, native Arrow extension dependencies, automatic
data-scanning schema inference, or speculative table partitioning. Native read-query resources are
governed by `.10x/specs/postgres-native-query-source.md`. This contract does not call `jsonb` text
lexically identical to the JSON originally submitted to PostgreSQL.

## Acceptance Criteria

- Existing Postgres source conformance, projection, filter, order, limit, cursor, retry, lifecycle,
  schema, and memory laws pass unchanged through the binary path.
- Live coverage proves JSON, JSONB, UUID, constrained Decimal128/256, unconstrained/wide numeric,
  negative and above-precision scales within supported Arrow bounds, NULL, `NaN`, and infinities.
- Fragmented COPY frames, malformed lengths/types/trailers, cancellation, early server errors, and
  over-bound text/numeric values fail with typed ownership and no visible partial batch.
- A fair release roofline reaches at least 0.90 of direct binary COPY OUT for representative narrow
  numeric and mixed text/decimal schemas.
