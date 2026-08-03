Status: active
Created: 2026-08-03
Updated: 2026-08-03

# Exact value text fallbacks

## Context

CDF should keep exact database values typed whenever Arrow can represent their complete declared
domain. Some native exact domains are wider than Arrow decimals or include special values Arrow
decimals cannot represent. Mapping those values to floating point would silently lose information,
while treating every exact numeric as text would unnecessarily discard useful type information.

The same issue appears in more than one adapter: PostgreSQL `numeric`, BSON Decimal128, and future
native exact types all need one predictable boundary. JSON-like native values also need terminology
that distinguishes preservation of the stored database value from preservation of an input lexeme
the database has already normalized.

## Decision

First-party adapters MUST use the following policy:

- An exact native numeric type maps to Arrow Decimal128 or Decimal256 when its complete declared
  finite domain fits the Arrow precision and scale. It MUST NOT map to floating point.
- When that complete domain cannot fit, the source maps the value to canonical exact `Utf8` and
  records both `cdf:physical_type` and a versioned `cdf:semantic` tag of the form
  `<system>_<type>_value_text_v1`.
- A text envelope is value-lossless, but it is not a claim that downstream consumers retain typed
  arithmetic semantics.
- A native special value encountered in a field compiled as Arrow Decimal fails as typed Data
  before a partial batch is published, with an explicit `Utf8` remedy. A text mapping may carry the
  native system's canonical spelling for special values.
- A destination for the same native type MAY reconstruct it only from the exact versioned semantic
  tag it owns. It MUST NOT infer reconstruction from `cdf:physical_type`, a field name, or ordinary
  `Utf8`. Other destinations preserve the value as ordinary text unless they own a separately
  specified exact conversion.
- JSON-family text fallbacks describe preservation of the stored native value. They MUST NOT claim
  lexical preservation when the source system has already normalized whitespace, ordering,
  duplicates, escapes, or number spelling.

Every first-party adapter mapping table MUST be checked against this policy when an exact native
numeric or a tagged exact-value text envelope is added.

## Alternatives considered

### Map every exact numeric to text

This is universally lossless but discards Arrow decimal arithmetic, validation, and efficient
columnar representation even where the declared domain fits exactly.

### Map every exact numeric to Decimal256

This preserves typing for many values but cannot represent domains wider than 76 digits, scales
outside Arrow's bounds, or native special values. It would make discovery claim a domain Arrow does
not actually possess.

### Use floating point as the common denominator

Rejected because rounding an exact value is silent data corruption.

### Infer native reconstruction from physical metadata

Rejected because physical provenance alone is not an instruction to reinterpret text. A versioned
semantic tag is the explicit round-trip contract and prevents accidental coercion of ordinary text.

## Consequences

PostgreSQL constrained numerics remain typed where possible and wider or unconstrained numerics use
tagged canonical text. BSON Decimal128 remains typed only when schema authority proves one Arrow
decimal domain; otherwise it uses tagged canonical Decimal128 text. PostgreSQL and MongoDB
destinations may reconstruct their own exact tagged envelopes, while unrelated destinations retain
them as text.

ClickHouse, Iceberg/Glue, SQLite, DuckDB, Parquet, MongoDB, PostgreSQL, and subsequent MySQL adapter
mappings must be audited against this boundary. A native destination's text-to-native restoration
is opt-in by exact semantic tag, never a general string cast.
