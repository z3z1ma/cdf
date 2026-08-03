Status: done
Created: 2026-08-03
Updated: 2026-08-03

# Exact-value adapter audit

## Question

Do the first-party source and destination adapters consistently preserve exact database numerics
and exact-value text envelopes without silent floating-point conversion or accidental native-type
reconstruction?

## Sources and methods

Inspected every current `crates/cdf-source-*` and `crates/cdf-dest-*` package, its active mapping
spec where present, the Arrow decimal limits used by CDF, and focused mapping tests. The review
distinguished a database's stored native value from an input lexeme or declared affinity that the
database may already have normalized.

Relevant implementation authorities included:

- `crates/cdf-source-postgres/src/catalog.rs` and `source.rs`
- `crates/cdf-dest-postgres/src/sheet.rs`, `rows.rs`, and `binary_copy.rs`
- `crates/cdf-source-clickhouse/src/types.rs` and `execution.rs`
- `crates/cdf-dest-clickhouse/src/mapping.rs`
- `crates/cdf-source-sqlite/src/catalog.rs` and `source/execution.rs`
- `crates/cdf-dest-sqlite/src/mapping.rs`
- `crates/cdf-source-glue/src/schema.rs` and `crates/cdf-source-iceberg/src/execution.rs`
- `crates/cdf-dest-duckdb/src/package.rs` and `crates/cdf-dest-parquet/src/sheet.rs`
- the active MongoDB source and destination specifications

## Findings

### PostgreSQL

The destination already preserves Arrow Decimal128/256 exactly as PostgreSQL NUMERIC using binary
base-10000 encoding. The source currently rejects NUMERIC/JSON/JSONB discovery and uses a row-wise
portal, so the approved binary source mapping and tagged exact-text reconstruction are genuine
gaps. They are owned by the PostgreSQL child tickets created with this audit.

### ClickHouse

The source and destination preserve ClickHouse Decimal32/64/128/256 as Arrow decimals through
precision 76 and reject declarations outside their explicit range. No text fallback or float
conversion is needed for the supported ClickHouse decimal domain.

### Glue and Iceberg

Glue's schema parser maps decimal precision through 38 to Decimal128 and through 76 to Decimal256,
rejecting Arrow-inexpressible declarations. Iceberg execution preserves Arrow decimal physical
widths. These mappings are consistent with the exact-domain rule.

### SQLite

SQLite has INTEGER and REAL storage classes, not a native arbitrary-precision decimal storage
class. Its NUMERIC/DECIMAL declaration is affinity rather than an exact stored numeric domain. The
source's Float64 discovery for REAL/NUMERIC affinity preserves stored REAL values and rejects
inexact integer or text coercion unless the contract explicitly enables lossiness. The destination
stores Arrow Decimal128/256 as canonical TEXT and labels the mapping lossless. No automatic
text-to-native restoration is appropriate because SQLite has no corresponding exact native type.

### DuckDB and Parquet

DuckDB accepts Arrow decimal precision through 38 and explicitly rejects Decimal256 because it
cannot preserve it losslessly. Parquet preserves Decimal128 and Decimal256 as logical decimals.
Both are consistent with the rule.

### Files, REST, and SQL destination commons

File formats retain the schema semantics of their selected format driver; REST does not own a
native exact database numeric type; and `cdf-dest-sql` owns transactional mirror lifecycle rather
than SQL dialect type mapping. None is an authority for implicit exact-value text reconstruction.

### MongoDB

The active source specification's former unconditional BSON Decimal128-to-Arrow-Decimal128 mapping
was not sound for schemaless fields, variable exponents, and special values. The source and
destination specifications now require a schema-proven Arrow Decimal128 domain or
`cdf:semantic=mongodb_decimal128_value_text_v1`, with same-native reconstruction based only on that
exact tag. The existing implementation tickets own this behavior before MongoDB ships.

## Conclusions

The shared decision in `.10x/decisions/exact-value-text-fallbacks.md` matches every shipped adapter
without requiring a broad retrofit. PostgreSQL is the only shipped code gap: its source lacks the
new discovery/binary path and its destination does not yet own tagged text reconstruction. MongoDB
had one specification-level inconsistency, corrected before implementation. MySQL must apply this
decision during its connector shaping rather than introduce a separate numeric policy.

## Limits

This was a static source/spec audit, not a live database round-trip or throughput run. The owning
implementation tickets require focused live evidence for PostgreSQL, and the existing MongoDB
tickets require Decimal128 live evidence when that adapter is implemented.
