Status: recorded
Created: 2026-08-03
Updated: 2026-08-03

# PostgreSQL source-to-destination exact integration evidence

## Observation

The ordinary compiled project runtime now has one retained PostgreSQL integration law that starts
from live catalog discovery, resolves the PostgreSQL source driver, reads canonical binary COPY,
publishes and verifies an Arrow package, commits through the PostgreSQL destination, verifies the
receipt, inspects native target declarations, and compares source and target values on the server.

The law covers JSON text including duplicate keys, JSONB canonical text, Decimal128(38,9),
Decimal256(60,18), tagged NUMERIC(77,1), tagged unconstrained NUMERIC infinities, and numeric-looking
ordinary TEXT. JSON/JSONB compare their native text output exactly; numerics and ordinary text
compare by native equality; the runtime package is independently inspected for its exact Arrow
types, semantic tags, and physical declarations.

The final parent red-team found no integration, destination reconstruction, or abstraction-boundary
defect. Its four source findings were repaired in the same authorized closure pass: UInt64 ordering
uses numeric source order while transfer remains text, decoder and batch allocations are leased
before allocation, PostgreSQL/I/O error provenance retains its owner, and finite/special NUMERIC
display-scale headers are validated including negative-scale behavior.

## Procedure and results

- `cargo check -p cdf-source-postgres --tests -j 12` passed.
- Focused source tests passed: seven `binary_copy::tests`, two `error::tests`, and the UInt64
  order/limit regression.
- The PostgreSQL 17.10 live
  `live_binary_copy_preserves_json_uuid_and_exact_numeric_domains` regression passed.
- The single conformance test
  `run_matrix::postgres_exact::postgres_binary_source_to_native_destination_preserves_exact_values`
  passed after the final source repairs: one passed, 108 filtered out.
- Strict test-target Clippy passed separately for `cdf-source-postgres` and `cdf-conformance` with
  `-D warnings`.
- The final release PostgreSQL 17.10 roofline used 500,000 rows and five alternating samples. The
  narrow cell passed at `0.955591x`; the mixed text/decimal cell passed at `1.227744x`. Raw evidence
  is `.10x/evidence/.storage/2026-08-03-postgres-source-roofline-final.json`.
- Formatting and `git diff --check` passed on the closure diff.

The first integration fixture attempt omitted the runtime-required ordered cursor; the second used
an unbound direct plan instead of compiler source/schedule authority. Both failed before source or
destination mutation. The retained law now uses an exact `id` cursor and the normal registered
compiled-source binding, which is the production execution path.

## Error-ownership audit

The frozen source scope is
`.10x/evidence/.storage/2026-08-03-postgres-source-error-scope.txt`. Its eight Internal construction
sites are individually classified in
`.10x/evidence/.storage/2026-08-03-postgres-source-internal-ledger.tsv`; there are no direct
`ErrorKind::Internal` constructions, so the arithmetic remainder is zero. Server SQLSTATE owners,
transport I/O, nested typed CDF errors, catalog discovery, transaction setup, COPY start, and COPY
stream reads now share one source-owned classifier.

## What this supports

- Exact source metadata reaches package authority and selects only native same-PostgreSQL
  reconstruction; ordinary TEXT remains TEXT.
- The binary source and native destination compose through generic orchestration without an
  adapter-specific branch or float conversion.
- The complete 32 MiB source authority is split between schema-sized decoder structure and the
  batch lease. Zero-capacity builders allocate only after admission, and one-third logical
  admission leaves bounded capacity/container headroom before retained-size reconciliation.
- Correctness repairs did not compromise the ratified `>=0.90` source roofline.

## Limits

Live and performance evidence is PostgreSQL 17 loopback on Apple Silicon. Remote transport fault
classification is unit-tested through nested I/O/typed owners rather than induced on a remote
database. Cross-connection partitioning, arbitrary SQL, and CDC remain excluded.
