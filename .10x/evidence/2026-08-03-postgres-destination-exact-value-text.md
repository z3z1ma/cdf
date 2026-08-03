Status: recorded
Created: 2026-08-03
Updated: 2026-08-03

# PostgreSQL destination exact-value text evidence

## Observation

The PostgreSQL destination reconstructs only its three owned exact-value `Utf8` tags as native
JSON, JSONB, or NUMERIC. The compiled `PostgresColumn` retains the semantic tag and resolved target
declaration, ordinary/physical-only/foreign-tagged strings remain TEXT, and the existing buffered
binary COPY path sends native wire values directly. NUMERIC text is encoded as base-10000 digits
or PostgreSQL's native special signs without floating point; JSON and JSONB are parsed by
PostgreSQL itself.

PostgreSQL 17 live evidence covers append, duplicate replay, replace, Merge `ON CONFLICT`, target
schema inspection, unconstrained and 77-digit NUMERIC, negative infinity/infinity/NaN, native JSON
numeric normalization, invalid JSON, constrained NUMERIC range rejection, and rollback with no
target or receipt residue. The pre-existing Decimal128 live path also remains exact.

## Procedure and results

- `cargo check -p cdf-dest-postgres --tests -j 12` passed.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-postgres --lib rows::tests -j 12` passed 5.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-postgres --lib binary_copy::tests -j 12`
  passed 8 and ignored only the established release benchmark.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-postgres --lib
  exact_value_plan_retains_semantic_and_rejects_existing_text_target -j 12` passed 1.
- `DUCKDB_DOWNLOAD_LIB=1 TEST_DATABASE_URL=postgresql://cdf@127.0.0.1:55440/postgres cargo test
  -p cdf-dest-postgres --lib
  live_exact_value_text_round_trips_all_dispositions_and_rolls_back_rejections -j 12 --
  --nocapture` passed 1 against PostgreSQL 17.10.
- The same focused live command for `live_decimal128_values_preserve_exact_numeric_text` passed 1.
- `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-dest-postgres --tests -j 12 -- -D warnings`
  passed on the final code and tests.
- `cargo fmt -p cdf-dest-postgres`, `git diff --check`, and focused schema compilation passed.

The first local test link omitted the repository's required `DUCKDB_DOWNLOAD_LIB=1` environment
and failed because `-lduckdb` was unavailable. Repeating the identical focused targets with the
established flag passed; no product failure was hidden.

## Error-ownership audit

The frozen scope is
`.10x/evidence/.storage/2026-08-03-postgres-destination-error-scope.txt`. Reproduce its complete
Internal inventory without a temporary manifest with:

```sh
xargs rg -n -- 'CdfError::internal|ErrorKind::Internal' < .10x/evidence/.storage/2026-08-03-postgres-destination-error-scope.txt
```

The two scoped files contain 24 Internal construction sites, all individually classified in
`.10x/evidence/.storage/2026-08-03-postgres-destination-internal-ledger.tsv`; there are zero direct
`ErrorKind::Internal` constructions, so the arithmetic remainder is zero. Malformed package
metadata/text and PostgreSQL SQLSTATE class 22 rejections are Data, raw destination COPY failures
remain Destination, and a typed `CdfError` nested through I/O retains its kind, message, and retry
delay while gaining safe COPY context.

## What this supports

- Exact owned tags, and only those tags, select native reconstruction.
- Planning retains and revalidates semantic identity plus the resolved declaration.
- The direct binary path preserves wide, constrained, finite, and supported special NUMERIC values
  without a float conversion.
- Native JSON/JSONB parsing and transactional rejection preserve package and receipt atomicity.
- Ordinary Arrow mappings and the existing Decimal128 path remain operational.

## Limits

This is focused PostgreSQL destination evidence, not a repeated workspace test suite. The parent
ticket owns one actual PostgreSQL source-to-destination integration gate and one independent final
red-team review.
