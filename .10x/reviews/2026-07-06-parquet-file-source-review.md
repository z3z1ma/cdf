Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md
Verdict: pass

# Parquet file source review

## Target

Review of the `cdf-formats` Parquet file-source implementation, tests, dependency changes, and quality evidence for `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md`.

## Assumptions tested

- The implementation avoids the blocked direct arrow-rs `parquet -> paste` dependency path.
- File-source scope, source position, schema hash, descriptor, and batch behavior match existing CSV/JSON/NDJSON reader conventions.
- DuckDB Parquet reads do not introduce SQL injection through path handling.
- Arrow 58 to Arrow 59 conversion is deterministic and does not bypass existing CDF batch-building checks.
- Tests exercise parser success, malformed data, package replay, and mutation-sensitive reader branches.

## Findings

No blocking findings.

The Parquet query uses `SELECT * FROM read_parquet(?)` with a bound parameter, so the file path is not interpolated into SQL text. The result is serialized through DuckDB Arrow IPC and then read by the existing Arrow 59 IPC reader path, which reuses `build_output` for descriptor, observed-schema hash, batch ids, file scope, and file-manifest source position.

The dependency diff adds `duckdb` and a renamed `duckdb-arrow` package for Arrow 58 IPC bridging. Lockfile and manifest scans show no direct `parquet` or `paste` package entries, and advisory scanners pass without policy ignores.

Mutation testing initially found a missed existing JSON branch: deleting the top-level `Value::Object(_)` handling in `json_document_to_ndjson` survived. A focused single-object JSON assertion was added, and the rerun finished with 0 missed mutants.

## Verdict

Pass. The ticket acceptance criteria are supported by `.10x/evidence/2026-07-06-parquet-file-source-quality.md`.

## Residual risk

Parquet remains a lossy Arrow projection, so field metadata is not treated as canonical at this boundary. That is consistent with the book and package lifecycle records: Arrow IPC remains canonical for exact schema metadata, while Parquet is archive/interchange/analytics.
