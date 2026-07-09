Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-b3-parquet-declared-schema-reconciliation.md
Verdict: pass

# P2 WS-B3 Parquet declared-schema reconciliation review

## Target

Reviewed the B3 implementation in `crates/cdf-formats/src/readers.rs`, `crates/cdf-formats/src/tests.rs`, `crates/cdf-formats/Cargo.toml`, `Cargo.lock`, and `crates/cdf-declarative/src/file_runtime.rs`.

## Findings

- Pass: `read_file_source_with_declared_schema` now has a Parquet-specific branch instead of delegating declared Parquet reads to the undeclared physical path.
- Pass: The implementation uses Parquet's physical Arrow schema as the observed fact, calls the shared B2 reconciler, and builds the `FormatRead` from the reconciled schema after successful reconciliation.
- Pass: Supported Arrow casts are materialized with Arrow 59.1 kernels, and focused tests prove `int32 -> int64` and `float32 -> float64` preserve values.
- Pass: Source-name projection and rename work through `cdf:source_name`, extra physical fields are dropped from the declared projection, and `cdf:physical_type` provenance survives into the output schema.
- Pass: A lossy narrowing fails at reconciliation time with a contract error naming observed type, declared type, and the B2 operator fix. The test exercises the fail-closed path before any caller receives batches.
- Pass: The undeclared Parquet path remains separate and covered by a regression test, reducing the risk that ordinary physical reads changed by accident.
- Pass: `cdf-declarative` now routes non-empty declared Parquet schemas through the declared-schema reader gate used by preview/run callers.
- Minor: The Parquet format boundary currently uses default type policy because the read API does not yet receive resource policy. This is acceptable for B3's automatic-width-widening scope and remains covered by later WS-B integration work.
- Minor: `jscpd` reported two small clone blocks. One is a local Parquet open/metadata pattern and one is existing test setup; neither justifies abstraction in this focused slice.

## Verdict

Pass. B3 satisfies its ticket acceptance criteria and does not widen into discovery, package, conformance, or policy-plumbing work that belongs to later P2 children.

## Residual Risk

The main residual risk is policy and conformance coverage depth: explicit `allow_lossy_mapping` and `coerce_types` cannot affect this Parquet path until project policy is threaded into `cdf-formats`, and WS-I still needs source-archetype conformance for the P2 golden paths.
