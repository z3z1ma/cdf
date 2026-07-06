Status: active
Created: 2026-07-05
Updated: 2026-07-05

# Types, contracts, and normalization

## Purpose and scope

This specification governs firn's type fidelity rules, identifier normalization, nested-data policy, contract compiler, validation chain, trust presets, and bounded transforms. It derives from book Chapters 6 and 10 and decisions D-3, D-6, D-9, D-14, and D-15.

## Type system

Arrow is firn's closed canonical type system. firn MUST NOT invent a parallel logical type lattice.

Arrow field metadata MUST carry these annotations where applicable: `firn:semantic`, `firn:source_name`, and `firn:null_origin`. Semantic metadata MUST influence policy, redaction, and destination mapping, but MUST NOT require custom physical kernels.

Decimals MUST remain Arrow decimal values and MUST NOT silently become floats. Zoned timestamp meaning MUST be preserved: UTC-normalized timestamps retain zone story in metadata when needed; naive timestamps MUST NOT be silently assumed UTC.

Destination type mapping MUST be declared data in destination sheets. Lossy mappings require explicit contract allowance. Unsupported mappings fail plan time.

## Identifiers and nested data

Source-original names MUST be preserved verbatim in schema metadata. Destination identifiers MUST be derived by versioned normalizer `namecase-v1`: Unicode NFC, lower snake case, destination charset filter, and deterministic truncation/hash suffix for over-length names or collisions. Post-normalization collisions are plan-time hard errors.

Arrow `Struct`, `List`, and `Map` values are first-class. The normalization policy MUST support keep-nested, deterministic child-table expansion, and variant capture into `_firn_variant` tagged as `json`. Promoting a variant to typed columns is a contract-evolution event.

## Contract compiler

Contracts MUST compile policy plus observed schema into a serializable validation program. The same program MUST be rendered by `firn explain`, executed by `ContractExec`, and stored in packages.

The validation program MUST include schema verdicts, column programs, and row dispositions. The verdict lattice MUST be total: every relevant cell/row receives exactly one disposition.

The operator chain SHOULD be:

```text
ResourceBatchStream
  -> SchemaFingerprintExec
  -> ContractExec
  -> NormalizeExec
  -> ProfileExec
  -> LineageExec
  -> PackageSink
```

Quarantine MUST be a framework-owned side channel with row, rule id, error code, source position, and redacted observed value. PII-tagged values MUST be hashed or otherwise redacted in artifacts.

## Policy and trust

Policy vocabulary MUST include schema evolution, type coercion/fidelity, row rules, verdicts, quarantine, and fail behavior named by the book. Default policy is `evolve`. `freeze` MUST be available per resource and through presets.

Trust levels MUST compile into concrete presets:

- `experimental`: evolve, variant capture, sampled profiling, quarantine off.
- `governed`: evolved columns with review artifact, full validation, quarantine on, packages retained.
- `financial`: frozen schema, decimal/timezone enforcement, full lineage, required receipts, reconciliation counts, long retention.
- `serving`: frozen schema, freshness SLO, sampled fast path after clean runs, demote on anomaly.

Promotion and demotion MUST be recorded ledger events. New resources run discovery-depth validation regardless of trust; stable clean runs MAY promote where trust permits; drift, anomalies, or quarantine demote back to full depth.

## Transforms

firn MAY perform only in-flight, per-batch, schema-stable transforms: rename, cast, redact, derive, filter, and nested expansion/variant policy. Cross-resource joins, whole-table modeling, and post-load model graphs belong downstream.

## Acceptance criteria

- Decimal and timestamp fidelity tests fail if silent float conversion or timezone assumption occurs.
- Identifier collision fixtures fail at plan time with rename hints.
- The contract compiler emits one serialized validation program used by explain, execution, and package evidence.
- Quarantine artifacts redact `pii:*` values while preserving enough evidence to diagnose rule failure.

## Explicit exclusions

This spec does not define package hashing, checkpoint commit, destination receipts, or concrete CLI command parsing.

