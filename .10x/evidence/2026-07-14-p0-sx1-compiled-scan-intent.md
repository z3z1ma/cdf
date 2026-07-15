Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# Compiled scan intent is the single source-execution work artifact

## Observation

Every planned partition now carries one required, versioned `CompiledScanIntent`. Generic engine validation proves that the artifact agrees with the canonical scan request, source predicate classification, and declared capability vocabulary. File, REST, and Postgres adapters consume that artifact directly; no source-specific serialized projection/filter/limit/order representation remains.

Exact file projection remains evidence-correct under pinned discovery. The plan projects the complete physical observation to the actual codec output schema and hashes that projected observation for admission, while the file adapter retains the complete discovery schema separately to detect source generation/schema changes before decode. Arrow IPC and Parquet both emit an observed hash matching the projected Arrow payload.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-format-arrow-ipc --lib`
  - Passed 2 tests; 1 release performance test ignored by design.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files -p cdf-source-postgres -p cdf-source-rest --lib`
  - Passed 53 file, 11 Postgres, and 7 REST tests.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine -p cdf-declarative -p cdf-format-parquet --lib`
  - Passed 132 engine tests with 6 intentional slow/release ignores, all 82 declarative tests, and all 4 Parquet tests.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-engine -p cdf-format-parquet -p cdf-format-arrow-ipc -p cdf-source-files -p cdf-source-rest -p cdf-source-postgres -p cdf-declarative -p cdf-subprocess -p cdf-conformance --all-targets --no-deps -- -D warnings`
  - Passed with no warnings.

Permanent falsification includes:

- planner plus package execution with pinned full-schema evidence and projected physical batches;
- production Parquet negotiation delivering projection to the registered codec;
- Arrow IPC projected payload/hash agreement;
- Tier-A file planning remaining a full scan;
- a generic engine rejection for any Tier-A adapter that attempts source pushdown, plus file/REST/Postgres/declarative full-scan conformance;
- Postgres/REST metadata absence with adapter execution derived from the typed intent;
- REST capabilities derived from the executable cursor parameter and fidelity, with missing parameters remaining unsupported;
- compiled file capability drift rejection;
- supported URI schemes normalized case-insensitively and unknown schemes rejected before source contact;
- reversed Parquet projection order for row-bearing and empty files, retaining field metadata and matching the observed schema hash.

## What it supports

- The bounded scan-intent prerequisite recorded by P3 B2 is complete.
- Projection no longer requires Parquet/file-specific partition metadata or generic runtime dispatch.
- Serialized evidence describes the same work source adapters execute.

## Limits

- File predicate pushdown remains unsupported because the format descriptor does not yet publish a safe operator vocabulary; B2 owns predicate/page-index compilation and equivalence evidence.
- SX1 remains active for the registry-open declarative envelope and remaining add/doctor/Python lifecycle hooks.
- This record is correctness and architecture evidence, not B2 throughput-envelope closure.
- The broader project/conformance runs still contain failures owned by active artifact-version, golden-promotion, lifecycle, and admission tickets; this record does not claim those suites are green. The observed failures include pre-current golden identities, renamed discovery evidence keys, the active receipt version, and an inexact-cursor lag aggregation mismatch.

## Review

The initial independent review failed on duplicate source-specific execution artifacts and six related capability/evidence contradictions; those were repaired. A second independent review failed on four remaining cross-adapter contradictions; those were repaired. Fresh re-review then found and repaired an empty-Parquet double permutation and missing REST capability revalidation. Final verdict: `pass`, with no critical or significant residual risk. The typed `FileTransportScheme` also replaced repeated string allowlists so compile, discovery, and runtime dispatch share one transport vocabulary.
