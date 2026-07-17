Status: recorded
Created: 2026-07-16
Updated: 2026-07-16

# P3 B2 exact Parquet pushdown and selective-range closeout

## Observation

The native Parquet driver now advertises a pinned comparison-operator vocabulary and applies exact predicates through Arrow/Parquet's late-materialized `RowFilter`. Projection and predicate evaluation share one Arrow-native expression implementation with engine residual/transform execution; no Parquet evaluator, expression parser, or source-specific semantic shim was added. Exact pushdown is admitted only when every planned partition has a recorded physical observation, the exact CDF function identity is supported, and every predicate column has the same physical/effective Arrow type. Unknown observations and every coercion remain residual, so filtering cannot bypass reconciliation or quarantine.

Adaptive remote access now distinguishes compiled full coverage from compiled selective coverage. Full/unknown Parquet scans retain the verified sequential growing/evicting spool policy. Only a projection that proves at least one physical root can be omitted enters the same generation-bound `ByteSource` range path used by the codec; predicate-only scans remain sequential until recorded statistics prove selectivity. This saves transfer bytes without restoring the pathological unconditional range strategy.

## Procedure

1. Extracted the engine's Arrow-native bound-expression implementation into the leaf `cdf-expression` crate. Engine execution and Parquet row filters both consume it; the superseded engine-local module was deleted.
2. Added `predicate_operators` to `FormatDriverDescriptor`, validated fidelity/vocabulary coherence, and pinned Parquet driver semantics as `1.1.0` with exact `=`, `!=`, `>`, `>=`, `<`, and `<=` support. Negotiation proves the expression against each planned physical observation through the shared lowering and requires type identity; absent observations, width coercions, unsupported types, and foreign namespace/version identities remain residual.
3. Mapped normalized logical predicate columns to `cdf:source_name` at the generic file boundary before codec invocation. The recorded scan plan remains logical; only the physical decode request is translated.
4. Built `RowFilter` predicates from the shared compiled Arrow evaluator. Predicate columns may be absent from the final projection; NULL and multiple-predicate behavior are exercised together. Eager whole-file page-index loading was rejected and removed because its retained parsed metadata was not ledger-accounted and could force a growing spool to the suffix before first decode. Exact row filtering stays bounded by row-group units. Page-level pruning may be added only as a measured, per-unit accounted access plan.
5. Kept adaptive full and unknown-selectivity scans on sequential spool and admitted strict-subset projection plans to exact ranges only when the opened source provides generation-bound exact ranges. Strong object-store sources take ranges; weak or non-range-capable remote sources complete through explicit full spool. The fixture proves both successful outcomes without a format or provider branch in orchestration.
6. Ran:
   - `cargo test -p cdf-format-parquet -p cdf-source-files --lib --locked -j 12`
   - `cargo test -p cdf-conformance property_fuzz_parquet_parser_never_panics_or_emits_partial_malformed_bytes --locked -j 12 -- --nocapture`
   - `cargo test -p cdf-conformance parquet_late_page_corruption_fails_without_publishing_partial_read --locked -j 12`
   - `cargo clippy -p cdf-format-parquet -p cdf-source-files --all-targets --locked -j 12 -- -D warnings`
   - `cargo clippy -p cdf-expression -p cdf-engine -p cdf-runtime -p cdf-project -p cdf-conformance --all-targets --locked -j 12 -- -D warnings`

## Results

- Parquet driver tests: 5 passed. The exact-predicate fixture evaluates two predicates over a nullable unprojected column, drops the NULL row under the same shared residual semantics, returns only the requested column and row, and reads less than the complete object after the prepared metadata phase.
- File source tests: 62 passed. Production negotiation records exact predicate pushdown only under per-partition physical equivalence, a widening observation remains residual, a hostile function namespace/version remains residual, physical `VendorID` is reached from logical `vendor_id`, and weak selective remote input completes through spool while generation-bound strict-subset input uses ranges.
- Malformed Parquet coverage passed 64 generated corrupt-footer cases plus a valid-footer late-column-page corruption case; neither panicked nor returned a partial materialized read.
- Strict Clippy passed across the new shared expression crate and every touched runtime, codec, source, project, engine, and conformance target.
- Existing closure measurements remain authoritative because this slice does not alter the full-scan hot path: raw FineWeb decode is approximately `0.90x` raw arrow-rs (`.10x/evidence/2026-07-14-p3-b2-prepared-parquet-decode-session.md`); the governed live HTTPS FineWeb run is `1.10x` its contemporaneous sequential-transfer floor (`.10x/evidence/2026-07-14-p3-g2-fineweb-growing-spool-overlap.md`); and the fixed-package jobs matrix proves identity invariance while the four-file FineWeb curve records multicore scaling (`.10x/evidence/2026-07-14-p3-c4-fineweb-local-scaling.md`).

## What this supports

This closes B2's remaining predicate and selective remote-range gaps while preserving the format/source/transport extension laws. Combined with the previously recorded prepared metadata session, deterministic row-group frontier, conservative byte envelopes, growing/evicting spool policy, jobs matrix, and measured envelope, every B2 acceptance criterion now has evidence. Whole-file page-index retention is an explicitly rejected implementation strategy, not a deferred compatibility path: it conflicts with the constant-memory and remote-overlap laws. The current exact row filter is unit-bounded; any future page-level pruning must be per-unit, accounted, and justified by a measured win.

## Limits

The deterministic corruption fixture exercises the current native writer's late compressed column payload; it is not every compressor's corpus. Arrow-rs remains the fail-closed page decoder, with generated malformed-footer coverage around the boundary. DataFusion statistics pruning over recorded package/segment evidence is a separate P3 J1 concern and is not represented as Parquet identity-bearing decode logic here.
