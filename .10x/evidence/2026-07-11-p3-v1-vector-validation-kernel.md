Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-v1-vector-kernel-plan.md, .10x/tickets/2026-07-11-p3-ws-v-vectorized-validation.md

# V1 vector-validation kernel evidence

## What was observed

`cdf-contract` now binds serialized validation programs to an Arrow schema once and evaluates native rules into packed bitmaps. Field lookup, type checks/downcast selection, range/domain literal preparation, and regex compilation are absent from the row loop. The output separates masks/aggregates from selected quarantine-value materialization. Nullability reuses Arrow validity bits; range and domain kernels downcast once and operate on typed values; timestamp units, canonical domain spellings, float NaN/signed-zero identity, regex, freshness, null behavior, disposition precedence, summaries, candidate order, and redaction match the scalar evaluator.

## Procedure

- `cargo test -p cdf-contract --lib` — 82 passed, two ignored performance tests before the final packed-mask assertion; the focused final suite also passed.
- `cargo clippy -p cdf-contract --all-targets -- -D warnings` — passed.
- `cargo test --release -p cdf-contract vector_numeric_range_reference_rate --lib -- --ignored --nocapture` — `1.73 GiB/s` over 131,072,000 rows for a 64k mixed range/string-domain/nullability mask workload.
- `cargo test --release -p cdf-contract vector_full_evaluation_scalar_ratio --lib -- --ignored --nocapture` — scalar `2152.935 us/batch`, vector `240.401 us/batch`, `8.96x` speedup for semantically identical full all-pass evaluation.
- Differential proptest generated nullable Int32 range/domain/null combinations up to 4,096 rows and compared accepted masks, candidate order/values, and complete summaries.
- Fixed matrices cover all signed/unsigned widths, Float32/64, Utf8/LargeUtf8, timestamp units, regex, freshness, nulls, NaN, signed zero, invalid domain spellings, schema drift, and binding errors.

## What this supports or challenges

The measured mixed-kernel rate exceeds the P3 ≥1 GB/s/core target without counting uninspected columns. The full evaluator comparison shows the speedup survives summary construction and the no-candidate evidence path. The all-failure test proves retained decision masks scale at one bit per rule/row plus accepted/quarantine unions rather than one object per row.

## Limits

This is the V1 kernel boundary, not production integration. V2 must replace the engine's scalar evaluator, account selected evidence, preserve golden packages, and show macro-phase improvement. V3 still owns the complete host/profile/density matrix and permanent slow-tier regression gate. Regex necessarily evaluates each string but is a prebound native kernel, not an untyped fallback.
