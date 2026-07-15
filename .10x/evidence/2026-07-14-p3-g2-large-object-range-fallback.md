Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 G2 large-object generation-bound range fallback

## Observation

Adaptive remote inputs no longer require a complete local spool when the finite object is larger than the configured spool ceiling or when the shared spill coordinator cannot atomically reserve its complete length. A strong, known-length source with enforceable exact ranges instead remains on its original generation-bound `ByteSource`; the registered format driver reads it without consuming spool disk.

Weak identities, sources without exact-range authority, transformed byte streams, and unknown lengths do not receive this fallback. They retain verified sequential-spool or streaming behavior and fail cleanly when their required disk admission cannot be satisfied.

## Procedure

The `remote_parquet_uses_admitted_spool_or_generation_bound_ranges` source-runtime test executes the same 100,000-row Parquet object through three disk states:

1. A complete spool reservation is available. Execution uses the growing spool, produces 100,000 rows, records spill peak at least as large as the object, and releases the reservation to zero.
2. The per-object spool ceiling is one byte. Execution succeeds through generation-bound exact ranges, produces the same 100,000 rows, and records zero current and peak spill bytes.
3. Another owner holds enough of the shared spill budget that the object cannot be reserved atomically. Execution again succeeds through generation-bound exact ranges; the pre-existing reservation remains unchanged during execution and releases to zero when its owner drops.

The growing-spool capability test also sets the spool ceiling below the object size while using a weak source. The source is rejected for lacking strong generation and exact-range authority before any fallback can be selected.

Verification:

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files --lib --locked -j 12
test result: ok. 46 passed; 0 failed; 0 ignored

CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-files --lib --no-deps --locked -j 12 -- -D warnings
Finished successfully

git diff --check
Finished successfully
```

## What this supports or challenges

This supports the bounded-disk rule in `.10x/specs/remote-local-io-overlap.md`: a retained full/growing spool is admitted only when its entire finite length is reserved before transfer, and reservation pressure is a policy selection signal rather than an unavoidable execution failure when a generation-safe range path exists.

The selection remains format- and destination-neutral. Source orchestration branches only on byte-source capabilities and atomic disk admission; the registered format driver owns its access pattern.

## Limits

This is a correctness and resource-boundedness slice, not G2 closeout. It does not yet establish high-BDP saturation, range coalescing, bounded readahead/waste telemetry, adaptive controller feedback, cryptographic cache promotion, or monotone prefix eviction. Direct exact ranges can be request-heavy until those remaining controller stages land. Unbounded row streams and rolling replay retention remain separate stream-epoch work and never enter this finite-object spool path.
