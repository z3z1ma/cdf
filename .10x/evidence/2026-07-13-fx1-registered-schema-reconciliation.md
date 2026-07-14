Status: recorded
Created: 2026-07-13
Updated: 2026-07-13

# Registered format schema reconciliation and Arrow IPC access parity

## Observation

Registered codecs correctly decoded physical Arrow batches, but the engine materialized schema constraints only when a pinned discovery observation supplied a precompiled coercion plan. Declared resources have no such observation, and the former first-party readers had hidden that gap by materializing inside their format paths. Once Arrow IPC/CSV/JSON moved behind the neutral registry, declared physical batches reached the contract evaluator unchanged. Arrow IPC therefore failed valid lossless widening and opt-in coercion during actual execution even though discovery and planning had accepted them. The same migration also described the file-framed Arrow IPC driver as sequential although it requires a known length and exact reads, causing transformed IPC inputs to bypass the generic verified-spool policy.

The repaired engine boundary accepts either a trusted precompiled observation plan or an unplanned physical batch. When no plan exists and the physical schema differs from the effective constraint, it derives one reconciliation under the resource's trust/type allowances, materializes it once, and returns the verdict plan for package evidence. Exact schemas retain the zero-work path. This is source- and format-neutral: `cdf-source-files` contains no schema-reconciliation implementation or `cdf-contract` dependency. The Arrow file driver declares seekable access; compressed inputs consequently use the same capability-driven spool policy as any other seek-requiring codec. No generic branch names Arrow IPC, Parquet, compression implementations, or a transport provider.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli --lib tests::declared_arrow_ipc_lossless_widening_records_physical_and_coercion_evidence -- --exact --nocapture` passed: 1 passed, 0 failed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli --lib tests::tier_zero_coerce_types_applies_to_actual_file_execution -- --exact --nocapture` passed: 1 passed, 0 failed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli --lib tests::local_arrow_ipc_discover_pin_show_diff_preview_and_run_share_pinned_schema -- --exact --nocapture` passed: 1 passed, 0 failed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli --lib tests::arrow_ipc_discovery_supports_compression_multi_file_and_remote_without_writes -- --exact --nocapture` passed: 1 passed, 0 failed. The fixture covers valid gzip auto-detection and override, malformed zstd, file-versus-stream framing, multi-file discovery/diff, and a local HTTP provider without schema-store mutation.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine execution::transform_kernel_tests::unplanned_physical_schema_is_constrained_once_at_the_engine_boundary -- --exact --nocapture` passed: 1 passed, 0 failed. The test feeds an unplanned `Int32` physical batch from a neutral kernel batch into an `Int64`/normalized effective constraint and asserts the engine emits the widened array plus typed coercion evidence.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files -j12 --lib` passed: 28 passed, 0 failed. This covers the registry's local/remote/object-store/transform/file-manifest composition after the capability change.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-engine -p cdf-source-files -p cdf-format-arrow-ipc --lib --tests --no-deps -j12 -- -D warnings` passed. The cold/incremental Clippy graph took 5m30s because `cdf-engine` currently reaches the complete DataFusion graph; this is verification evidence, not a runtime benchmark.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine --lib -j12` reached 108 passed, 4 failed, 6 ignored. The new engine-boundary test passed. The failures were the existing HTTP `thread::spawn` static ownership violation, two validation-program planner failures that occur before package execution, and a hard-coded package hash whose preceding one-batch-versus-many-batch equality assertion passed. This command is recorded as a residual inventory, not a green suite claim.
- `cargo fmt --all` completed and `git diff --check` reported no whitespace errors before this record was written.

## What this supports or challenges

This supports FX1's invariant that physical decode and shared schema reconciliation are separate, format-neutral stages using one driver interpretation across discovery/preview/run. The schema constraint now lives at the engine boundary shared by every source rather than leaking into the file adapter. It supports B3's remote/compressed file-framing path and removes the known first-party Arrow IPC compatibility regressions from the FX1 closure tail.

It challenges the prior ticket statement that compressed or remote Arrow IPC discovery remained excluded: those exclusions were legacy assertions and are now replaced with positive conformance.

## Limits

The focused commands do not prove the full workspace, storage-backed Arrow throughput, fuzz robustness, or the not-yet-implemented sequential Arrow stream driver. The execution boundary currently derives a coercion plan only when a source emitted no trusted precompiled observation plan; that is a source-neutral safety repair, not the final P2 architecture. `.10x/decisions/data-onramp-schema-discovery-reconciliation.md` requires declared observations and verdicts to compile into the plan, so FX1 must add that compiler binding and delete the fallback before closure. FX1 also still requires its project-level external-provider add/pin law and aggregate adversarial closure review.

## Subsequent correction

The temporary execution-derived fallback and the limits it imposed were removed by the compiler-bound work recorded in `.10x/evidence/2026-07-13-fx1-compiler-bound-schema-observations.md`. This record remains the evidence for the original regression and Arrow IPC access correction; it is not authority for the current reconciliation architecture.
