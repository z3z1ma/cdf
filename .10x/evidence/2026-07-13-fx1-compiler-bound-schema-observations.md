Status: recorded
Created: 2026-07-13
Updated: 2026-07-13
Relates-To: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Compiler-bound declared and pinned file-schema observations

## Observation

File resources now compile physical-schema observations and coercion verdicts before extraction for both declared constraints and pinned discovery baselines. The evidence carrier names the baseline kind explicitly (`declared` or `pinned`), catalogs every physical Arrow schema in the matched file set, binds each planned partition to one observation and source identity, and records the effective schema identity separately from its structural Arrow hash.

Runtime observation is exhaustive even when the initial discovery snapshot was sampled. Sampling remains truthful initial evidence; it no longer permits a runnable plan to omit a file that execution may open. Declared constraints preserve their declaration hash as the effective schema identity while recording the independently computed physical and structural Arrow hashes.

The engine no longer derives a reconciliation plan from an unplanned physical batch. Exact batches retain the zero-work path; a physical mismatch without compiled evidence fails closed with a re-plan instruction. Width widening, explicit parse coercion, and lossy allowances are taken from the resource policy during planning and serialized as per-observation verdicts.

The public project auto-pin helper now returns a plan-ready resource. After publishing the snapshot it rehydrates the verified baseline, observes the complete runtime file set through the injected provider, and attaches the evidence before returning. This closes the prior gap where auto-pin produced a pinned resource that still depended on execution-time inference.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo check -p cdf-kernel -p cdf-project -p cdf-engine -p cdf-cli --tests -j12` passed after the evidence-shape and compiler changes.
- `target/debug/deps/cdf_engine-dfcb8528621c2b24 'execution::transform_kernel_tests::unplanned_physical_schema_fails_closed_before_execution' --exact --nocapture` passed: 1 passed, 0 failed.
- `target/debug/deps/cdf_cli-b846818c2ea33476 'tests::declared_arrow_ipc_lossless_widening_records_physical_and_coercion_evidence' --exact --nocapture` passed: 1 passed, 0 failed. The verified package contains a declared baseline plus per-observation `Int32` to `Int64` widening evidence.
- `target/debug/deps/cdf_cli-b846818c2ea33476 'tests::tier_zero_coerce_types_applies_to_actual_file_execution' --exact --nocapture` passed: 1 passed, 0 failed. The declared NDJSON resource succeeds only because its opt-in parse coercion is compiled; the execution fallback is absent.
- `target/debug/deps/cdf_project-ed4b00a3ca96b01c 'tests::declared_multi_file_parquet_compiles_every_physical_schema_before_execution' --exact --nocapture` passed: 1 passed, 0 failed. Two Parquet files produce exhaustive manifest participation, two partition observations, and two widening plans.
- `target/debug/deps/cdf_project-ed4b00a3ca96b01c 'tests::http_parquet_auto_pin_plan_preview_and_run_use_file_runtime' --exact --nocapture` passed: 1 passed, 0 failed. An injected HTTP provider completes discovery publication, verified rehydration, planning, preview, package construction, DuckDB commit, and `FileManifest` checkpointing.
- `cargo fmt --all` completed and `git diff --check` reported no whitespace errors.

## What this supports or challenges

This supports the P2 rule that discovery and reconciliation are compiler stages and the FX1 law that codecs emit physical facts while shared planning owns constraints and verdicts. It also proves the behavior is not a single-file Parquet exception: registered Arrow IPC, streaming NDJSON, multi-file Parquet, and an injected remote provider use the same evidence path.

It supersedes the temporary fallback described in `.10x/evidence/2026-07-13-fx1-registered-schema-reconciliation.md`. That fallback was useful regression evidence but is not present in the resulting architecture.

## Limits

These focused checks do not claim the full workspace, performance envelope, fuzz corpus, or every source archetype. SQL and REST compiler-stage observation remain owned by their P2/P3 source tickets; this observation closes FX1's registered file-format boundary. Aggregate FX1 closure still requires a fresh adversarial review of the complete extension surface and any findings it produces.
