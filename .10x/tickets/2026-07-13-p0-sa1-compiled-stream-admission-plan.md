Status: active
Created: 2026-07-13
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md

# P0 SA1: compiled stream-admission plan IR

## Scope

Compile one source/codec-neutral, total stream-admission operation against a fixed output schema, integrate it at the neutral decode/engine boundary, and serialize its exact physical-observation/verdict evidence into packages without execution-time reparsing, reoptimization, or policy invention.

## Non-goals

No observation cache, retained cold-discovery payload handoff, dynamic-producer lifecycle, or destination behavior. SA1 may remove the generic pre-decode discovery fallback required to consume its admission operation; SA3 retains ownership of cold retained-window/spool fusion.

## Acceptance criteria

- Plan pins baseline/effective schema, codec semantics, normalizer, contract/type allowances, control fields, cache-key shape, and total verdict choices.
- Execution can instantiate the operation from a physical Arrow observation without compiler/source crate imports.
- Plan/package/replay validation rejects missing or mismatched observation/verdict authority.

## References

- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/specs/residual-variant-capture.md`

## Assumptions

Exact semantics are user-ratified in `.10x/decisions/fixed-schema-discovery-and-stream-admission.md`.

## Journal

- 2026-07-14: Execution began after SA0 removed pinned current-schema discovery. The first pinned HTTP Parquet run failed closed because `VendorID`'s physical schema differs from the normalized fixed schema and the engine had no serialized generic admission operation. SA1 will compile the type lattice, identifier policy, schema/residual verdicts, control fields, fixed schema identities, cache-key shape, and exact compiled-source semantics into `EnginePlan`; runtime observations may instantiate that frozen program but may not invent policy.
- 2026-07-14: `CompiledSchemaAdmissionPlan` is now mandatory in `EnginePlan`. It binds baseline/effective/resource/constraint schemas, normalizer and identifier policy, type allowances, schema/residual/row verdict programs, control-critical fields, cache-key shape, contract-program hash, and available compiled-source driver/options/physical-plan identity. Execution caches one instantiated coercion plan per physical schema hash, records per-partition evidence, and never mutates the schema epoch.
- 2026-07-14: `PhysicalDecodeRequest` now carries `DecodeSchemaPlan`: either verified physical evidence from cold/deep discovery or a fixed-admission schema for an ordinary pinned run. Parquet and Arrow IPC observe their actual schema while opening decode; row codecs receive a source-name decoder schema derived from the immutable pin, avoiding a normalized-name recovery slow path. The source-level `driver.discover` execution fallback was deleted.
- 2026-07-14: Packages record `plan/schema-admission.json` and `schema/stream-admission-evidence.json`; replay validates the program against the recorded validation program, validates evidence-to-plan identities, and requires physical-observation evidence for nonempty packages before destination mutation.
- 2026-07-14: Removed the remaining assumption that every planned partition was preobserved. Sampled cold evidence binds source generation for all candidates, binds physical schema only for probed candidates, and compiles unobserved candidates into the same immutable admission program. Preview and run now open those candidates once and classify them in-stream.
- 2026-07-14: Added terminal in-stream schema quarantine and residual capture. An incompatible first batch quarantines its file/partition with exact field evidence and advances the processed manifest; extra fields become `_cdf_variant` candidates without expanding the fixed output schema. A schema epoch change after an admitted batch fails closed unless the codec isolates it as a separate partition.
- 2026-07-14: Materialized REST and SQL sources now retain truthful physical observation identity while validating their already-applied coercion evidence. Temporarily nullable REST fields are admitted only when exact residual candidates justify the nullable decode window. Exact materialized SQL batches generate replay-verifiable preserved admission evidence instead of using an evidence-free shortcut.
- 2026-07-14: Explicit discovery against a pinned baseline once again honors configured file sampling. The deleted `runtime_baseline => sample_files=None` branch was the test-protected source of exhaustive pre-observation called out by the user.
- 2026-07-14: Tightened total-verdict enforcement for runtime-instantiated and source-materialized coercion plans. Unknown fields and widenings now require the exact compiled verdict, codec-provided residual candidates are reused without duplication, missing extra-field evidence is completed from the physical batch, and an admitted unknown field under a fixed evolve schema is preserved through the compiled residual capture rather than silently dropped.

## Blockers

None. FX1's compiled format binding prerequisite is committed and evidenced.

## Evidence

- Mandatory plan authority and semantic mismatch: `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine engine_plan_requires_recorded_schema_authorities compiled_stream_admission_is_replay_verifiable_and_rejects_mismatched_evidence -- --nocapture` passed as two focused invocations.
- Format-neutral decode boundary: all tests in `cdf-format-parquet`, `cdf-format-arrow-ipc`, `cdf-format-delimited`, and `cdf-format-json` passed; focused file-source Parquet, CSV, and JSON stream tests passed.
- Package/replay gate: focused project tests `artifact_replay_reconstructs_delta_and_commit_request_from_package_files` and `artifact_replay_rejects_corrupted_or_missing_preimages_before_mutation` passed. The latter now covers missing/tampered admission plan and stream evidence before mutation.
- Pinned multi-stage integration: `tests::http_parquet_auto_pin_plan_preview_and_run_use_file_runtime` passed and the verified pinned package contains both admission artifacts.
- Stream admission/replay integrity: `compiled_stream_admission_is_replay_verifiable_and_rejects_mismatched_evidence` passes and now rejects a recorded lossy verdict not authorized by the compiled type policy.
- Multi-file fixed-schema behavior: `pinned_multi_file_parquet_keeps_fixed_schema_and_admits_new_physical_schemas_in_stream` passes with a pinned one-column plan, extra-field residual capture during extraction, exact three-file checkpoint position, replay without source contact, and tamper detection on stream-admission evidence.
- Sampled drift: `sampled_discovery_renders_every_cli_path_and_routes_unseen_drift_to_package_quarantine` passes. Its explicit pin remains sampled, preview opens the previously unobserved candidate through the payload path rather than attesting a plan-time quarantine, and run records two admitted stream observations plus exact quarantine/checkpoint evidence for the incompatible file.
- Materialized sources: the broad `cdf-cli` run passed both REST strict-coercion regressions and both Postgres discovery/run regressions, including validation of the Postgres package's stream-admission evidence against its compiled plan.
- Broad CLI observation: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli --lib` passed 278/291 tests. Every SA0/SA1 lifecycle, multi-file, sampled, REST, Postgres, ad-hoc, replay, and quarantine regression passed. The 13 remaining failures are previously recorded renderer/CLI-registry/schema-promotion integration residuals owned outside SA0/SA1; this result is not claimed as a globally green suite.
- Final focused guard matrix: `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine --lib compiled_stream_admission -- --nocapture` passed both admission/replay tests; `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli --lib pinned_multi_file_parquet_keeps_fixed_schema_and_admits_new_physical_schemas_in_stream -- --nocapture` passed after proving fixed-schema extra fields remain in `_cdf_variant` with no duplicate residual candidates.

## Review

Pending fresh adversarial review of the integrated SA0/SA1 batch.

## Retrospective

The hidden coupling was stronger than the initial compile error suggested: planning required every partition to have a physical observation, execution assumed every admitted batch needed a coercion object, and reports read quarantine only from plan-time evidence. Making `Unobserved` and terminal stream quarantine explicit removed all three pre-scan dependencies. Materialized sources need a distinct validation path because their Arrow payload already has the fixed schema while their evidence still describes the physical input.
