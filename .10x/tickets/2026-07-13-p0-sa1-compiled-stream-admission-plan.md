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
- 2026-07-14: Fresh adversarial review returned `fail` with one critical and four significant findings: preobserved branches bypassed verdict validation; replay did not bind the exact admitted/quarantined set, coercions, physical observations, schema epoch, generations, and positions; sparse materialized extras looked for removed physical columns; control-critical `Missing` could be admitted; and materialized row codecs mislabeled output schemas as physical Arrow schemas. Closure remained open while these were repaired.
- 2026-07-14: Replaced the weak evidence shape with typed `PhysicalObservationEvidence` (`ArrowSchema` or explicitly `MaterializedOutput`), exact `StreamAdmissionCompletion`, source-position-bound quarantine evidence, and package companions for both admitted and quarantined observations. Replay now validates the compiled admission epoch, exact observation sets and outcomes, exact coercion plans, physical observation identities, quarantine artifact binding, and source positions before destination mutation. The obsolete per-observation coercion artifact was deleted rather than retained as a compatibility shim.
- 2026-07-14: All execution paths now pass through the compiled verdict program. Control-critical missing fields become the named `schema-observation:control-critical-missing` quarantine; sparse materialized extras reuse their already-captured residual evidence; repeated partitions coalesce identical quarantine evidence and reject conflicting evidence.
- 2026-07-14: Closed the same authority gap for terminal evidence: a recorded quarantine must bind an exact physical Arrow schema and match the compiled admission action (rule, error, policy, and remediation). Planning and replay now reject a quarantine that the compiled program would admit or classify differently.
- 2026-07-14: Empty Parquet and Arrow IPC inputs now carry physical schema evidence in a zero-row batch. Parquet owns its schema-only decode unit inside the format driver, so the generic engine neither performs a discovery fallback nor branches on format identity.
- 2026-07-14: The closure repair replaced per-observation embedded schemas with a hash-keyed physical-observation catalog. Admitted and quarantined evidence now reference an exact catalog set, so package size is O(unique physical schemas) rather than O(files), unused observations are rejected, raw Arrow admissions are recomputed against the compiled program, and quarantine field evidence must match the compiled action exactly.
- 2026-07-14: Added an explicit batch observation representation at the kernel boundary. Raw Parquet/Arrow inputs remain `ArrowSchema`; fixed-schema JSON/CSV, REST, and Postgres identify decoder-materialized output. Replay validates materialized output against the compiled effective schema, while per-row residual completeness remains non-serialized batch evidence and cannot destabilize physical-schema identity across batches.
- 2026-07-14: Limited multi-batch execution now records a non-checkpointing partial attempt with exact attempted position, observed-row extent, and canonical planned-partition/source binding. Replay binds that evidence back to `plan/scan.json`, verifies complete file generation identity or the declared cursor/log scope, and fails closed for a position shape the plan cannot prove.
- 2026-07-14: Fully consumed sources that intentionally have no checkpoint position now record `CompleteUnpositioned` with the same canonical planned-partition binding. This keeps snapshot-style REST/SQL/custom sources replay-verifiable without manufacturing a cursor or misclassifying a complete scan as partial; only positioned completions enter checkpoint evidence.
- 2026-07-14: CSV and NDJSON codecs now emit one schema-bearing zero-row batch for empty inputs, matching the columnar codec law. Source-materialized extra fields must carry an explicit complete residual-capture attestation; sparse candidates are accepted only under that attestation, while missing, duplicate, or out-of-batch evidence fails closed.
- 2026-07-14: Broad engine verification exposed and repaired the remaining exact-authority regressions. Terminal quarantine fixtures now obtain the complete action from the compiled program rather than duplicating its diagnostics; materialized evidence derives its canonical output schema from the fixed effective schema while retaining the decoder observation hash and allowed nullable residual fields; and the package rechunking golden was regenerated only after proving one-batch and multi-batch inputs still produce the same identity.
- 2026-07-14: Immutable review of `efe8d6fb` failed with three significant replay findings: omission of the only admission artifact was accepted for partial/unpositioned packages; materialized provenance and nullable residual identities were not canonical/exact; and partial row extent plus cursor/log value was not cross-bound to execution evidence. Closure remained open. The repair makes the stream-admission artifact unconditional, validates every materialized decision's target/types/outcome/reason/fixes and exact nullable delta, and adds per-observation execution lineage so replay matches the exact planned partition, source generation, observed input rows, and aggregate attempted position.

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
- Exact artifact replay: `CARGO_BUILD_JOBS=12 cargo test -p cdf-project --lib artifact_replay_ -- --nocapture` passed 7/7, covering reconstruction and pre-mutation rejection of corrupted identities and preimages.
- Engine matrix after review repair: `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine --lib` produced 113 passes and 6 ignored tests. Its SA1 failure exposed repeated quarantine evidence consumption; after the coalescing repair, the exact failing test passed. The remaining unrelated failure is `production_runtime_ownership_is_centralized`, already owned by `.10x/tickets/2026-07-11-p3-g1-streaming-transport-byte-sources.md` because `cdf-transport-http` still contains `thread::spawn`.
- Empty columnar evidence: `CARGO_BUILD_JOBS=12 cargo test -p cdf-format-parquet empty_parquet_file_emits_one_schema_bearing_batch` and `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime schema_bearing_empty_batch_retains_its_arrow_container_bytes` passed.
- Fixed-schema and quarantine integrations: the focused pinned three-file Parquet, sampled-discovery drift, and financial-freeze CLI tests all passed, including the admitted and quarantine companion evidence.
- Quarantine authority: `recorded_schema_quarantine_must_match_the_compiled_admission_action`, all three terminal schema-attestation tests, preview terminal quarantine, and zero-segment receipt recovery passed after replacing their formerly arbitrary quarantine fixtures with compiled-valid physical deviations.
- Static guard: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-engine -p cdf-project -p cdf-cli -p cdf-format-parquet -p cdf-format-arrow-ipc -p cdf-runtime --tests --no-deps -- -D warnings` passed.
- Closure-repair focused checks: exact engine tests for compiled-evidence tampering, completed-versus-attempted partition handling, and positioned partial attempts passed. `cdf-format-json` and `cdf-format-delimited` exact empty-input tests each passed with one schema-bearing materialized batch.
- Affected graph: `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine -p cdf-project -p cdf-cli -p cdf-format-json -p cdf-format-delimited -p cdf-formats -p cdf-source-postgres -p cdf-source-rest --no-run` completed successfully.
- Replay and multi-file integration: the exact partial source-position/generation binding test passed; `CARGO_BUILD_JOBS=12 cargo test -p cdf-project --lib artifact_replay_ -- --nocapture` passed 7/7; and the pinned multi-file Parquet fixed-schema/in-stream-admission CLI regression passed.
- Final engine matrix: `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine --lib` passed 116 tests with 6 intentional performance/stress ignores. The sole failure remains the pre-existing P3 G1 ownership guard for `cdf-transport-http` containing `thread::spawn`; no SA0/SA1 test failed.
- Static guard: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-engine -p cdf-project --tests --no-deps -- -D warnings` passed after moving the new replay test module behind production items; the preceding all-affected-crate invocation reached the same sole ordering lint and exposed no other warning.
- Post-immutable-review repair: the new canonical materialized-provenance/nullable-delta regression and exact partial lineage row/position tamper regression passed. `CARGO_BUILD_JOBS=12 cargo test -p cdf-project --lib artifact_replay_ -- --nocapture` again passed 7/7 with mandatory stream evidence and lineage fixtures. The final engine matrix passed 117 tests with 6 intentional ignores; its sole failure remains the separately owned P3 G1 `thread::spawn` ownership guard.

## Review

Pending fresh adversarial review of the integrated SA0/SA1 batch.

## Retrospective

The hidden coupling was stronger than the initial compile error suggested: planning required every partition to have a physical observation, execution assumed every admitted batch needed a coercion object, and reports read quarantine only from plan-time evidence. Making `Unobserved` and terminal stream quarantine explicit removed all three pre-scan dependencies. Materialized sources need a distinct validation path because their Arrow payload already has the fixed schema while their evidence still describes the physical input. Exact replay also requires physical evidence for zero-row and quarantined inputs; absence of rows is not absence of schema, so codecs must surface schema-bearing empty outcomes at their own boundary.
