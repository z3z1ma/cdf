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

## Blockers

None. FX1's compiled format binding prerequisite is committed and evidenced.

## Evidence

- Mandatory plan authority and semantic mismatch: `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine engine_plan_requires_recorded_schema_authorities compiled_stream_admission_is_replay_verifiable_and_rejects_mismatched_evidence -- --nocapture` passed as two focused invocations.
- Format-neutral decode boundary: all tests in `cdf-format-parquet`, `cdf-format-arrow-ipc`, `cdf-format-delimited`, and `cdf-format-json` passed; focused file-source Parquet, CSV, and JSON stream tests passed.
- Package/replay gate: focused project tests `artifact_replay_reconstructs_delta_and_commit_request_from_package_files` and `artifact_replay_rejects_corrupted_or_missing_preimages_before_mutation` passed. The latter now covers missing/tampered admission plan and stream evidence before mutation.
- Pinned multi-stage integration: `tests::http_parquet_auto_pin_plan_preview_and_run_use_file_runtime` passed and the verified pinned package contains both admission artifacts.

## Review

The engine previously had two schema authorities: preobserved per-file coercion evidence and an implicit exact-schema fallback. Making the admission program mandatory removed that split. The necessary source change was smaller and more general than teaching each codec about planning: codecs expose physical Arrow reality; the engine alone applies the frozen admission program. Carrying both authority and decoder schemas prevents destination normalization from penalizing row-codec fast paths.

## Retrospective

Pending.
