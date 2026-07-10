Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-a10c-exhaustive-local-binary-discovery.md, .10x/tickets/done/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md

# P2 WS-A10d effective schema runtime and per-file evidence

## Scope

Execute compatible multi-file resources against an immutable baseline pin and a plan-time effective schema. Materialize missing nullable fields, allow distinct verified per-file coercion plans targeting one effective schema, and stamp baseline/effective/manifest/per-file evidence into plans and packages.

## Acceptance criteria

- Plans distinguish baseline snapshot, effective schema, and discovery manifest hashes; package identity includes all three and their verified references.
- `evolve` admits compatible additions/widenings into the effective schema without modifying `cdf.lock` or the baseline snapshot.
- `freeze` keeps the baseline effective; incompatible disposition is deferred only to A10e, not misclassified or crashed.
- Readers materialize typed null arrays for compatible missing fields and preserve per-file physical provenance.
- Different preserved/widened/missing decisions may coexist across files when they target the exact effective schema; malformed or spoofed per-file evidence fails closed.
- Validation/package artifacts serialize deterministic per-file coercion/verdict evidence rather than collapsing it into one plan.
- Destination planning/normalization consumes the effective schema uniformly and replay needs no source contact.
- Legacy plan/package/snapshot deserialization remains compatible through additive omitted defaults.

## Evidence expectations

Compatible multi-file package runs, widening/missing/null/provenance inspection, package verification/replay, tamper/legacy fixtures, destination plan checks, semver/golden gates, and adversarial review.

## Explicit exclusions

No terminal file quarantine, quarantine-only package, processed-file checkpoint advancement, remote enumeration, or final S2/S6/S8 promotion.

## Progress and notes

- 2026-07-09: Opened after ratification of the immutable-baseline/effective-schema split.
- 2026-07-10: Implemented the runtime evidence spine in progress: immutable baseline snapshot authority remains on the descriptor/lock; `EnginePlan` now has a one-time pre-1.0 non-exhaustive optional extension carrying the effective snapshot-schema identity, a separately named/recomputed effective Arrow-schema fingerprint, discovery-manifest reference, and exact typed `SchemaCoercionPlan` per file. Kernel runtime facts are neutral identities plus a physical-schema catalog deduplicated by canonical physical schema hash; planner recomputes every catalog hash and compiles each file against the exact effective schema through the existing reconciliation lattice. Partition metadata is only a redundant physical-hash binding, not resource-level authority, and repeated partitions for one file are supported.
- 2026-07-10: Compatible missing nullable fields now compile as typed-null materialization decisions; readers emit typed null arrays and a zero-row typed evidence batch for empty valid files. Engine package evidence is deterministic per file, distinct plans may coexist, spoofed/malformed batch plans fail closed, and package artifacts separately name baseline snapshot, effective snapshot, effective Arrow, discovery manifest, physical schemas, and coercion programs. Destination planning consumes the effective schema while the baseline pin and `cdf.lock` remain unchanged.
- 2026-07-10: Focused verification in progress. `cargo check -p cdf-cli -p cdf-engine -p cdf-formats -p cdf-contract` passed before final report/test naming adjustments; `cdf-contract` passed 59/59. The end-to-end multi-file CLI regression is being hardened to cover widening, missing typed nulls, an empty compatible file, package verification/tamper rejection, and replay after source deletion.
- 2026-07-10: Execution verification complete for handoff. The focused multi-file regression now proves three planned files (widened, missing nullable field, and empty compatible input), typed-null destination rows, distinct deterministic per-file coercion evidence, package verification, replay after source/state/destination deletion, and tamper rejection. The engine limit regression proves a bounded limit opens and attests only the attempted file while retaining typed authority for both planned files. Full `cargo nextest run --workspace --locked --no-fail-fast` passed 855/855; all three committed live-run destination goldens passed unchanged. Formatting, workspace all-target/default/all-feature/no-default checks, all-feature Clippy with `-D warnings`, doctests, docs, and `cargo semver-checks --workspace --baseline-rev origin/main` passed. Ticket remains open for parent-owned independent evidence, adversarial review, and closure bookkeeping.
- 2026-07-10: Parent review repairs replaced delimiter-based Arrow fingerprints with a length-prefixed recursive structural encoding over every Arrow `DataType`, schema/field metadata, child names, nullability, and nested identity. Adversarial tests prove metadata delimiter safety, deterministic map ordering, and nested child name/nullability/metadata sensitivity. `cdf-formats` delegates to this single `cdf-contract` authority.
- 2026-07-10: P0 boundary repair moved all concrete local Parquet/Arrow runtime-observation selection behind the project schema-discovery adapter. Generic CLI scan orchestration now issues one `prepare_pinned_resource_effective_schema` request for both existing and newly pinned resources and contains no concrete format branch. A source-free regression proves non-observable formats return unchanged without snapshot or source contact.
- 2026-07-10: The kernel/engine seam is now source-neutral: typed `cdf:schema_observation_id` metadata binds partitions to unique schema observations, the file adapter maps canonical file locations to that identity, generic planner/executor code contains no path lookup, and repeated partitions may share one observation. Runtime authority, catalogs, plan coercions, execution checks, and package sidecars use observation-neutral names while preserving exact per-observation coercion plans and a deduplicated physical-schema catalog.
- 2026-07-10: New kernel and engine authority structs are non-exhaustive with intrinsic validated constructors/accessors and separate resource-attachment validation; this avoids requiring a temporarily `Discover` probe descriptor to impersonate pinned authority. Existing public `ResourceSchemaDiscoveryArtifacts` and `EnginePlan` received the same narrow pre-1.0 non-exhaustive migration, with a constructor for the former. Package-specific semver checks against `HEAD` each report exactly that intentional major-class migration and no other failure.
- 2026-07-10: Regression hardening now pins one-file `int32` baseline bytes first, then adds `int64` widening, an added nullable field, a missing-field file, and an empty file. Plan/run prove `cdf.lock`, baseline snapshot bytes/reference, and snapshot inventory remain byte-identical while effective plan/destination/package evidence evolves; replay remains source-free and tamper detection remains closed. A Financial/freeze regression proves multiple conforming observations keep the exact baseline-effective schema, while a compatible addition/widening fails at plan time with the named A10e disposition boundary before package, state, or destination writes.
- 2026-07-10: Post-review package gates passed: `cdf-engine` 35/35, `cdf-project` 133/133, and `cdf-cli` 243/243. Focused fingerprint, source-boundary, repeated-observation, immutable-baseline evolve, and freeze regressions all pass. Parent owns the final workspace-wide rerun, evidence record, adversarial review, and closure.
- 2026-07-10: Parent closure verification passed 859/859 workspace tests, strict workspace Clippy, doctests/docs, supply-chain gates, kernel/contract semver, and the two isolated recorded pre-1.0 extension migrations. Evidence is `.10x/evidence/2026-07-10-p2-a10d-effective-schema-runtime.md`; adversarial review is `.10x/reviews/2026-07-10-p2-a10d-effective-schema-runtime-review.md`; the governing extension decision is `.10x/decisions/effective-schema-runtime-authority.md`.

## Blockers

None.
