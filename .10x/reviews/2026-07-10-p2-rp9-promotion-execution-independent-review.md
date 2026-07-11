Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-p2-rp9-promotion-execution-recovery-gc.md
Verdict: fail

# RP9 promotion execution, recovery, and GC independent review

## Target and method

This review independently inspected the active RP9 ticket against `.10x/specs/schema-promotion-corrections.md`, its RP4-RP8 dependency records, the correction-receipt decision, fenced-lease knowledge, the RP9 implementation, command integration, package reader/replay behavior, checkpoint and publication persistence, GC reporting, and the named tests and evidence. The implementer's concerns review was read as an input, but its conclusions were not adopted as authority.

Primary implementation inspected:

- `crates/cdf-project/src/runtime/promotion.rs`
- `crates/cdf-cli/src/schema_command.rs`
- `crates/cdf-cli/src/package_command.rs`
- `crates/cdf-cli/src/tests.rs`
- `crates/cdf-package/src/reader.rs`
- `crates/cdf-package/src/replay.rs`
- `crates/cdf-kernel/src/state.rs`
- `crates/cdf-state-sqlite/src/lease.rs`
- `crates/cdf-state-sqlite/src/run_ledger.rs`
- `crates/cdf-state-sqlite/src/migrations.rs`

## Findings

### Critical — recovery after packaging still depends on mutable source packages

`build_or_load_correction_packages` constructs `correction_package_artifact` before deciding whether an immutable correction package already exists (`crates/cdf-project/src/runtime/promotion.rs:421-487`). That construction rebuilds a source-package index and re-extracts addressed operations from the original residual packages (`:672-776`, `:1219-1236`). Consequently, resume after `AfterCorrectionPackages`, after a destination receipt, after a checkpoint, or after lock publication cannot proceed solely from the persisted correction package if the original residual packages have since been removed, archived, tombstoned, or become unreadable.

This violates the specification's packaged/no-receipt recovery rule and its requirement that later phases recover from verified persisted artifacts without source re-extraction. The named crash test (`schema_promote_execute_recovers_every_persisted_crash_boundary`) leaves all original packages in place, so it does not prove the required source-free replay. Destination settlement completing after lease expiry can be valid only when the resulting receipt/package path remains idempotently recoverable; this dependency currently breaks that guarantee.

### Critical — staged execution authority is not cryptographically re-derived

`SchemaPromotionExecutionPlanArtifact::validate` checks only the artifact version, duplicated promotion/resource identifiers, old lock SHA, and the proposed snapshot's internal hash input (`crates/cdf-project/src/runtime/promotion.rs:393-411`). It does not recompute the RP5 promotion identity or prove that targets, strategies, evidence paths, receipt associations, correction scope, and dry-plan fields are the canonical projection of the typed version-3 snapshot and lock authority. `load_resumable_schema_promotion` trusts that shallow validation (`:1323-1360`). A consistently edited staged `plan.json` can therefore become execution authority without the required identity derivation.

In addition, staging writes the content-addressed schema snapshot through the ordinary snapshot-store write before create-or-verify validation of the plan (`:356-389`). Existing staged snapshot bytes are overwritten rather than rejected or proven identical. Exact staged authority and crash recovery are therefore not yet fail-closed.

### Significant — source package and receipt authority can be silently weakened

`source_package_index` ignores directories whose package reader cannot open and silently replaces duplicate package-hash entries in its map (`crates/cdf-project/src/runtime/promotion.rs:1219-1236`). Selection can therefore change without a named conflict. During extraction, source receipts are reduced to a set of receipt IDs filtered by destination and target (`:688-708`). The code does not verify the full receipt against the package state delta, schema/disposition authority, segment graph, or destination verification protocol. Package manifest verification does not make receipt content part of package identity. A replacement receipt carrying the same ID can pass this comparison.

The immutable correction package may contain exact addresses once built, but the pre-package authority used to select them is insufficiently authenticated. RP9 requires exact source package, receipt, and row-address authority rather than identifier-only association.

### Significant — correction checkpoints can use a different scope from the acquired lease

The executor derives its lease/head scope through `promotion_scope(resource)`, which respects an explicit resource contract. `build_correction_package`, however, hardcodes every state segment to `ScopeKey::SchemaContract { contract: resource_id }` (`crates/cdf-project/src/runtime/promotion.rs:810-831`). A resource with a custom contract can therefore acquire and inspect one scope while writing a checkpoint under another. The current DuckDB fixtures use the implicit resource scope and do not exercise this case.

The same package always writes a null input-checkpoint artifact (`:832`) even when its state-delta preimage contains a parent checkpoint and input position. Package replay validation treats that combination as inconsistent. The correction executor also does not use the ordinary replay-input proof path. Multi-target correction chains therefore lack coherent immutable checkpoint preimages and have no test coverage.

### Significant — post-lock recovery does not reverify complete target authority

When the lock already contains the staged snapshot, `committed_target_report` accepts a committed checkpoint after comparing only package hash, destination, and target on its receipt (`crates/cdf-project/src/runtime/promotion.rs:930-970`). It does not reconstruct and validate the correction plan/receipt contract or call the destination's `verify_correction` operation. Publication can then proceed from weaker evidence than normal settlement.

If a publication event already exists, `verify_publication_authority` validates promotion/resource/schema/lock fields but does not prove that its target set exactly equals the staged target set or reverify the associated checkpoints and receipts (`:1131-1154`). A partial or otherwise mismatched target list can be reported as complete. The append-only publication store is sound only after the authority supplied to it is complete.

### Significant — checkpoint and publication advances are not atomically fenced

The executor performs a lease assertion before checkpoint commit and another before publication append (`crates/cdf-project/src/runtime/promotion.rs:207-243`), but checkpoint commit (`:973-1004`) and publication persistence are not conditional on the same fencing token inside their atomic mutations. The lease can expire or be superseded between the assertion and either write. Exact lock publication is different: it uses the RP4 fenced exact-CAS boundary and is correctly protected.

The specification does not require destination settlement itself to be atomically fenced. A destination operation may validly finish after lease expiry if it is idempotent and its receipt remains recoverable; the expired executor must then stop before checkpoint, lock, or publication advancement. RP9 currently has no renewal path and does not provide an atomic fence for checkpoint or publication, so the required boundary is not established. This is a TOCTOU defect, not a claim that destination settlement must be rolled back on expiry.

### Significant — failure and recovery output does not satisfy the P1/JSON contract

The phase enum names intermediate states, but successful report construction always emits `Complete` and `remaining_action = "none"` (`crates/cdf-project/src/runtime/promotion.rs:249-270`, `:1186-1212`). Failures return ordinary errors rather than a structured report containing current persisted phase, committed targets, remaining action, and exact recovery command. The crash test invokes the project API, checks the error, and reruns; it does not exercise command-level human or JSON recovery output at each boundary.

### Significant — required cross-destination and concurrency acceptance is unproved

The command-level execution and six-boundary crash matrix cover a single DuckDB target. There is no multi-target test, no failure on a later target, no proof of parent-checkpoint ordering across targets, and no proof that publication waits for every target. RP6 proves the Postgres destination protocol independently, but RP9 has no live or command-level Postgres promotion execution. Parquet promotion was explicitly blocked by `.10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md` and is now resolved.

Promotion-vs-promotion and pin-vs-promotion have lease/CAS protection at publication, but ordinary runs do not acquire the schema-contract promotion lease. No test proves that a run using the old schema cannot commit concurrently while promotion packages residual corrections and publishes the new snapshot. The active ticket's named run/pin/promotion conflict criterion is therefore not met.

### Significant — GC's “last locally promotable” classification is not authority-backed

`promotion_gc_availability` counts non-null UTF-8 bytes in `_cdf_variant` without decoding and validating canonical `residual-json-v1`, without requiring a structurally verified receipt/target association, and without proving that the planner/executor could promote those bytes (`crates/cdf-cli/src/package_command.rs:462-532`). It marks the sole counted package as `last_locally_promotable_for_resource` irrespective of that package's planned GC action, rather than answering whether the proposed collection removes the last promotable local copy.

The only dedicated test, `package_gc_reports_last_locally_promotable_residual_bytes`, uses one packaged fixture with no receipt authority. It therefore demonstrates byte presence, not local promotability, and does not cover retained/collectable mixtures, tombstones, malformed residual envelopes, or multiple packages for one resource. The implementation correctly makes no destination-readback claim; that restraint does not make the local promotability label accurate.

### Minor — additive publication persistence lacks an explicit legacy-v3 migration proof

The dedicated `PromotionPublicationEvent` and `cdf_promotion_publications` table avoid changing `RunEventKind`, and publication is append-only/idempotent for equal authority. The run-ledger schema bump from version 3 to 4 is additive. Existing tests cover fresh/early migrations and publication idempotence, but the inspected suite does not explicitly create a version-3 database, migrate it, and prove both prior run-event reads and promotion-publication operations. This is a bounded backward-compatibility evidence gap, not evidence of a destructive migration.

## What passed adversarial inspection

- The top-level path visibly follows stage, package, destination receipt, checkpoint, exact lock CAS, then publication ordering.
- Correction artifacts carry old/new schema hashes, strategy, validation program, source package hashes, and exact addressed operations.
- Destination dispatch is capability/protocol driven and calls runtime readiness hooks; the promotion orchestrator contains no DuckDB/Postgres/Parquet name branch.
- Exact lock publication uses the RP4 helper with integrated fencing and exact old-authority CAS.
- Publication storage is append-only and idempotent for the same authority, and rejects conflicting authority for the same promotion ID.
- GC does not infer verified destination readback from local package state.
- Execution consumes a typed version-3 promotion snapshot rather than rebuilding discovery output.
- CLI error rendering accumulates configured destination secret redactions. No RP9-specific secret-bearing fixture proves staged artifacts, human errors, and JSON output together, so secret safety remains an evidence limit rather than a confirmed defect.

These correct pieces do not compensate for the recovery, authority, fence, scope, output, and coverage failures above.

## Verification and limits

Inspected named RP9 tests include:

- `schema_promote_execute_commits_correction_checkpoint_lock_and_idempotent_publication`
- `schema_promote_execute_recovers_every_persisted_crash_boundary`
- `package_gc_reports_last_locally_promotable_residual_bytes`
- publication-store idempotence/conflict tests and package replay validation tests

Static search found no RP9 multi-target, custom-contract-scope, source-package-removal recovery, lease-expiry-at-checkpoint/publication, live Postgres command, successful Parquet command, or old-schema run-vs-promotion concurrency scenario.

A fresh `cargo test -p cdf-cli schema_promote_execute -- --nocapture` was attempted. The shared working tree did not compile because unrelated concurrent changes in `crates/cdf-engine/src/execution.rs` use an incompatible `EnginePreviewLimits` shape (including `u64`/`Option` mismatches, a missing `max_batches_per_partition` field, and missing output fields). The prior RP9 evidence record reports passing targeted tests, but this review could not independently rerun them. This build blockage does not cause the fail verdict; the source-level authority defects and missing acceptance coverage do.

## Verdict

Fail. RP9 must remain active. The implementation has the intended orchestration skeleton and a sound exact lock-CAS step, but it does not yet establish source-free recovery from immutable correction packages, canonical staged-plan authority, complete receipt/checkpoint verification, contract-consistent checkpoint scope, atomically fenced checkpoint/publication advances, required recovery output, or the cross-destination/multi-target/concurrency and GC proofs required by the ticket and governing specification. The existing Parquet policy ticket already owns that semantic blocker; all other findings remain within RP9's active scope for parent-directed repair and re-review.
