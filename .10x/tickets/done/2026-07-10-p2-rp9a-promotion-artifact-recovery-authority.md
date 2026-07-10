Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-rp9-promotion-execution-recovery-gc.md
Depends-On: .10x/tickets/done/2026-07-10-p2-rp5-schema-promote-dry-planner-cli.md, .10x/tickets/done/2026-07-10-p2-rp6-postgres-in-place-corrections.md, .10x/tickets/done/2026-07-10-p2-rp7-duckdb-in-place-corrections.md, .10x/tickets/done/2026-07-10-p2-rp8-parquet-correction-sidecars.md

# RP9A self-authenticating promotion artifacts and package-only recovery

## Scope

Make staged promotion and correction packages sufficient, canonical recovery authority after packaging. Remove every post-package dependency on original residual source packages and make pre-package source/receipt selection exact and fail-closed.

## Acceptance criteria

- The RP5 planner exposes one typed canonical identity validator/recomputation path; staged execution hydration recomputes the promotion id and cross-validates the dry plan, typed version-3 snapshot, old lock bytes/hash, target/path/package/receipt associations, strategies, and execution preconditions.
- Staged snapshot and plan installation are create-or-verify/no-clobber. Existing conflicting content-addressed bytes fail rather than being overwritten.
- Before correction packaging, source enumeration rejects malformed entries and duplicate package hashes. Every source receipt is structurally verified against exact package/state/schema/disposition/segments and live destination verification is performed through the resolved protocol before its row addresses can authorize correction operations.
- A completed immutable correction package is loaded and replayed from its own verified manifest, typed correction artifact, operations, state/checkpoint preimages, and receipts without enumerating or opening original residual source packages.
- Package state scope is exactly the acquired `promotion_scope(resource)`, including custom contract refs. Input checkpoint artifact, parent checkpoint id, and input/output positions are mutually consistent and accepted by the ordinary package replay validator.
- Recovery after packaged/no-receipt, receipt/no-checkpoint, target checkpoint, lock publication, and publication event succeeds after original residual source packages are removed; corrupted/tampered correction packages fail before destination/checkpoint/lock mutation.
- No source-format or destination-name branch is introduced.

## Evidence expectations

Canonical staged-plan tamper matrix, content-addressed no-clobber test, malformed/duplicate source inventory tests, full source-receipt structural/live verification, custom-contract scope fixture, ordinary replay validation, source-deletion recovery at every post-package failpoint, package/receipt/checkpoint inspection, strict Clippy/formatting, and independent review.

## Explicit exclusions

No checkpoint/publication atomic-fence API, Parquet identifier-policy choice, multi-target command conformance, GC classification, source re-extraction, or distributed scheduler.

## Progress and notes

- 2026-07-10: Opened from critical/significant findings in `.10x/reviews/2026-07-10-p2-rp9-promotion-execution-independent-review.md`. The initial RP9 implementation skeleton is reusable, but recovery may not recompute expected packages from source once an immutable correction package exists.
- 2026-07-10: Activated and assigned to `/root/impl_d5`. Repair must reuse the initial capability-driven skeleton while replacing its shallow staged validation, overwrite staging, mutable source indexing/receipt checks, and source-dependent package hydration with one exact typed authority path.
- 2026-07-10: Implemented one public RP5 canonical identity recomputation/validation path and made RP9 stage/hydrate through it. Staged snapshot/plan installation is create-or-verify/no-clobber; canonical staged paths, exact old lock bytes/hash, typed version-3 snapshot lineage, selected path/package/receipt associations, locked destination sheets, and uniquely selected strategies are revalidated before mutation.
- 2026-07-10: Replaced permissive source indexing with deterministic fail-closed inventory, exact ordinary replay/receipt structural checks, and live original-receipt verification through the resolved destination protocol. Existing completed correction packages now hydrate from their own manifest, typed artifact, operation segment, ordinary replay preimages, and exact correction receipt without enumerating or opening original residual packages.
- 2026-07-10: Correction package state now uses the exact custom contract promotion scope, writes coherent input-checkpoint/state/commit-plan preimages accepted by `PackageReader::replay_inputs`, and validates stored correction receipts against a reconstructed correction plan plus live destination verification. The source-deletion matrix passes every post-package failpoint; staged/correction tamper cases fail before destination/checkpoint/lock mutation. Evidence: `.10x/evidence/2026-07-10-p2-rp9a-promotion-artifact-recovery-authority.md`.
- 2026-07-10: Repaired every finding in `.10x/reviews/2026-07-10-p2-rp9a-promotion-artifact-recovery-independent-review.md`. A create-only per-target authority now binds exact package hash, canonical operation count/digest, staged path count/value digests, deterministic checkpoint id, and exact input checkpoint. Lock replacement is parsed from staged bytes; publication recovery re-verifies exact live receipts, committed checkpoints, and publication tuples; non-file artifact conflicts fail boundedly. Hostile fully rebuilt subset/value-substitution packages and divergent caller locks fail before mutation. Full project 163/163 and CLI 255/255 suites plus strict Clippy/format/diff pass. Closure review: `.10x/reviews/2026-07-10-p2-rp9a-promotion-artifact-recovery-repair-review.md`.
- 2026-07-10: Closed. Retrospective: internal manifest consistency is not recovery authenticity; immutable recovery authority must bind the expected semantic operation set and its lineage outside the replaceable package. This invariant is now represented by the typed target-authority artifact and hostile canonical-rebuild tests. Atomic store fencing remains explicitly owned by RP9B, not hidden in RP9A.

## Blockers

None.
