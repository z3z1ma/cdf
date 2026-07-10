Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-p2-rp9a-promotion-artifact-recovery-authority.md
Verdict: fail

# Independent review: RP9A promotion artifact recovery authority

## Target and boundaries

This review independently inspected RP9A against `.10x/specs/schema-promotion-corrections.md`, the owning ticket, RP5 typed promotion authority, the initial RP9 fail review, current evidence, and the implementation in `crates/cdf-project/src/promotion.rs` and `crates/cdf-project/src/runtime/promotion.rs`.

RP9B atomic fencing, RP9C multi-target/command conformance, RP9D GC classification, and the Parquet identifier policy are excluded. Findings below concern only RP9A's persisted authority and verification boundaries.

## Assumptions attacked

- A staged plan cannot become executable by recomputing a hash over a weaker or divergent projection.
- Existing staged paths are create-or-verify and conflicting filesystem state fails without clobber or recursion.
- Source inventory and recorded receipts are exact, unique, structurally complete, and live verified before row addresses authorize operations.
- Once packaged, the immutable correction package is sufficient recovery authority without original residual packages.
- A replacement/rebuilt package cannot omit or alter corrections while remaining admissible under the staged plan.
- Package checkpoint id, contract scope, parent checkpoint, input/output positions, and ordinary replay preimages are both internally coherent and exactly the values selected by promotion authority.
- All caller-supplied mutable projections are checked against the exact old lock bytes before destination/checkpoint/lock mutation.
- A persisted publication event cannot bypass correction receipt/checkpoint verification or substitute a different target/package authority.

## Findings

### Critical — a canonically rebuilt correction package can omit or alter operations and still publish

The staged plan binds each path's `observed_count` and `affected_address_value_digest`, but `load_correction_package` and `validate_correction_artifact_for_staged` never compare package operations to either value. They require a nonempty operation list and check that every included operation names an allowed path/package, has the expected field, and has a unique `(row address, path)`. They do not prove that every staged address/value is present or unchanged.

The correction package directory is deterministic but its resulting package hash is not committed into the staged plan before recovery. Therefore, after `AfterCorrectionPackages`, an attacker or corrupting process can replace the directory with a newly canonical package that:

- retains the staged promotion/resource/destination/target/schema/strategy/source-package fields;
- removes one of several operations, or substitutes another valid value of the selected Arrow type;
- rewrites the typed artifact and operation segment consistently;
- regenerates coherent state/commit preimages and a valid package manifest.

`PackageReader::verify`, artifact validation, operation-segment equality, and ordinary `replay_inputs` all accept that self-consistent replacement. The loader has no expected package hash, operation digest, count, or staged address/value digest to distinguish it from the originally built package. Execution can then apply incomplete or altered corrections, commit its checkpoint, and publish the new schema.

The existing correction tamper test appends one byte to a manifest-owned artifact without rebuilding the manifest. It proves detection of accidental byte corruption, not semantic replacement under a fresh valid manifest.

Required repair: bind an exact correction-operation authority per target before a completed package becomes recoverable. A staged or create-only post-build authority must commit the expected package hash and/or a canonical operation digest/count that is recomputable from the staged path address/value evidence. Loader validation must reject missing, extra, or value-altered operations even when all package bytes and preimages are internally valid. Add a canonically rebuilt subset/value-substitution tamper test with original source packages deleted and assert zero destination/checkpoint/lock mutation.

### Critical — lock publication trusts a caller-supplied `CdfLock` that is not checked against the exact old bytes

`validate_schema_promotion_plan_identity` correctly parses and validates `old_lock_authority.bytes`. However, `SchemaPromotionExecutionRequest` separately carries `lock: &CdfLock`, and no validation proves that this value is the parse of those exact bytes. `publish_lock` clones `request.lock`, changes the promoted resource snapshot, and installs that derived TOML through an exact CAS against `old_lock_authority`.

A direct API caller can therefore supply the correct old bytes/hash and canonical dry plan while supplying a divergent `request.lock` object with altered unrelated resources, destinations, or policy. Source verification and destination settlement can complete, after which the CAS atomically publishes the divergent lock projection because its compare side checks disk bytes but its replacement side came from the unchecked object.

This violates the RP9A requirement that old lock bytes/hash are exact execution authority and violates mutation-before-verification discipline. Exact CAS does not make an unauthenticated replacement safe.

Required repair: either derive the replacement lock exclusively by parsing `staged.old_lock_authority.bytes`, or reject unless `request.lock` is exactly equal to that parse before any staging/destination mutation. Add an API-level mismatched-lock projection test proving rejection before package, destination, checkpoint, or lock writes.

### Significant — correction checkpoint/preimage authority is internally coherent but not bound to the staged target

Build time derives a deterministic `correction_checkpoint_id(promotion_id, target)` and current head. On load, the package passes ordinary replay validation and scope/pipeline/resource/schema checks, but the loader does not compare `state_delta.checkpoint_id` to the deterministic expected id and does not compare its parent/input checkpoint projection to the current authoritative head selected for this promotion.

A canonically rebuilt package can therefore choose another internally coherent checkpoint id and preimage chain. Settlement and publication then use that package-selected id. This is adjacent to, but independently observable from, the missing operation-completeness binding above.

The custom-contract fixture and `PackageReader::replay_inputs` correctly prove scope and internal preimage consistency. They do not prove exact staged checkpoint identity. Required repair: validate the deterministic checkpoint id and the intended input checkpoint authority during hydration, and include them in the persisted target authority used for recovery.

### Significant — an existing publication event bypasses target/package/receipt/checkpoint revalidation

`execute_under_lease` loads correction packages and then, if a publication event exists, calls `verify_publication_authority` and returns `report_from_publication`. The verifier checks event version, promotion/resource/schema/installed-lock fields, and that the lock contains the staged snapshot. It does not compare publication targets to the loaded correction packages or staged targets, and it does not call `verify_stored_correction_receipt` or `committed_target_report` on this branch.

`PromotionPublicationEvent::validate` only requires a nonempty sorted unique target list. A structurally valid event for the canonical promotion id can name a different correction package hash, receipt id, or checkpoint id and still be reported complete so long as the outer schema/lock fields match. This is persisted-authority verification, not RP9B's atomic-fencing concern and not RP9C's multi-target concern; it is reproducible with one target.

Required repair: reconstruct the exact expected publication target tuple from loaded packages plus live-verified correction receipts and committed checkpoints, compare it byte-for-byte with the persisted event, and only then report completion.

### Minor — a non-file conflict at a staged artifact path can recurse instead of failing cleanly

`write_create_or_verify` handles equal bytes and conflicting readable files. If the target exists but `fs::read(path)` fails—for example, the content-addressed target is a directory—the helper proceeds to hard-link installation. `AlreadyExists` recursively calls `write_create_or_verify`, which repeats the same state without a termination condition.

This does not clobber bytes, but it does not meet the create-or-verify requirement's named fail-closed behavior and can end in stack exhaustion. Handle non-`NotFound` read errors as conflicts, or re-read once after `AlreadyExists` and return a bounded error for non-regular/unreadable targets. Add a directory/special-entry conflict test.

## Confirmed behavior

- RP5 now has one public recomputation/validation path. It checks exact old lock bytes/hash, typed version-3 snapshot lineage, path/type/package/receipt associations, locked sheets, strategy selection, execution preconditions, and canonical promotion id. A staged target-set edit plus recomputed id is rejected because it diverges from typed snapshot/lock authority.
- Snapshot and plan staging preserve equal bytes and reject conflicting readable file bytes with create-only hard-link installation.
- Selected source packages pass manifest and ordinary replay validation. Receipt ids are exact, full package/state/schema/disposition/token/segment authority is checked, and the resolved live destination protocol must verify each receipt before operation extraction.
- Source enumeration rejects malformed package directories and duplicate package hashes deterministically.
- Existing completed correction packages are loaded before source indexing. The source-deletion matrix successfully recovers every post-package failpoint without opening the original residual package.
- Correction packages use the custom `promotion_scope(resource)`, write input checkpoint/state/commit-plan preimages, and pass ordinary replay validation. The `events-contract` fixture proves the custom contract rather than a resource-id fallback.
- Stored correction receipts are reconstructed against the correction request/plan and live verified on normal recovery branches; committed checkpoint receipts are compared to that verified receipt.
- Generic orchestration remains capability driven; no DuckDB/Postgres/Parquet branch was found in promotion execution.

## Exact checks

The following commands passed:

```text
cargo test -p cdf-project promotion::tests:: -- --nocapture
# 18/18

cargo test -p cdf-project runtime::promotion::tests:: -- --nocapture
# 2/2

cargo test -p cdf-cli --lib schema_promote_execute -- --nocapture
# 3/3: custom-contract execution, source-deletion crash matrix, real Parquet receipt path

cargo test -p cdf-cli --lib schema_promote_rejects_tampered_staged_and_correction_authority_before_mutation -- --nocapture
# 1/1 across shallow staged and byte-corruption package tamper cases

git diff --check
# pass
```

Static authority checks:

```text
rg affected_address_value_digest|observed_count crates/cdf-project/src/runtime/promotion.rs
# no loader validation use

rg correction_checkpoint_id|state_delta.checkpoint_id crates/cdf-project/src/runtime/promotion.rs
# deterministic id is used during build; loaded state id is used during settlement without equality validation

rg request.lock.clone crates/cdf-project/src/runtime/promotion.rs
# publish_lock derives replacement from the separately supplied unchecked object

rg verify_publication_authority|report_from_publication crates/cdf-project/src/runtime/promotion.rs
# early publication branch does not verify loaded package receipts/checkpoints
```

## Verdict

Fail. RP9A materially fixes the original source-package dependency, shallow staged validation, receipt-id-only verification, scope mismatch, and ordinary replay-preimage gaps. It is not yet safe to close because semantically rebuilt correction packages are not bound to staged operation completeness, lock replacement can derive from an unauthenticated duplicate projection, checkpoint identity is package-selected on hydration, and the publication-present branch trusts incomplete target authority.

## Limits and excluded work

- This review does not treat checkpoint/publication atomic fencing as an RP9A defect; RP9B owns that mutation primitive.
- It does not require multi-target command behavior, cross-target ordering, or later-target failure handling; RP9C owns those scenarios. The publication finding above applies to one target and concerns exact persisted authority.
- It does not assess GC promotability classification; RP9D owns it.
- It does not reopen Parquet identifier policy or external object-store conformance.
- The semantic-repackage and mismatched-lock exploits were established by direct authority/dataflow inspection. Existing public tests do not expose helpers for constructing those hostile packages without adding a regression fixture; the absence of such tests is part of the finding.
