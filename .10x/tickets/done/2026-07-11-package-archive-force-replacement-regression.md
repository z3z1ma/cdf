Status: done
Created: 2026-07-11
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/specs/package-lifecycle-determinism.md

# Package archive force-replacement regression

## Scope

Restore the ratified `force` archive behavior when persisted Parquet archive sidecars are tampered or missing. Determine why the force path verifies and returns the stale archive failure instead of replacing the archive, fix the owning archive layer, and preserve strict failure without force.

## Acceptance criteria

- `persisted_archive_default_fails_on_tamper_and_force_replaces` passes.
- `force_archive_reports_replaced_when_manifest_metadata_survives_missing_tree` passes.
- Default mode still rejects tampered or incomplete archive sidecars.
- Force mode replaces atomically and the rebuilt archive verifies.
- The fix does not weaken canonical package identity or archive verification.

## Evidence expectations

Focused reproductions, package archive suite, diff inspection, and adversarial review of default-versus-force failure routing.

## Explicit exclusions

No archive format expansion, compatibility shim, or change to canonical package identity.

## Assumptions

- Record-backed: `.10x/specs/package-lifecycle-determinism.md` requires strict default rejection of stale archive state and permits `--force` replacement only after canonical package verification succeeds.
- Record-backed: `persist_package_parquet_archive` already calls `verify_package_identity` before archive-state selection or canonical segment reads; the fix must not bypass or weaken that gate.

## Blockers

None. The failure reproduces independently in the current tree.

## References

- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/tickets/done/2026-07-06-package-archive-contract-ratification.md`

## Progress and notes

- 2026-07-11: Discovered while verifying the unrelated FX1 payload-retention refactor. Both tests fail independently; the error is stale archive verification surfacing from a force invocation. No FX1 retention code participates in archive replacement selection.
- 2026-07-12: Execution inspection localized the regression to the archive writer's call to `PackageReader::verified_canonical_segment_stream`. Commit `dcc519a1c` changed that reader method from canonical-identity verification to `verify_for_consumption`, whose full package verification correctly includes archive sidecars. That full verification is appropriate for ordinary consumption but incorrectly re-enters stale archive verification after `persist_package_parquet_archive` has already verified canonical identity and selected forced replacement. The focused reproduction command is serialized behind the shared Cargo build lock; no outcome is claimed yet.
- 2026-07-12: Reproduction observed with `cargo test -p cdf-package persisted_archive_default_fails_on_tamper_and_force_replaces -- --nocapture`: 1 test ran and failed at the forced replacement call with `package archive verification failed: tampered archive sidecar ...` (0 passed, 1 failed). An earlier invocation with `--exact` matched 0 tests because the harness name includes the `tests::` module; it provides no behavioral evidence.
- 2026-07-12: Implemented the archive-layer fix: after the entry point's successful `verify_package_identity`, the temporary-tree writer reads and transcodes one manifest segment at a time without invoking the full-consumption verifier that includes stale archive metadata. Per-segment checked row-count validation remains in the streamed path; default archive verification and final post-install `verify_package` remain unchanged.
- 2026-07-12: Post-fix `cargo test -p cdf-package persisted_archive_default_fails_on_tamper_and_force_replaces -- --nocapture` passed (1 passed, 0 failed, 55 filtered). This proves the named tampered-sidecar default failure still occurs in the test and the subsequent forced replacement now succeeds and verifies.
- 2026-07-12: Scoped static inspection passed: `rustfmt --edition 2024 --check crates/cdf-package/src/archive.rs` exited 0, and `git diff --check -- crates/cdf-package/src/archive.rs .10x/tickets/done/2026-07-11-package-archive-force-replacement-regression.md` produced no findings. Diff scope is only `archive.rs` and this ticket; the pre-existing DX3/J0 files remain untouched.
- 2026-07-12: Post-fix `cargo test -p cdf-package force_archive_reports_replaced_when_manifest_metadata_survives_missing_tree -- --nocapture` passed (1 passed, 0 failed, 55 filtered). This proves manifest archive metadata still selects `replaced` when its sidecar tree is missing, and the rebuilt archive passes package verification.
- 2026-07-12: Focused `cargo test -p cdf-package archive -- --nocapture` passed (15 passed, 0 failed, 41 filtered). The suite includes both named regressions plus strict tampered, missing, orphaned, source-mismatched, bad-fidelity, unsupported-type, status-gate, and unsafe-segment-id failures; clean skip/temp cleanup; canonical identity preservation; and IPC replay preference.
- 2026-07-12: Significant-review repair restores a package-boundary `package-parquet-archive-segment` reservation before each canonical segment read/transcode. The one 64 MiB minimum-working-set lease remains live across allocation-aware Arrow retained-byte measurement, Parquet encoding/output retention measurement, sidecar write, and metadata hashing, then drops before the next segment. Both Arrow-only overflow and simultaneous Arrow-plus-Parquet-output overflow fail cleanly; force routing continues to use identity-only verification and does not re-enter optional archive verification. Added a focused injectable-window regression covering the named consumer, single-window peak/release across two segments, combined input/output rejection before a sidecar write, Arrow-retention rejection, and the production 64 MiB constant. No Cargo command has been run for this repair pending orchestrator authorization.
- 2026-07-12: The single authorized `CARGO_BUILD_JOBS=4 cargo test -p cdf-package archive --locked -- --nocapture` invocation exited 101 during compilation, before tests ran. It found seven `E0425` errors: a mechanical patch had placed the bounded-output call in the legacy in-memory report path where window variables do not exist, and the crate-level test could not access the private archive test seam/constants. Corrected the call site to the persisted writer loop and made only the test seam/constants `pub(crate)`. Per orchestration constraint, no second Cargo command was run; the repair remains unverified after these compile-only corrections.
- 2026-07-12: Authorized post-correction rerun of `CARGO_BUILD_JOBS=4 cargo test -p cdf-package archive --locked -- --nocapture` exited 0: 16 passed, 0 failed, 41 filtered. The new `persisted_archive_enforces_one_accounted_input_output_window` passed together with both force/default regressions and all archive verification cases. No further Cargo command was run.

## Evidence

- `persisted_archive_default_fails_on_tamper_and_force_replaces`: reproduced failing before the change, then passed alone and in the 15-test archive suite. This maps to the first criterion and proves default rejection remains strict before forced replacement succeeds.
- `force_archive_reports_replaced_when_manifest_metadata_survives_missing_tree`: passed alone and in the archive suite, mapping to the second criterion and the missing-tree force path.
- The 15-test archive suite passed its tampered, missing, orphaned, source-mismatched, bad-fidelity, and unreadable-sidecar cases, supporting strict default verification behavior.
- Both force regression tests call `verify_package` after replacement; both passed. Diff inspection confirms the operation-scoped temporary tree, rename installation, manifest-last atomic rewrite, and final full `verify_package` remain unchanged. Limits: these focused tests exercise successful replacement, not a process kill at every rename boundary.
- `persisted_archive_writes_sidecars_manifest_metadata_and_fidelity_json`, `archive_report_records_parquet_bytes_and_preserves_canonical_package`, and `archive_transcode_keeps_replay_and_read_segment_on_ipc` passed in the focused suite, supporting unchanged package identity, signing input, archive verification, and canonical IPC preference.
- `rustfmt --edition 2024 --check crates/cdf-package/src/archive.rs` and scoped `git diff --check` exited 0. Scoped status/diff inspection shows only `crates/cdf-package/src/archive.rs` and this ticket changed; no DX3/J0 file was touched by this execution.
- Post-review repair evidence: the 16-test locked archive-filtered suite passed, including `persisted_archive_enforces_one_accounted_input_output_window`. Its assertions map the restored memory rail to a named Package-class consumer, one admitted/released window across successive segments, a 64 MiB production cap, allocation-aware Arrow retention rejection, and bounded Parquet output rejection before any sidecar write. Limits: the injected small windows falsify the same policy without allocating a 64+ MiB fixture; the test does not measure allocator-internal/native Parquet scratch RSS.

## Review

### Findings

- **significant — the fix removes the archive reader's managed 64 MiB retained-memory fence instead of preserving it behind canonical-only verification.** `write_streamed_archive_temp_tree` now calls `PackageReader::read_segment` directly and holds the resulting `Vec<RecordBatch>` together with the newly allocated `parquet_bytes`. The removed `verified_canonical_segment_stream` path reserved a named package-memory window, computed allocation-aware retained Arrow bytes, rejected a segment above 64 MiB, and enforced that the previous segment was dropped before advancing. The replacement still processes only one segment at a time and adds checked row-count arithmetic, but it has no reservation, retained-byte accounting, or clean over-budget failure. The active canonical-segmentation contract caps logical segment bytes at 64 MiB, not retained Arrow memory, and the active memory contract requires CDF-owned Arrow/package buffers to be accounted and package readers to yield bounded accounted batches/streams. Therefore a high-expansion or legacy canonical segment can now drive unaccounted process memory (with input and Parquet output simultaneously live) where the prior code failed through a bounded safety rail. The two small focused regression fixtures cannot falsify this regression.

No other requested behavior was falsified by source/assertion review. `verify_package_identity` still precedes archive-state routing and every segment read; default mode still rejects tampered, missing, source-mismatched, and orphaned archive state; force mode intentionally bypasses only stale archive metadata, reports `replaced` whenever metadata or the final archive tree existed, uses the unchanged temporary-tree/rename/manifest-last path, and finishes with full `verify_package`; checked row-count equality and overflow handling remain in the new loop. Canonical package identity fields are copied from the already identity-verified manifest and are not rewritten.

### Verdict

**fail** for closure. The force-replacement regression itself is repaired without weakening default archive verification or canonical identity verification, but the implementation weakens an existing memory/admission fence in the same code path. Restore an identity-only way to consume the bounded accounted segment stream, or provide an equivalent named reservation, retained-memory bound, and simultaneous input/output peak policy before accepting the slice.

### Residual Risk

No test or broad check was repeated; the executor's focused test evidence remains authoritative within its stated limits. Review inspected the scoped `archive.rs` diff, the named archive assertions, the package reader's bounded-stream implementation, and governing package/segmentation/memory records. Successful rename behavior is structurally unchanged, but process-kill behavior at each replacement boundary, filesystem rename portability, concurrent package mutation between verification and segment read, and peak memory during Parquet encoding remain unmeasured. The requested `5.6-sol/high` model/reasoning setting could not be passed because the collaboration API exposes no such fields; this review was nevertheless performed by a fresh independently spawned reviewer.

### Fresh re-review after managed-memory repair — 2026-07-12

#### Findings

No critical, significant, minor, or nit finding was identified in the repaired slice.

The repair satisfies the prior significant finding by routing both production and the injectable test seam through `write_streamed_archive_temp_tree_with_memory`. Production supplies one fixed 64 MiB coordinator/window. Before each canonical segment read, that shared path acquires a minimum-working-set reservation for the named `Package` consumer `package-parquet-archive-segment`; the lease remains live while allocation-aware Arrow retained bytes are measured, the bounded Parquet sink reserves only the remaining capacity, the sidecar is written, and its metadata/hash are derived. Rust scope/drop ordering releases Parquet bytes and Arrow batches before the lease and before the next loop iteration. Error returns unwind the same objects, and the combined-input/output and Arrow-overflow assertions confirm release with no sidecar write. The two-segment production-path fixture, one-window coordinator, zero ending balance, and exactly one-window peak would hang or fail if the first segment's reservation survived into the second; the passing focused evidence therefore supports sequential release as well as the peak bound. The seam changes only injected memory authority/window size and exercises the production loop, bounded writer, file write, and metadata logic rather than a parallel policy helper.

The force/default fences also remain intact by source and assertion inspection. `verify_package_identity` runs before archive-state selection and `PackageReader::open` is metadata-only, so forced rebuilding does not re-enter optional archive verification; non-force routing still calls `verify_parquet_archive_metadata` and rejects tampered, missing, mismatched, and orphaned state. Both replacement regressions finish with full `verify_package`; force status still reflects pre-existing metadata or tree state. Segment reads remain manifest-selected, checked row-count accumulation/equality precedes transcode, canonical identity fields are copied unchanged, and installation retains the existing temporary-tree, backup/rename, directory-sync, manifest-last, and final-verification sequence.

#### Verdict

**pass** for the repaired package-archive slice. This fresh verdict supersedes the earlier fail finding, whose named managed-memory fence has been restored without reintroducing stale optional-archive verification.

#### Residual Risk

The journaled 16-test focused run is accepted without repetition. Its small injected windows exercise the production policy but do not allocate a real 64+ MiB segment or measure allocator/native Parquet-writer scratch RSS. The fixed lease accounts decoded Arrow retention plus output `Vec` capacity, not ArrowWriter's internal/native scratch; that pre-existing broader RSS limitation remains governed by the constant-memory contract rather than falsified by this slice. Process-kill coverage at every rename boundary, cross-filesystem rename portability, and concurrent mutation between identity verification and segment reads also remain unmeasured. The requested `5.6-sol/high` setting was unavailable in the collaboration API; this was a fresh independent review agent.

## Retrospective

The regression came from broadening a specialized canonical-segment stream to use the ordinary full-consumption verification authority. That refactor was locally reasonable but erased the force-replacement boundary: stale optional metadata must be rejected for normal consumption yet intentionally ignored only while rebuilding it after canonical identity succeeds. Commit archaeology exposed the semantic shift quickly. Reusing the existing force tests avoided speculative test machinery, while the focused archive suite demonstrated that the surgical routing change preserved strict default verification and canonical identity behavior. The first reproduction command used `--exact` without the harness's `tests::` prefix and ran zero tests; future focused evidence should either use the full harness name or omit `--exact`, and must always record the executed test count.

The memory review exposed a second boundary hidden by the verification repair: “one segment at a time” is not itself a memory guarantee when decoded Arrow allocations and an expanding in-memory Parquet sink coexist. Reserving the complete archive window before either allocation, measuring retained Arrow allocations, and giving the writer only the remaining preallocated output capacity makes admission and failure behavior agree. A post-edit search should have checked both transcode call sites before the first compile; the mechanical replacement landed in the legacy report path and cost one failed compile-only run. The focused rerun then passed all 16 archive tests. Fresh review still owns closure because the prior significant finding and residual native-writer scratch risk must be judged independently.

## Closure judgment

Closed 2026-07-12. Every acceptance criterion maps to journaled evidence from the final repaired state: both named regressions and the strict default archive cases passed in the locked 16-test archive-filtered suite; both replacement regressions finish with full package verification; and source/assertion inspection confirms identity-first routing, bounded accounted segment/output handling, temporary-tree installation, manifest-last replacement, and unchanged canonical identity. The fresh independent re-review passed and supersedes the earlier managed-memory finding. The retrospective is complete. Remaining native-writer RSS, process-kill boundary, rename-portability, and concurrent-mutation risks are explicit evidence limits; the broader RSS proof remains governed by `.10x/specs/runtime-memory-backpressure.md` and `.10x/specs/constant-memory-proof.md`.
