Status: done
Created: 2026-07-12
Updated: 2026-07-13
Parent: .10x/tickets/done/2026-07-12-p0-cargo-product-build-graph.md
Depends-On: None

# P0 CG3: stable package contract leaf

## Scope

Extract the canonical package artifact/replay models and verified-access capability contracts into `cdf-package-contract`. Make `cdf-package` the sole filesystem/IPC/Parquet implementation and remove `cdf-runtime`'s dependency on full `cdf-package` and concrete package readers.

## Non-goals

- Changing package layout/bytes/hash/lifecycle/replay semantics, adding an artifact version, or retaining old Rust import paths.
- Rewriting package I/O, verification, streaming, archive, or hashing implementations.
- Changing E3's verification authority, containment policy, replay read/hash strategy, or performance semantics; CG3 moves existing facts and adds only the minimum neutral access contract needed to remove the runtime implementation edge.
- Moving package artifacts into kernel or DataFusion.

## Acceptance criteria

- Each manifest/file/segment/lifecycle/replay-preimage type has one canonical owner in `cdf-package-contract`; all workspace consumers migrate directly and no old-owner re-export/conversion mirror remains.
- `cdf-package-contract` performs no filesystem, IPC, Parquet, hashing, archive, tempfile, or verification implementation work. `cdf-package` implements durable verified access against its capability contracts.
- Static graph checks prove `cdf-package-contract` reaches only admitted lower contract dependencies and does not reach DataFusion, project/product, runtime, package implementation, state, network/database clients, or concrete source/format/transform/destination crates; failures print an offending dependency path.
- `cdf-runtime` has no normal dependency/import of `cdf-package`, concrete reader/builder, filesystem path, or package codec. Final-binding/staged-ingress validation consumes supplied verified facts/cursors and preserves one runtime lifecycle authority.
- `cargo tree -p cdf-runtime -e normal` excludes `cdf-package`, `parquet`, `arrow-ipc`, and `tempfile` and contains <=67 unique packages. Its normal+dev graph also excludes `cdf-package`, `parquet`, and `arrow-ipc`; concrete integration fixtures move to package/product/conformance owners instead of restoring the implementation as a dev edge. Architecture tests print an offending path on regression.
- Package golden bytes/hashes, replay inputs, verified segment ordering/accounting, destination staging/finalization, receipts/checkpoints, corruption rejection, and crash-matrix behavior remain unchanged in focused owner tests.
- Before/after graph and timing evidence records limits; deleted superseded imports/helpers/tests are counted.

## References

- `.10x/specs/product-build-graph-boundaries.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/destination-extension-runtime-contract.md`
- `.10x/decisions/lean-cli-and-package-contract-build-boundaries.md`
- `.10x/decisions/destination-staged-ingress-final-package-binding.md`
- `.10x/research/2026-07-12-cargo-product-build-graph-audit.md`
- `.10x/tickets/done/2026-07-11-p3-e3-streaming-verification-replay-io.md`

## Assumptions

- **Record-backed:** Package artifact identity remains package-domain authority rather than kernel authority; runtime validates lifecycle over verified facts but must not own I/O.
- **Record-backed:** No production consumer requires compatibility re-exports or old artifact/Rust API paths.

## Journal

- 2026-07-12 (shaping): Source tracing found runtime imports of `SegmentEntry`, `PackageReader`, `VerifiedPackage`, and `PackageReplayInputs`; the leaf must cover both canonical models and verified-access capabilities to remove the implementation edge rather than merely relocating structs.
- 2026-07-13: Began execution after rereading every governing reference. Source tracing confirmed finalized ingress accepted a package filesystem path and concrete reader, while Postgres retained the path into finalization. The selected boundary is one `VerifiedPackageAccess` capability over verified identity segments, recorded plan/schema, replay inputs, and quarantine evidence; it deliberately exposes no path, codec, verifier, or destination identity.
- 2026-07-13: Moved manifest/lifecycle/archive, replay-preimage, processed-observation, and quarantine evidence models into the new leaf as their sole owner. `cdf-package` retains only artifact I/O, verification, hashing, IPC/Parquet, archive, and persistence implementation. Every workspace consumer now imports canonical models directly; no old-owner re-export or conversion mirror exists.
- 2026-07-13: Replaced finalized-ingress and correction filesystem arguments with `SharedVerifiedPackageAccess`. Package verification remains minted and enforced by `cdf-package::VerifiedPackageReader`; neutral runtime and destinations see only verified facts. Postgres now carries the capability into commit finalization for quarantine evidence and validates correction identity through it instead of reopening a package path. Generic orchestration no longer forwards a package path or concrete reader to a destination.
- 2026-07-13: Added resolved-graph tests for the leaf and runtime. Both normal and normal+dev runtime graphs exclude `cdf-package`, Parquet, and Arrow IPC; normal also excludes tempfile. Current canonicalized unique counts are 76 normal and 83 normal+dev, down from the recorded 90-node normal baseline but still above the governing <=67 threshold. The remaining excess is not package implementation: `cdf-contract` alone resolves 65 packages and is now the dominant neutral-runtime edge. The threshold remains an open CG3 closure criterion; it must be solved without weakening the test or reintroducing package implementation.
- 2026-07-13: Verification: workspace all-target check passed; package suite passed 56/56 with 3 performance tests ignored; runtime suite passed 36/36 with 1 performance test ignored; four exact project replay/staging/checkpoint tests passed; both resolved-graph tests passed; touched-owner Clippy passed with `--no-deps -D warnings`. Full dependency Clippy was attempted and stopped on pre-existing `cdf-contract/src/expression.rs` `map_identity`, outside this batch.
- 2026-07-13: Closed the numerical graph gap by tracing all three remaining runtime uses of `cdf-contract`. `PhysicalSchemaObservation.observed_schema` was a derived duplicate that no consumer read; runtime's `ResolvedDestination`/`DestinationOutputSchema` facade was an unused duplicate of project orchestration; and structural Arrow-schema hashing was identity logic living above kernel's existing canonical Arrow vocabulary. Deleted both duplicates, moved the unchanged hash encoder and its goldens to kernel, migrated every caller directly, and removed now-unused `cdf-contract` edges from runtime plus all four native format drivers and the file source. No replacement facade or compatibility re-export remains.
- 2026-07-13: Final graph measurement is 55 unique normal packages and 62 normal+dev packages for `cdf-runtime`, versus the recorded 90-package baseline. The runtime graph test now enforces the <=67 normal ceiling in addition to the exact forbidden implementation/codec edges. The staged replay helper now accepts one `VerifiedPackageReader` instead of independently carrying a reader and verification token. The closure slice is 56 insertions and 433 deletions (net -377); extraction commit `36d7ee12` recorded 873 insertions and 662 deletions under Git's rename accounting.
- 2026-07-13: Final verification passed: focused kernel/contract/runtime/native-format/file-source suites; both resolved-graph tests; strict touched-owner all-target Clippy with `--no-deps -D warnings`; workspace all-target check; workspace formatting; and diff check. The previously observed identity-map lint was removed mechanically, allowing the strict gate to complete without an allowance.

## Blockers

None. Package bytes and lifecycle behavior are preservation constraints governed by active records.

## Evidence

- Canonical ownership: a workspace `rg` finds each manifest/file/segment/lifecycle/replay-preimage/quarantine type definition only under `crates/cdf-package-contract/src/`; a second search finds no canonical model imported through `cdf_package::`.
- Leaf/runtime graph laws: `cargo test -p cdf-package-contract --test build_graph -p cdf-runtime --test build_graph -j 12` passed. The tests resolve Cargo's graph and print the complete offending tree on failure.
- Package identity and bytes: `CARGO_BUILD_JOBS=12 cargo test -p cdf-package --lib -j 12` passed 56 tests, including fixed fixture hash determinism, replay preimages, exact verification/tamper rejection, archive identity preservation, receipts, runtime schema, status transitions, and verified segment streams; 3 explicit performance tests remained ignored.
- Runtime lifecycle: `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime --lib -j 12` passed 36 tests, including exact staged final binding, finalized-only failure closure, capability typing, and source/destination registries; 1 explicit performance test remained ignored.
- Product replay/checkpoint preservation: the exact project tests `generic_lock_plan_replay_and_recovery_drive_mock_runtime_without_destination_branch`, `generic_replay_streams_verified_segments_through_staged_final_binding`, `artifact_replay_reconstructs_delta_and_commit_request_from_package_files`, and `checkpoint_failure_after_receipt_keeps_receipt_recoverable_and_state_unadvanced` each passed.
- Integration compilation: `CARGO_BUILD_JOBS=12 cargo check --workspace --all-targets -j 12` passed after all direct-owner migrations.
- Static quality: `cargo fmt --all`, `git diff --check`, and touched-owner `cargo clippy -p cdf-package-contract -p cdf-package -p cdf-runtime --all-targets --no-deps -j 12 -- -D warnings` passed.
- Runtime graph ceiling: canonicalized `cargo tree` counts are 55 normal and 62 normal+dev for `cdf-runtime`, down from the 90-package baseline. `crates/cdf-runtime/tests/build_graph.rs` asserts the <=67 normal ceiling and all named forbidden-edge laws; the test passed after the final cut.
- Native discovery/hash preservation: focused unit suites passed for `cdf-kernel` (31), `cdf-contract` (89 with 2 release-only performance tests ignored), `cdf-runtime` (36 with 1 performance test ignored), Arrow IPC (1 with 1 release-only performance test ignored), JSON (4), Parquet (1), delimited (0), and file source (28). This includes the moved structural-hash goldens and remote/local registered-driver discovery and decode paths.
- Final static/product compilation: strict touched-owner all-target Clippy passed with `--no-deps -D warnings`; `CARGO_BUILD_JOBS=12 cargo check --workspace --all-targets -j 12` passed in 3m28s; `cargo fmt --all -- --check` and `git diff --check` passed.

## Review

### Fresh adversarial shaping review (2026-07-12)

#### Findings

No unresolved shaping finding after the ownership and graph-law repairs.

#### Confirmed boundaries

- The leak is concrete: runtime normal code imports `SegmentEntry`, `PackageReader`, `VerifiedPackage`, and `PackageReplayInputs`; staged final binding and destination preparation accept concrete readers and package paths. The 90-node runtime graph therefore reaches full IPC/Parquet/filesystem implementation.
- Canonical manifest/lifecycle/replay-preimage values already separate from I/O within `cdf-package`; filesystem helpers and readers remain package implementation. Moving the canonical values does not require a duplicate model or compatibility re-export.
- A narrow verified-access capability is required by multiple destination/staging consumers to preserve bounded streaming without letting runtime open package paths. This is a named dependency-inversion requirement, not a speculative plugin framework; `cdf-package` remains the sole implementation authority.
- E3 retains verification, containment, hash/read fusion, and I/O strategy ownership. CG3 may move its existing authority across the new contract but may not repair or redefine E3 semantics.

#### Verdict

**Pass for shaping.** CG3 is independently executable, provided the orchestrator schedules around the currently dirty package/runtime worktree and preserves E3's explicit boundary.

#### Residual risk

Golden bytes alone do not prove lifecycle equivalence. Closure still needs the named staging/final-binding, replay, receipt/checkpoint, corruption, and crash-matrix observations, plus direct graph evidence for both contract and runtime normal/dev resolutions.

### Execution-batch adversarial review (2026-07-13)

#### Findings

- **Significant / closure — runtime package-count threshold remains red.** The package implementation and every named codec/filesystem edge are gone, but the canonicalized runtime normal graph contains 76 unique packages rather than <=67. Adding a test with a relaxed number would encode behavior contrary to the active spec, so this batch deliberately adds only the exact forbidden-edge laws and leaves the numerical closure gate unclaimed.
- **No semantic regression found in the moved authority.** Canonical models have one owner; serialization derives and field order are unchanged; package canonical JSON still consumes the same Rust values; the fixed-hash suite passed. The access capability is destination-neutral and package-owned, and both ordinary and correction ingress use it without destination identity branches or filesystem exposure in runtime.
- **No legacy surface found.** The former `cdf-package` model module is deleted, the package crate does not re-export contract models, all workspace imports moved directly, and obsolete package-path parameters were removed from orchestration and destination traits rather than retained as ignored shims.

#### Verdict

**Concerns / batch is safe to commit, ticket is not closable.** The architecture and behavior slice is supported by the recorded evidence. CG3 must remain active until the <=67 runtime graph law is achieved and pinned.

#### Residual risk

Postgres live integration was compile-checked but not exercised against a live server in this batch. E3's already-recorded post-verification filesystem mutation/TOCTOU limits are unchanged: the new capability delegates to the same package implementation authority and does not claim to repair E3.

### Closure adversarial review (2026-07-13)

#### Findings

- No critical or significant closure finding. The remaining graph excess was removed by deleting unused/derived runtime state and relocating existing identity logic to its canonical lower owner; no threshold, feature, or test was weakened.
- Structural schema hashing has one definition in `cdf-kernel`, beside canonical Arrow vocabulary and existing SHA-256/hex dependencies. Every workspace caller migrated directly; `cdf-contract` has no legacy re-export.
- `PhysicalSchemaObservation` retains the exact Arrow schema and bounded sampling evidence. Removing an unconsumed `ObservedSchema::from_arrow` copy cannot change discovery output and removes format-driver coupling to the contract compiler.
- Runtime no longer contains the unused `ResolvedDestination` facade or destination identifier-policy adaptation. `cdf-project::ResolvedProjectDestination` remains the single product orchestration owner and performs the same destination-sheet/compiled-policy equality check before returning its private output-schema value.
- The staged replay call now binds reader and verification in one `VerifiedPackageReader`; it no longer permits those arguments to drift independently.

#### Verdict

**Pass.** Every CG3 acceptance criterion maps to recorded evidence, the exact graph ceiling is enforced, focused semantics and full workspace compilation are green, and no compatibility or duplicate owner remains.

#### Residual risk

No new residual risk from the graph-closure slice. The earlier live-Postgres and E3 filesystem-mutation limits remain owned by their existing integration/performance authorities and were neither widened nor claimed closed here.

## Retrospective

- Removing a concrete dependency exposed a second, independent build-graph concentration rather than making it disappear. Resolved-graph measurement must follow every boundary extraction immediately; direct-manifest checks alone would have falsely declared success.
- The decisive graph reduction came from asking why runtime needed each heavy edge, not from extracting another crate. Two uses were dead duplication and the third was identity logic already adjacent to its proper lower owner. This is the preferred graph-optimization sequence: eliminate, relocate to an existing authority, then consider a new boundary only if real independent consumers remain.
- Carrying one verified capability through ordinary, correction, and staged ingress removed more code than adapting only the first replay call and prevented structurally identical path/token leaks from surviving elsewhere.
- Strict Clippy exposed both a trivial identity map and a more meaningful reader-plus-verification parameter split. Fixing the underlying shapes produced cleaner code than allowances and made the final verified-package invariant more explicit.
