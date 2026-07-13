Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-12-p0-cargo-product-build-graph.md
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
- `.10x/tickets/2026-07-11-p3-e3-streaming-verification-replay-io.md`

## Assumptions

- **Record-backed:** Package artifact identity remains package-domain authority rather than kernel authority; runtime validates lifecycle over verified facts but must not own I/O.
- **Record-backed:** No production consumer requires compatibility re-exports or old artifact/Rust API paths.

## Journal

- 2026-07-12 (shaping): Source tracing found runtime imports of `SegmentEntry`, `PackageReader`, `VerifiedPackage`, and `PackageReplayInputs`; the leaf must cover both canonical models and verified-access capabilities to remove the implementation edge rather than merely relocating structs.

## Blockers

None. Package bytes and lifecycle behavior are preservation constraints governed by active records.

## Evidence

Pending execution.

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

## Retrospective

Pending execution.
