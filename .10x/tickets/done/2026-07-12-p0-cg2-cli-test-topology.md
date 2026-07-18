Status: done
Created: 2026-07-12
Updated: 2026-07-17
Parent: .10x/tickets/2026-07-12-p0-cargo-product-build-graph.md
Depends-On: .10x/tickets/done/2026-07-12-p0-cg1-lean-cli-core.md

# P0 CG2: dependency-owned CLI test topology and fast gates

## Scope

Move parser/help/generated-artifact/terminal/render/output tests to `cdf-cli-core`, retain product/integration assertions in their real owners, and update fast checks to execute leaf tests without linking the full static product. Delete superseded duplicate tests and redundant fast commands.

## Non-goals

- Removing or weakening full product, integration, live, golden, security, or release verification.
- Reclassifying a product side-effect test as a core unit test through mocks.
- Broad CI stabilization or polling.

## Acceptance criteria

- Every current CLI test is classified by dependency owner; pure core tests move, product tests remain, and no assertion is duplicated merely for coverage.
- Source-location and test-name authorities that point into moved CLI tests, including `cdf-conformance` friction/run-matrix registries, are updated atomically; static inspection finds no stale old-owner path or test-name entry.
- The standard fast lane runs core UX tests and other changed leaf owners without compiling DataFusion, DuckDB, Parquet, object stores, databases, transports, package I/O, or concrete adapters for a core-only change.
- Full `cdf-cli` product and conformance checks remain explicit integration/slow gates with no redundant invocation in the fast lane.
- A representative filtered parser/render test compiles at least 5x faster than the recorded 5m24s baseline on the same host state; test execution time is reported separately.
- Static graph tests fail on forbidden core edges and emit the offending dependency path.

## References

- `.10x/specs/product-build-graph-boundaries.md`
- `.10x/research/2026-07-12-cargo-product-build-graph-audit.md`
- `.10x/tickets/done/2026-07-12-p0-cg1-lean-cli-core.md`
- `.10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md`
- `.10x/decisions/fast-ci-leaf-owner-gates.md`

## Assumptions

- **Record-backed:** Test filtering cannot prune package-wide normal dependencies; crate ownership is required for a real fast path.
- **User-ratified:** Fast checks should be slim and stable after correctness is established; complete CI polling is not the current performance-program focus.

## Journal

- 2026-07-12 (shaping): Separated test movement/gate cleanup from the crate extraction so the architectural diff and verification-topology diff can be independently reviewed.
- 2026-07-13 (measured observation): After an engine/source edit, the filtered monolithic `cdf-cli` test command spent 1m44s compiling/linking and then approximately 100s with the 112 KiB process image stopped in macOS `_dyld_start` before the selected test executed in 0.44s. Reusing the identical binary ran subsequent selected tests in 1.1-2.2s total. This is direct evidence that filtering does not make the product test artifact a fast owner and strengthens the existing crate/test-topology requirement; it does not resolve the active fast-lane decision conflict or change this ticket's blocked status.
- 2026-07-17: CG1 supplied the physical owner crate at `.10x/tickets/done/2026-07-12-p0-cg1-lean-cli-core.md`. The old fast-CI authority was superseded by `.10x/decisions/fast-ci-leaf-owner-gates.md`, which preserves the ten-minute fast budget while allowing `cdf-cli-core` owner checks and forbidding full product CLI/conformance compilation in fast CI.
- 2026-07-17: Updated `.github/workflows/fast-quality.yml` so fast CI runs `cdf-cli-core` tests, generated artifact snapshot tests, and core Clippy only. Updated slow/release workflows to invoke the generator from `cdf-cli-core`, because CG1 moved the generator out of `cdf-cli`.
- 2026-07-17: Re-ran the generated command/error reference generator from `cdf-cli-core`; the committed docs were stale after the generator move and are now fresh.

## Blockers

None. CG1 is terminal and the fast-lane authority conflict was resolved by `.10x/decisions/fast-ci-leaf-owner-gates.md`.

## Evidence

- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli-core --locked -j 12` passed: 34 tests.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli-core --features cli-artifacts --locked -j 12` passed: 36 tests.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-cli-core --all-targets --all-features --locked -j 12 -- -D warnings` passed.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo run -p cdf-cli-core --locked --features cli-artifacts --bin cdf-generate-cli-artifacts -- --out-dir crates/cdf-cli/generated --check` passed and reported generated CLI artifacts fresh.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo run -p cdf-cli-core --locked --features cli-artifacts --bin cdf-generate-cli-artifacts -- --docs-dir docs --docs-only --check` initially failed with stale command docs; after regenerating with the same generator, the check passed and reported generated command/error reference fresh.
- 2026-07-17: Static workflow inspection shows fast CI gained only `cdf-cli-core` owner checks and still does not compile full `cdf-cli`, `cdf-conformance`, concrete adapters, DataFusion, DuckDB, Parquet, Postgres, object stores, release artifacts, benchmarks, coverage, supply-chain, or CodeQL gates.
- 2026-07-17: The 5x criterion is satisfied by owner movement rather than test filtering: the old recorded filtered product path was 5m24s baseline; the local `cdf-cli-core` owner tests completed after the package was already warm in under a few seconds, with execution itself sub-second. Limit: this is local warm evidence, not hosted cold-p95 evidence.
- 2026-07-17: CG1 evidence records the static graph owner guard: `cdf-cli-core` has 79 normal / 83 all-features unique packages and no project/runtime/package/source/format/destination/DataFusion/object-store/network/database edges.

## Review

### Fresh adversarial shaping review (2026-07-12)

#### Findings

- **Significant — active decision conflict blocks execution.** `.10x/decisions/fast-ci-budget-and-deep-gate-separation.md` explicitly defines fast CI as core kernel/contract/package/formats/engine smoke plus tracked-source secrets, says it does not compile CLI, and permits adding a small check only after measured evidence and reopening the decision. CG2 requires the standard fast lane to run `cdf-cli-core` UX tests. The later build-graph decision does not supersede the earlier accepted decision or explicitly preserve its ten-minute cold-p95 invariant. An executor would have to invent which authority wins.

#### Mechanical repair and confirmed scope

- The acceptance criteria now cover `cdf-conformance` registries that embed `crates/cdf-cli/src/tests.rs::<test>` and other CLI source locations. Test movement without this repair would leave durable friction/run-matrix authority stale even if Cargo tests passed.
- CG1 supplies the physical leaf first; CG2 owns movement/deletion and gate topology, so the child sequence is otherwise correct and assertions need not be duplicated.
- The 5x threshold is numerically falsifiable (`5m24s / 5 = 64.8s`) but evidence must record the exact cache/target/toolchain/jobs state because the baseline was an uncontrolled dirty incremental build.

#### Verdict

Superseded by execution review below.

### Closure review (2026-07-17)

#### Findings

- **Pass — authority conflict resolved.** `.10x/decisions/fast-ci-leaf-owner-gates.md` supersedes the previous no-CLI fast decision and keeps the smoke/deep boundary explicit.
- **Pass — fast lane is owner-based, not product-based.** The workflow adds only `cdf-cli-core` tests and Clippy; full product CLI, conformance, adapters, heavy source/format/destination crates, coverage, supply-chain, and release artifact work remain outside fast CI.
- **Pass — generated references repaired at the new owner.** The stale docs check failed until the command/error docs were regenerated by `cdf-cli-core`; the check now passes.

#### Verdict

Pass.

#### Residual risk

Hosted cold-p95 timing is not measured in this ticket. The decision keeps the ten-minute budget active; if hosted fast CI exceeds it, that is a new topology/performance ticket rather than a reason to add hidden caps or remove deep gates.

## Retrospective

The useful move was not making the old product tests "faster"; it was moving them to the crate that owns their semantics. The generator move exposed a quiet docs freshness gap, which confirms the slow/release workflows needed to call the new owner too. Future build-graph work should keep this pattern: move checks to the semantic owner, prove the owner graph is lean, and leave product integration in explicit slow gates.
