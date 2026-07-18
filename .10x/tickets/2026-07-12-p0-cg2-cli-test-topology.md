Status: blocked
Created: 2026-07-12
Updated: 2026-07-13
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
- `.10x/decisions/fast-ci-budget-and-deep-gate-separation.md`

## Assumptions

- **Record-backed:** Test filtering cannot prune package-wide normal dependencies; crate ownership is required for a real fast path.
- **User-ratified:** Fast checks should be slim and stable after correctness is established; complete CI polling is not the current performance-program focus.

## Journal

- 2026-07-12 (shaping): Separated test movement/gate cleanup from the crate extraction so the architectural diff and verification-topology diff can be independently reviewed.
- 2026-07-13 (measured observation): After an engine/source edit, the filtered monolithic `cdf-cli` test command spent 1m44s compiling/linking and then approximately 100s with the 112 KiB process image stopped in macOS `_dyld_start` before the selected test executed in 0.44s. Reusing the identical binary ran subsequent selected tests in 1.1-2.2s total. This is direct evidence that filtering does not make the product test artifact a fast owner and strengthens the existing crate/test-topology requirement; it does not resolve the active fast-lane decision conflict or change this ticket's blocked status.

## Blockers

Depends on CG1's physical crate boundary. Separately, its fast-lane acceptance conflicts with `.10x/decisions/fast-ci-budget-and-deep-gate-separation.md`; execution requires an explicit superseding decision that preserves the cold-p95 budget or a narrowed CG2 scope that leaves the decision-fixed fast workflow unchanged.

## Evidence

Pending execution.

## Review

### Fresh adversarial shaping review (2026-07-12)

#### Findings

- **Significant — active decision conflict blocks execution.** `.10x/decisions/fast-ci-budget-and-deep-gate-separation.md` explicitly defines fast CI as core kernel/contract/package/formats/engine smoke plus tracked-source secrets, says it does not compile CLI, and permits adding a small check only after measured evidence and reopening the decision. CG2 requires the standard fast lane to run `cdf-cli-core` UX tests. The later build-graph decision does not supersede the earlier accepted decision or explicitly preserve its ten-minute cold-p95 invariant. An executor would have to invent which authority wins.

#### Mechanical repair and confirmed scope

- The acceptance criteria now cover `cdf-conformance` registries that embed `crates/cdf-cli/src/tests.rs::<test>` and other CLI source locations. Test movement without this repair would leave durable friction/run-matrix authority stale even if Cargo tests passed.
- CG1 supplies the physical leaf first; CG2 owns movement/deletion and gate topology, so the child sequence is otherwise correct and assertions need not be duplicated.
- The 5x threshold is numerically falsifiable (`5m24s / 5 = 64.8s`) but evidence must record the exact cache/target/toolchain/jobs state because the baseline was an uncontrolled dirty incremental build.

#### Verdict

**Fail / not executable.** Obtain a superseding fast-CI decision with measured budget evidence, or narrow CG2 so it changes local/slow owner commands without modifying the decision-fixed fast workflow.

#### Residual risk

Even after authority is reconciled, classifying the monolithic CLI test module must preserve product side-effect/security assertions in the product or conformance owner. A core-only mock of those tests would satisfy compile speed while weakening coverage and is expressly forbidden.

## Retrospective

Pending execution.
