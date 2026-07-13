Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: None

# P0: Cargo product build-graph boundaries

## Scope

Reduce focused compile latency by enforcing the lean CLI core, complete static product, dependency-owned test topology, and package-contract leaf specified in `.10x/specs/product-build-graph-boundaries.md`. This parent coordinates children; it is not executable.

## Child sequence and integration points

- CG1 and CG3 are independent child outcomes and MAY proceed in parallel only after CG1's active-DX3 dependency closes; CG3 has no dependency on DX3.
- CG1 reuses the existing SX1/FX1/DX3/DX4 composition catalogs; it does not recreate them.
- CG2 depends on CG1 and migrates tests/gates only after the leaf boundary exists.
- CG3 changes package type ownership while preserving package bytes and runtime lifecycle semantics; product imports reconcile at integration.
- DataFusion adapter containment is already owned by WS-J/J6. This parent adds no duplicate child and accepts J6's resulting graph evidence.

## Acceptance criteria

- CG1-CG3 close with evidence mapped to every criterion in the governing spec.
- The production `cdf` binary retains its full command and first-party adapter catalog.
- Fast checks target dependency owners and do not link the complete product; full product verification remains an explicit integration gate.
- Static graph laws pin the exact forbidden edges and threshold metrics from the governing spec.
- SX1, FX1, DX3/DX4, and J6 references/statuses identify one authority for their respective catalogs/adapters.

## References

- `.10x/research/2026-07-12-cargo-product-build-graph-audit.md`
- `.10x/decisions/lean-cli-and-package-contract-build-boundaries.md`
- `.10x/specs/product-build-graph-boundaries.md`
- `.10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md`
- `.10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md`
- `.10x/tickets/2026-07-11-p0-dx3-generic-lock-doctor-replay.md`
- `.10x/tickets/2026-07-11-p0-dx4-conformance-extension-law.md`
- `.10x/tickets/2026-07-12-p3-j6-datafusion-selective-adoption-audit.md`

## Assumptions

- **Record-backed:** Compile-time isolation is now justified under the active CLI renderer decision by the measured 377-package graph and 5m24s filtered compile.
- **Record-backed:** The standard source/destination/format registries remain the only composition authorities under SX1/FX1/DX3/DX4.
- **User-ratified:** Fast checks and the Cargo graph must be made materially lean while complete production correctness remains available outside the fast lane.

## Journal

- 2026-07-12 (shaping): Fished active/terminal owners and measured the default normal graph. Selected one parent with three bounded children. J6 already fully owns DataFusion selective-adapter containment, so no competing CG4 was opened.

## Blockers

CG2 has an unresolved conflict with `.10x/decisions/fast-ci-budget-and-deep-gate-separation.md`; see its review. CG1/CG3 may follow their own dependency/ownership gates, but the parent cannot close until CG2 is superseded or narrowed and executed.

## Evidence

Pending children.

## Review

### Fresh adversarial shaping review (2026-07-12)

#### Findings

- **Significant — CG2 conflicts with the active fast-CI decision and is not executable as written.** `.10x/decisions/fast-ci-budget-and-deep-gate-separation.md` fixes fast CI to two jobs, explicitly says it does not compile CLI, and requires measured evidence plus a reopened decision before adding a small nonredundant check. The new build-graph decision/spec and CG2 require `cdf-cli-core` UX tests in the standard fast lane but neither supersedes that accepted decision nor records the required hosted/cold-budget evidence. Accepted decisions are immutable authority; an executor cannot silently choose between them. Shape a superseding decision that preserves the ten-minute cold-p95 invariant and names the exact added core command, or remove the fast-CI mutation from CG2 while retaining local/slow owner tests.

#### Mechanical repairs and confirmations

- CG1 now depends on active DX3 rather than only DX3A. DX3 already depends on DX3A and currently owns `cdf-cli` report/doctor/replay paths whose imports CG1 must rewire, so the repaired edge prevents both tickets from becoming executable against the same product crate at once.
- CG2 now names the source-location/test-name registries that must move with tests and references the active fast-CI decision. This prevents stale `cdf-conformance` friction authority from surviving a path move.
- CG3 now explicitly preserves E3's verification/read-strategy authority and owns a contract-leaf graph assertion. The leaf is justified by a current lower-layer leak and multiple neutral destination/staging consumers; it is not a speculative plugin/provider framework.
- Offline locked `cargo tree` inspection reproduced the research snapshot after canonicalizing duplicate annotations: `cdf-cli` 377 unique packages/33 workspace packages, `cdf-engine` 209/7, `cdf-runtime` 90/6, and `cdf-package` 62/3. `cdf-cli` has 41 direct normal edges, 30 workspace edges. No build or test command was run.
- Source inspection confirmed one CLI composition module for the three source drivers, five format drivers, all current transform drivers, and the three destination drivers. The new records preserve those catalogs in `cdf-cli`; SX1/FX1/DX3/DX4 retain behavioral/catalog authority. J6/WS-J remain the sole DataFusion selective-adoption owners.

#### Verdict

**Fail / shaping blocked at CG2.** The research facts, decision boundary, spec, CG1 ordering after repair, CG3 boundary, SX1 edit, and J6 edit were not otherwise falsified. The parent must not dispatch CG2 until the active fast-CI decision conflict is explicitly superseded or CG2 is narrowed.

#### Residual risk

The 5x timing criterion is falsifiable only if the executor records toolchain, `CARGO_BUILD_JOBS`, target/cache state, exact filter, and compile/test times separately; the existing 5m24s dirty incremental observation is supporting evidence, not a controlled causal benchmark. CG1/CG3 still require the governing full product/golden/crash evidence at execution; this review intentionally ran no build or test.

## Retrospective

Pending closure.
