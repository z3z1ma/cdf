Status: done
Created: 2026-08-01
Updated: 2026-08-01

# Rust crate architecture cleanup program

## Scope

Close every actionable finding from the 2026-08-01 repository-wide audit of all 52 Rust crates and 480 Rust files without changing product behavior.

This parent coordinates the following executable children:

1. `.10x/tickets/done/2026-08-01-engine-execution-decomposition.md`
2. `.10x/tickets/done/2026-08-01-destination-module-boundaries.md`
3. `.10x/tickets/done/2026-08-01-product-runtime-module-boundaries.md`
4. `.10x/tickets/done/2026-08-01-core-public-facade-exports.md`
5. `.10x/tickets/done/2026-08-01-iceberg-public-boundary.md`
6. `.10x/tickets/done/2026-08-01-task-store-crate-root-split.md`
7. `.10x/tickets/done/2026-08-01-format-transport-crate-root-splits.md`
8. `.10x/tickets/done/2026-08-01-memory-foreign-stream-crate-root-splits.md`
9. `.10x/tickets/done/2026-08-01-cli-test-layout.md`
10. `.10x/tickets/done/2026-08-01-engine-test-layout.md`
11. `.10x/tickets/done/2026-08-01-project-test-layout.md`
12. `.10x/tickets/done/2026-08-01-crate-documentation-polish.md`

Independent children may run in parallel. The project-test child follows the product/runtime module-boundary child because both touch project test-module declarations. Documentation polish follows the format/transport split and core-facade enumeration to avoid root-file conflicts. All other children are independent unless an executor records a newly discovered overlap.

Integration occurs only after every child has journaled evidence and independent review. The aggregate closure judge checks the final workspace dependency graph, formatting, strict lint, tests, public-API preservation, module reachability, and absence of production wildcard ownership edges.

## Non-goals

- No product behavior, artifact schema, persistence, error taxonomy, CLI shape, performance default, or runtime policy change.
- No broad symbol renaming or public API redesign.
- No removal of `cdf-wasm`; active records intentionally park it as a future boundary.
- No compatibility layer for obsolete CDF behavior.

## Acceptance Criteria

- Every child is terminal with acceptance criteria mapped to journaled evidence.
- No production `use crate::*`, `use super::*`, or equivalent internal prelude glob remains in the scoped production modules; test-only aggregation is allowed.
- Scoped public facades enumerate exports and do not accidentally add or remove public symbols, except the explicitly approved Iceberg visibility narrowing.
- Large crate roots and white-box test monoliths named by the audit are split into focused, compiler-visible modules with stable behavior.
- `cdf-engine` package execution is decomposed so no single orchestration function owns the complete source-open through package-finalization lifecycle.
- The complete workspace passes the proportionate final quality gates recorded by the children and aggregate judge.

## References

- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/evidence/2026-07-31-connector-mode-readiness-closure.md`

## Assumptions

- Record-backed: this is organization and visibility hardening, not net-new product behavior, so the active architecture and organization records govern without a new behavioral spec.
- Record-backed: the current Rust tree is the exact tree covered by the 2026-07-31 21-job quality certificate; later commits changed only records.
- User-ratified: the user explicitly authorized execution of every audit finding on 2026-08-01.

## Journal

- 2026-08-01: Opened this parent and twelve bounded children from the completed read-only audit. No implementation was performed in the shaping turn.
- 2026-08-01: Began orchestration wave 1 with independent executors for engine execution decomposition, destination boundaries, and project/Python/runtime boundaries. Parent remains coordination-only.
- 2026-08-01: All twelve child implementations handed back with ticket-scoped evidence and retrospectives. Independent OCR-delegated red-team review was then completed for every child by an agent that did not author that child.
- 2026-08-01: Independent review passed core facade enumeration and the Iceberg public-boundary narrowing. The other ten children remain active with named repair or verification blockers below; no review finding was silently repaired.
- 2026-08-01: User explicitly authorized every named closure repair and privileged validation. Began repair wave 1 on engine execution lifecycle equivalence, destination module cycles, and project/Python/runtime globs and cycles.
- 2026-08-01: Every review finding was repaired under its owning child and independently re-reviewed. Aggregate execution additionally found and closed three stale architecture-test assumptions exposed by the new focused test trees: runtime-owner classification, destination-import allow roots, and conformance registry source paths.
- 2026-08-01: Final integration completed on the reconciled tree. All twelve children have passing final reviews, their acceptance criteria map to journaled evidence, and the aggregate quality profile below passed with no unresolved defect.

## Blockers

None.

## Evidence

- Implementation, focused validation, independent review, repair, and re-review evidence is recorded in every child ticket. Each child has a final pass verdict and no blocker.
- Final formatting and compilation: `cargo fmt --all -- --check` passed; locked workspace all-target checks passed with default features, all features, and no default features.
- Final lint: locked workspace all-target Clippy passed with `-D warnings` in both default and all-feature modes.
- Final behavior: workspace Nextest excluding the separately owned benchmark and conformance packages passed 1,934/1,934 with 40 scheduled or ignored cases skipped. The complete conformance package passed 97/97 with seven scheduled cases skipped. The benchmark package passed 53/53 outside the host-restricted sandbox with two scheduled cases skipped.
- Focused behavior: destination suites passed 58 DuckDB, 46 Parquet, and 35 PostgreSQL tests; CLI passed 300/300; project passed 270/270; HTTP transport passed 16/16. Existing benchmark-style ignores are recorded by their child tickets.
- Product integration: `tools/product-smoke-matrix.sh` passed all eleven selected paths: five CLI, two project, one conformance parity law, and three Iceberg projection/authority tests.
- Documentation: all workspace doctests passed, including the `cdf-runtime` compile-fail contract; warning-denied workspace documentation generated successfully for all crates.
- Public API: the facade children reproduce their exact public-name inventories. Patch-level `cargo semver-checks` for Iceberg ran 223 checks: 218 passed and the five failures were exactly the user-approved pre-1.0 visibility narrowing recorded by the Iceberg child; no additional API drift appeared.
- Dependency and patch hygiene: `cargo deny check` passed advisories, bans, licenses, and sources; `cargo audit` passed with the repository-allowed `paste 1.0.15` unmaintained warning; `cargo machete` found no unused dependency; final `git diff --check` passed.

## Review

Every child has an independent review with findings, verdict, and residual risk in its `Review` section. Every significant and minor finding raised during review or aggregate execution was repaired and received a fresh pass verdict. Aggregate verdict: **Pass**. The final source, module ownership, public facades, documentation, tests, feature modes, dependency policy, and integration behavior agree with the program's behavior-preserving contract.

Residual risk is bounded and accepted: scheduled/ignored stress and benchmark cases were not forced outside their normal profiles; RustSec continues to report the already-allowed unmaintained `paste 1.0.15` dependency; and the Iceberg compatibility break is the exact user-ratified pre-1.0 upstream-type boundary closure. None was introduced accidentally or leaves an unowned repair.

## Retrospective

- The dominant failure mode was not the module moves themselves but architecture checks that encoded historical filenames. Compiler-visible module ownership and structural Rust parsing are materially more durable than filename substrings or raw source-text matching.
- Exact public-name inventories, item-level test parity, and independent adversarial review made large mechanical moves falsifiable without confusing text churn for semantic change.
- Running the full aggregate profile found cross-crate consumers that focused crate suites could not expose. Keeping conformance and product smoke as separate closure gates was necessary and justified.
- No follow-up code ticket is required. The remaining limits are scheduled validation policy, an accepted advisory warning, and the explicitly approved Iceberg compatibility narrowing recorded above.
