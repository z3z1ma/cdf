Status: active
Created: 2026-07-17
Updated: 2026-07-17
Supersedes: `.10x/decisions/fast-ci-budget-and-deep-gate-separation.md`

# Fast CI leaf-owner gates

## Context

The prior fast-CI decision correctly removed the thirty-minute pseudo-deep workflow and made tracked-source Gitleaks structural. It also explicitly excluded CLI compilation because the product CLI then linked the complete source/format/destination/runtime graph for parser and renderer tests.

CG1 changed that premise. `cdf-cli-core` now owns grammar, terminal policy, renderer/output/progress primitives, error catalog mappings, and generated CLI artifact checks while linking only the leaf presentation graph plus `cdf-kernel`. The current measured local evidence records 79 normal / 83 all-features unique packages for `cdf-cli-core`, with no project/runtime/package/source/format/destination/DataFusion/object-store/network/database edges. The user has explicitly ratified making fast checks materially lean and avoiding redundant slow product checks.

## Decision

Fast CI remains a smoke gate with a cold p95 budget of ten minutes. It keeps exactly two jobs:

1. **Core Rust smoke:** locked metadata parse, formatting, one nonredundant core Clippy path for kernel/contract/package/runtime/engine libraries, focused core library tests, and the `cdf-cli-core` UX owner checks.
2. **Tracked-source secrets:** the pinned Gitleaks binary scans a `git archive` of `HEAD` so build output cannot enter the source boundary.

The `cdf-cli-core` checks are:

- `cargo test -p cdf-cli-core --locked`
- `cargo test -p cdf-cli-core --features cli-artifacts --locked`
- `cargo clippy -p cdf-cli-core --all-targets --all-features --locked -- -D warnings`

Fast CI MUST NOT compile the complete `cdf-cli` product crate, conformance harness, concrete destinations, source transports, DataFusion, DuckDB, Parquet, Postgres, object stores, release artifacts, benchmarks, generated-reference slow docs outside the `cdf-cli-core` artifact snapshot owner, duplication, supply-chain, coverage, or CodeQL gates. Those remain slow/manual/release responsibilities.

Local change-set verification remains risk-driven under `QUALITY.md`. A hot-path performance change still needs direct benchmark or end-to-end timing evidence; a fast smoke pass is not performance evidence.

## Alternatives considered

- Keep the old no-CLI fast gate. Rejected because it would ignore the new leaf owner and let parser/render/help regressions wait for slow product verification despite the core graph now being cheap and isolated.
- Move full `cdf-cli` tests into fast CI. Rejected because that recreates the exact adapter/product link cost the build-graph program exists to remove.
- Path-filter the workflow by changed crate. Deferred because maintaining a second dependency model remains more complexity than the current two-job smoke gate needs.
- Drop Gitleaks while performance work is active. Rejected because source-only scanning is already structurally bounded and protects a high-cost trust boundary.

## Consequences

Fast CI covers CLI grammar/render/help regressions at their dependency owner without pulling the production adapter graph into every push. Slow quality and release workflows must call the `cdf-cli-core` artifact generator because the old `cdf-cli` generator feature/bin no longer exists. If hosted cold p95 exceeds ten minutes again, the answer is to remove redundant work or improve cache/build topology, not to hard-code hidden performance caps or silently weaken deep gates.
