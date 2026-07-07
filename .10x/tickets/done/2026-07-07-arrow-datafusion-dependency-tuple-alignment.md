Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/decisions/datafusion-tier-b-delegation-boundary.md, .10x/decisions/arrow-datafusion-tuple-policy.md, .10x/decisions/datafusion-git-pin-arrow59-tuple.md

# Align the Arrow/DataFusion dependency tuple

## Scope

Resolve the D-28 dependency tuple mismatch that blocks real DataFusion `TableProvider` execution over CDF resource streams.

This ticket owns the dependency decision and, once unblocked, the smallest implementation work needed to make CDF first-party Arrow types and DataFusion execution types compatible without a permanent hot-path Arrow-major bridge.

## Current facts

As of 2026-07-07:

- `cargo info datafusion` reports latest/current `datafusion 54.0.0`.
- `cargo info arrow-array` reports latest/current `arrow-array 59.0.0`.
- `crates/cdf-engine/Cargo.toml` depends directly on `arrow-array 59.0.0`, `arrow-schema 59.0.0`, `arrow-select 59.0.0`, and `datafusion 54.0.0`.
- `cargo tree -p cdf-engine --locked -i arrow-array@58.3.0` shows DataFusion 54's dependency graph uses Arrow 58.3.0.
- `cargo tree -p cdf-engine --locked -i arrow-array@59.0.0` shows CDF kernel/package/engine/native Parquet paths use Arrow 59.0.0.

## Acceptance criteria

- Decide and record one dependency tuple path:
  - wait for/upgrade to a DataFusion release compatible with CDF's Arrow major;
  - deliberately repin CDF first-party Arrow crates to DataFusion's Arrow major;
  - or explicitly ratify a temporary bridge with scope, expiry, benchmarks, and artifact-safety gates.
- Preserve `.10x/decisions/native-arrow-datafusion-parquet-policy.md` and its narrow `RUSTSEC-2024-0436` exception unless explicitly superseded.
- If dependency versions change, run the golden-package suite as an artifact-compatibility gate and record byte-stability evidence.
- If dependency versions change, run supply-chain scanners and prove no unratified advisory is introduced.
- Update the eventual lockfile/dependency tuple record once that spec exists.
- Leave kernel public APIs free of DataFusion types.

## Evidence expectations

- Registry and lockfile evidence for selected Arrow/DataFusion versions.
- `cargo tree --locked` evidence proving the resulting tuple.
- Golden-package determinism and artifact compatibility evidence if versions change.
- Focused compile/test/clippy evidence for crates affected by the tuple.
- Supply-chain evidence covering `cargo deny`, `cargo audit`, OSV, and cargo-vet under the then-current policy.

## Explicit exclusions

No generic `TableProvider` adapter, no explain/operator metadata changes, no predicate-language expansion, no package format change, no new supply-chain advisory exception, and no permanent Arrow-major bridge unless a new decision explicitly ratifies it.

## References

- `VISION.md` D-28
- `.10x/decisions/datafusion-tier-b-delegation-boundary.md`
- `.10x/decisions/native-arrow-datafusion-parquet-policy.md`
- `.10x/research/2026-07-07-datafusion-delegation-pushdown-triage.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/specs/package-lifecycle-determinism.md`

## Progress and notes

- 2026-07-07: Opened from DataFusion delegation triage. The recommended default is no permanent Arrow 58/59 engine hot-path bridge. The execution-critical blocker is whether CDF should wait for DataFusion to align with Arrow 59, repin first-party Arrow to DataFusion's Arrow major after golden-suite proof, or explicitly ratify a temporary bridge.
- 2026-07-07: User ratified `.10x/decisions/arrow-datafusion-tuple-policy.md` with a hard clarification that DataFusion is mandatory day-zero architecture. This ticket is no longer blocked on product preference; next execution must inspect the current registry/lockfile tuple and choose the smallest same-major-compatible path under that decision.
- 2026-07-07: Recorded current tuple research in `.10x/research/2026-07-07-arrow-datafusion-current-tuple.md`. Parent-observed registry and lockfile evidence shows `datafusion 54.0.0` uses Arrow/Parquet 58.3.0, current CDF first-party crates use Arrow/Parquet 59.0.0, and `pyo3-arrow 0.19.0` is also on Arrow 59. The smallest same-major path under the active policy is a deliberate first-party repin to the Arrow/Parquet 58.3.0 tuple, including Python bridge tuple fallout, followed by golden-package and supply-chain evidence.
- 2026-07-07: Worker mechanically repinned first-party Arrow/Parquet manifests and lockfile to the published DataFusion 54 / Arrow 58.3.0 tuple and moved `cdf-python` to `pyo3-arrow 0.17.0` with the PyO3/Numpy 0.28 line. `cargo metadata --locked --format-version 1`, requested `cargo tree` tuple checks, `cargo check --workspace --all-targets --locked`, and `cargo test -p cdf-conformance golden --locked --no-fail-fast` passed in the attempted worktree. Parent verification found unratified supply-chain failures: `cargo audit` reports `RUSTSEC-2026-0176` and `RUSTSEC-2026-0177` for `pyo3 0.28.3`, and OSV reports `GHSA-2f9f-gq7v-9h6m` for `thrift 0.17.0` introduced through `parquet 58.3.0`. Parent reverted the manifest/lockfile attempt from the worktree; do not commit the published-crate Arrow 58 repin without a new policy decision.
- 2026-07-07: Recorded follow-up supply-chain research in `.10x/research/2026-07-07-arrow-datafusion-git-tuple-supply-chain.md`. A temporary `/tmp` Cargo graph using DataFusion git rev `7ff7278edc1bf7446303bff51e5883a38414bbdf` resolved to Arrow/Parquet 59.1.0, `pyo3 0.29.0`, and `pyo3-arrow 0.19.0`; `cargo audit` and OSV reported only the already-ratified `paste 1.0.15` advisory. This candidate still needs user ratification because it pins an unreleased git source for DataFusion.
- 2026-07-07: User ratified the DataFusion git-pin path. Recorded `.10x/decisions/datafusion-git-pin-arrow59-tuple.md`, reopening this ticket for implementation with DataFusion rev `7ff7278edc1bf7446303bff51e5883a38414bbdf`.
- 2026-07-07: Implemented the ratified DataFusion git-pin tuple in the current worktree. `cdf-engine` now depends on `https://github.com/apache/datafusion.git` at rev `7ff7278edc1bf7446303bff51e5883a38414bbdf`; CDF first-party Arrow/Parquet manifests and lockfile entries are on `59.1.0`; `cdf-python` remains on `pyo3-arrow 0.19.0`, `pyo3 0.29.0`, and `numpy 0.29.0`; `deny.toml` allows only the exact Apache DataFusion git URL; and `supply-chain/config.toml` has exact `safe-to-deploy` exemptions for the newly resolved Arrow/Parquet `59.1.0` crates plus `itertools 0.15.0`. No source compile fixes were required. Verification passed: `cargo metadata --locked --format-version 1`, `cargo tree --workspace --locked -i arrow-array@59.1.0`, `cargo tree --workspace --locked -i pyo3@0.29.0`, `cargo tree --workspace --locked -i pyo3-arrow@0.19.0`, `cargo check --workspace --all-targets --locked`, `cargo test -p cdf-package --locked --no-fail-fast`, `cargo test -p cdf-conformance golden --locked --no-fail-fast`, `cargo fmt --all -- --check`, `git diff --check -- . ':(exclude).gitignore'`, `cargo deny check`, `cargo audit`, and `cargo vet --locked`. `cargo tree --workspace --locked -i thrift@0.17.0` found no package. `osv-scanner --lockfile Cargo.lock` exited nonzero only for the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` advisory.
- 2026-07-07: Parent closure verification recorded in `.10x/evidence/2026-07-07-datafusion-git-pin-arrow59-tuple.md` and reviewed in `.10x/reviews/2026-07-07-datafusion-git-pin-arrow59-tuple.md`. The final quality pass included full nextest, doc/doc-warning gates, feature matrix, semver checks against `HEAD`, cargo-machete, supply-chain scanners, Semgrep, and reusable CodeQL. The remaining DuckDB Arrow 58 duplicate path is outside the DataFusion engine tuple and is owned by `.10x/tickets/done/2026-07-07-duckdb-arrow58-transitive-residual.md`.

## Blockers

None. The DataFusion engine tuple is aligned on Arrow 59.1.0. The separate DuckDB Arrow 58 residual is tracked by `.10x/tickets/done/2026-07-07-duckdb-arrow58-transitive-residual.md`.
