Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md
Verdict: pass

# DataFusion git-pin Arrow 59 tuple review

## Target

Review of the ratified DataFusion git-pin tuple implementation, including Cargo manifests, `Cargo.lock`, supply-chain policy, test fixture tuple literals, and the closure evidence in `.10x/evidence/2026-07-07-datafusion-git-pin-arrow59-tuple.md`.

## Findings

No blocking findings.

The implementation follows `.10x/decisions/datafusion-git-pin-arrow59-tuple.md`: DataFusion is pinned to the exact Apache git rev `7ff7278edc1bf7446303bff51e5883a38414bbdf`, first-party Arrow/Parquet crates resolve to `59.1.0`, and the Python bridge remains on the current `pyo3 0.29` / `pyo3-arrow 0.19` line. The published-crate Arrow 58 repin path that introduced unratified PyO3 and Thrift findings was not revived.

The lower-layer boundary is preserved. The direct source scan found no DataFusion references in `cdf-kernel` or other lower crates, while `cargo tree -i datafusion@54.0.0` shows the dependency enters through `cdf-engine`.

The supply-chain posture is honest. `cargo deny`, `cargo audit`, `cargo vet`, Semgrep, CodeQL, and cargo-machete passed. OSV reports only the already-ratified `paste 1.0.15` advisory. CodeQL refreshed the reusable database for a legitimate input change and produced 0 SARIF findings.

The main residual risk is not hidden: the workspace still carries Arrow `58.3.0` through `duckdb 1.10504.0`. That is outside the DataFusion engine tuple and is now owned by `.10x/tickets/2026-07-07-duckdb-arrow58-transitive-residual.md`.

## Verdict

Pass. The DataFusion tuple alignment ticket can close. The next DataFusion adapter ticket is no longer blocked by Arrow/DataFusion type incompatibility, but it must still preserve the D-1 boundary and the residual-filter semantics in `.10x/decisions/datafusion-tier-b-delegation-boundary.md`.

## Residual Risk

The git dependency is intentionally time-boxed and must be removed when crates.io publishes a DataFusion release with the same Arrow 59.x tuple. The DuckDB Arrow 58 residual may continue to produce duplicate-version warnings until the follow-up evaluates a duckdb-rs upgrade, feature change, or explicit acceptance policy.
