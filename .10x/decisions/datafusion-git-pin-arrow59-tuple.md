Status: active
Created: 2026-07-07
Updated: 2026-07-07

# DataFusion Git Pin for Arrow 59 Tuple

## Context

`.10x/decisions/arrow-datafusion-tuple-policy.md` requires DataFusion as a day-zero engine dependency and rejects a permanent Arrow-major bridge in the engine hot path.

`.10x/research/2026-07-07-arrow-datafusion-current-tuple.md` found that the latest crates.io `datafusion 54.0.0` release uses Arrow/Parquet 58.3.0 while CDF first-party crates use Arrow/Parquet 59.0.0.

`.10x/research/2026-07-07-arrow-datafusion-git-tuple-supply-chain.md` tested the mechanical published-crate repin to Arrow 58.3.0 and found it compiles, but it introduces unratified supply-chain findings through `pyo3 0.28.3` and `thrift 0.17.0`. The same research found that DataFusion git rev `7ff7278edc1bf7446303bff51e5883a38414bbdf` has already moved the DataFusion workspace to the Arrow 59 line. A temporary graph using that rev resolved to Arrow/Parquet 59.1.0, `pyo3 0.29.0`, and `pyo3-arrow 0.19.0`; advisory scanners reported only the already-ratified `paste 1.0.15` advisory.

On 2026-07-07, the user ratified the recommended path: a time-boxed pinned DataFusion git rev starting with `7ff7278edc1bf7446303bff51e5883a38414bbdf`, until crates.io publishes the Arrow 59.x DataFusion tuple.

## Decision

CDF will temporarily depend on Apache DataFusion from pinned git rev `7ff7278edc1bf7446303bff51e5883a38414bbdf` to obtain a same-major Arrow/DataFusion tuple without downgrading CDF's Arrow/PyO3 line or introducing a permanent Arrow-major bridge.

The target tuple for this implementation is:

- DataFusion packages from `https://github.com/apache/datafusion.git` at rev `7ff7278edc1bf7446303bff51e5883a38414bbdf`.
- First-party Arrow and Parquet crates on the Arrow/Parquet 59.x line selected by that tuple.
- `pyo3-arrow 0.19.x` and `pyo3 0.29.x`.
- No advisory exceptions beyond the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` path.

This git dependency is time-boxed. It MUST be replaced by the next crates.io DataFusion release that publishes the same Arrow 59.x tuple, unless a later decision ratifies a different source pin.

Implementation MUST preserve kernel public APIs as Arrow-only and MUST NOT expose DataFusion types from `cdf-kernel`.

## Alternatives considered

Repin all CDF Arrow/Parquet crates to the published DataFusion 54 / Arrow 58.3.0 tuple.

Rejected for now. The mechanical repin compiled and passed focused golden checks, but it introduced unratified `pyo3 0.28.3` and `thrift 0.17.0` advisory findings.

Accept narrow advisory exceptions for PyO3 0.28 and Thrift 0.17.

Rejected. The git-pin path avoids those findings while staying closer to CDF's current Arrow/PyO3 line.

Wait for the next crates.io DataFusion release before doing tuple alignment.

Rejected as the immediate path. Waiting is allowed as a dependency tactic, but the user has clarified that DataFusion is mandatory day-zero architecture and ratified jumping through compatibility hoops to use it.

Introduce a permanent Arrow-major bridge.

Rejected by `.10x/decisions/arrow-datafusion-tuple-policy.md`.

## Consequences

`.10x/tickets/done/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md` implemented this decision using the pinned DataFusion git rev.

The implementation must run golden-package determinism and artifact-compatibility gates because Arrow/Parquet bytes and manifests may change.

The implementation must run supply-chain gates and prove no unratified advisory appears. `cargo vet` may need explicit git-source handling or reviewed exemptions; any such policy change must be recorded and scoped.

The dependency tuple should be revisited at each dependency review and removed as soon as a crates.io DataFusion release publishes the same Arrow 59.x tuple.
