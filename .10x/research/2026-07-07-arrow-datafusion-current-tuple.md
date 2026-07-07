Status: done
Created: 2026-07-07
Updated: 2026-07-07

# Arrow/DataFusion Current Tuple

## Question

Under `.10x/decisions/arrow-datafusion-tuple-policy.md`, what same-major Arrow/DataFusion dependency tuple is available on 2026-07-07, and what is the smallest compatible implementation path for `.10x/tickets/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md`?

## Sources and methods

- Inspected `.10x/tickets/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md`.
- Inspected `.10x/decisions/arrow-datafusion-tuple-policy.md`.
- Inspected `.10x/decisions/native-arrow-datafusion-parquet-policy.md`.
- Ran `cargo info datafusion --verbose`.
- Ran `cargo info arrow-array --verbose`.
- Ran `cargo info pyo3-arrow@0.17.0 --verbose`.
- Ran `cargo info pyo3-arrow@0.19.0 --verbose`.
- Ran `cargo tree --workspace --locked -d` with Arrow/DataFusion/Python filtering.
- Ran `cargo tree -p cdf-engine --locked -i arrow-array@58.3.0`.
- Ran `cargo tree -p cdf-engine --locked -i arrow-array@59.0.0`.
- Ran `rg` across workspace manifests for first-party Arrow, Parquet, DataFusion, and Python bridge pins.

An exploratory subagent independently inspected the same tuple shape before this parent-observed pass. This record relies on parent-observed command output for the registry and lockfile facts below.

## Findings

- `cargo info datafusion --verbose` reports `datafusion 54.0.0` as the current crate version and shows direct dependencies on `arrow 58.3.0`, `arrow-schema 58.3.0`, and `parquet 58.3.0` when the Parquet feature is enabled.
- `cargo info arrow-array --verbose` reports `arrow-array 59.0.0` as the current Arrow array crate version.
- `cargo tree -p cdf-engine --locked -i arrow-array@58.3.0` shows Arrow 58 entering the workspace through `datafusion 54.0.0`.
- `cargo tree -p cdf-engine --locked -i arrow-array@59.0.0` shows Arrow 59 entering first-party CDF paths through `cdf-engine`, `cdf-kernel`, `cdf-package`, `parquet 59.0.0`, and `arrow-ipc 59.0.0`.
- `cargo tree --workspace --locked -d` shows duplicate Arrow 58.3.0 and 59.0.0 crate families in the current lockfile.
- `cargo info pyo3-arrow@0.19.0 --verbose` shows the current Python bridge line depends on Arrow 59 crates and `pyo3 0.29`/`numpy 0.29`.
- `cargo info pyo3-arrow@0.17.0 --verbose` shows the Arrow 58 bridge line depends on Arrow 58 crates and `pyo3 0.28`/`numpy 0.28`.
- Workspace manifest search shows first-party CDF Arrow and Parquet pins currently use `59.0.0` across kernel, engine, package, destination, format, CLI, conformance, subprocess, project, declarative, contract, and Python crates; `cdf-engine` depends on `datafusion 54.0.0`; `cdf-python` depends on `pyo3 0.29.0` and `pyo3-arrow 0.19.0`.

## Conclusions

There is no currently published DataFusion release in the inspected registry output that gives CDF a same-major Arrow 59/DataFusion tuple today. Under `.10x/decisions/arrow-datafusion-tuple-policy.md`, waiting remains allowed only as a dependency tactic, not as an excuse to defer DataFusion execution. The smallest implementation path that keeps DataFusion day-zero and avoids a permanent Arrow-major bridge is therefore a deliberate first-party CDF repin from Arrow/Parquet 59.0.0 to DataFusion 54's Arrow/Parquet 58.3.0 line.

The repin is not just an Arrow manifest edit. `cdf-python` must also move to the compatible `pyo3-arrow 0.17.0` tuple, which implies downgrading the local bridge dependencies from `pyo3 0.29`/`numpy 0.29` to the `pyo3 0.28`/`numpy 0.28` line unless a different Arrow 58-compatible bridge is selected and proven.

The implementation should keep `.10x/decisions/native-arrow-datafusion-parquet-policy.md` intact. The scoped `RUSTSEC-2024-0436` exception remains expected because Arrow/DataFusion Parquet still routes through `paste`; the implementation must prove no new unratified advisory appears.

## Recommended implementation gates

- `cargo metadata --locked --format-version 1`
- `cargo tree --workspace --locked -d`
- `cargo tree --workspace --locked -i arrow-array@58.3.0`
- `cargo tree --workspace --locked -i arrow-array@59.0.0`
- Golden-package determinism and artifact compatibility tests from `.10x/specs/package-lifecycle-determinism.md`.
- Focused compile/test/clippy for Arrow, package, engine, project, destination, conformance, CLI, and Python bridge crates affected by the tuple.
- Supply-chain gates from `.10x/knowledge/quality-gate-execution.md`, including `cargo deny`, `cargo audit`, OSV, and cargo-vet, with only the ratified `RUSTSEC-2024-0436` advisory accepted if it remains present.

## Limits

Registry state is time-sensitive. Re-run `cargo info datafusion --verbose` before a future tuple review if this record is reused after 2026-07-07.

This research records the dependency tuple path. It does not implement the repin, prove golden-package byte stability, or close `.10x/tickets/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md`.
