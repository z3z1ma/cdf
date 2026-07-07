Status: done
Created: 2026-07-07
Updated: 2026-07-07

# Arrow/DataFusion Git Tuple Supply-Chain Check

## Question

After the published DataFusion 54 / Arrow 58 repin introduced unratified advisories, is there a supply-chain-clean same-major tuple path that keeps CDF on the current Arrow/PyO3 line while still using real DataFusion?

## Sources and methods

- Inspected the worker's attempted first-party Arrow/Parquet 58.3.0 repin.
- Ran `cargo audit` against the attempted Arrow 58.3.0 lockfile.
- Ran `osv-scanner --lockfile Cargo.lock` against the attempted Arrow 58.3.0 lockfile.
- Ran `cargo tree --workspace --locked -i thrift@0.17.0`.
- Ran `cargo tree --workspace --locked -i pyo3@0.28.3`.
- Ran `cargo info pyo3-arrow@0.18.0 --verbose`.
- Ran `cargo search datafusion --limit 10` and `cargo search pyo3-arrow --limit 10`.
- Cloned `https://github.com/apache/datafusion.git` into `/tmp` and inspected rev `7ff7278edc1bf7446303bff51e5883a38414bbdf`.
- Created a temporary Cargo project outside the repository that depends on:
  - `datafusion` from the pinned git rev;
  - Arrow/Parquet 59;
  - `pyo3 0.29`;
  - `pyo3-arrow 0.19`.
- Ran `cargo metadata`, `cargo tree -d`, and Arrow inverse tree checks against the temporary git tuple.
- Ran `cargo audit --file /tmp/cdf-df-git-tuple-9bHaCX/Cargo.lock`.
- Ran `osv-scanner --lockfile /tmp/cdf-df-git-tuple-9bHaCX/Cargo.lock`.

## Findings

The mechanically clean published-crate repin to the DataFusion 54 / Arrow 58.3.0 tuple compiled and passed focused golden-package tests, but it is not acceptable under the active supply-chain policy without new ratification:

- `pyo3-arrow 0.17.0` is the inspected published Arrow 58 bridge line and depends on `pyo3 0.28`.
- The attempted lockfile selected `pyo3 0.28.3`.
- `cargo audit` reports `RUSTSEC-2026-0176` and `RUSTSEC-2026-0177` for `pyo3 0.28.3`; both list upgrade to `>=0.29.0` as the solution.
- `pyo3-arrow 0.18.0` is not a rescue path: it uses Arrow 59 and still depends on the PyO3/Numpy 0.28 line.
- `pyo3-arrow 0.19.0` uses Arrow 59 and the PyO3/Numpy 0.29 line.
- `parquet 58.3.0` introduces `thrift 0.17.0`; OSV reports `GHSA-2f9f-gq7v-9h6m` for that crate.
- These findings are separate from the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` advisory path.

The current DataFusion git main branch has already moved its workspace Arrow/Parquet dependencies to the Arrow 59 line even though the published crates.io `datafusion` release is still 54.0.0:

- DataFusion git rev `7ff7278edc1bf7446303bff51e5883a38414bbdf` declares Arrow 59 workspace dependencies.
- The temporary graph resolved to Arrow/Parquet 59.1.0, `pyo3 0.29.0`, and `pyo3-arrow 0.19.0`.
- The temporary graph had no `arrow-array@58.3.0`.
- `cargo audit` against the temporary git tuple reported only the already-ratified `paste 1.0.15` warning.
- OSV against the temporary git tuple reported only `RUSTSEC-2024-0436` for `paste 1.0.15`.
- The temporary graph did not contain the PyO3 0.28 or Thrift 0.17 advisory paths.

## Conclusions

The published-crate Arrow 58 repin should not be committed under the active quality policy unless the user explicitly ratifies new advisory exceptions or a local patch/fork path. It trades a DataFusion/Arrow major alignment problem for new unratified security findings in Python and Parquet dependency paths.

The best available supply-chain direction is a time-boxed, pinned DataFusion git rev that keeps CDF on the Arrow 59/PyO3 0.29 line until a crates.io DataFusion release publishes the same tuple. This is a real dependency-source policy decision, not a mechanical version bump, because it replaces crates.io DataFusion packages with an unreleased git source.

## Limits

The DataFusion git check was performed in `/tmp`, not in the CDF workspace. It proves a plausible dependency graph and scanner outcome for the candidate tuple, not that CDF compiles or passes golden-package tests on that tuple.

`cargo vet` was not run against the temporary git tuple. If the git-pin path is ratified, cargo-vet policy will need explicit handling for the git-sourced DataFusion packages and any newly locked crates.

The candidate git rev still reports package version `54.0.0` in the DataFusion workspace even though its dependency tuple differs from the published crates.io `54.0.0` release.

This record does not ratify a git dependency, a new advisory exception, or a permanent bridge.
