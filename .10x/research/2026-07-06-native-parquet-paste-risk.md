Status: done
Created: 2026-07-06
Updated: 2026-07-06

# Native Parquet and RUSTSEC-2024-0436 risk

## Question

Can CDF replace the DuckDB-backed Parquet workaround with the native arrow-rs/DataFusion Parquet stack by using the latest package versions, or should it temporarily accept `RUSTSEC-2024-0436` because native Parquet is architecturally central?

## Sources and methods

- `cargo search parquet --limit 8`, `cargo search datafusion --limit 8`, and `cargo search paste --limit 5` on 2026-07-06.
- crates.io API checks for newest versions of `parquet`, `datafusion`, and `paste` on 2026-07-06.
- `cargo info parquet@59.0.0 -v`, `cargo info datafusion@54.0.0 -v`, and `cargo info paste@1.0.15 -v`.
- Local registry manifests:
  - `/Users/alexanderbut/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/parquet-59.0.0/Cargo.toml`
  - `/Users/alexanderbut/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/parquet-58.3.0/Cargo.toml`
  - `/Users/alexanderbut/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/datafusion-54.0.0/Cargo.toml`
- Local RustSec advisory database entry `/Users/alexanderbut/.cargo/advisory-db/crates/paste/RUSTSEC-2024-0436.md`.
- Current CDF manifests and policy:
  - `crates/cdf-engine/Cargo.toml`
  - `crates/cdf-package/Cargo.toml`
  - `crates/cdf-formats/Cargo.toml`
  - `crates/cdf-dest-parquet/Cargo.toml`
  - `deny.toml`
- Public source pages checked on 2026-07-06:
  - https://crates.io/crates/parquet
  - https://crates.io/crates/datafusion
  - https://crates.io/crates/paste
  - https://rustsec.org/advisories/RUSTSEC-2024-0436.html

## Findings

- `parquet` latest is `59.0.0`. That latest version still has an unconditional `[dependencies.paste] version = "1.0"` entry. Upgrading to the latest native arrow-rs Parquet crate does not remove the advisory path.
- `datafusion` latest is `54.0.0`. Its `parquet` feature includes `dep:parquet`, and the manifest pins `parquet = "58.3.0"` with Arrow/object_store/async features. `parquet 58.3.0` also has an unconditional `[dependencies.paste] version = "1.0"` entry.
- Current CDF intentionally uses `datafusion = { version = "54.0.0", default-features = false }` in `cdf-engine`, so the workspace can use the DataFusion planning/execution boundary without activating DataFusion's Parquet feature yet.
- Current CDF Parquet source, destination, and archive paths use DuckDB's bundled Parquet support:
  - `cdf-package` depends on `duckdb` with `bundled, parquet`.
  - `cdf-formats` depends on `duckdb` with `bundled, parquet` and bridges DuckDB Arrow 58 IPC into CDF Arrow 59.
  - `cdf-dest-parquet` depends on `duckdb` with `bundled, parquet`.
- The current lockfile graph does not contain `parquet` or `paste`: `cargo tree --workspace --locked -i parquet` and `cargo tree --workspace --locked -i paste` both fail because those packages are absent from the graph.
- The ratified local supply-chain policy currently has `deny.toml` with `[advisories] ignore = []`. Therefore adding native arrow-rs/DataFusion Parquet would make advisory gates fail unless the policy is deliberately superseded or an upstream/fork removes the `paste` dependency.
- `RUSTSEC-2024-0436` is an informational unmaintained advisory, not a known memory-safety, remote-code-execution, or data-corruption vulnerability. The advisory has no patched versions. The affected crate is a proc-macro used at compile time by the Parquet crate. The risk is supply-chain and maintenance risk: no upstream fixes if a future issue appears, archived upstream repository, and advisory noise that can hide worse findings if ignored too broadly.

## Conclusions

There is no architectural reason CDF cannot use native arrow-rs/DataFusion Parquet. In fact, native Parquet aligns better with the book because DataFusion and Arrow are load-bearing infrastructure, while the DuckDB Parquet path is an FFI workaround with Arrow-major bridging.

The reason CDF has not simply switched is narrower: latest upstream versions do not remove `paste`, and the current active supply-chain policy deliberately allows no advisory ignores. The workaround was a policy-preserving implementation path, not a technical preference.

The recommended policy is to ratify a narrow, time-boxed exception for exactly `RUSTSEC-2024-0436` on `paste 1.0.15` only when introduced through the native arrow-rs/DataFusion Parquet dependency path, then replace the DuckDB-backed Parquet source, destination writer, and package archive transcode path with native implementations behind bounded tickets.

The exception should have these constraints:

- Owner: CDF dependency policy / parent implementation plan.
- Scope: `paste` via `parquet` / DataFusion Parquet only; no broad unmaintained-advisory ignore.
- Expiry/revisit: every Arrow/DataFusion upgrade and no later than the next CDF minor dependency-pin review.
- Gates: `cargo audit`, `cargo deny`, OSV, `cargo vet`, and CodeQL remain required; advisory output must prove only the ratified `paste` advisory is ignored.
- Exit path: remove the exception when arrow-rs/DataFusion removes `paste`, or evaluate a patch/fork only if the exception blocks release/compliance or upstream shows no movement over a full dependency-pin cycle.

Patch/fork is not the first recommendation because carrying a fork of the Apache Arrow Rust Parquet crate would likely create more supply-chain and maintenance burden than accepting this informational advisory with a narrow expiry.
