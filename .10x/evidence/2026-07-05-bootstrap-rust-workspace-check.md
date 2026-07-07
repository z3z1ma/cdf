Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Relates-To: .10x/tickets/done/2026-07-05-bootstrap-rust-workspace.md, .10x/specs/architecture-layering-runtime.md

# Rust workspace bootstrap check

## What was observed

The root Cargo workspace contains the crate map required by `.10x/specs/architecture-layering-runtime.md` as compile-only package scaffolding. `cargo check --workspace` succeeds. `cdf-kernel` has no normal dependencies, which supports the required boundary against DataFusion, DuckDB, Python, network, project, and CLI dependencies.

## Procedure

Commands were run from `/Users/alexanderbut/code_projects/personal/cdf` on 2026-07-05:

```text
cargo check --workspace
cargo tree -p cdf-kernel --edges normal
cargo metadata --no-deps --format-version 1
```

## Observations

`cargo check --workspace` output:

```text
    Checking cdf-cli v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-cli)
    Checking cdf-wasm v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-wasm)
    Checking cdf-dest-parquet v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-dest-parquet)
    Checking cdf-declarative v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-declarative)
    Checking cdf-contract v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-contract)
    Checking cdf-project v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-project)
    Checking cdf-dest-postgres v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-dest-postgres)
    Checking cdf-state-sqlite v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-state-sqlite)
    Checking cdf-formats v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-formats)
    Checking cdf-http v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-http)
    Checking cdf-package v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-package)
    Checking cdf-python v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-python)
    Checking cdf-engine v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-engine)
    Checking cdf-conformance v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-conformance)
    Checking cdf-dest-duckdb v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-dest-duckdb)
    Checking cdf-subprocess v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-subprocess)
    Checking cdf-kernel v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-kernel)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.45s
```

Kernel dependency graph output:

```text
cdf-kernel v0.1.0 (/Users/alexanderbut/code_projects/personal/cdf/crates/cdf-kernel)
```

Cargo metadata reported `dependencies: []` for `cdf-kernel`. Metadata also reported these workspace members:

```text
cdf-kernel
cdf-engine
cdf-contract
cdf-package
cdf-state-sqlite
cdf-http
cdf-formats
cdf-declarative
cdf-python
cdf-wasm
cdf-subprocess
cdf-dest-duckdb
cdf-dest-parquet
cdf-dest-postgres
cdf-project
cdf-cli
cdf-conformance
```

Parent review independently reran the core checks after the worker returned:

```text
cargo check --workspace
cargo tree -p cdf-kernel --edges normal
rg -n 'TODO|todo!|unimplemented!|panic!' Cargo.toml crates .10x/tickets/done/2026-07-05-bootstrap-rust-workspace.md .10x/evidence/2026-07-05-bootstrap-rust-workspace-check.md
```

`cargo check --workspace` passed. The kernel dependency graph still contained only `cdf-kernel`. The placeholder search found only the ticket's own acceptance sentence about avoiding TODOs.

## What this supports or challenges

This supports the bootstrap ticket acceptance criteria for crate-map presence, compile-only placeholder boundaries, workspace check success, and kernel dependency cleanliness.

## Limits

This evidence does not prove any cdf runtime, package, checkpoint, destination, contract, DataFusion, Python, WASM, or CLI behavior. Those surfaces are intentionally left to later child tickets.
