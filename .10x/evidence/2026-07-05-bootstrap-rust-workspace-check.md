Status: recorded
Created: 2026-07-05
Updated: 2026-07-05
Relates-To: .10x/tickets/done/2026-07-05-bootstrap-rust-workspace.md, .10x/specs/architecture-layering-runtime.md

# Rust workspace bootstrap check

## What was observed

The root Cargo workspace contains the crate map required by `.10x/specs/architecture-layering-runtime.md` as compile-only package scaffolding. `cargo check --workspace` succeeds. `firn-kernel` has no normal dependencies, which supports the required boundary against DataFusion, DuckDB, Python, network, project, and CLI dependencies.

## Procedure

Commands were run from `/Users/alexanderbut/code_projects/personal/firn` on 2026-07-05:

```text
cargo check --workspace
cargo tree -p firn-kernel --edges normal
cargo metadata --no-deps --format-version 1
```

## Observations

`cargo check --workspace` output:

```text
    Checking firn-cli v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-cli)
    Checking firn-wasm v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-wasm)
    Checking firn-dest-parquet v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-dest-parquet)
    Checking firn-declarative v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-declarative)
    Checking firn-contract v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-contract)
    Checking firn-project v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-project)
    Checking firn-dest-postgres v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-dest-postgres)
    Checking firn-state-sqlite v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-state-sqlite)
    Checking firn-formats v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-formats)
    Checking firn-http v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-http)
    Checking firn-package v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-package)
    Checking firn-python v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-python)
    Checking firn-engine v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-engine)
    Checking firn-conformance v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-conformance)
    Checking firn-dest-duckdb v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-dest-duckdb)
    Checking firn-subprocess v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-subprocess)
    Checking firn-kernel v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-kernel)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.45s
```

Kernel dependency graph output:

```text
firn-kernel v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-kernel)
```

Cargo metadata reported `dependencies: []` for `firn-kernel`. Metadata also reported these workspace members:

```text
firn-kernel
firn-engine
firn-contract
firn-package
firn-state-sqlite
firn-http
firn-formats
firn-declarative
firn-python
firn-wasm
firn-subprocess
firn-dest-duckdb
firn-dest-parquet
firn-dest-postgres
firn-project
firn-cli
firn-conformance
```

Parent review independently reran the core checks after the worker returned:

```text
cargo check --workspace
cargo tree -p firn-kernel --edges normal
rg -n 'TODO|todo!|unimplemented!|panic!' Cargo.toml crates .10x/tickets/done/2026-07-05-bootstrap-rust-workspace.md .10x/evidence/2026-07-05-bootstrap-rust-workspace-check.md
```

`cargo check --workspace` passed. The kernel dependency graph still contained only `firn-kernel`. The placeholder search found only the ticket's own acceptance sentence about avoiding TODOs.

## What this supports or challenges

This supports the bootstrap ticket acceptance criteria for crate-map presence, compile-only placeholder boundaries, workspace check success, and kernel dependency cleanliness.

## Limits

This evidence does not prove any firn runtime, package, checkpoint, destination, contract, DataFusion, Python, WASM, or CLI behavior. Those surfaces are intentionally left to later child tickets.
