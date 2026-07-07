Status: active
Created: 2026-07-07
Updated: 2026-07-07

# DuckDB Arrow 58 private driver residual

## Context

`.10x/decisions/arrow-datafusion-tuple-policy.md` requires one same-major Arrow/DataFusion tuple per CDF minor release and rejects a permanent Arrow-major bridge in the DataFusion engine hot path.

`.10x/decisions/datafusion-git-pin-arrow59-tuple.md` ratifies a temporary Apache DataFusion git pin at rev `7ff7278edc1bf7446303bff51e5883a38414bbdf`, giving CDF's engine an Arrow `59.1.0` DataFusion tuple without downgrading first-party Arrow APIs.

`.10x/evidence/2026-07-07-duckdb-arrow58-residual-audit.md` finds that the remaining Arrow `58.3.0` path is private to `duckdb 1.10504.0`. Current `duckdb-rs` latest/current is `1.10504.0`, and its Arrow dependency is unconditional. No released version or feature setting currently removes Arrow 58 while preserving CDF's existing DuckDB destination wrapper contract.

The same evidence audits CDF's DuckDB destination boundaries and finds that CDF decodes packages as Arrow 59, lowers data into `duckdb::types::Value`, and verifies receipts through JSON mirror rows. `duckdb::arrow` / Arrow 58 structs are not used at the CDF public Arrow API boundary.

## Decision

CDF will temporarily accept the `duckdb 1.10504.0 -> arrow 58.3.0` transitive residual as a private destination-driver dependency.

This acceptance is not a permission to introduce an Arrow-major bridge in the engine hot path. It does not weaken the DataFusion requirement, the DataFusion git-pin time box, or the target policy of one public Arrow/DataFusion tuple per CDF minor release.

CDF MUST NOT expose `duckdb::arrow` or Arrow 58 structs from public CDF APIs. The DuckDB destination MUST keep Arrow 59 package data lowering on the CDF side and pass only DuckDB row values, SQL parameters, JSON receipts, and primitive query outputs across the `duckdb-rs` boundary unless a later decision supersedes this one.

No remediation ticket is opened now because the only current ways to remove the Arrow 58 dependency are not low-risk: replacing `duckdb-rs` with lower-level `libduckdb-sys`, carrying a fork, or waiting for an upstream release that removes or upgrades the unconditional Arrow dependency.

## Revisit triggers

Revisit this decision immediately when any of these becomes true:

- crates.io publishes a `duckdb` / `duckdb-rs` release newer than `1.10504.0`;
- `duckdb-rs` makes its Arrow dependency optional, removes it, or upgrades it to the active CDF Arrow major;
- CDF changes the DuckDB destination to use `duckdb::arrow`, `query_arrow`, `stream_arrow`, `appender-arrow`, `vtab-arrow`, Arrow virtual tables, or any API that accepts or returns DuckDB Arrow structs;
- CDF changes receipt verification, package replay, or DuckDB mirror reads to move Arrow arrays across the DuckDB driver boundary;
- a new advisory, license, source, or cargo-vet finding appears on the DuckDB Arrow 58 path;
- a CDF minor dependency tuple decision or public release review is performed.

At each revisit, the preferred outcome is removal of the Arrow 58 residual through an upstream `duckdb-rs` release or a tightly scoped migration ticket with golden package, DuckDB destination, receipt verification, and supply-chain evidence.

## Alternatives considered

Upgrade `duckdb-rs`.

Rejected for now. `cargo search duckdb` and `cargo info duckdb` report `1.10504.0` as latest/current on 2026-07-07.

Disable a feature to remove Arrow 58.

Rejected. The registry source shows `arrow` is an unconditional dependency in `duckdb 1.10504.0`; default features are already empty, and optional Arrow-related features only add extra Arrow-facing surfaces.

Replace `duckdb-rs` with `libduckdb-sys`.

Rejected for this workstream. It may remove the Rust Arrow dependency but would rewrite the destination driver boundary and is not low-risk remediation for a private transitive residual.

Keep the ticket open without a decision.

Rejected. The boundary audit provides enough evidence to make a temporary acceptance decision with explicit triggers.

## Consequences

`cargo deny check` will continue to emit duplicate-version warnings for Arrow 58/59 until upstream remediation exists.

Supply-chain policy remains strict: unknown git sources are denied, only the Apache DataFusion git URL is allowed, and no new advisory exception is created for this residual.

Future DuckDB destination work must treat any use of `duckdb::arrow` as a behavior-changing dependency-boundary event requiring a ticket, evidence, and either this decision's supersession or a focused implementation decision.
