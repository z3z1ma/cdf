Status: open
Created: 2026-07-08
Updated: 2026-07-08

# cdf-cli unused Parquet dependency candidate

## Scope

Investigate and either remove or explicitly justify `cdf-dest-parquet` in `crates/cdf-cli/Cargo.toml`.

## Context

During P1 E3 closure, the scoped touched-crate `cargo machete --with-metadata ./crates/cdf-contract ./crates/cdf-package ./crates/cdf-engine ./crates/cdf-project` pass was clean, but full-workspace `cargo machete --with-metadata` reported:

```text
cdf-cli -- ./crates/cdf-cli/Cargo.toml:
    cdf-dest-parquet
```

Targeted `rg` found `cdf-dest-parquet` in the workspace member/dependency metadata and textual test data, but no `cdf_dest_parquet` import in `crates/cdf-cli`.

## Acceptance criteria

- Confirm whether `cdf-cli` requires `cdf-dest-parquet` as a direct dependency under any feature/test/build path.
- If unused, remove the direct dependency and verify `cdf-cli` and relevant workspace checks still pass.
- If intentionally present, add a narrow documented `cargo machete` ignore with the reason.
- Record evidence from `cargo machete --with-metadata`, `cargo check -p cdf-cli --all-targets --locked`, and any focused CLI tests affected by the decision.

## Evidence expectations

Record the cargo-machete before/after result, the dependency-use search, and the compile/test gate that proves the chosen action.

## Explicit exclusions

No CLI architecture refactor, no destination registry change, and no Parquet destination behavior change.

## Blockers

None.
