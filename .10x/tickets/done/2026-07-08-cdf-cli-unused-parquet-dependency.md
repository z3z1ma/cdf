Status: done
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

## Progress and notes

- 2026-07-08: Confirmed `crates/cdf-cli` had no direct `cdf_dest_parquet` API use and removed the unused `cdf-dest-parquet` direct dependency from `crates/cdf-cli/Cargo.toml`.
- 2026-07-08: Refreshed `Cargo.lock` through Cargo check flows. Post-edit Cargo metadata for `cdf-cli` no longer includes `cdf-dest-parquet` as a direct dependency.
- 2026-07-08: Full-workspace `cargo machete --with-metadata` is clean after the cleanup. Closure evidence recorded in `.10x/evidence/2026-07-08-unused-dependency-cleanup.md`; adversarial review recorded in `.10x/reviews/2026-07-08-unused-dependency-cleanup-review.md`.
