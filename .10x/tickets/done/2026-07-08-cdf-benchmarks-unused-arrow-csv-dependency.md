Status: done
Created: 2026-07-08
Updated: 2026-07-08

# cdf-benchmarks unused Arrow CSV dependency candidate

## Scope

Investigate and either remove or explicitly justify the direct `arrow-csv` dependency in `crates/cdf-benchmarks/Cargo.toml`.

## Context

While executing `.10x/tickets/done/2026-07-08-cdf-cli-unused-parquet-dependency.md`, full-workspace `cargo machete --with-metadata` reported:

```text
cdf-benchmarks -- ./crates/cdf-benchmarks/Cargo.toml:
    arrow-csv
```

Targeted search found `arrow-csv` used by `cdf-formats`, but no `arrow_csv` import or direct API use in `crates/cdf-benchmarks`.

## Acceptance criteria

- Confirm whether `cdf-benchmarks` requires `arrow-csv` as a direct dependency under any build, bench, test, or binary path.
- If unused, remove the direct dependency and verify benchmark crate checks still pass.
- If intentionally present, add a narrow documented `cargo machete` ignore with the reason.
- Record evidence from `cargo machete --with-metadata`, dependency-use search, and benchmark-crate compile/test gates.

## Evidence expectations

Record before/after cargo-machete output, the dependency-use search, `cargo check -p cdf-benchmarks --all-targets --locked`, `cargo test -p cdf-benchmarks --locked`, and any relevant focused quality metrics.

## Explicit exclusions

No benchmark workload changes, no benchmark result regeneration, no performance claim, and no changes to `cdf-formats` CSV behavior.

## Blockers

None.

## Progress and notes

- 2026-07-08: Confirmed `crates/cdf-benchmarks` had no direct `arrow_csv` API use and removed the unused `arrow-csv` direct dependency from `crates/cdf-benchmarks/Cargo.toml`.
- 2026-07-08: Refreshed `Cargo.lock` through Cargo check flows. Post-edit Cargo metadata for `cdf-benchmarks` no longer includes `arrow-csv` as a direct dependency.
- 2026-07-08: Verified `arrow-csv` remains available where legitimately used by `cdf-formats`; this ticket only removed the benchmark-crate direct edge. Closure evidence recorded in `.10x/evidence/2026-07-08-unused-dependency-cleanup.md`; adversarial review recorded in `.10x/reviews/2026-07-08-unused-dependency-cleanup-review.md`.
