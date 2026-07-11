Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-10-p3-ws-l1-catalog-report-schema.md

# P3 L1 dataset catalog and report schema evidence

## What was observed

`cdf-benchmarks` now has a separate schema-versioned P3 dataset/workload catalog and benchmark report model while retaining the legacy smoke fixture catalog. Catalog entries cover full-year TLC, TPC-H SF10/SF100, wide/nested/dirty/schema-varying JSON, and a 100 GiB constant-memory stream. Generated recipes declare deterministic seed/version, streaming delivery, and an 8 MiB chunk bounded below the 64 MiB schema maximum; no large data is committed.

The report model records sanitized advertised/effective host authority, typed supported/unavailable/failed capabilities, warm/cold/uncontrolled mode, comparability key, observed/failed/timed-out/unavailable/inconclusive cells, raw samples, wall/CPU/rows/logical and physical bytes/RSS/spill/phases, distribution summary, reference identity, and bias labels. Legacy flat trend JSON loads only as explicitly inconclusive because it lacks comparability authority.

Canonical fixture hashes are pinned:

- dataset/workload catalog: `sha256:dae7f48ee019980f9d8dce30755d1ab8a25e36b807e00757073e78936500ae73`;
- report fixture: `sha256:24a9156bdd8e34d3a608e314f32b246e28501722015d7dc57d1c62dc3750c2d3`.

## Procedure

From the repository root:

```text
cargo check -j 1 -p cdf-benchmarks
cargo fmt --all -- --check
CARGO_INCREMENTAL=0 cargo test -j 1 -p cdf-benchmarks --test fixtures --locked --no-fail-fast
CARGO_INCREMENTAL=0 cargo clippy -j 1 -p cdf-benchmarks --all-targets --locked -- -D warnings
git diff --check
```

The first ordinary parallel test invocation stalled with two sleeping rustc processes holding incremental artifacts and no linker/CPU activity; it was cancelled. The clean single-job, nonincremental verification above completed normally and is the evidence-bearing run.

## Results

- Targeted Cargo check passed and updated only the existing `cdf-benchmarks` dependency edges for already-locked `hex 0.4.3` and `sha2 0.10.9`; no new package/version entered the lockfile.
- Seven focused integration tests passed: legacy fixture determinism/matrix/coverage plus P3 catalog bounds, report status/sanitization, fail-closed validation, fixed canonical hashes, and legacy trend incompatibility.
- Clippy passed for every benchmark crate target with warnings denied.
- Formatting and diff checks passed.

## What this supports

This supports every L1 acceptance criterion: regeneration-grade typed recipes, separate workloads/timed regions, sanitized typed host/capability/report authority, deterministic versioned serialization, fail-closed malformed/incomparable behavior, and explicit legacy compatibility.

## Limits

This ticket defines and validates recipes/report authority only. It does not generate large datasets, collect a live host fingerprint, run benchmarks, add runtime telemetry/providers, establish reference rooflines, or publish performance claims; L2-L5 own those actions.
