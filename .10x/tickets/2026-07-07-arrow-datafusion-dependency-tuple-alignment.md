Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/decisions/datafusion-tier-b-delegation-boundary.md, .10x/decisions/arrow-datafusion-tuple-policy.md

# Align the Arrow/DataFusion dependency tuple

## Scope

Resolve the D-28 dependency tuple mismatch that blocks real DataFusion `TableProvider` execution over CDF resource streams.

This ticket owns the dependency decision and, once unblocked, the smallest implementation work needed to make CDF first-party Arrow types and DataFusion execution types compatible without a permanent hot-path Arrow-major bridge.

## Current facts

As of 2026-07-07:

- `cargo info datafusion` reports latest/current `datafusion 54.0.0`.
- `cargo info arrow-array` reports latest/current `arrow-array 59.0.0`.
- `crates/cdf-engine/Cargo.toml` depends directly on `arrow-array 59.0.0`, `arrow-schema 59.0.0`, `arrow-select 59.0.0`, and `datafusion 54.0.0`.
- `cargo tree -p cdf-engine --locked -i arrow-array@58.3.0` shows DataFusion 54's dependency graph uses Arrow 58.3.0.
- `cargo tree -p cdf-engine --locked -i arrow-array@59.0.0` shows CDF kernel/package/engine/native Parquet paths use Arrow 59.0.0.

## Acceptance criteria

- Decide and record one dependency tuple path:
  - wait for/upgrade to a DataFusion release compatible with CDF's Arrow major;
  - deliberately repin CDF first-party Arrow crates to DataFusion's Arrow major;
  - or explicitly ratify a temporary bridge with scope, expiry, benchmarks, and artifact-safety gates.
- Preserve `.10x/decisions/native-arrow-datafusion-parquet-policy.md` and its narrow `RUSTSEC-2024-0436` exception unless explicitly superseded.
- If dependency versions change, run the golden-package suite as an artifact-compatibility gate and record byte-stability evidence.
- If dependency versions change, run supply-chain scanners and prove no unratified advisory is introduced.
- Update the eventual lockfile/dependency tuple record once that spec exists.
- Leave kernel public APIs free of DataFusion types.

## Evidence expectations

- Registry and lockfile evidence for selected Arrow/DataFusion versions.
- `cargo tree --locked` evidence proving the resulting tuple.
- Golden-package determinism and artifact compatibility evidence if versions change.
- Focused compile/test/clippy evidence for crates affected by the tuple.
- Supply-chain evidence covering `cargo deny`, `cargo audit`, OSV, and cargo-vet under the then-current policy.

## Explicit exclusions

No generic `TableProvider` adapter, no explain/operator metadata changes, no predicate-language expansion, no package format change, no new supply-chain advisory exception, and no permanent Arrow-major bridge unless a new decision explicitly ratifies it.

## References

- `VISION.md` D-28
- `.10x/decisions/datafusion-tier-b-delegation-boundary.md`
- `.10x/decisions/native-arrow-datafusion-parquet-policy.md`
- `.10x/research/2026-07-07-datafusion-delegation-pushdown-triage.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/specs/package-lifecycle-determinism.md`

## Progress and notes

- 2026-07-07: Opened from DataFusion delegation triage. The recommended default is no permanent Arrow 58/59 engine hot-path bridge. The execution-critical blocker is whether CDF should wait for DataFusion to align with Arrow 59, repin first-party Arrow to DataFusion's Arrow major after golden-suite proof, or explicitly ratify a temporary bridge.
- 2026-07-07: User ratified `.10x/decisions/arrow-datafusion-tuple-policy.md` with a hard clarification that DataFusion is mandatory day-zero architecture. This ticket is no longer blocked on product preference; next execution must inspect the current registry/lockfile tuple and choose the smallest same-major-compatible path under that decision.

## Blockers

None from user. If current registry evidence still lacks a same-major tuple, implementation must either prove a safe CDF Arrow repin with golden-package evidence or return with a focused temporary-bridge/release-wait proposal under `.10x/decisions/arrow-datafusion-tuple-policy.md`.
