Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: crates/cdf-format-parquet, crates/cdf-runtime/src/format.rs, crates/cdf-memory/src/lib.rs
Verdict: pass

# Review: native Parquet driver extraction

## Assumptions tested

- Parser dependencies and behavior are localized to the codec crate.
- Parquet never receives a concrete transport or executor.
- Range payloads remain accounted for their actual parser lifetime.
- Parallel range completion cannot reorder logical decoder output.
- Schema drift is checked against the full physical schema before projection.
- Row groups are deterministic planned units rather than scheduler-derived artifacts.

## Findings

No critical or significant finding remains in this slice. Review found and corrected two issues before verdict: projection originally compared the projected schema to the full planned physical hash, and multi-range fetch initially ignored the source's useful-concurrency declaration. Full-schema verification now precedes projection; fetches are bounded by the source capability and restored to requested order before returning to Parquet.

## Verdict

Pass. The crate boundary and ownership model are sound, and the driver composes solely through neutral contracts.

## Residual risk

Parquet can allocate a decoded batch before its retained size is reconciled to the byte target. The driver reserves the configured target before polling and fails cleanly if the retained batch exceeds it; adaptive byte-to-row sizing and stress evidence remain within open B2/F work. Production registry migration remains within open FX1 and must delete the superseded Parquet path before closure.
