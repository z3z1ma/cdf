Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-08-p2-ws-c1-declarative-schema-normalization.md
Verdict: pass

# P2 WS-C1 declarative schema normalization review

## Target

Review of the C1 implementation in `crates/cdf-declarative/src/compiled.rs`, `crates/cdf-declarative/src/tests.rs`, `crates/cdf-declarative/Cargo.toml`, and the `Cargo.lock` dependency-edge update.

## Findings

No blocking findings.

## Assumptions tested

- The implementation reuses the existing `cdf-contract` normalizer instead of introducing a duplicate declarative-only algorithm. Confirmed: `compile_schema` calls `normalize_arrow_schema` with `IdentifierPolicy::default()`.
- Omitted `source_name` is treated as the source-original name. Confirmed: `compile_schema` now inserts `Some(field.name.clone())` when no explicit override exists before normalization.
- Explicit `source_name` is preserved as source-original metadata. Confirmed by the focused test using `source_name = "VendorIDExplicit"`.
- Collision handling is compile-time and names the relevant sources. Confirmed by the `userName`/`user_name` regression test.
- The path dependency is justified. Confirmed: `cdf-contract` is the crate that owns `namecase-v1`; `Cargo.lock` only records that existing workspace crate as a new dependency of `cdf-declarative`.

## Residual risk

Destination-specific identifier sheet rules and package/schema-snapshot normalizer evidence are not covered by C1. They remain in the WS-C parent scope and should be split into later executable tickets.

The normalizer collision message says to add an explicit rename, but C1 does not introduce a new rename field or destination-sheet-specific rename semantics. That wording already comes from the existing normalizer boundary; broader rename ergonomics remain a later source-experience concern.

## Verdict

Pass. The implementation satisfies the bounded C1 acceptance criteria, and the remaining risks are already excluded by the ticket or owned by the WS-C parent.
