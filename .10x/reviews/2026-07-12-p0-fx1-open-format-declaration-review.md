Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-source-files/src/lib.rs, crates/cdf-source-files/src/driver.rs, crates/cdf-source-files/src/runtime.rs, crates/cdf-declarative/src/compiled.rs, crates/cdf-project/src/schema_discovery.rs
Verdict: pass

# FX1 open format declaration review

## Target

The closed-enum removal and registry-id propagation.

## Findings

No critical or significant unresolved finding in this slice.

The declaration validates through the neutral runtime id contract, stays transparent on the wire, and does not add an unknown/legacy variant. Registry membership is checked only after the composition root creates runtime dependencies, which correctly permits different embedders to supply different catalogs. Dynamic diagnostics no longer assume one of five names.

The remaining first-party discovery adapter and legacy row decoder are explicit FX1 blockers, not disguised as completion. The full declarative test target currently has pre-existing file-runtime dependency failures introduced by the ongoing registry migration; focused declaration/inference tests and affected checks isolate this slice, while FX1 closure still requires the full matrix.

## Verdict

Pass. This removes a real closed-world architectural leak and is a prerequisite for the external codec law.

## Residual risk

Schemars now intentionally describes a validated string rather than a fixed enum, but its regex-level editor constraint is not yet generated. Registry-generated product schemas remain an FX1 acceptance item.

