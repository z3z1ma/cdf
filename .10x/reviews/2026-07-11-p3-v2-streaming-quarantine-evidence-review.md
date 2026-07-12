Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: crates/cdf-contract/src/vector.rs, crates/cdf-engine/src/execution.rs, crates/cdf-package/src/builder.rs, crates/cdf-package/src/quarantine.rs
Verdict: pass

# Adversarial review: streaming quarantine evidence milestone

## Findings

No critical or significant milestone defect remains. The callback boundary lives in `cdf-contract` and speaks only contract candidates; package-specific conversion stays in the engine. All evidence origins join one engine accumulator, and package-specific Parquet/atomic/hash behavior stays in `cdf-package`. There is no source, format, or destination-name dispatch.

The writer owns the streaming artifact rather than borrowing a self-referential sink. `ArrowWriter::into_inner` finalizes the Parquet footer and returns the artifact, whose ordinary `finish` performs flush, file sync, atomic rename, directory sync, hash receipt, and journal registration. Drop before finish leaves the existing temporary-sibling cleanup behavior. Chunking changes internal Arrow batch boundaries only; readback and package identity laws pass.

The old byte APIs and their temp-file round trip were deleted. Package archive transcode still has a differently owned byte API and is not disguised as complete here.

## Verdict

Pass for this milestone. V2 remains open for explicit shared-ledger ownership and macro profiling.

## Residual risk

The dedicated ledger lease and too-small-budget failure path are now implemented. Residual risk is calibration rather than ownership: the flat-schema 3x simultaneous-buffer multiplier is conservative and needs RSS/profile confirmation in V2/V3. Any quarantine schema or writer-policy change must rerun that calibration.
