Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-06-package-state-commit-artifact-contract.md
Verdict: pass

# Package state and commit artifact contract review

## Target

Review of the implementation for `.10x/tickets/done/2026-07-06-package-state-commit-artifact-contract.md`, including:

- typed package preimage artifacts in `firn-package`;
- engine pre-finalization hook support;
- live local-file package artifact writing and DuckDB artifact replay/recovery in `firn-project`;
- conformance/golden updates for prepared and live package artifact replay;
- mutation-hardened package replay reconstruction validation.

## Findings

None unresolved.

Parent review did find a significant test-hardening gap before closure: `PackageReplayInputs::from_preimages` accepted several malformed preimage combinations without independent negative coverage. Focused tests were added for unsupported state preimage versions, non-committed and non-head input checkpoints, checkpoint tuple mismatches by pipeline/resource/scope, parent checkpoint mismatch, input-position mismatch, null checkpoint references, empty state segment lists, and package-manifest row/byte mismatches. The final bounded mutation pass over `crates/firn-package/src/artifacts.rs` reported 27 mutants tested, 22 caught, 5 unviable, and 0 missed.

The apparent recovery helper concern around merge keys is not a ticket blocker: artifact replay reconstructs merge keys for the actual destination write path, while recovery verifies a supplied durable receipt and commits checkpoint state without performing a destination write.

## Assumptions Tested

- Identity-participating package artifacts are preimages, not final post-hash runtime structs.
- `PackageReader::replay_inputs()` verifies the package before reconstructing replay inputs.
- Artifact replay/recovery does not contact the source after package creation.
- Package preimage validation fails before destination/checkpoint mutation for corrupted, missing, or mismatched artifacts.
- This ticket does not alter native Parquet policy or introduce direct `parquet`/`paste` dependencies.

## Verdict

Pass. Evidence in `.10x/evidence/2026-07-07-package-state-commit-artifact-contract.md` supports the ticket acceptance criteria, and no unresolved review findings remain.

## Residual Risk

CodeQL still reports the documented local Rust extractor macro-expansion warning pattern, but the final run had 0 extraction errors and 0 SARIF results. Mutation testing was bounded to the package artifact reconstruction module; runtime integration is covered by focused runtime/conformance tests and workspace `nextest`.
