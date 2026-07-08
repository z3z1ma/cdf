Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p0-c3-cross-destination-chaos.md
Verdict: pass

# P0 C3 cross-destination chaos review

## Target

Review of the C3 implementation and evidence:

- `crates/cdf-conformance/src/runtime_chaos/**`
- `crates/cdf-conformance/src/lib.rs`
- `crates/cdf-conformance/src/run_matrix/mod.rs`
- `.10x/evidence/2026-07-08-p0-c3-cross-destination-chaos.md`

## Findings

No blocking findings.

The first parent review did find one semantic alignment issue before closure: the worker implementation fired the checkpoint-proposed/before-destination-write case at `RuntimeStage::CheckpointProposed`. Existing DuckDB lifecycle chaos defines that named window at `RuntimeStage::DestinationWriteReady`, after checkpoint proposal and package status `Loading` but before destination write. The implementation was adjusted to `DestinationWriteReady`, and the focused chaos tests were rerun successfully.

## Assumptions tested

- Generic seam: the helper invokes `replay_package_from_artifacts_with_stage_hook` and maps public `RuntimeStage` values, not deleted destination-specific replay wrappers.
- Destination breadth: the test loops over DuckDB, filesystem Parquet, and Postgres and asserts four cases per destination with no exclusions.
- Crash realism: the helper exits the child process with code 87 at the selected stage, so normal replay cleanup cannot run after the injected crash.
- Recovery source contact: recovery uses package artifact replay/recovery inputs and prepared package replay for the no-receipt proposed-row case; no source stream or `run_project` source execution is used.
- Checkpoint safety: tests assert empty/no-head state before durable destination data, proposed non-head state after the proposal window, committed head only after the checkpoint-committed window, and recovered checkpoints carrying the verified receipt.
- Destination idempotence: durable-receipt recovery and duplicate retry compare destination footprints, receipt identity, and trait-level receipt verification.

## Residual risk

Postgres footprint comparison is count-based. For the tested package-token duplicate/recovery invariant this is acceptable because a second write would increase target/load counts, and the harness also validates stable receipt identity through `DestinationProtocol::verify`. A full row-hash Postgres footprint would be stronger, but it is not required to prove the C3 acceptance criteria.

Mutation testing was not run for this harness slice. The helper-process crash tests are integration-style and already expensive because they start Postgres and child test processes; C5 owns property/fuzz expansion, and C6 owns aggregate Workstream-C closure.

## Verdict

Pass. C3 has implementation, evidence, and review sufficient to close. Workstream C remains open because C4 live-run goldens, C5 property/fuzz targets, and C6 closure rollup are still open.
