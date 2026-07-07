Status: done
Created: 2026-07-06
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-package-builder-reader.md, .10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md, .10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md

# Ratify and implement package state/commit evidence artifacts

## Scope

Implement the ratified package artifact contract for `state/input_checkpoint.json`, `state/proposed_delta.json`, and `destination/commit_plan.json` in engine/live-run packages and replay consumers.

Expected ownership:

- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/destination-receipts-guarantees.md` if commit-plan evidence semantics need clarification
- `crates/cdf-kernel/**` only for small serializable artifact structs if lower-level ownership is required
- `crates/cdf-package/**` for package reader/writer helpers
- `crates/cdf-project/**` for live-run and replay reconstruction
- `crates/cdf-conformance/**` golden/evidence updates consuming the new artifacts
- `crates/cdf-cli/**` only if the ratified artifact contract makes a narrow `cdf replay package` slice executable without guessing

## Ratified contract

The active package spec and book require packages to contain proposed state-delta and destination commit-plan evidence, but the current runtime structs contain the final package hash/idempotency token. Exact serialization of those structs into package identity files is circular because the final package hash is computed from the identity files themselves.

This is analyzed in `.10x/research/2026-07-06-package-state-commit-artifact-circularity.md` and ratified by `.10x/decisions/package-state-commit-preimage-artifacts.md`.

Implement identity-participating preimage artifacts:

- `state/input_checkpoint.json`: the committed checkpoint head used as input, or `null`.
- `state/proposed_delta.json`: a state-delta preimage containing checkpoint id, pipeline id, resource id, scope, state version, parent checkpoint id, input position, output position, schema hash, and state segments, but omitting `package_hash`.
- `destination/commit_plan.json`: a commit-plan preimage containing target, disposition, merge keys, schema hash, state segments, and `idempotency_token_source = "package_hash"` instead of a concrete package-hash token.

The runtime reconstructs the final `StateDelta` and concrete destination commit request by combining these preimage artifacts with the finalized manifest package hash.

## Acceptance criteria

- Active specs define the package state/commit artifact schemas and state whether the artifacts participate in `manifest.identity`.
- Engine/live local-file runs write `state/input_checkpoint.json`, `state/proposed_delta.json`, and `destination/commit_plan.json` before package finalization, without introducing package-hash circularity.
- Package verification catches tampering or missing identity-participating state/commit evidence.
- `PackageReader` or `cdf-project` exposes a typed reconstruction path from package artifacts to the explicit `StateDelta` and destination replay inputs needed by prepared replay.
- Live-run golden expected evidence is updated to include the new identity files and their hashes.
- Conformance proves a package reconstructed from those artifacts can replay into DuckDB and commit the checkpoint without contacting the source.
- Negative tests prove corrupted state preimage, corrupted commit-plan preimage, missing artifact files, and mismatched manifest package hash fail before destination or checkpoint mutation.
- The implementation does not change native Parquet policy or reintroduce `parquet`/`paste`.

## Evidence expectations

Record focused tests for package artifact writing/reconstruction, live-run golden conformance updates, replay-from-artifact behavior, and corrupted-artifact failures. Run relevant `QUALITY.md` gates with independent checks parallelized where possible and CodeQL through the reusable database wrapper.

## Explicit exclusions

No run-ledger default id semantics, no generic destination finalization trait, no REST/SQL execution, no native Arrow/DataFusion Parquet policy change, no package GC retention behavior, no signing implementation, no distributed execution, and no CLI `resume` unless a separate ticket ratifies run-ledger recovery semantics.

## Progress and notes

- 2026-07-06: Opened after parent inspection found the package evidence contract blocks safe `cdf replay package` CLI progress and reveals drift between current live-run packages and the active package spec. This ticket is blocked until the artifact schema is ratified in active specs or by user confirmation.
- 2026-07-06: Ratified `.10x/decisions/package-state-commit-preimage-artifacts.md` and updated active package/destination specs. The ticket is now executable and should be assigned to a worker in a later turn. Do not implement in the ratification turn.
- 2026-07-07: Implemented typed package preimage artifacts and replay reconstruction. `cdf-package` now owns `StateDeltaPreimage`, `DestinationCommitPlanPreimage`, and `PackageReader::replay_inputs()`. Engine package execution has a pre-finalization hook consumed by live local-file runs to write `state/input_checkpoint.json`, `state/proposed_delta.json`, and `destination/commit_plan.json` before manifest finalization. `cdf-project` exposes DuckDB replay/recovery from verified package artifacts and normalizes live file-manifest state paths to the resource file scope so package identity stays deterministic across temp roots. `cdf-conformance` fixtures and goldens now consume the artifacts and prove artifact-based prepared/live replay and recovery. Focused evidence is recorded in `.10x/evidence/2026-07-07-package-state-commit-artifact-contract.md`.
- 2026-07-07: Parent review hardened package artifact reconstruction validation after bounded mutation testing exposed missed negative cases. Final evidence is recorded in `.10x/evidence/2026-07-07-package-state-commit-artifact-contract.md`; closure review passed in `.10x/reviews/2026-07-07-package-state-commit-artifact-contract-review.md`.

## Blockers

None.
