Status: done
Created: 2026-07-06
Updated: 2026-07-07

# Package state and commit artifact circularity

## Question

How should CDF satisfy the book/spec requirement that packages contain `state/proposed_delta.json` and `destination/commit_plan.json` when the runtime `StateDelta` and destination commit request include values derived from the package hash itself?

## Sources and methods

- Read `VISION.md` Chapter 11, especially the package layout and hashing sections.
- Read `.10x/specs/package-lifecycle-determinism.md`, `.10x/specs/checkpoint-state-commit-gate.md`, and `.10x/specs/destination-receipts-guarantees.md`.
- Inspected `crates/cdf-package/src/storage.rs`, `crates/cdf-package/src/builder.rs`, `crates/cdf-package/src/model.rs`, and package tests.
- Inspected `crates/cdf-engine/src/execution.rs` for current engine-produced package artifacts.
- Inspected `crates/cdf-project/src/runtime.rs` for runtime `StateDelta` and DuckDB commit construction.
- Inspected CLI and conformance tests that consume current packages.

## Findings

The active book and package spec require load packages to contain or reference the planned, observed, decided, state, and destination evidence of a run. The book's normative layout includes `state/input_checkpoint.json`, `state/proposed_delta.json`, and `destination/commit_plan.json`. The active package spec says packages MUST contain or reference the proposed state delta and destination commit plan, and current package identity hashing includes ordinary files under `state/` and `destination/`.

Current source does not yet satisfy that contract for engine/live-run packages. `crates/cdf-engine/src/execution.rs` writes `plan/scan.json`, `plan/explain.json`, `plan/validation-program.json`, `schema/output.json`, `stats/profile.json`, `lineage/lineage.json`, `data/*.arrow`, and `trace.jsonl`, but it does not write state input/proposed-delta artifacts or destination commit-plan artifacts. The committed live-run golden fixture therefore proves the current implementation's determinism, but not the full package evidence contract.

The current runtime structs are self-referential if serialized exactly into identity files before finalization:

- `StateDelta` includes `package_hash`.
- DuckDB replay constructs `DestinationCommitRequest` with `package_hash` and `idempotency_token = package_hash`.
- `package_hash` is derived from `manifest.identity`, whose `files` list includes `state/` and `destination/` files except for the explicitly excluded `destination/receipts.json`.

Storing an exact `StateDelta` or exact concrete commit request as an identity file would require the package hash to include a file that itself contains the final package hash. That is a circular artifact identity. No active record or current source test resolves this exact circularity.

Current package tests do write fixture `state/proposed_delta.json` and `destination/commit_plan.json` before finalization, but those are simple maps, not exact `StateDelta`/`DestinationCommitRequest` values. They prove the package builder can include such artifacts in identity, not the runtime schema contract for live packages.

Receipts are intentionally outside package identity in current source and records because they are appended after destination interaction. That precedent does not automatically apply to state and commit-plan evidence: the book treats planned/proposed state and commit-plan evidence as part of the package's planned identity, while receipts are destination responses appended as they arrive.

## Conclusions

The executable fix needs a ratified artifact schema, not an ad hoc write of current runtime structs.

Recommended contract: packages SHOULD store identity-participating preimage artifacts rather than exact post-hash runtime structs.

- `state/input_checkpoint.json` records the committed head, or `null`, that was used as the run input.
- `state/proposed_delta.json` records a `StateDelta` preimage: checkpoint id, pipeline id, resource id, scope, state version, parent checkpoint id, input position, output position, schema hash, and state segments. It deliberately omits the final `package_hash`; the runtime reconstructs the committed `StateDelta` by adding the manifest identity hash after package finalization.
- `destination/commit_plan.json` records target, disposition, merge keys, schema hash, state segments, and `idempotency_token_source = "package_hash"` rather than a concrete token value. The runtime constructs the concrete destination commit request with the final package hash.

This preserves the book's intent that package identity covers the planned state/destination evidence, while avoiding an impossible hash cycle. It also unblocks a future `cdf replay package` path that can reconstruct the explicit state delta and commit inputs from package evidence without guessing.

## Limits

This research does not itself ratify the artifact schema or update source. Because this affects serialized package artifacts and replay semantics, the next step should update the active package specification and open an executable child ticket before implementation.
