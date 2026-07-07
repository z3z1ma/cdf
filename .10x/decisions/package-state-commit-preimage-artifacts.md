Status: active
Created: 2026-07-06
Updated: 2026-07-06

# Package State And Commit Preimage Artifacts

## Context

The book and active package spec require load packages to contain state and destination evidence, including `state/proposed_delta.json` and `destination/commit_plan.json`, and require package identity to be the hash of manifest identity evidence.

Current runtime structs cannot be serialized exactly as identity files without a hash cycle:

- `StateDelta` contains `package_hash`.
- Concrete destination commit requests contain `package_hash` and use `idempotency_token = package_hash`.
- `package_hash` is computed from the identity file list that would include `state/proposed_delta.json` and `destination/commit_plan.json`.

Storing exact runtime structs as identity files is therefore impossible without changing the package hash model. Storing the artifacts as non-identity files would make package identity omit planned state/destination evidence, weakening the book's "evidence is identity" rule. Receipts remain different: they are destination responses appended after commit attempts and are already explicitly excluded from identity.

The investigation is recorded in `.10x/research/2026-07-06-package-state-commit-artifact-circularity.md`.

## Decision

CDF packages MUST store identity-participating preimage artifacts for state and destination commit evidence.

`state/input_checkpoint.json` records the committed checkpoint head used as input, or `null`.

`state/proposed_delta.json` records a state-delta preimage: checkpoint id, pipeline id, resource id, scope, state version, parent checkpoint id, input position, output position, schema hash, and state segments. It MUST NOT contain `package_hash`. Runtime code reconstructs the concrete `StateDelta` by adding the finalized manifest package hash.

`destination/commit_plan.json` records a commit-plan preimage: target, disposition, merge keys, schema hash, state segments, and `idempotency_token_source = "package_hash"`. It MUST NOT contain a concrete package hash or idempotency-token value. Runtime code reconstructs the concrete destination commit request by using the finalized manifest package hash as both package hash and idempotency token.

These preimage artifacts participate in `manifest.identity`, package verification, signing input, and golden-package comparisons. `destination/receipts.json` remains outside package identity because receipts arrive after destination interaction.

## Alternatives considered

- Serialize exact `StateDelta` and concrete destination commit request as identity files. Rejected because the final package hash would need to be embedded in files that define that same hash.
- Store exact runtime structs as non-identity files after finalization. Rejected because state and commit-plan evidence would not be covered by package identity, weakening package replay and golden evidence.
- Keep omitting state and commit-plan artifacts from live packages. Rejected because it contradicts the book and active package spec and blocks safe `cdf replay package` reconstruction.

## Consequences

Package reconstruction becomes two-step: verify package identity, then combine the verified preimage artifacts with the manifest package hash to produce runtime `StateDelta` and destination commit inputs.

Live-run golden package hashes will change once the artifacts are implemented, because the identity file set becomes more complete. That is intended and must be reviewed as a golden diff.

Future serialized artifact migrations must preserve this distinction between package identity preimages and post-finalization runtime values.
