Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md
Verdict: pass

# Parquet promotion identifier-policy independent re-review

## Target

This independent re-review evaluates the repaired ticket against `.10x/decisions/parquet-column-and-object-key-identifier-rules.md`, `.10x/decisions/destination-protocol-capabilities-extension-seam.md`, `.10x/knowledge/source-destination-extension-invariant.md`, and `.10x/specs/types-contracts-normalization.md`. It specifically re-tests the two significant findings from `.10x/reviews/2026-07-10-parquet-promotion-identifier-policy-independent-review.md` rather than relying on the implementer's repair response.

## Findings

No blocking findings remain.

### Pass — public sheet compatibility and durable typed authority

`DestinationSheet` is field-for-field unchanged. `ObjectKeyRules` is an optional member of the existing `#[non_exhaustive]` `DestinationProtocolCapabilities` aggregate, defaults to absent, and is omitted from legacy/default serialization. `DestinationSheetArtifact` validates and hashes non-default capabilities; `LockedDestination` snapshots and round-trips the same aggregate. Parquet's static and runtime sheet artifacts both publish `ObjectKeyRules::component_v1()` alongside the column-only sheet rules. The lock regression proves the Parquet artifact, including `object-key-component-v1`, survives TOML serialization and reconstructs exactly.

Independent `cargo semver-checks check-release -p cdf-kernel --baseline-rev HEAD` completed with 196/196 checks passing and no semver update required.

### Pass — declared object-key policy governs every construction path

`ObjectKeyEncoder::from_capabilities` rejects missing object-key rules, validates their version, and exhaustively dispatches `ObjectKeyPolicy`. The component-v1 byte encoder is private behind that dispatcher. Repository inspection found every production object-key constructor accepts the capability-derived encoder: ordinary package manifest and segment objects, replace pointers, correction sidecar objects and manifests, correction receipts, version manifests, and the version target pointer. No destination-name switch or untyped fallback controls those paths.

The exact-byte regression independently passed for slash, space, and colon escaping (`orders/by region`, `sha256:abc/def`), producing `targets/orders~2fby~20region/packages/sha256~3aabc~2fdef/manifest.json`. Inspection confirms the retained component-v1 algorithm preserves ASCII letters, digits, `.`, `-`, `_`, and `=`, and encodes every other UTF-8 byte as lowercase `~xx`, matching the pre-repair implementation.

### Pass — column normalization and source provenance remain separate

The Parquet sheet declares column `namecase-v1`, no length cap, and the normal output pattern; object-key rules exist only in protocol capabilities. Generic promotion planning reads the locked column rules, normalizes the verbatim source name, and retains collision checks through the proposed schema. The proposed promoted field records both `cdf:source_name` and `cdf:promoted_path`; correction-package operations carry the canonical field unchanged. Ordinary Parquet batches and correction sidecars share that canonical field path, while object-key construction remains on the distinct encoder.

The ordinary/sidecar parity regression independently passed for `VendorID -> vendor_id`, and the fail-closed destination-policy suite independently passed, including rejection of unsupported non-column rules.

### Pass — real Parquet promotion follows the durable execution pipeline

The CLI conformance fixture removes the synthetic source receipt, performs a real `ParquetDestination::commit_package`, and then executes promotion against the resulting destination state. The shared executor verifies stored source receipts through the resolved destination protocol before building a correction package. It then plans and finalizes the Parquet correction sidecar, verifies and appends the correction receipt, commits that receipt into the promotion checkpoint, publishes the lock by compare-and-swap, and records the publication event. A `committed: true` target report is constructed only from a committed checkpoint carrying the same verified receipt authority.

The focused CLI test independently passed and observed phase `complete`, the `parquet_object_store` target committed, lock publication, publication-event recording, and the published correction manifest in Parquet storage.

## Independent checks

The following commands were run from the repository root during this review:

```text
cargo test -p cdf-kernel destination_correction_vocabulary_is_backward_compatible_and_semver_stable -- --nocapture
cargo test -p cdf-project lockfile_generation_round_trips_and_diffs_semantic_changes -- --nocapture
cargo test -p cdf-dest-parquet object_key_construction_requires_declared_policy_and_preserves_component_v1_bytes -- --nocapture
cargo test -p cdf-dest-parquet ordinary_objects_and_correction_sidecars_share_column_policy_without_changing_object_keys -- --nocapture
cargo test -p cdf-contract destination_identifier_policy_ -- --nocapture
cargo test -p cdf-cli --lib schema_promote_execute_routes_parquet_through_correction_sidecar -- --nocapture
cargo semver-checks check-release -p cdf-kernel --baseline-rev HEAD
```

All checks passed: kernel 1/1, lock artifact 1/1, object-key authority 1/1, Parquet column/object parity 1/1, identifier policy 4/4, CLI Parquet execution 1/1, and semver 196/196.

## Verdict

Pass. The repaired design preserves `DestinationSheet` source compatibility, makes the lock-snapshotted typed capability executable authority for every current Parquet key family, keeps column normalization and source provenance on `namecase-v1`, and exercises the real Parquet receipt/checkpoint/lock/publication path without a generic promotion branch on destination name.

## Residual risk and limits

- The filesystem and in-memory object-store implementations share the audited encoder and receipt paths, but this ticket does not add a live external object-store service test.
- Versioned rematerialization is deliberately a non-executable planning boundary; its manifest and target-pointer names are policy-driven, but atomic publication remains outside this ticket.
- Component-v1 currently has one valid typed policy. Exhaustive matches make a future enum variant a compile-time integration obligation; this review does not invent a second policy solely to test dispatch.
