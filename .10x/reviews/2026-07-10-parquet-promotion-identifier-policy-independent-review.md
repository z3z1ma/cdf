Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md
Verdict: fail

# Independent review: Parquet promotion identifier policy

## Target

The completed implementation owned by `.10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md`, governed by `.10x/decisions/parquet-column-and-object-key-identifier-rules.md`, with compatibility and extension constraints from `.10x/decisions/destination-protocol-capabilities-extension-seam.md` and `.10x/knowledge/source-destination-extension-invariant.md`.

## Assumptions tested

- Column and object-key namespaces occupy distinct typed authority positions.
- Legacy serialized sheets remain byte-compatible and public Rust construction remains compatible as required by the active capability-extension decision.
- Adding a destination with existing capability shapes does not require destination-name branches or edits across existing adapters.
- Ordinary Parquet output and promotion sidecars share one column-normalization policy, preserve `cdf:source_name`, and retain plan-time collision failure.
- Declared object-key policy governs the implementation and existing ordinary/correction key bytes do not drift accidentally.
- Promotion execution uses a real Parquet source receipt verified against live destination state.
- Sheet/artifact identity changes and golden changes are intentional and deterministic.
- Passing evidence actually invokes the named fail-closed tests.

## Findings

### Significant — public `DestinationSheet` source compatibility violates the active extension seam

`object_key_rules` was added directly as a new public field on the externally constructible `cdf_kernel::DestinationSheet`. `#[serde(default, skip_serializing_if = "Option::is_none")]` preserves legacy serialized bytes, and the focused legacy JSON round-trip test passes, but it does not preserve Rust source compatibility. Every downstream struct literal must add the field. The current diff demonstrates that cost in DuckDB, Postgres, Parquet, conformance, kernel tests, project tests, and runtime tests.

This conflicts with the still-active `.10x/decisions/destination-protocol-capabilities-extension-seam.md`, which explicitly requires `DestinationSheet` to remain field-for-field compatible and routes new capability families through the defaulted, non-exhaustive `DestinationProtocolCapabilities` aggregate. It also contradicts the new decision's characterization of the object-key position as backward-compatible unless "backward-compatible" is narrowed to wire format, which the decision does not do.

Exact check:

```text
cargo semver-checks check-release -p cdf-kernel --baseline-rev HEAD
```

Result: 195 checks passed and `constructible_struct_adds_field` failed for `DestinationSheet.object_key_rules`; cargo-semver-checks reports that a major version is required. The existing test named `destination_correction_vocabulary_is_backward_compatible_and_semver_stable` proves wire compatibility only and therefore overstates what it establishes.

Required disposition before ticket closure: either move `ObjectKeyRules` into the existing defaulted protocol-capability aggregate/artifact/lock seam, preserving legacy sheet construction, or explicitly supersede the active extension-seam decision and ratify the major source break. The latter would conflict with the currently stated backward-compatibility requirement and is not inferred by this review.

### Significant — object-key policy is declared and validated but does not drive object-key construction

The typed `ObjectKeyPolicy::ComponentV1` appears in its declaration, sheet construction, artifact validation, and tests. Production object-key construction in `cdf-dest-parquet/src/store.rs` still calls `encode_component` directly; it does not consume or exhaustively dispatch on `ObjectKeyRules`/`ObjectKeyPolicy`. A repository-wide Rust search found no production `ObjectKeyPolicy` match or adapter dispatch.

Current bytes remain stable because `store.rs` and `corrections.rs` are unchanged, and receipt verification recomputes the same keys. However, the capability is currently descriptive rather than falsifiable authority: a future valid policy variant could be declared while key construction silently continues using component-v1. That violates the governing decision's requirement that destination code dispatch on typed rule positions and versions.

Required disposition before ticket closure: bind Parquet key construction to a validated typed object-key policy at the adapter boundary with exhaustive variant dispatch, while preserving the existing component-v1 byte outputs. Add a negative or alternate-policy test that would fail if sheet authority and key construction diverge.

### Minor — recorded fail-closed command names the wrong crate

`.10x/evidence/2026-07-10-parquet-promotion-identifier-policy.md` records:

```text
cargo test -p cdf-project destination_identifier_policy_rejects_unsupported_rules
```

The test is in `cdf-contract`, not `cdf-project`; the recorded command can succeed while running zero matching tests. The independent exact command below ran the real test successfully. Correct the evidence procedure so a cold reader cannot mistake filter success for exercised coverage.

## Confirmed behavior

- `DestinationSheet.identifier_rules` is now column-only for Parquet (`namecase-v1`, no length cap), while object-key metadata is separately typed. Legacy JSON without object-key rules round-trips byte-for-byte.
- Generic promotion planning consumes locked `identifier_rules`; generic runtime lookup consumes `destination_sheet()` and contains no DuckDB/Postgres/Parquet name branch. The filesystem Parquet adapter's static sheet hook avoids materializing storage during planning.
- Shared normalizer tests prove unsupported object-key-as-column input fails closed and normalized collisions remain errors. Promotion's proposed-schema construction rejects duplicate normalized output fields before execution.
- The Parquet parity test proves ordinary Parquet data and correction-sidecar operations both use `vendor_id` while retaining `VendorID` in `cdf:source_name` metadata.
- Ordinary and correction object-key construction code is unchanged. The parity test asserts the exact ordinary manifest key and correction namespace, while Parquet receipt verification recomputes exact ordinary and content-addressed correction keys.
- The CLI scenario removes the fixture receipt, commits the retained source package through `ParquetDestination`, and promotion execution revalidates exact receipt/package/segment authority and calls live `verify_receipt` before correction packaging. The scenario reaches committed correction receipt/checkpoint/lock/event completion.
- The changed live Parquet golden is deterministic across 100 rebuilds/runs; the package and validation-program hashes changed intentionally with the column policy.

## Exact checks

```text
cargo semver-checks check-release -p cdf-kernel --baseline-rev HEAD
# FAIL: constructible_struct_adds_field for DestinationSheet.object_key_rules

cargo test -p cdf-kernel destination_correction_vocabulary_is_backward_compatible_and_semver_stable -- --nocapture
# PASS: 1/1 (wire compatibility and typed artifact validation only)

cargo test -p cdf-contract destination_identifier_policy_ -- --nocapture
# PASS: 4/4, including unsupported object-key-as-column and collision regressions

cargo test -p cdf-dest-parquet ordinary_objects_and_correction_sidecars_share_column_policy_without_changing_object_keys -- --nocapture
# PASS: 1/1

cargo test -p cdf-cli --lib schema_promote_execute_routes_parquet_through_correction_sidecar -- --nocapture
# PASS: 1/1

cargo test -p cdf-conformance live_local_file_parquet_v1_matches_committed_golden_across_100_runs -- --nocapture
# PASS: 1/1, containing 100 deterministic repetitions

rg over production promotion/runtime code for concrete destination literals
# No DuckDB/Postgres/Parquet destination-name branch found.

rg for ObjectKeyPolicy/object_key_rules production consumers
# Only artifact validation consumes object_key_rules; Parquet key construction has no typed policy dispatch.

git diff for cdf-dest-parquet/src/store.rs and corrections.rs
# No changes; existing object-key byte algorithms are untouched.
```

## Verdict

Fail. Column normalization, collision behavior, real-receipt execution, golden stability, and current object-key bytes are well supported. The ticket must not close while the public sheet-field addition violates an active compatibility decision and while the new object-key capability does not govern the code that constructs object keys.

## Residual risk

- The parity test covers the filesystem-backed Parquet adapter, not a live external object store; this is an accepted scope limit once the two blocking findings are resolved.
- There is no dedicated end-to-end promotion fixture with two residual paths that collide after destination normalization. Shared schema/collision tests cover the mechanism, but a future promotion-specific regression would improve locality of diagnosis.
