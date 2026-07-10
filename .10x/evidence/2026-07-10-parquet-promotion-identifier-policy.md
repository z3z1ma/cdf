Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md, .10x/decisions/parquet-column-and-object-key-identifier-rules.md

# Parquet promotion identifier-policy evidence

## What was observed

- The Parquet destination sheet publishes `namecase-v1` as its column identifier policy with no destination length cap. The separately typed optional `ObjectKeyRules` authority publishes `object-key-component-v1` through the defaulted, non-exhaustive `DestinationProtocolCapabilities` aggregate and its artifact/lock seam; `DestinationSheet` remains field-for-field source compatible.
- The generic project destination adapter obtains the destination sheet from the runtime and compiles its column policy without branching on destination name. The filesystem Parquet runtime exposes the sheet without materializing destination storage.
- Ordinary Parquet objects and Parquet correction sidecars both write the normalized `vendor_id` field while preserving `VendorID` as `cdf:source_name` provenance. Existing manifest and correction-sidecar object-key shapes remain unchanged.
- The public CLI promotion scenario creates a real Parquet source commit and receipt, verifies that receipt against live Parquet state, then executes `correction_sidecar` promotion through correction package, receipt, committed checkpoint, lock publication, and publication event.
- A stale object-key rule presented to the column adapter remains a fail-closed error.
- Every ordinary, replacement, version, correction-sidecar, manifest, and receipt object-key constructor requires a validated `ObjectKeyEncoder` derived from protocol capabilities. Missing object-key rules fail closed; component-v1 retains its exact bytes.

## Procedure

Commands run from the repository root:

```text
cargo test -p cdf-dest-parquet ordinary_objects_and_correction_sidecars_share_column_policy_without_changing_object_keys -- --nocapture
cargo test -p cdf-dest-parquet object_key_construction_requires_declared_policy_and_preserves_component_v1_bytes -- --nocapture
cargo test -p cdf-contract destination_identifier_policy_ -- --nocapture
cargo test -p cdf-kernel destination_correction_vocabulary_is_backward_compatible_and_semver_stable -- --nocapture
cargo test -p cdf-project lockfile_generation_round_trips_and_diffs_semantic_changes -- --nocapture
cargo test -p cdf-cli --lib schema_promote_execute_routes_parquet_through_correction_sidecar -- --nocapture
cargo test -p cdf-cli --lib
cargo test -p cdf-project --lib
cargo test -p cdf-conformance --lib
cargo test -p cdf-conformance live_local_file_parquet_v1_matches_committed_golden_across_100_runs -- --nocapture
cargo semver-checks check-release -p cdf-kernel --baseline-rev HEAD
cargo clippy -p cdf-kernel -p cdf-contract -p cdf-dest-parquet -p cdf-project -p cdf-cli -p cdf-conformance --all-targets -- -D warnings
cargo fmt --check
git diff --check
```

Observed results:

- Parquet shared-identity/object-key regression: pass;
- typed-policy construction/missing-declaration regression: pass;
- fail-closed non-column rule regression: pass;
- kernel legacy artifact/default/invalid-version coverage: pass;
- artifact-aware lock round-trip coverage, including Parquet object-key rules: pass;
- CLI Parquet promotion execution: 1/1 pass;
- full CLI suite: 253/253 pass;
- full project suite: 160/160 pass;
- full Parquet destination suite: 27/27 pass;
- full kernel suite: 22/22 pass;
- kernel semver compatibility: 196/196 checks pass; no semver update required.
- full conformance suite: 83/83 pass;
- Parquet live-run golden: 100/100 deterministic repetitions pass.
- strict Clippy, formatting, and whitespace checks: pass.

## What this supports or challenges

This supports every acceptance criterion in the owning ticket: the two identifier namespaces are typed separately, promotion uses the ratified column rule, the full Parquet sidecar execution path is proven, and unsupported column rules still fail closed. It also resolves the Parquet limitation recorded by RP9 without weakening RP9's live source-receipt verification.

The independent review challenged two earlier implementation claims. Moving object-key rules from public `DestinationSheet` to `DestinationProtocolCapabilities` restored Rust source compatibility, and binding every key constructor to the validated typed encoder changed the capability from descriptive metadata into executable authority. `.10x/reviews/2026-07-10-parquet-promotion-identifier-policy-repair-review.md` maps both findings to repaired code and evidence.

The first integrated RP9A run challenged the test fixture: it had rewritten a DuckDB receipt's destination id rather than establishing Parquet destination state. The repaired fixture performs a real Parquet commit and independently verifies the resulting receipt before promotion.

## Limits

- This evidence covers the filesystem-backed Parquet destination used by the project URI and conformance suite; it does not add a live external object-store service.
- Object-key stability is asserted for the committed ordinary-manifest and correction-sidecar layouts exercised by current Parquet protocols; it does not freeze future versioned object-key policies.
