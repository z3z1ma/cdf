Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-rp8-parquet-correction-sidecars.md, .10x/specs/schema-promotion-corrections.md

# P2 RP8 Parquet correction-sidecar evidence

## What was observed

The Parquet/object-store destination now settles schema-promotion corrections as immutable, content-addressed sidecars through the ordinary destination correction session and canonical `Receipt`. Kernel-owned receipt evidence binds the correction manifest, every sidecar object, byte and operation counts, hashes, atomic manifest publication, and the fact that the base target is unchanged.

The adapter advertises only `correction_sidecar`. It does not claim persisted or targetable row provenance, residual readback, in-place update, or executable versioned rematerialization. Its rematerialization plan names required packages, the proposed version manifest and target pointer, and remains non-executable until a compare-and-swap pointer primitive is proven.

## Procedure

Parent-observed verification on 2026-07-10:

- `cargo nextest run -p cdf-dest-parquet --all-features`: 25 tests passed, 0 skipped.
- `cargo nextest run -p cdf-kernel --all-features`: 19 tests passed, 0 skipped.
- `cargo nextest run -p cdf-dest-parquet --all-features tests::interrupted_sidecar_publication_reuses_orphan_object_and_publishes_manifest_once tests::correction_sidecar_is_content_addressed_verifiable_and_leaves_base_immutable tests::versioned_rematerialization_is_an_explicit_non_executable_plan_boundary`: 3 selected tests passed.
- `cargo clippy -p cdf-kernel -p cdf-dest-parquet --all-features --lib -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.

The interruption regression covers both durable pre-receipt states: sidecar object without manifest, and complete manifest without receipt marker. Neither verifies as a committed correction. Replay reuses the exact immutable bytes and publishes one canonical package-token receipt. Tampering with an object invalidates receipt verification. A seeded base Parquet manifest and data object remain byte-identical before and after correction settlement.

## What this supports

This supports every RP8 acceptance criterion: exact addressed sidecar contents, honest base-target immutability, content-addressed and independently verifiable publication, idempotent replay, capability-sheet truthfulness, and an explicit non-executable rematerialization boundary.

## Limits

The deterministic suite uses filesystem and in-memory `object_store` implementations. It proves the adapter protocol and `PutMode::Create` publication contract, not live S3/GCS/Azure service behavior. Cloud transport and live object-store conformance remain owned by P2 WS-E/WS-I. RP9 still owns generic promotion execution, checkpointing, recovery orchestration, and lock publication.
