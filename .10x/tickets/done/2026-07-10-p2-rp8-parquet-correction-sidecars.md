Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/done/2026-07-10-p2-rp2-residual-verdict-runtime-package.md, .10x/tickets/done/2026-07-10-p2-rp3-correction-capabilities-row-provenance.md

# P2 RP8 Parquet correction sidecars and materialization contract

## Scope

Implement the safe non-UPDATE strategy for the Parquet/object-store destination: immutable addressed correction sidecars with manifest/receipt evidence, plus an explicit versioned-rematerialization plan boundary where a consumer requires a fully materialized replacement.

## Acceptance criteria

- Correction sidecars contain original provenance address, promoted fields, residual-path operation, schema hashes, and correction package identity.
- Base Parquet objects are never silently rewritten; human/JSON output says the base target is unchanged.
- Sidecar manifests are atomic, idempotent, content-addressed, and independently verifiable through destination receipts.
- Destination sheet declares sidecar support and does not claim in-place update/readback it cannot prove.
- Versioned-rematerialization planning names required source packages, target version, pointer/manifest advance, and unsupported cases; implementation MUST NOT fake atomic pointer support.
- Replay of the same correction package produces no duplicate sidecar effect.

## Evidence expectations

Filesystem/object-store fixture sidecars, manifest hashes, receipt verification, interrupted manifest writes, duplicate replay, base-object immutability, and destination conformance.

## Explicit exclusions

No table-format merge engine, arbitrary object overwrite, Iceberg/Delta implementation, or generic promotion orchestration.

## Progress and notes

- 2026-07-10: Opened as Parquet's honest append-only correction strategy.
- 2026-07-10: Activated after RP2/RP3 closure and assigned to `/root/impl_d5`. Generic correction request/plan/receipt/sidecar evidence stays kernel/protocol-owned; object layout and atomic manifest mechanics stay inside the Parquet adapter. The sheet must not claim in-place mutation or atomic rematerialization it cannot prove.
- 2026-07-10: Extended the shared correction receipt validator by strategy without adding a parallel settlement protocol. Existing in-place receipt behavior remains unchanged; `correction_sidecar` receipts now report physical delta rows as inserted operations (`rows_written/rows_inserted = correction_count`, updates/deletes zero) and require closed kernel-owned `cdf.correction.sidecar.v1` evidence. That evidence binds manifest/object keys, SHA-256 identities, byte and operation counts, atomic manifest publication, and the invariant that the base target is unchanged. `DestinationCorrectionCommitPlan::validate_receipt` dispatches only on `CorrectionStrategy`, never destination identity.
- 2026-07-10: Implemented Parquet correction-sidecar planning and settlement through the existing `DestinationProtocol` correction session and canonical `Receipt`. The adapter validates the compiler-produced exact `residual-json-v1` authority, sorts operations canonically, and writes one immutable content-addressed JSON delta containing original package/segment/ordinal addresses, promoted output fields and exact values, residual removal operations, promotion/schema transition, operation digest, target/disposition, and correction package/token identity. A content-addressed manifest binds the delta plus correction segment acknowledgements and is published last with atomic create-only `object_store` puts; a create-only package-token receipt marker makes replay return the exact stored receipt.
- 2026-07-10: Verification independently re-reads the durable receipt marker, manifest, and every sidecar object; recomputes content-address paths and SHA-256 hashes; revalidates canonical correction values and the kernel operation digest; and cross-checks package, target, disposition, promotion, old/new schema, segment, count, and base-unchanged evidence. Interrupted publication after the sidecar object but before the manifest leaves no verified correction effect; retry reuses the immutable orphan and publishes exactly one manifest/receipt. Abort writes nothing, tampering fails verification, duplicate replay is byte-identical, and a seeded base Parquet manifest/object remains byte-unchanged.
- 2026-07-10: Parquet now truthfully advertises only `correction_sidecar` with package-token idempotency and atomic-target scope explicitly narrowed in JSON/receipt evidence to `immutable_correction_manifest_only`. Row-provenance persistence/targetability and residual readback remain Unsupported; in-place update and versioned rematerialization are absent from the capability strategy list. A public non-executable versioned-rematerialization plan boundary names the verified source packages, target version, version manifest, target pointer, and exact blocker: atomic pointer advance is Unsupported until the configured store/table format proves compare-and-swap semantics.
- 2026-07-10: Focused verification passed: parent-observed Parquet all-feature nextest 25/25; child Parquet unit suite 25/25 after final content-address validation; kernel suite 19/19 plus the focused closed-sidecar/count regression; all-target/all-feature check for kernel and Parquet; strict kernel/Parquet library Clippy with `-D warnings`; scoped formatting and diff checks; and semver 196/196 for both `cdf-kernel` and `cdf-dest-parquet` with no update required. A final dependency-inclusive selected-package Clippy attempt reached a concurrent RP5 `cdf-contract/src/compiler.rs` `unnecessary_sort_by` lint outside RP8; no full workspace suite or lint is claimed by this child. Parent owns integrated verification, adversarial review, evidence records, closure, and commit.
- 2026-07-10: Closed the second publication crash boundary explicitly. The interruption regression now stages both the content-addressed sidecar object and its complete atomic manifest while omitting the create-only receipt marker; `verify_correction` rejects that state as unrecorded, and replay reuses the exact object/manifest bytes before publishing and verifying one canonical receipt. The focused crash-boundary test passed after this final change.

## Blockers

None.
