Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/specs/schema-promotion-corrections.md, .10x/specs/destination-receipts-guarantees.md

# P2 RP3 destination correction capabilities and row provenance

## Scope

Add backward-compatible kernel destination-sheet vocabulary for persisted/targetable row provenance, residual readback, correction strategies, and their transaction/idempotency claims. Define the logical `(package, segment, ordinal)` address plus executor-neutral correction request/plan values, and reconcile existing Postgres system columns plus DuckDB/Parquet declarations.

## Acceptance criteria

- Serialized sheets gain additive defaulted correction capabilities without breaking legacy sheet/lock fixtures.
- Row provenance is one kernel type using original package hash, segment id, and zero-based row ordinal.
- Postgres declares current provenance persistence only after uniqueness/targetability is proven; DuckDB and Parquet do not overclaim.
- Strategy values are exactly `in_place_update`, `correction_sidecar`, and `versioned_rematerialization`, each with transaction/idempotency evidence.
- Correction request/plan values carry promotion id, original row address, old/new schema hashes, promoted path/value, residual operation, and selected strategy without importing a destination driver or CLI type.
- Planner-facing validation rejects impossible combinations such as in-place update without targetable provenance.
- Destination conformance can falsify every new claim.

## Evidence expectations

Semver/serialization fixtures, lockfile sheet snapshots, Postgres provenance inspection, negative capability validation, and kernel/destination conformance scaffolding.

## Explicit exclusions

No actual correction write, readback implementation, promotion planner, or lockfile publication.

## Progress and notes

- 2026-07-10: Source audit found Postgres already writes `_cdf_load` from the package idempotency token plus `_cdf_segment` and segment-local `_cdf_row`; this ticket must reuse that tuple.
- 2026-07-10: Added additive, defaulted, version-1 kernel correction capabilities plus the typed original `(package hash, segment id, zero-based row ordinal)` address. Legacy sheets deserialize to an unsupported correction sheet and reserialize without a new field, preserving old sheet and lockfile bytes. Added executor-neutral correction request/plan values carrying promotion identity, original row address, old/new schema hashes, promoted JSON pointer/value, residual removal operation, selected strategy, and transaction/idempotency guarantees.
- 2026-07-10: Added structural planner validation for unsupported capability versions, targetability without persistence, readback without provenance, duplicate strategies, strategy/sheet guarantee mismatch, non-atomic/non-idempotent strategies, in-place update without targetable provenance, rematerialization without atomic-target support, unsupported selected strategies, and plan/capability guarantee mismatch. The serialized strategy vocabulary is exactly `in_place_update`, `correction_sidecar`, and `versioned_rematerialization`.
- 2026-07-10: Reconciled destination declarations without adding execution behavior. Postgres declares provenance persistence because its existing target columns and COPY rows preserve package token, segment id, and segment-local ordinal; it does not claim targetability because target DDL lacks tuple uniqueness, and it claims no residual readback or correction strategy. DuckDB and Parquet retain the default unsupported declaration. Append disposition and key requirements are unchanged.
- 2026-07-10: Extended generic destination conformance with adapter-supplied correction evidence and negative self-tests. The kernel vocabulary and conformance law contain no concrete destination branches; Postgres, DuckDB, and Parquet supply declarations/evidence only at their adapter boundaries, preserving `.10x/knowledge/source-destination-extension-invariant.md`.
- 2026-07-10: Focused verification passed: kernel tests (14), correction destination-conformance tests (3), DuckDB tests (12), Parquet tests (20), Postgres tests including live provenance inspection (34), and the project lockfile round-trip fixture (1). Scoped all-target check, strict clippy, formatting, and diff checks passed for kernel, conformance, all three destinations, and project.
- 2026-07-10: Parent adversarial review identified that the first serialized-sheet design added a public `DestinationSheet.corrections` field. `cargo semver-checks check-release -p cdf-kernel --baseline-rev HEAD` reproduced the significant `constructible_struct_adds_field` break: serde defaults preserved bytes but not downstream Rust struct literals.
- 2026-07-10: Repaired the extension seam without weakening capability serialization. `DestinationSheet` is restored byte-for-byte and field-for-field. `DestinationProtocol` now exposes a backward-compatible provided `correction_capabilities()` method and a generic provided `sheet_artifact()` method. The new `DestinationSheetArtifact` flattens the legacy sheet and carries a defaulted, omitted-when-unsupported versioned correction slot; deserializing and reserializing a legacy sheet through the artifact preserves exact bytes, while Postgres artifacts serialize the non-default correction declaration. Generic planning/conformance consumes only the protocol methods; adapters override the capability method locally, with no concrete destination branches.
- 2026-07-10: Post-repair semver verification passed all 196 checks with no version update required. Kernel (14), correction conformance (3), DuckDB (12), Parquet (20), Postgres including live tests (34), project lock fixture (1), and scoped all-target check passed again. RP3 source formatting and diff checks pass; the final multi-package strict clippy rerun is temporarily blocked only by concurrent RP4 `cdf-project/src/lock_cas.rs` OpenOptions lint outside this ticket.
- 2026-07-10: Parent review required the serialized lock snapshot to share the same durable extension seam rather than leave correction claims only in a protocol method. Generalized the method to defaulted `DestinationProtocol::protocol_capabilities()`, returning a versioned non-exhaustive `DestinationProtocolCapabilities` aggregate. DuckDB, Parquet, and any new adapter inherit truthful unsupported capabilities with no override or sheet-literal field; Postgres alone opts in locally. Generic conformance and sheet-artifact production consume the protocol aggregate without destination-name branches.
- 2026-07-10: Added the typed aggregate to `LockedDestination` as a defaulted, omitted-when-default lock slot and made `LockedDestination` non-exhaustive with `LockedDestination::new(DestinationSheetArtifact)`. The legacy `generate_lockfile` API remains source-compatible and produces exact legacy bytes/hashes with default capabilities; `generate_lockfile_with_destination_artifacts` snapshots verified adapter claims. CLI schema diff preserves locked artifacts rather than dropping claims. The project lock fixture proves legacy bytes/hash stability and a typed Postgres lock round trip.
- 2026-07-10: Recorded the extension-boundary tradeoff in `.10x/decisions/destination-protocol-capabilities-extension-seam.md`. Kernel semver remains clean (196/196). An honest `cdf-project` semver check reports the one intentional major source change, `struct_marked_non_exhaustive` for `LockedDestination`; parent review selected this pre-1.0 constructor migration under the user's P0 extension-architecture invariant. The user did not grant a general compatibility exception.
- 2026-07-10: Final verification passed after the aggregate/lock repair: kernel tests (14), correction conformance (3), DuckDB (12), Parquet (20), Postgres including live tests (34), project legacy/typed lock fixture (1), scoped all-target check, strict scoped clippy, workspace formatting, and diff checks.
- 2026-07-10: Parent integration verification and P0 extension-cost review passed. Evidence: `.10x/evidence/2026-07-10-p2-a10c-rp3-rp4-integration.md`. Review: `.10x/reviews/2026-07-10-p2-a10c-rp3-rp4-integration-review.md`.

## Blockers

None.
