Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/done/2026-07-10-p2-rp2-residual-verdict-runtime-package.md, .10x/tickets/done/2026-07-10-p2-rp3-correction-capabilities-row-provenance.md

# P2 RP6 Postgres addressed in-place corrections

## Scope

Make Postgres conformance-testable for atomic addressed correction: enforce/verify uniqueness of `_cdf_load`, `_cdf_segment`, `_cdf_row`; plan nullable column migrations; stage correction rows; update only exact provenance tuples; return verifiable package receipts and idempotent counts.

## Acceptance criteria

- Existing and newly loaded targets have a safe unique/validated provenance address without changing user merge keys.
- Correction DDL/DML is dry-runnable and destination-sheet-consistent.
- One transaction applies migrations, addressed updates, `_cdf_variant` residual removal only for promoted paths, and receipt mirror.
- Missing/duplicate addresses fail before partial mutation; unrelated residual paths remain intact.
- Replaying a correction package is a package-token no-op with a verifiable receipt.
- Append still requires no semantic key.

## Evidence expectations

Live Postgres migrations/updates, duplicate/missing address negatives, partial residual preservation, receipt/checkpoint-ready output, rollback/failpoint tests, and destination conformance.

## Explicit exclusions

No generic promotion orchestrator, DuckDB/Parquet behavior, lock publication, or source rediscovery.

## Progress and notes

- 2026-07-10: Opened around the existing Postgres provenance columns and transactional reference role.
- 2026-07-10: Activated after RP2/RP3 closure and assigned to `/root/impl_i5`. Generic correction request/plan/receipt semantics remain kernel-owned; Postgres-specific DDL/DML and transaction code stay inside the adapter.
- 2026-07-10: Added one destination-neutral batched correction protocol rather than a Postgres-only executor. `DestinationCorrectionCommitRequest` binds the correction package/token, target/resource disposition, checkpoint-ready segments, compiled nullable `CanonicalArrowField`s, exact per-row provenance/path operations, and a kernel-recomputed SHA-256 operation digest. `DestinationProtocol` has provided-unsupported plan/begin/verify/readback hooks and `CorrectionCommitSession` returns the canonical `Receipt`/`ReceiptVerification`, so RP9 can settle through `CheckpointStore::commit` without destination matching or receipt translation.
- 2026-07-10: Hardened execution authority under `.10x/decisions/promotion-correction-value-authority.md`: the compiler-produced one-field `residual-json-v1` envelope is the sole promoted-value authority. Shared contract validation proves one field, exact path, exact canonical Arrow type, and digest agreement before every Postgres plan/begin boundary. Legacy `promoted_value_json` remains inspection-only and is excluded from operation identity; regression tests prove display changes do not alter the digest while exact bytes/type/path do.
- 2026-07-10: Kept correction operation semantics distinct from resource disposition per `.10x/decisions/correction-receipt-operation-and-disposition.md`. The canonical plan/receipt retains append/replace/merge as target context, while closed versioned receipt evidence binds typed `addressed_correction`, promotion id, old/new schema hashes, strategy, operation digest, operation count, addressed-row count, and removed-path count. No `cdc_apply` claim or parallel receipt model was introduced.
- 2026-07-10: Implemented Postgres dry planning and physical execution in the adapter. Plans derive Postgres types only from the compiled canonical Arrow field, add only nullable promoted columns, create deterministic target-specific unique provenance indexes, stage exact typed values, lock and require exactly one target row per original package/segment/ordinal, update through all three provenance columns, remove only promoted residual paths with the shared codec, and write/verify the ordinary `_cdf_loads` receipt mirror in the same transaction. New target DDL also enforces tuple uniqueness; ordinary loads now persist the original package hash in `_cdf_load` even when the operator idempotency token differs.
- 2026-07-10: Postgres now truthfully declares persisted/targetable NOT-NULL provenance, canonical residual readback, and atomic package-token `in_place_update`. Planning rejects missing, mistyped, or nullable address columns; generic validation rejects duplicate address/path operations and non-bijective path-to-output-field mappings. The generic readback hook returns canonical residual bytes plus the exact original address, and the live test reproduces and decodes that envelope before and after correction.
- 2026-07-10: Live verification covers a keyless append target with two addressed corrections, nullable `age` migration, exact 42/84 typed updates, one retained `/untouched` residual, null `_cdf_variant` after the last path is removed, unique-index inspection, canonical receipt verification, package receipt persistence, and package-token replay as an identical no-op. Negative live cases prove missing and duplicate addresses fail before durable mutation; a deliberately invalid post-update verify statement rolls back the promoted column, row update, and receipt mirror. The original residual remains byte-exact after failure.
- 2026-07-10: Focused evidence: `cargo nextest run -p cdf-kernel -p cdf-contract -p cdf-dest-postgres --all-features --no-fail-fast` passed 123/123 with 0 skipped, including bounded live Postgres. `cargo clippy -p cdf-kernel -p cdf-contract -p cdf-dest-postgres --all-targets --all-features -- -D warnings` passed. Targeted rustfmt checks and `git diff --check` passed. `cargo semver-checks check-release ... --baseline-rev HEAD` passed 196/196 for both `cdf-kernel` and `cdf-dest-postgres` with no version update required.
- 2026-07-10: Ticket remains active for parent-owned adversarial review, integration verification, durable evidence/graph reconciliation, and closure. No staging or commit was performed; concurrent RP7/A10g edits in the shared worktree were preserved.
- 2026-07-10: Parent integration verification passed 913/913 all-feature workspace tests, strict workspace Clippy, formatting, and diff checks. Evidence: `.10x/evidence/2026-07-10-p2-a10g-rp6-rp7-integration.md`. Adversarial review passed after correction-value, receipt-operation, digest, provenance, bijection, and readback repairs: `.10x/reviews/2026-07-10-p2-a10g-rp6-rp7-integration-review.md`. All RP6 acceptance criteria are supported.

## Blockers

None.
