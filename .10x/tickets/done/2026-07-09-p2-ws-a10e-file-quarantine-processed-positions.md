Status: done
Created: 2026-07-09
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-a10d-effective-schema-runtime-evidence.md, .10x/decisions/data-onramp-file-source-transport-manifest.md

# P2 WS-A10e file quarantine and processed positions

## Scope

Add terminal file-level contract verdicts, quarantine evidence, and processed-file positions independent of accepted output segments so incompatible drift completes through the normal package/receipt/checkpoint gate, including all-quarantine runs.

## Acceptance criteria

- Incompatible/narrowing files receive a stable named file-level rule with path, physical versus baseline/effective type, policy, and remediation; they never surface as an internal stack error.
- `freeze` quarantines any schema deviation from baseline; `evolve` quarantines only differences without a ratified compatible join/coercion.
- Quarantine artifacts record exact runtime file identity and per-field evidence without leaking sensitive locations/values.
- Admitted and quarantined processed positions exist independently of output segments and aggregate deterministically into checkpoint state.
- An all-quarantine run produces a valid evidence package, destination receipt, and committed checkpoint without inventing data rows/segments.
- A quarantined identity advances only after receipt verification and checkpoint commit; an unchanged identity is skipped later, while a changed identity retries.
- Removed files never delete destination data or historical manifest entries.
- Crash/replay/recovery tests cover failures before package, after package, after receipt, and before checkpoint.

## Evidence expectations

Mixed and all-quarantine runs, package/quarantine/receipt/checkpoint inspection, manifest rerun/change tests, crash matrix, redaction, replay, destination conformance, and adversarial review.

## Explicit exclusions

No row-level policy redesign, destination quarantine-sheet expansion beyond existing sheets, cloud transport implementation, or HTTP enumeration.

## Progress and notes

- 2026-07-09: Opened from the ratified gate-backed quarantine advancement rule.
- 2026-07-10: Activated after A10d closure. Implementation must consume the source-neutral schema-observation authority from `.10x/decisions/effective-schema-runtime-authority.md`; file-specific identity belongs at the file adapter/package presentation boundary, while processed-position and commit-gate semantics remain generic.
- 2026-07-10: Implemented terminal `freeze`/`evolve` schema-observation quarantine as typed kernel evidence. Per-field facts use validated `SchemaObservationScope` plus exact kernel `CanonicalArrowField` values, including nullability/metadata and explicit missing sides; whole-schema facts use a typed scope rather than a sentinel path.
- 2026-07-10: Implemented exact execution attestation before terminal disposition. File adapters revalidate the planned file identity and physical Arrow schema under the same plan-recorded discovery executor budget. Engine plans bind a source-neutral observation identity fingerprint, reject conflicting repeated partitions, and cache one attestation per observation.
- 2026-07-10: Added versioned processed-observation package evidence independent of data segments. The kernel owns the single position aggregation lattice used by both segment and processed-observation paths; append retains historical/removed files and replace ignores prior position. Zero state segments require a zero-data package plus typed evidence whose input/output/disposition exactly match the state and commit preimages.
- 2026-07-10: Added normal zero-data destination sessions for DuckDB, Parquet, and Postgres. DuckDB uses an explicit `NoData` effect, Parquet writes a receipt/empty object manifest without changing the replace pointer, and Postgres omits target DDL/write SQL while retaining transactional receipt/state mirrors. Append and replace no-data conformance tests are green.
- 2026-07-10: Added mixed and all-quarantine CLI paths, exact-identity skip/retry, source-free zero-segment replay, durable-receipt recovery before checkpoint, attestation mutation failure, typed-evidence tamper/missing checks, baseline-member freeze handling, governed-evolve incompatibility, nested missing-field evidence, repeated-observation cache/conflict, budget tamper, and quarantine value-redaction coverage.
- 2026-07-10: Focused verification green so far: `cdf-package` 34/34; `cdf-dest-duckdb` 13/13; `cdf-dest-parquet` 21/21; `cdf-dest-postgres` 35/35 including live local Postgres; A10e engine/declarative/project focused regressions; and four CLI golden-path regressions (mixed freeze, heterogeneous pinned baseline, all quarantine with replay, evolve incompatible). A combined all-feature run reached unrelated concurrent RP2 REST tests and exposed six failures; final all-feature/clippy rerun remains pending RP2 stabilization.
- 2026-07-10: Final A10e-focused rerun is green: the shared kernel position lattice passed 5/5 divergent/conflicting/mixed-position checks; mixed freeze, heterogeneous pinned-baseline freeze, all-quarantine zero-segment skip/change/replay, and governed-evolve exact Arrow evidence each passed their exact all-feature CLI regression; `cargo fmt --all -- --check` and `git diff --check` passed. A final affected-crate Clippy rerun compiled through the A10e project boundary after correcting the fallible effective-schema-hash call site, then stopped in the concurrently owned RP2 `cdf-conformance::live_run` migration because its new normal-library imports were still declared only as dev dependencies and `ResourceStream` was not yet imported. The program owner confirmed RP2 retains that repair; no conformance changes were retained by this ticket.
- 2026-07-10: `cargo-semver-checks 0.48.0` against `HEAD` with all features passed `cdf-package`, `cdf-dest-parquet`, and `cdf-dest-postgres` with 196/196 applicable checks. It identified intentional pre-1.0 A10e API migrations in `cdf-engine` (`EnginePackageDraft` and `EngineRunOutputWithSegmentPositions` made non-exhaustive), `cdf-project` (`ResolvedProjectDestination::plan_resource_commit` gained the zero-data/schema input), and `cdf-dest-duckdb` (`DuckDbCommitPlan` became non-exhaustive and replaced public `bulk_path`/`target_exists` fields with the explicit data/no-data effect). The `cdf-kernel` finding is attributable to concurrent RP2 hardening of `BatchHeader` (`#[non_exhaustive]` and changed unwind auto-traits), not A10e. These require a deliberate pre-1.0 compatibility/release decision; they are not silently characterized as semver-compatible.
- 2026-07-10: Verification limits are explicit: remote cloud transports were excluded by scope; Postgres coverage used the local ephemeral live fixture; the complete all-quarantine project path was exercised through DuckDB while Parquet/Postgres zero-data append/replace behavior is covered at their destination conformance/unit boundaries; and final workspace-wide RP2 conformance/Clippy stabilization remains owned by the parent program lane. This ticket remains active for parent evidence/review/closure reconciliation and was not moved or committed by the executor.
- 2026-07-10: Final shared-tree recheck: `cargo check --all-features -p cdf-project --lib` passed after the fallible hash call-site correction. An immediately later workspace `cargo fmt --all -- --check` reached a new, concurrently edited RP2 file and reported only import ordering in `cdf-conformance/src/run_matrix/plan_json.rs`; per parent direction this ticket did not edit that owned lane. The prior independent `git diff --check` remained clean.
- 2026-07-10: Parent closure reconciled the shared A10e/RP2 tree. Evidence: `.10x/evidence/2026-07-10-p2-a10e-rp2-runtime-outcomes.md`. Adversarial review: `.10x/reviews/2026-07-10-p2-a10e-rp2-runtime-outcomes-review.md` (pass). Parent-observed `cargo nextest run --workspace --all-features --no-fail-fast` passed 883/883 with zero skipped; format and diff checks passed. All acceptance criteria map to direct tests or package/destination inspection. No A10e blocker remains.

## Blockers

None. A10d is complete.
