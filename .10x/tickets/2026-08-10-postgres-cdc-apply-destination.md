Status: done
Created: 2026-08-10
Updated: 2026-08-10
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/2026-08-07-a1-5-package-native-keyed-effects.md`

# PostgreSQL CDC apply destination

## Scope

Implement first-party PostgreSQL `cdc_apply` for single and routed target families using the
existing package-native keyed upsert/delete authority, explicit ignore/hard/Boolean-soft delete
policy, package-atomic transaction, exact outcome receipt, duplicate replay, and checkpoint gate.
Remove the internal-ticket rejection and publish the capability only when executable.

## Non-goals

- PostgreSQL logical-replication source ingestion.
- New delete semantics, partial/patch updates, or compatibility artifacts.
- Parallel target transactions or weakened package-atomic settlement.

## Acceptance Criteria

- [x] PostgreSQL sheet and planner accept `cdc_apply` only for valid keyed-change authority.
- [x] Complete upserts and ignore/hard/soft deletes apply transactionally with exact receipt counts.
- [x] One routed package applies every PostgreSQL physical target in one transaction or none.
- [x] Duplicate replay performs no second mutation and verifies the same package receipt.
- [x] Failures before durable receipt leave checkpoint authority unchanged.
- [x] Focused unit/live tests, formatting, check, and strict affected-package Clippy pass.

## References

- `.10x/specs/cdc-resource-authoring-and-continuous-run.md`
- `.10x/specs/package-keyed-delete-effects.md`
- `.10x/specs/routed-destination-target-families.md`
- `.10x/tickets/2026-08-07-a6-2-routed-target-families.md`

## Assumptions

- PostgreSQL and DuckDB are the user-ratified first `cdc_apply` destinations.
- PostgreSQL uses its existing binary COPY, transaction, mirror, receipt, and correction machinery.

## Journal

- 2026-08-10: Opened after the audit found the active CDC contract still rejected by PostgreSQL.
- 2026-08-10: Replaced the rejection with package-native `cdc_apply` validation and direct
  single-target application. Complete upserts and key-only deletes use binary COPY staging and
  set-based PostgreSQL mutations; delete ignore, hard delete, and Boolean soft delete retain
  distinct exact outcome counts. Upserts always clear a configured soft-delete marker.
- 2026-08-10: Implemented PostgreSQL routed-family planning and application through the neutral
  runtime boundary. Every output is independently schema/key planned before mutation, then all
  target DDL, effects, state/segment/load mirrors, receipt verification, and commit occur in one
  PostgreSQL transaction. The same path honors routed append/replace/merge so advertised routed
  support is truthful rather than CDC-only. Empty packages remain data no-ops.
- 2026-08-10: Added transaction-wide effect-key guards, exact insert/update/delete/missing/ignored
  accounting, canonical segment-order checks, package-token duplicate receipt recovery, and a
  failure certificate proving a late second-target schema failure rolls back the first target and
  destination mirrors.
- 2026-08-10: Closed the verified-package authority gap at the neutral runtime boundary. Direct
  single and routed destination commits now receive the exact already-verified package access used
  by replay, validate its package hash, and mirror quarantines in the same PostgreSQL transaction as
  target effects, load/state/segment authority, and the durable receipt. Duplicate replay remains
  idempotent; conflicting quarantine evidence rolls the entire commit back.
- 2026-08-10: Made the configured Boolean soft-delete marker destination-owned. Source/package
  schemas cannot claim it, planning creates and validates it, live execution revalidates it against
  catalog drift, upserts explicitly clear it, and repeated deletes of an already-soft-deleted key
  are no-ops rather than false transitions.
- 2026-08-10: Replaced permissive duplicate-receipt count validation with recursive validation of
  the exact row/keyed/routed receipt shape, disposition, delete policy, segment totals, and routed
  target bindings. The validator preserves the contract's permitted omission for repeated-soft
  no-transition outcomes while rejecting impossible or policy-incompatible counts.

## Blockers

None.

## Evidence

- `DUCKDB_DOWNLOAD_LIB=1 cargo check -p cdf-dest-postgres --all-targets --locked` passed. This
  proves the affected crate and its reverse test/build consumers typecheck with the current
  runtime capability contract.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-postgres --lib cdc::tests --locked` passed two
  focused tests. They prove empty routed planning emits no target migration and keyed receipt
  counts partition applied and missing delete outcomes exactly.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-postgres
  cdc_planning_requires_exact_keyed_authority_and_validates_soft_marker --locked` passed. It
  proves ordinary row content, key drift, and invalid soft markers are rejected before execution,
  while valid keyed authority plans position-scoped delivery and marker-safe DDL/DML.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-postgres
  live_cdc_applies_hard_ignore_soft_missing_and_duplicate_replay --locked -- --nocapture` passed
  against an ephemeral local PostgreSQL server. It proves complete insert/update application,
  hard/ignore/soft/missing delete outcomes, soft-marker clearing on later upsert, and duplicate
  replay without a second mutation.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-postgres
  live_routed_cdc_commits_two_targets_atomically_and_rolls_back_family_failure --locked --
  --nocapture` passed against an ephemeral local PostgreSQL server. It proves one receipt settles
  two physical targets, its verified quarantine mirror is written exactly once across replay, and a
  failure in the second output rolls back both target data and load-mirror authority.
- `DUCKDB_DOWNLOAD_LIB=1 cargo check -p cdf-runtime -p cdf-project -p cdf-dest-duckdb
  -p cdf-dest-postgres --all-targets --locked` passed after the direct-commit signature change. It
  proves the neutral verified-package authority is wired through every current implementation and
  replay caller covered by these crates.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-postgres
  live_cdc_mirrors_verified_quarantine_once_and_rolls_back_conflicting_evidence --locked --
  --nocapture` passed against ephemeral local PostgreSQL servers. It proves a persisted package is
  verified before direct commit, quarantine evidence is mirrored once across duplicate replay, and
  conflicting evidence aborts target, quarantine, and receipt publication atomically.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-postgres
  duplicate_cdc_receipt_counts_reject_tampering_and_allow_soft_noop_omission --locked` passed. It
  proves duplicate replay rejects impossible upsert partitions and delete-policy count shapes while
  accepting the active receipt contract's repeated-soft no-transition omission.
- The focused planning and single-target live tests above were rerun after destination ownership of
  the soft-delete marker was enforced. They prove source-owned markers and incompatible live target
  columns are rejected, valid DDL/DML does not COPY the marker, repeated soft deletion is a no-op,
  and a later complete upsert resurrects the row by clearing the marker.
- `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-dest-postgres --all-targets --locked -- -D warnings`,
  `cargo fmt --all`, and `git diff --check` passed. These are affected-package lint, formatting,
  and patch-integrity certificates; they are not a workspace-wide behavioral certificate.

## Review

Independent red-team verdict: `pass`. The reviewer first found destination/source ownership drift
for the soft-delete marker, repeated-soft transition overcounting, incomplete recursive duplicate
receipt validation, and a quarantine rollback assertion that did not address the isolated live
schema. Each was repaired and re-read. Residual risk: routed quarantines do not have a separate
nonempty fixture, but they use the same unconditional transactional mirror path exercised by the
direct CDC test; the reviewer classified this as non-blocking.

## Retrospective

The existing single-table commit session assumed one row schema and therefore could not safely
admit key-only delete segments or heterogeneous routed outputs. Reusing its PostgreSQL binary
encoder, transaction mirrors, and receipt verification behind one explicit finalized-package
transaction kept those authorities intact without teaching the ordinary ingress session about
routing. The important integration trap was capability granularity: routed support is advertised
at destination scope, so implementing only routed CDC would have made ordinary routed
append/replace/merge claims false. A second trap was empty-package DDL; honoring
`DestinationCommitRequest::is_data_noop` requires skipping target tables and key guards as well as
payload writes.

The closure repair exposed three broader boundary lessons. First, a destination that owns atomic
quarantine publication must receive the exact verified package authority; reconstructing or
rereading package state at the destination would split the trust boundary. Second, a soft-delete
marker is destination state, not source data: allowing it into the package schema makes resurrection
and transition accounting ambiguous. Third, duplicate replay validation must recursively validate
the receipt's semantic shape rather than merely its total row count. The current receipt contract
has no field for keys already soft-deleted, so omitting the optional soft/missing partition for that
case is truthful; inventing a count would not be.
