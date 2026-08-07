Status: active
Created: 2026-08-06
Updated: 2026-08-06
Parent: `.10x/tickets/2026-08-06-state-backed-schema-authority-program.md`
Depends-On: `.10x/tickets/done/2026-08-06-s1-state-schema-authority-foundation.md`, `.10x/tickets/done/2026-08-06-s2-state-backed-preparation-portable-plan.md`, `.10x/tickets/done/2026-08-06-s3-schema-drift-dispositions.md`

# S4 state-backed promotion settlement

## Scope

Move schema promotion from lockfile CAS plus ledger publication to one state-backed fenced
promotion lifecycle:

- add generation-bound renewable ordinary-run settlement permits acquired only immediately before
  destination mutation and held through receipt verification/checkpoint commit;
- transition exact Active(V1,G1) to fenced Promoting(V1→V2), block new V1 permits, drain/expire old
  permits, and establish the complete committed V1 correction cutoff;
- persist promotion plan, version, target settlements, checkpoints, residual summary references,
  recovery status, and ordered events in state;
- reconcile complete retained top-level exact residual history through compiler-produced coercions,
  framework row provenance, and declared in-place/sidecar/rematerialization strategies;
- preserve unrelated residual paths and null empty envelopes;
- atomically validate all target settlements and publish Active(V2,G2) plus publication event;
- replace `installed_lock_sha256` and lockfile recovery artifacts with state version/head identity;
- make dry promotion report every concurrency/evidence/DDL/correction/recovery precondition and
  keep `--execute` exact.

## Non-goals

- Postgres store, distributed scheduler, or long-held database transaction;
- automatic, future-only, lossy-without-explicit-authority, nested, or partial promotion;
- raw residual values in state;
- arbitrary business keys or destination schema as logical authority;
- changing immutable old package bytes.

## Acceptance criteria

1. Dry promotion writes nothing and reports current/proposed authority, residual inventory,
   promotability, target strategy/DDL, cutoff, fence, and recovery state.
2. Run settlement permit validates exact domain/key/generation immediately before destination
   mutation and fences receipt/checkpoint commit after expiry/head change.
3. Promotion blocks new old-generation permits and cannot set its correction cutoff until earlier
   permits settle or expire safely.
4. A run packaged under V1 cannot commit an uncorrected V1 result after V2 publication.
5. Complete top-level reconciliation fills historical typed values, removes only promoted paths,
   preserves unrelated paths, and nulls empty envelopes.
6. Redacted/tombstoned/missing/unverified required evidence blocks execution precisely.
7. All declared correction strategies pass destination conformance; false capabilities fail.
8. Every target receipt and promotion checkpoint is verified before one atomic head/event advance.
9. Failpoints at every lifecycle boundary resume idempotently; stale/expired/foreign-domain
   executors cannot publish.
10. Old packages remain byte-identical/replayable and repeated completed promotion is a no-op.

## References

- `.10x/decisions/state-backed-schema-authority.md`
- `.10x/specs/schema-promotion-corrections.md`
- `.10x/specs/schema-drift-dispositions.md`
- `.10x/specs/residual-variant-capture.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/decisions/compact-lossless-destination-row-provenance.md`
- `.10x/decisions/promotion-correction-value-authority.md`
- `.10x/knowledge/fenced-lease-lock-publication.md`
- `.10x/knowledge/destination-receipt-authority.md`

## Assumptions

- User-ratified: state-atomic head/publication, complete top-level reconciliation, and no
  future-only/nested initial escape hatch.
- Record-backed: ordinary run permit is short and never held during extraction.
- Record-backed: verified packages or declared verified destination readback own exact values.
- Record-backed: after any durable target correction, recovery rolls forward unless evidence proves
  a complete safe no-effect abort.

## Journal

- 2026-08-06: Opened behind S1–S3; implementation intentionally deferred.
- 2026-08-06: S1–S3 are closed and pushed. Re-read every governing record and the prior
  residual-promotion implementation history, then inspected the kernel schema head/version store,
  SQLite transactions, ordinary replay settlement path, promotion planner/executor, CLI reports,
  and recovery tests. The existing correction package and destination strategy machinery remains
  useful; lockfile CAS, filesystem-staged plan authority, and ledger-only publication do not.
- 2026-08-06: Began with the shared state settlement boundary: generation-bound ordinary-run
  permits, promotion fencing/cutoff, and atomic publication must serialize before the CLI and
  planner can safely consume the state-backed lifecycle.
- 2026-08-06: Added state-clock-owned renewable settlement permits and durable checkpoint-to-
  logical-authority bindings. Permit acquisition serializes against the active head, a promoting
  head refuses new permits while allowing an already-issued generation to drain, expiry fences
  commit, and receipt/checkpoint/authority settlement plus permit release commit atomically.
- 2026-08-06: Bound normal `run` and backfill execution services to the exact prepared state head.
  The replay path now acquires immediately at `DestinationWriteReady`, renews process-locally
  during the destination call, performs a final renewal, and uses the fenced atomic checkpoint
  commit. Ad-hoc and artifact-only paths remain outside project authority until their own typed
  authority is supplied; no schema permit is held during extraction or packaging.
- 2026-08-06: Added the durable promotion lifecycle itself. Promotion begin now atomically stores
  canonical credential-free plan bytes/hash, required targets, residual summary identities, the
  proposed immutable version, the fenced head, and its event. Cutoff establishment refuses live
  ordinary permits and snapshots every committed checkpoint/package bound to the source logical
  generation. Target receipt/checkpoint settlement is atomic and idempotent. Publication rechecks
  permit drain and every exact planned target before atomically advancing the head, lifecycle
  state, and ordered event to the next generation.

## Blockers

None. S1–S3 are closed with state heads, state-backed preparation, and total residual
dispositions.

## Evidence

- `DUCKDB_DOWNLOAD_LIB=1 cargo check -p cdf-kernel -p cdf-state-sqlite` passed after the store
  boundary was introduced.
- Focused SQLite tests passed: `schema_settlement_permit_fences_and_atomically_commits_checkpoint`,
  `expired_schema_settlement_permit_cannot_commit`, and
  `promoting_head_blocks_new_permits_but_drains_an_existing_generation`.
- `DUCKDB_DOWNLOAD_LIB=1 cargo check -p cdf-project -p cdf-cli` passed after runtime/CLI binding.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli
  run_ndjson_discovery_establishes_schema_authority_and_commits -- --nocapture` passed and asserts
  the durable generation-one checkpoint settlement. This proves the ordinary CLI path; it does
  not yet prove promotion cutoff/publication or correction execution.
- Focused state lifecycle tests passed:
  `promotion_publishes_head_only_after_cutoff_and_exact_target_settlement`,
  `promoting_head_blocks_new_permits_but_drains_an_existing_generation`, and
  `sqlite_schema_authority_passes_shared_conformance`. They prove cutoff refusal while a permit is
  live, publication refusal before target settlement, atomic target checkpoint settlement,
  persisted recovery state, and the final generation-two head/event advance.
- Strict affected-package Clippy passed for `cdf-kernel`, `cdf-runtime`, and `cdf-state-sqlite`
  (including all targets). Boxing the settlement binding in `ExecutionServices` kept the shared
  runtime service handle compact and avoided inflating downstream execution enums.

## Review

Pending handback review.

## Retrospective

Pending execution.
