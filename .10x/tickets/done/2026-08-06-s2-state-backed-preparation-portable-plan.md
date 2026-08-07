Status: done
Created: 2026-08-06
Updated: 2026-08-06
Parent: `.10x/tickets/done/2026-08-06-state-backed-schema-authority-program.md`
Depends-On: `.10x/tickets/done/2026-08-06-s1-state-schema-authority-foundation.md`

# S2 state-backed preparation and portable plans

## Scope

Cut selected preparation, compile, plan, run first use, compiled artifacts, and portable plans from
lockfile bindings to the S1 state store:

- require typed `[project].id`; make `cdf init` generate it and static validate check syntax without
  state access;
- resolve selected environment state once and read only selected schema keys;
- compile active resources against exact domain/key/generation/hash;
- build complete absent first-use proposals without writes during plan;
- let compile establish missing selected heads and make `compile --locked` require existing heads;
- make multi-resource run prepare all resources, atomically establish the complete proposal set,
  then execute exact prepared plans without rediscovery/recompilation;
- make `run --locked` require existing heads;
- bind immutable compiled artifacts/index entries to state authority rather than `LockedResource`;
- replace global lock/proposed-lock portable fields with per-resource absent/exact state
  preconditions and exact proposal versions;
- make `run --plan` perform whole-plan no-repair preflight and exact batch establishment;
- add state authority/drift facts to typed plan/run reports while preserving terminal plan UX.

This ticket removes lockfile authority from these paths. S5 owns deleting dormant lock-only models,
commands, fixtures, generated references, and dependencies after S4 no longer consumes them.

## Non-goals

- policy type/drift runtime redesign (S3);
- promotion settlement/corrections (S4);
- complete lockfile code/docs deletion (S5);
- Postgres state, cross-domain import, or compatibility portable-plan reader;
- destination repair or automatic migration.

## Acceptance criteria

1. Missing/malformed project id is a static project error; init produces one current valid id.
2. Plan with absent/active authority writes no project cache or state bytes and renders exact
   proposed/active facts.
3. Selected compile/run never read or invalidate unrelated state keys/resources/environments.
4. Ordinary compile establishes absent state idempotently; `--locked` fails absence before cache
   publication.
5. Multi-resource run proves all-selected preparation and one all-or-none state transaction before
   package/destination/run-ledger effects; a race executes nothing.
6. Exact prepared objects reach execution with no second discovery/hydration/compilation pass.
7. Compiled artifacts verify exact state domain/key/generation/hash and rebuild independently when
   authored/compiler bindings change.
8. Portable artifacts carry per-resource absent/exact preconditions; relevant changes fail before
   effects and unrelated promotions do not invalidate them.
9. Existing human plan output remains primary; JSON/human/redaction/effects facts share one typed
   report authority.
10. Focused selector, negative-I/O, mutation-counter, portable-plan, concurrency, and generated CLI
    artifact tests pass with affected fmt/check/strict Clippy.

## References

- `.10x/decisions/state-backed-schema-authority.md`
- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/resource-selector-batch-commands.md`
- `.10x/specs/portable-plan-artifact.md`
- `.10x/specs/cli-command-intent-and-effects.md`
- `.10x/specs/project-compilation-manifest.md` (generated index/compiler artifact clauses only;
  lock/refresh clauses are superseded)
- `.10x/knowledge/cli-report-authority.md`

## Assumptions

- User-ratified: ordinary compile/run establish absence; `--locked` requires active authority.
- User-ratified: plan remains no-write and `plan --out` remains orthogonal to terminal rendering.
- User-ratified: missing project id has no inferred/backward-compatible fallback.
- Record-backed: only relevant per-resource keys fence portable plans; global revisions are
  forbidden.
- Record-backed: source/destination/checkpoint/input attestations remain exact and no-repair.

## Journal

- 2026-08-06: Opened dependency-gated behind S1; implementation intentionally deferred.
- 2026-08-06: Activated after S1 closed at
  `.10x/tickets/done/2026-08-06-s1-state-schema-authority-foundation.md`. Read the complete ticket,
  governing records, CLI report-authority skill, and project-file publication skill. S2 will reuse
  the existing guarded derived-artifact transaction and typed report seams; state is the sole
  schema authority.
- 2026-08-06: Added required stable `[project].id`; `cdf init` emits a UUID and static validation
  rejects missing/blank identity without opening state. Extended the state contract with checked
  all-selected establishment, read-only schema/checkpoint inspection, planned domain creation, and
  fail-closed orphan detection.
- 2026-08-06: Cut selected compile/plan/run/backfill loaders from live `cdf.lock` parsing and
  hydration. Compiled artifacts now bind exact state domain/project/environment/resource,
  generation, and schema hash. Ordinary compile establishes first use, while `--locked` requires
  an active head and publishes no failed cache entry.
- 2026-08-06: Reworked ordinary run into one exact preparation pass followed by one checked batch
  state transaction before execution. Portable plans now carry per-resource absent/exact state
  preconditions and first-use versions; `run --plan` performs no-repair preflight and checked batch
  establishment without publishing project files.
- 2026-08-06: Preserved the existing terminal plan report and added typed schema-authority facts to
  plan, compile, and run output. Regenerated command help, completions, manpages, and command docs.
- 2026-08-06: Release-binary sandbox certificate: `compile fineweb.documents` established schema
  generation 1; `plan fineweb.documents --out .cdf/s2-fineweb-plan.json` rendered the normal plan
  plus an exact portable artifact; ordinary `run fineweb.documents` loaded 1.1M rows / 2.1 GiB in
  14 segments and committed its receipt/checkpoint. `plan local.events --out ...` followed by
  `run --plan ...` passed preflight, established first-use authority, and committed two rows.
  Immediate FineWeb portable execution exposed an HTTP source-attestation defect, durably owned by
  `.10x/tickets/2026-08-06-http-portable-plan-source-attestation.md`; schema/state preflight was not
  implicated.

## Blockers

None.

## Evidence

1. AC1: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli --lib tests::init_validate -- --nocapture`
   passed 13/13, including generated UUID identity and missing/blank offline rejection.
2. AC2, AC6: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli --lib
   tests::source_planning -- --nocapture` passed 4/4; absent and active plan snapshots were
   byte-for-byte unchanged, and first-use/cold-source run used one prepared payload.
3. AC3, AC4, AC7: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli --lib compile_ -- --nocapture`
   passed 10/10. State-bound locked rebuilds ignored deliberately invalid legacy lock bytes;
   missing locked authority published no cache/state; selected compile and secret-redaction cases
   passed.
4. AC5, AC8, AC9: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli --lib tests::planning --
   --nocapture` passed 23/23. This covers all-selected preparation barriers, portable tamper/source
   preflight, first-use establishment, plan artifact publication, and terminal/JSON reports.
5. AC5: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-state-sqlite
   sqlite_schema_authority_passes_shared_conformance -- --nocapture` passed; checked batches fence
   exact and absent keys atomically. The focused orphan-version corruption test also passed.
6. AC10: affected all-target `cargo check` passed. Strict affected-package Clippy passed with
   `-D warnings`; the cognitive-complexity diagnostic reported only existing unrelated functions.
   `cargo machete --with-metadata` found no unused dependencies. Both generated CLI artifact checks
   passed.
7. Release certificate limit: the bundled-DuckDB release build passed in 10m23s. FineWeb ordinary
   run and local portable run passed as journaled above. FineWeb portable execution failed before
   effects at HTTP source attestation and has a separate current owner.

## Review

Fresh self-review performed under the user's explicit no-red-team instruction for this tranche.
The review found and repaired three material issues before closure: selected loaders still parsed
legacy lock bytes; proposal reuse was process-global and unsafe for parallel in-process commands;
and portable artifacts did not reject mixed domain/project/environment keys. Strict Clippy then
found and drove removal of an oversized enum representation and two argument-list regressions.

Verdict: pass for S2. Dormant lock-only promotion/inspection models remain intentionally owned by
S4/S5. HTTP weak-generation portable attestation is isolated and owned by the follow-up ticket; it
does not weaken or invalidate S2's state-schema authority guarantees.

## Retrospective

The decisive simplification was to carry one typed prepared schema authority beside the already
prepared resource, instead of trying to refresh a second filesystem authority. This removed the
double compile/discovery pass and made the run effect barrier literal. The main trap was assuming
that deleting lock fields from portable JSON also removed live lock influence; inspecting the
selected project loader exposed the remaining hydration. Release testing also paid for itself:
unit tests proved portable state fencing, while the real CDN separated a source-attestation defect
from the schema-authority cutover and gave that defect a precise owner.
