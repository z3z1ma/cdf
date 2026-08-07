Status: open
Created: 2026-08-06
Updated: 2026-08-06
Parent: `.10x/tickets/2026-08-06-state-backed-schema-authority-program.md`
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

## Blockers

S1 must close with reusable SQLite schema-authority conformance.

## Evidence

Pending execution.

## Review

Pending handback review.

## Retrospective

Pending execution.
