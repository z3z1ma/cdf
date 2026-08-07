Status: active
Created: 2026-08-06
Updated: 2026-08-06
Parent: `.10x/tickets/2026-08-06-state-backed-schema-authority-program.md`
Depends-On: `.10x/tickets/done/2026-08-06-s2-state-backed-preparation-portable-plan.md`, `.10x/tickets/done/2026-08-06-s3-schema-drift-dispositions.md`, `.10x/tickets/done/2026-08-06-s4-state-backed-promotion-settlement.md`

# S5 delete lockfile product surface

## Scope

Delete every remaining first-party path whose only purpose is `cdf.lock` or superseded lock-backed
policy/commands, after S2–S4 have removed live consumers:

- `CdfLock`, `LockedResource`, locked destinations/contracts/compiler bindings, parser/serializer,
  generation/validation/hydration, lock constants, lock CAS and mutation guard;
- proposed-lock/global lock precondition portable-plan shapes and inline schema sidecars retained
  only for them;
- `cdf inspect lock`, `cdf contract freeze`, and lock-backed `cdf contract test`; retain current
  `cdf contract show` only;
- lock fields/hashes from manifests, compiled artifacts, run/promotion events, system SQL,
  reports, destination inspection, diagnostics, suggestions, telemetry, and debug output;
- init/add/discovery/project publication behavior and recovery ordering whose only commit point was
  the lockfile;
- lock fixtures, helpers, snapshots, generated help/completions/manpages/docs, examples, VISION,
  tests, dependencies, module exports, and stale active record wording;
- `.cdf/schemas` authority claims; keep only derived/cache copies with explicit last-known labels
  where still useful.

Retain generic guarded project-file transactions, immutable sidecar publication, source/destination
capability sheets inside compiled/plan evidence, semantic definitions, package/receipt/checkpoint
authority, and historical terminal records under `tickets/done`/`superseded`.

## Non-goals

- compatibility reader, warning shim, rejected-legacy tests, alias, transitional dual write, or
  migration from an old lockfile;
- deleting generic project publication because one former consumer disappeared;
- schema export/import or replacing the lockfile with another required project artifact;
- broad cleanup unrelated to a proven orphan or stale public surface.

## Acceptance criteria

1. Init/validate/plan/compile/run/schema/doctor/sql work without `cdf.lock`; init never creates it and
   validate never inspects it.
2. Current CLI help contains no lock inspection or lock-backed contract command.
3. No production Rust type/function/module/dependency survives solely to parse, create, hydrate,
   compare, report, or publish the removed artifact.
4. Current serialized plans/manifests/events/reports/system tables use schema/state/compiled
   authority terminology and contain no lock hash.
5. Project publication tests retain crash safety for actual multi-file authored/generated writes
   without lock-last behavior.
6. Generated docs/completions/manpages/examples/VISION are current and contain no removed product
   workflow.
7. Behavioral tests certify current grammar/artifacts; no source-token or rejection-only legacy
   test is added.
8. `cargo machete --with-metadata`, affected strict Clippy, generated checks, diff check, and scoped
   current-only `rg` sweep pass.

## References

- `.10x/decisions/state-backed-schema-authority.md`
- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/portable-plan-artifact.md`
- `.10x/specs/cli-command-intent-and-effects.md`
- `.10x/knowledge/project-file-publication-recovery.md` (generic clauses only)
- `.10x/knowledge/net-new-no-compatibility-policy.md`
- `QUALITY.md`

## Assumptions

- User-ratified: full deletion of lockfile, inspect lock, freeze/test registry, and compatibility.
- User-ratified: no initial schema export/import replacement.
- Record-backed: S2–S4 leave state as sole live schema/promotion authority before deletion.
- Record-backed: terminal/superseded records remain historical evidence and are not product surface.

## Journal

- 2026-08-06: Opened dependency-gated behind the complete authority and promotion cutover.
- 2026-08-06: S2–S4 are closed and pushed. Began the current-only lockfile deletion after reading
  the governing CLI report and project publication audit procedures. The integrated broad quality
  and independent review barrier remains consolidated in S6 per explicit user direction.

## Blockers

None. S2–S4 are closed with state as the sole live schema and promotion authority.

## Evidence

Pending execution.

## Review

Pending handback review.

## Retrospective

Pending execution.
