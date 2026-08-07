Status: done
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
- 2026-08-06: Deleted the lock parser/model, CAS and mutation guard, hydration, destination and
  contract registries, portable-plan lock preconditions, manifest/report/system-SQL hashes, and
  all command dispatch for lock inspection, contract freeze/test, and duplicate top-level schema
  diff. No compatibility reader, removed-command alias, or rejection-only legacy test remains.
- 2026-08-06: Recompiled project artifacts directly from immutable state schema authority plus the
  currently resolved destination capability artifact. Ordinary selected compile now prepares from
  an active state head before considering bounded discovery, so repeated compile/plan/run share
  one schema rather than rediscovering and contradicting their own compiled authority.
- 2026-08-06: Made `cdf schema show` read the active state head/version and made `cdf schema diff`
  compare that baseline to one bounded fresh source observation without writing project, state,
  cache, package, destination, or checkpoint state. Renamed remaining schema snapshot references
  as derived caches where they are not active authority.
- 2026-08-06: Preserved the generic guarded project-file transaction under the neutral
  `.cdf/project-files.mutation.lock` coordination path and rewrote its tests around actual
  generated project files. Discovery generation retains crash-safe multi-file publication without
  a lock-last commit point.
- 2026-08-06: Regenerated completions, help, manpages, command docs, and error references; removed
  obsolete generated files; aligned README, VISION, architecture, onboarding, connector docs,
  benchmark tooling, and active specifications/knowledge with state-backed authority.
- 2026-08-06: Promotion planning exposed one premature live-protocol access for filesystem
  Parquet. Added a runtime-owned destination capability artifact query, allowing Parquet to answer
  planning introspection statically without materializing or connecting to the destination. The
  focused correction-sidecar promotion test now reaches durable `published` state.

## Blockers

None. S2–S4 are closed with state as the sole live schema and promotion authority.

## Evidence

- Focused CLI behavior passed for `compile_discovers_only_selected_source_and_publishes_schema_authority`,
  `active_plan_enforces_exact_state_authority_without_project_writes`,
  `schema_show_and_diff_use_state_authority_without_writes_or_secret_leak`,
  `validate_is_static_when_data_and_secret_values_are_unavailable`, and
  `contract_show_remains_project_free`. These prove selected state-backed preparation, active-plan
  no-write behavior, state-owned show/diff, offline validation, and the retained project-free
  contract surface.
- Focused conformance passed for
  `mvp_acceptance_demo_fixture_proves_rest_duckdb_recovery_replay_and_drift` and
  `rest_compile_preview_run_package_checkpoint_conformance` after replacing lock assertions with
  state head/version and derived discovery-cache evidence.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-project project_files -- --nocapture` passed all 38
  focused generic publication/recovery tests using a neutral generated project file rather than a
  lockfile fixture.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli
  schema_promote_execute_routes_parquet_through_correction_sidecar -- --nocapture` passed and
  proves destination capability inspection remains connection-free while execution commits the
  Parquet correction and publishes state.
- `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-kernel -p cdf-runtime -p cdf-project -p cdf-cli-core
  -p cdf-cli -p cdf-conformance -p cdf-dest-duckdb -p cdf-dest-parquet --all-targets -- -D
  warnings` passed. The separate cognitive-complexity diagnostic reported only pre-existing
  functions outside the changed production paths.
- Both generated CLI artifact checks passed, including the docs-only check. `cargo machete
  --with-metadata` found no unused dependencies. `cargo fmt --all`, `git diff --check`, and the
  scoped product sweep for `cdf.lock`, removed commands, lock report fields, and deleted type names
  passed.
- `graphify update .` could not run because `graphify` is not installed in this environment. This
  limits only graph freshness; compilation, behavioral, generated-artifact, and dependency evidence
  above is unaffected.

## Review

Verdict: pass for focused implementation handback. Self-review traced every surviving lock
reference in active product code/docs and found none; historical terminal/superseded records remain
intentionally immutable. It also caught and repaired the Parquet protocol-materialization hazard
introduced when destination capability authority moved out of the lockfile.

The user explicitly consolidated the independent adversarial review at S6. Residual integrated
risk is therefore owned by S6's one broad workspace barrier, release-binary sandbox journeys,
current-only sweep, and fresh reviewer rather than repeated here.

## Retrospective

Deleting the artifact exposed which responsibilities had been bundled into it: schema authority,
destination capability evidence, compiler provenance, and project publication ordering. Moving
each to its literal owner made most of the old surface deletable rather than replaceable. The key
ergonomic repair was ensuring compile consults active state before discovery; otherwise removing
the lock would have preserved the exact compile/plan/run contradiction this program exists to end.

The Parquet failure reinforced that planning evidence must not require operational destination
materialization. A runtime-owned capability-artifact method preserves adapter-specific protocol
facts while keeping plan/diff introspection thin and connection-free. Focused behavioral tests plus
one affected-package quality barrier were sufficient for this deletion tranche; the expensive
cross-workspace and sandbox certificates remain correctly deferred to S6.
