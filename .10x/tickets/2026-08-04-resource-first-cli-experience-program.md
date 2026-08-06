Status: active
Created: 2026-08-04
Updated: 2026-08-06

# Resource-first CLI experience program

This is a parent planning/orchestration ticket. It is not executable implementation scope.

## Objective

Replace CDF's phase-oriented, whole-project command workflow with a resource-first experience in
which plan/run own ordinary preparation, selectors bound work before I/O, validation is static,
schema authority is explicit, portable plans can cross machines, discovery can generate thin
resources, and every adjacent command has one scope and effect ceiling.

## Scope and sequence

1. **U0 — manifest text and diagnostic ownership.** Repair the multiline-SQL manifest regression
   and remove blanket compile-refresh remediation without waiting for the authority redesign.
   Owner: `.10x/tickets/done/2026-08-04-u0-manifest-text-diagnostic-ownership.md`.
2. **U1 — selectors and static validate.** Introduce deterministic resource selection, cut
   validate off from secrets/network/source/destination state, remove `--deep`, and publish one
   aggregate typed report. Owner:
   `.10x/tickets/done/2026-08-04-u1-resource-selectors-static-validate.md`.
3. **U2 — independent compilation authority.** Replace monolithic exact-project lock/manifest
   execution authority with per-resource lock entries, immutable artifacts, and a bounded status
   index; make selected and aggregate compile useful under partial failure. Owner:
   `.10x/tickets/done/2026-08-04-u2-independent-resource-compilation-authority.md`.
4. **U3 — shared selected preparation and multi-resource plan/run.** Route plan/run/compile through
   one prepared-resource seam, remove refresh/pin/discover/no-pin legacy grammar, enforce the
   all-selected preparation barrier, and retain independent runtime outcomes.
   Owner: `.10x/tickets/done/2026-08-05-u3-selected-preparation-multi-resource-plan-run.md`.
5. **U4 — portable plan export/consumption.** Add `plan --out`, canonical plan artifact authority,
   `run --plan`, strict portability validation, and whole-plan no-repair preflight while preserving
   the existing terminal plan document.
   Owner: `.10x/tickets/done/2026-08-05-u4-portable-plan-export-consumption.md`.
6. **U5 — source discovery, generation, and add.** Add source/resource discovery and canonical
   artifacts, adapter-owned catalog enumeration, thin create-or-verify generation, then reconcile
   the single-location add path against the ratified authoring model.
   Owner: `.10x/tickets/done/2026-08-05-u5-source-resource-discovery-generation-add.md`.
7. **U6 — operational and recovery command coherence.** Implement scoped doctor; fold package
   execution and interrupted-run recovery into explicit `run` input modes; audit
   preview/backfill/status/inspect loaders, reports, negative-I/O laws, and remediation against the
   command-effect contract.
   Owner: `.10x/tickets/done/2026-08-05-u6-operational-recovery-command-coherence.md`.
8. **U6b — useful default live telemetry.** Add typed phase-local metrics, clock-driven TTY and
   headless liveness, immediate retry/wait status, and stable completed/failure telemetry without
   durable animation events. Owner:
   `.10x/tickets/done/2026-08-06-u6b-default-live-telemetry.md`.
9. **U7 — cutover and sandbox certificate.** Delete all superseded code/fixtures/docs/generated
   artifacts, run bounded integration/quality checks, and prove the supplied sandbox journeys.
   Owner: `.10x/tickets/2026-08-05-u7-resource-first-cutover-certificate.md`.

Children U2-U7, including U6b, open only when their immediate dependency's implementation evidence
is complete and pushed and any remaining focused ratification blocker is closed. A dependency may
remain `active` solely when its review is intentionally deferred to the named combined barrier with
the new child; both close only after that review passes. No child may introduce a compatibility
parser, alias, fallback artifact reader, or dual authority; superseded existing surfaces are
deleted in the child that replaces them and must all be absent at cutover.

## Integration boundaries

- The other executor owns
  `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`; this program works from
  a dedicated worktree and branch and must preserve their unrelated changes.
- D3's current `cdf/<namespace>/<resource>.cdf.sql`, shared `[sources.<name>]`, path identity, typed
  source bindings, and native relational IR are starting authority, not legacy to restore.
- Existing crash-safe multi-file publication, content-addressed sidecars, portable source plans,
  worker protocol, package/receipt/checkpoint/run authority, and CLI renderer/report vocabulary are
  reused rather than reimplemented.
- U2 is the first material overlap with manifest/lock authority. It must inspect the final upstream
  state and rebase/merge deliberately without switching the shared checkout.

## Governing references

- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/resource-selector-batch-commands.md`
- `.10x/specs/portable-plan-artifact.md`
- `.10x/specs/source-discovery-resource-generation.md`
- `.10x/decisions/static-validation-operational-readiness-boundary.md`
- `.10x/specs/cli-command-intent-and-effects.md` (draft only for U5/U6 pending its named blocker)
- `.10x/research/2026-08-04-resource-preparation-ergonomics-inventory.md`
- `.10x/research/2026-08-04-selector-plan-discovery-authority-inventory.md`
- `.10x/knowledge/cli-report-authority.md`

## Review and validation cadence

The primary agent implements every child. Per the user's execution override, no intermediate
subagent or red-team review runs; one combined review and broader certificate runs at U7 after the
entire tranche is frozen. Focused tests/check/Clippy run during each child.

## Acceptance criteria

1. Every executable child has active governing authority, complete implementation evidence, and a
   bounded commit pushed before the next dependent child begins; review-deferred siblings remain
   active and close together when their named logical review barrier passes, before work proceeds
   beyond that barrier.
2. All parent/focused-spec scenarios pass, including negative-I/O/mutation counters and aggregate
   partial outcomes.
3. Existing plan terminal output remains the primary beautiful report while artifact effects are
   additive and JSON/human facts share one typed authority.
4. The sandbox can add/discover/validate/plan/export and run from resource, portable-plan, package,
   or interrupted-run authority while unrelated broken or credential-inaccessible resources remain
   irrelevant.
5. Removed grammar/artifact shapes have no aliases, shims, fallbacks, dead fixtures, or
   rejection-only compatibility tests.

## Journal

- 2026-08-04: Opened after the user ratified the resource-first core, exact/glob batch selection,
  portable plan/run, source/resource discovery with thin generation, lock-as-schema-fence model,
  and static validate boundary. The proposed doctor/add/recovery command details remain one
  focused ratification blocker and do not block U0/U1.
- 2026-08-04: Work is isolated in
  `/Users/alexanderbut/code_projects/personal/cdf-usage-ergonomics` on
  `codex/cdf-usage-ergonomics`; the shared checkout was not switched.
- 2026-08-04: The user conditionally confirmed doctor/add but rejected separate replay/resume verbs
  as surprising. The focused draft now recommends one `run` verb with mutually exclusive resource,
  portable-plan, package, and interrupted-run authority; exact confirmation remains U6-only.
- 2026-08-04: The user confirmed the revised one-run-verb model. The command-intent/effects spec is
  active and no behavioral ratification blocker remains for U5/U6.
- 2026-08-05: U3 completed directly on `main`: selected plan/run share one preparation authority,
  run enforces a complete preparation barrier and retains exact first-use payloads, plan remains
  read-only, and the superseded pin/discover/no-pin command grammar is deleted. U4 is now unblocked.
- 2026-08-05: U4 completed directly on `main`: `plan --out` publishes a canonical bounded portable
  artifact while preserving terminal output, `run --plan` performs whole-plan no-repair preflight,
  exact first-use authority publication, and execution with aggregate plan-hash evidence. Three
  PostgreSQL executable-backfill schema-coercion test failures observed in the broader planning
  module are explicitly owned by U6's backfill coherence audit.
- 2026-08-05: U5 completed directly on `main`: configured-source and authored-resource discovery
  are explicit scopes, Files and SQLite provide bounded adapter-owned catalogs, selected schema
  observation remains temporary, generation creates or verifies explicit field projections with
  an honest star fallback, and add no longer compiles unrelated project resources.
- 2026-08-05: U6 activated on `main` with the ratified single-run-verb and scoped-doctor model,
  including explicit ownership of the release-mode sandbox schema/progress regression certificate.
- 2026-08-05: U6 completed and was pushed as `2d398cfa`: run now owns resource-set, portable-plan,
  exact-package, and interrupted-run authority; doctor has explicit operational scopes; adjacent
  commands load named authority; and the optimized bundled release binary loaded the 1.1M-row,
  2.1-GiB FineWeb sandbox resource with live progress and no schema mismatch. U7 is unblocked.

## Blockers

None at the program-model level. Children remain dependency-gated by the sequence above.

## Evidence

Pending child execution.

## Review

Pending executable-child reviews.

## Retrospective

Pending program closure.
