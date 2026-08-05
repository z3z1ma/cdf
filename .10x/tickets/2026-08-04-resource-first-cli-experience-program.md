Status: open
Created: 2026-08-04
Updated: 2026-08-04

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
   `.10x/tickets/2026-08-04-u1-resource-selectors-static-validate.md`.
3. **U2 — independent compilation authority.** Replace monolithic exact-project lock/manifest
   execution authority with per-resource lock entries, immutable artifacts, and a bounded status
   index; make selected and aggregate compile useful under partial failure. Owner:
   `.10x/tickets/2026-08-04-u2-independent-resource-compilation-authority.md`.
4. **U3 — shared selected preparation and multi-resource plan/run.** Route plan/run/compile through
   one prepared-resource seam, remove refresh/pin/discover/no-pin legacy grammar, enforce the
   all-selected preparation barrier, and retain independent runtime outcomes.
5. **U4 — portable plan export/consumption.** Add `plan --out`, canonical plan artifact authority,
   `run --plan`, strict portability validation, and whole-plan no-repair preflight while preserving
   the existing terminal plan document.
6. **U5 — source discovery, generation, and add.** Add source/resource discovery and canonical
   artifacts, adapter-owned catalog enumeration, thin create-or-verify generation, then reconcile
   the single-location add path against the ratified authoring model.
7. **U6 — operational and recovery command coherence.** Implement scoped doctor; fold package
   execution and interrupted-run recovery into explicit `run` input modes; audit
   preview/backfill/status/inspect loaders, reports, negative-I/O laws, and remediation against the
   command-effect contract.
8. **U7 — cutover and sandbox certificate.** Delete all superseded code/fixtures/docs/generated
   artifacts, run bounded integration/quality checks, and prove the supplied sandbox journeys.

Children U2-U7 open only when their immediate dependency's implementation evidence is complete and
pushed and any remaining focused ratification blocker is closed. A dependency may remain `active`
solely when its review is intentionally deferred to the named combined barrier with the new child;
both close only after that review passes. No child may introduce a compatibility parser, alias,
fallback artifact reader, or dual authority; superseded existing surfaces are deleted in the child
that replaces them and must all be absent at cutover.

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

The primary agent implements every child. A separate read-only subagent reviews frozen logical
cuts: U0 alone, U1+U2 authority foundations, U3+U4 planning/execution, U5+U6 command family, and U7
end-to-end. Reviews use deterministic open-code-review file/rule selection when available, return
one severity-ranked batch, and do not create open-ended nit loops. Focused tests/check/Clippy run
during each child; broader certificates run only at the named cut barriers and final sandbox
closure.

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

## Blockers

None at the program-model level. Children remain dependency-gated by the sequence above.

## Evidence

Pending child execution.

## Review

Pending executable-child reviews.

## Retrospective

Pending program closure.
