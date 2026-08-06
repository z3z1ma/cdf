Status: active
Created: 2026-08-05
Updated: 2026-08-05
Parent: `.10x/tickets/2026-08-04-resource-first-cli-experience-program.md`
Depends-On: `.10x/tickets/2026-08-04-u2-independent-resource-compilation-authority.md`

# U3 selected preparation and multi-resource plan/run

## Scope

Route plan and run through one selected-resource preparation authority already established by U2,
extend both commands to exact/glob resource sets with exclusions, and enforce a complete
preparation barrier before execution effects. Execute the exact prepared schema, relational plan,
admission program, and source/destination bindings without partial rehydration.

Delete the superseded `plan --no-pin`, `schema discover`, and `schema pin` grammar. Replace
compile-directed runtime remediation with the action that owns the actual failure. Preserve the
existing per-resource terminal plan document inside one deterministic aggregate report.

## Non-goals

- portable plan export or consumption (U4);
- source-catalog discovery, resource generation, or add redesign (U5);
- package/resume input modes and scoped doctor (U6);
- cross-resource atomicity, fail-fast execution, or cross-resource SQL dependencies.

## Acceptance Criteria

1. `cdf plan SELECTOR... [--exclude GLOB]` and `cdf run SELECTOR... [--exclude GLOB]` use the
   shared exact/glob selector authority and require an explicit nonempty selected set.
2. Plan attempts every selected resource without writes and renders ordered aggregate readiness,
   each existing plan document, and every scoped preparation failure.
3. Run prepares and preflights the complete selected set before package, destination, receipt,
   checkpoint, or run-ledger mutation; one preparation failure executes none.
4. After the barrier, independent resources run to terminal outcomes and the aggregate exits
   nonzero if any fail without rolling back successful independent effects.
5. Existing first-use run publishes only the selected baseline/artifact/index/lock, then executes
   the exact prepared object without a second discovery or schema/plan hydration pass.
6. The sandbox `fineweb.documents` journey no longer admits a compiled artifact whose runtime
   resource schema differs from its relational input authority, never recommends another compile
   after successful preparation, and does not create an extracting package for that preparation
   failure.
7. Selected plan/run do not parse, validate, resolve secrets for, or contact unselected resources
   or sources.
8. `plan --no-pin`, `schema discover`, and `schema pin` are absent from parser, generated help,
   completions, docs, fixtures, and implementation; no aliases or compatibility paths remain.
9. Human and JSON output consume one typed aggregate report with redaction parity while existing
   single-resource plan facts remain stable.
10. Focused behavioral tests, affected-package check, strict Clippy, formatter, generated CLI
    artifact checks, and diff check pass.

## References

- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/resource-selector-batch-commands.md`
- `.10x/research/2026-08-04-resource-preparation-ergonomics-inventory.md`
- `.10x/knowledge/cli-report-authority.md`
- `.10x/knowledge/error-ownership-taxonomy.md`
- `.10x/knowledge/project-file-publication-recovery.md`
- `.agents/skills/audit-cli-report-authority/SKILL.md`
- `.agents/skills/audit-error-ownership/SKILL.md`
- `.agents/skills/audit-project-file-publication/SKILL.md`

## Assumptions

- User-ratified: selector grammar, all-selected preparation barrier, independent runtime outcomes,
  plan no-write behavior, run-owned first-use preparation, and removal of superseded preparation
  grammar.
- Record-backed: current SQL resources have exactly one upstream dependency, so selected closure is
  one path-derived resource plus its selected source/environment/destination/semantic authority.
- Record-backed: U2 current resource artifacts and guarded publisher are the reusable preparation
  and publication authority; U3 must not introduce a second cache or transaction protocol.

## Journal

- 2026-08-05: Activated after U2 landed on main and its Fast Quality workflow passed. Direct
  sandbox inspection reproduced the motivating split: the compiled resource schema retained
  `cdf:source_name` metadata while the relational input schema did not, runtime rejected the batch
  only after creating an empty `extracting` package, and the diagnostic incorrectly recommended
  compile immediately after compile succeeded.

## Blockers

None.

## Evidence

Pending execution.

## Review

Deferred by user instruction to the final combined U-tranche barrier; no U3 red-team review runs.

## Retrospective

Pending execution.
