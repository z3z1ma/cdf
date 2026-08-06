Status: done
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
- 2026-08-05: Added exact/glob selectors and exclusions to plan/run. Plan now prepares every
  selected resource read-only and reports ordered ready/failed outcomes; run completes the entire
  preparation barrier before any selected resource executes, then preserves independent terminal
  outcomes.
- 2026-08-05: First-use run now publishes the selected baseline and compiled artifact from the
  retained prepared object, then executes that same source payload, schema, relational plan, and
  runtime binding. The relational input boundary also canonicalizes admitted physical-batch
  metadata to the effective logical schema before execution.
- 2026-08-05: Deleted `plan --no-pin`, `schema discover`, and `schema pin` from parser,
  implementation, generated help/man/completions, command docs, and obsolete CLI fixtures. Updated
  diagnostics to name preparation, compilation, schema diff, or schema promotion according to the
  owning action.

## Blockers

None.

## Evidence

- Criteria 1-5, 7, and 9: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli-core --lib --locked`
  passed 49/49; the focused `cdf-cli` source-planning set passed 4/4; the focused plan set passed
  8/8; and the run preparation-barrier and human-progress tests passed. These tests exercise
  selector aggregation, no-write planning, all-selected preflight, exact first-use payload reuse,
  selective lock publication, and typed human/JSON reports.
- Criterion 6: the focused engine regression
  `admitted_physical_batch_is_rebound_to_effective_logical_metadata` passed. A debug sandbox plan
  for `fineweb.documents` succeeded and a debug run advanced through 493 MiB of packaging without
  reproducing the old relational-schema mismatch; the run was interrupted because debug execution
  was too slow for a useful completion certificate. Release-mode end-to-end proof remains owned by
  U7.
- Criterion 8: generated CLI help, man pages, completions, and command docs were regenerated after
  deleting the superseded grammar. Current-model surface and promotion tests use compile/plan
  authority rather than hidden schema-pin commands.
- Criterion 10: affected all-target `cargo check` passed. Strict affected-package Clippy passed
  with `-D warnings`; the explicit cognitive-complexity diagnostic reported only existing
  production/test findings outside changed U3 functions. `cargo fmt --all -- --check` and
  `git diff --check` passed. `graphify update .` could not run because the `graphify` executable is
  unavailable in this checkout's shell.

## Review

Deferred by user instruction to the final combined U-tranche barrier; no U3 red-team review runs.

## Retrospective

- The actual runtime defect was not stale compilation; it was allowing physical connector metadata
  to masquerade as relational input identity. Canonicalizing the admitted batch at that boundary
  removes an entire class of misleading compile-again loops.
- A first-use run must retain and consume one prepared object. Publishing and then hydrating again
  is both slower and observably wrong for remote sources because it can issue a second request or
  see different bytes.
- Aggregate commands are easiest to reason about when preparation and execution are explicit
  phases with a typed outcome per selected resource. That made the zero-effects barrier and
  independent post-barrier failures literal rather than emergent control flow.
