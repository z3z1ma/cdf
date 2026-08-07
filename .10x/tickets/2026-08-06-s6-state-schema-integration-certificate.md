Status: open
Created: 2026-08-06
Updated: 2026-08-06
Parent: `.10x/tickets/2026-08-06-state-backed-schema-authority-program.md`
Depends-On: `.10x/tickets/done/2026-08-06-s1-state-schema-authority-foundation.md`, `.10x/tickets/done/2026-08-06-s2-state-backed-preparation-portable-plan.md`, `.10x/tickets/done/2026-08-06-s3-schema-drift-dispositions.md`, `.10x/tickets/done/2026-08-06-s4-state-backed-promotion-settlement.md`, `.10x/tickets/2026-08-06-s5-delete-lockfile-product-surface.md`

# S6 state-schema integration certificate

## Scope

Freeze and certify the complete state-backed cutover after S1–S5:

- reconcile child statuses, active specs/decisions, generated artifacts, docs, examples, VISION,
  system SQL, and current command grammar;
- run one broad workspace behavior/quality barrier proportional to the final change range;
- build the bundled release binary without developer-only DuckDB linkage;
- exercise disposable copies of the supplied sandbox through first use, portable planning,
  active-schema drift, variant, quarantine, fail, promotion, exact-package/recovery, state history,
  and scoped doctor;
- perform one fresh final review under the then-current coordination policy, focusing on authority,
  fencing, negative effects, redaction/report parity, and current-only deletion;
- close every child and the parent only when evidence maps every criterion.

## Non-goals

- new functionality beyond a concrete integration defect;
- Postgres/distributed testing, schema export/import, nested/future-only promotion;
- repeated broad suites during repair; use targeted checks after the one broad barrier exposes a
  concrete owner;
- preserving a lockfile to ease sandbox migration.

## Acceptance criteria

1. Workspace behavior suite and doc tests pass, with declared skips/limits recorded honestly.
2. Formatting, all-target/all-feature check, strict Clippy, generated-reference checks, scoped
   duplication, dependency/module cleanup, product smoke matrix, and diff checks pass.
3. Release first-use journey proves validate offline, plan no-write plus `--out`, run-from-plan exact
   batch establishment, schema show from state, and no lockfile.
4. Variant journey proves new safe fields create accepted residuals, no migration, unchanged head,
   and exact output/counts.
5. Quarantine/fail journeys prove durable quarantine settlement versus pre-mutation failure and
   checkpoint semantics.
6. Promotion journey proves dry plan, complete top-level historical correction, settlement fence,
   exact one-generation advancement, and subsequent use of the new version.
7. Portable/preparation adversarial cases prove relevant-state invalidation, unrelated-state
   tolerance, selector isolation, and all-selected preflight.
8. Package/recovery/doctor journeys remain source/compiler-isolated where their named authority is
   sufficient.
9. Final current-only sweep finds no shipped lockfile/evolve/freeze/global-quarantine/removed-command
   surface or compatibility machinery.
10. Final review has no unresolved critical/significant authority, correctness, security, or data
    loss finding; residual risks are explicit.

## References

- `.10x/tickets/2026-08-06-state-backed-schema-authority-program.md`
- `.10x/decisions/state-backed-schema-authority.md`
- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/portable-plan-artifact.md`
- `.10x/specs/schema-drift-dispositions.md`
- `.10x/specs/schema-promotion-corrections.md`
- `.10x/knowledge/cli-report-authority.md`
- `.10x/knowledge/error-ownership-taxonomy.md`
- `QUALITY.md`

## Assumptions

- User-ratified: release binary and supplied sandbox are meaningful final integration authority.
- Record-backed: broader verification runs once at this final integration boundary; repairs use
  focused checks and do not spiral into repeated reassurance suites.
- Record-backed: current-only closure deletes rather than tests compatibility.

## Journal

- 2026-08-06: Opened dependency-gated behind S1–S5; no certificate work starts during S0.

## Blockers

S1–S5 must close with complete evidence and no live lock authority.

## Evidence

Pending execution.

## Review

Pending final review.

## Retrospective

Pending execution.
