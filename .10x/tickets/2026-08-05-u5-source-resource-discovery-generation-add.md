Status: active
Created: 2026-08-05
Updated: 2026-08-05
Parent: `.10x/tickets/2026-08-04-resource-first-cli-experience-program.md`
Depends-On: `.10x/tickets/done/2026-08-05-u4-portable-plan-export-consumption.md`

# U5 source/resource discovery, generation, and add

## Scope

Implement the two ratified discovery identity spaces: adapter-owned catalog discovery for one
configured source and bounded schema discovery for selected authored resources. Add canonical
optional discovery artifacts, explicit create-or-verify thin-resource generation with namespace
override and partial conflict reporting, and reconcile `cdf add` to the same thin current-only
authoring model and `plan` next action.

## Non-goals

- schema pinning, compilation, lock/index publication, destination contact, or execution;
- generic inference of database/collection/object identity outside source adapters;
- overwrite/force behavior or compatibility aliases;
- doctor/package/resume/recovery work owned by U6.

## Acceptance Criteria

1. CLI grammar/help exposes `discover source` and `discover resource`, selectors bind before I/O,
   `--namespace` requires `--generate`, and removed schema-discover grammar remains absent.
2. Source adapters expose bounded deterministic catalog candidates with canonical relation id,
   safe label, resource-token proposal, complete upstream options, schema summary when available,
   and generation/truncation evidence; unsupported drivers fail precisely.
3. Resource discovery invokes only selected source drivers, observes schema into temporary
   authority, reports coverage/schema/generation, and writes no project/runtime/destination state.
4. `--out` adds one canonical bounded content-hashed artifact without replacing terminal output or
   overwriting nonidentical bytes.
5. `--generate` publishes nonconflicting thin `SELECT * FROM upstream(...)` resources through
   guarded create-or-exact transactions, preserves configured-source identity under namespace
   override, retains partial successes, and reports created/unchanged/conflicted outcomes.
6. `cdf add` writes the same thin query shape, supports explicit configured-source identity, loads
   only the selected proposal boundary, and points to `cdf plan <resource>` without lock/compile.
7. Focused CLI/report/redaction/negative-write/generation-conflict tests plus affected check,
   formatter, strict Clippy, and diff check pass. Broader suite/review remains U7 by user direction.

## References

- `.10x/specs/source-discovery-resource-generation.md`
- `.10x/specs/cli-command-intent-and-effects.md`
- `.10x/specs/resource-selector-batch-commands.md`
- `.10x/knowledge/cli-report-authority.md`
- `.10x/knowledge/error-ownership-taxonomy.md`
- `.10x/knowledge/project-file-publication-recovery.md`
- `.agents/skills/audit-cli-report-authority/SKILL.md`
- `.agents/skills/audit-error-ownership/SKILL.md`
- `.agents/skills/audit-project-file-publication/SKILL.md`

## Assumptions

- User-ratified: source/resource discovery scopes, read-only default, explicit generation,
  namespace override, partial generation success, thin generated files, and ergonomic add.
- Record-backed: configured source, upstream relation, and path-derived resource identities remain
  distinct; source adapters own catalog identity/options while generic code owns selection,
  bounds, publication, redaction, and reports.

## Journal

- 2026-08-05: Activated after U4 completed and pushed. Implementation will extend the existing
  source registry rather than add generic source-kind switches, and will reuse guarded project
  publication plus existing in-memory schema discovery.

## Blockers

None.

## Evidence

Pending execution.

## Review

Deferred by explicit user direction to the single final U7 tranche review.

## Retrospective

Pending execution.
