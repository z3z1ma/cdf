Status: open
Created: 2026-08-03
Updated: 2026-08-03
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/2026-08-03-d1-project-compilation-manifest-core.md`

# D1 compile CLI and manifest SQL

## Scope

Add `cdf compile`/`cdf compile --refresh`, minimal stable project/environment location loading,
manifest verification, and the seven ratified read-only manifest tables in the existing `cdf sql`
SQLite catalog. Update scaffold/docs/generated CLI artifacts and focused command reports.

## Non-goals

- replacing SQLite with DataFusion;
- SQL resource authoring or native scalar IR;
- implicit compile/refresh/recovery from `cdf sql` or another read-only command;
- destination/state/package/checkpoint mutation;
- manifest history or remote serving.

## Acceptance criteria

1. CLI grammar, help, generated inventory, JSON reports, and human reports expose only `cdf compile`
   and `cdf compile --refresh` with clear offline/external-observation behavior.
2. Offline compile performs no network contact and publishes only a lock-matching manifest;
   missing/stale locked authority fails with exact `--refresh` remediation.
3. Refresh contacts only source-side read authorities required to update observations and publishes
   manifest plus changed lock atomically with `cdf.lock` last. It never mutates a destination,
   execution state, package, receipt, or checkpoint.
4. `cdf sql` loads project location/environment and a verified matching manifest without compiling
   declarative/SQL resource files or constructing source/destination registries.
5. `manifest_project`, `manifest_inputs`, `manifest_resources`, `manifest_fields`,
   `manifest_semantics`, `manifest_lineage`, and `manifest_diagnostics` expose stable columns and
   canonical JSON for nested facts.
6. Missing, stale, tampered, wrong-environment, or pending-publication manifests fail read-only with
   exact remediation and no filesystem change.
7. Existing checkpoint/package SQL tables and read-only/mutating-keyword/file-shape protections
   remain intact.
8. Scaffold ignores `.cdf/` while continuing to commit `cdf.lock`; examples/docs reflect current
   behavior.
9. Focused CLI/project tests prove no recompile/source contact, deterministic query rows, JSON/human
   parity, publication recovery separation, and error ownership.
10. Formatting, generated-artifact checks, `git diff --check`, focused tests/checks, and targeted
    strict Clippy pass without a whole-workspace suite.

## References

- `.10x/specs/project-compilation-manifest.md`
- `.10x/decisions/project-manifest-path-compile-and-query-policy.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/skills/audit-project-file-publication/SKILL.md`
- `.10x/skills/audit-cli-report-authority/SKILL.md`

## Assumptions

- Command spelling, manifest path, table names, read-only behavior, publication policy, and latest-
  generation retention are user-ratified.

## Journal

- 2026-08-03: Opened after D1 ratification. No product code changed in this shaping turn.

## Blockers

None after the manifest-core dependency closes.

## Evidence

Pending execution.

## Review

Pending one independent D1 lane-boundary red-team review.

## Retrospective

Pending execution.
