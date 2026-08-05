Status: open
Created: 2026-08-04
Updated: 2026-08-04
Parent: `.10x/tickets/2026-08-04-resource-first-cli-experience-program.md`
Depends-On: `.10x/tickets/2026-08-04-u0-manifest-text-diagnostic-ownership.md`

# U1 resource selectors and static validate

## Scope

Implement the first reusable resource-first boundary and cut validate down to its ratified intent:

- exact/glob positive selectors, repeated exclusions, canonical union/sort/dedup, per-positive
  match errors, and explicit all behavior;
- path-shape-only glob expansion and exact direct resolution before parsing or driver work;
- `cdf validate [selector...]` using pure project/config/path/SQL/reference/driver-schema checks and
  local generated-authority integrity/status only;
- deterministic aggregate typed report and existing renderer vocabulary for counts, ordered
  per-resource diagnostics, checked/skipped effect facts, and JSON/human parity;
- removal of `validate --deep` and every validate-time secret/environment/source/destination/state
  access path, help entry, generated artifact, test, and fixture.

## Non-goals

- applying selectors to plan/run/compile/discover/preview/backfill in this child;
- resource-sharded lock or manifest publication;
- doctor implementation or moving deep observations beyond ensuring validate no longer owns them;
- source discovery, schema commitment/promotion, destination compatibility, or any write;
- compatibility grammar for `validate --deep`.

## Acceptance criteria

1. Exact, overlapping glob, repeated exclusion, duplicate, typo/miss, empty-final-set, quoted-help,
   and canonical-order behaviors match the active selector spec.
2. Exact resolution does not inventory unselected resource files; glob expansion reads only path
   shape/token identity before selection. Unselected malformed SQL/config/credentials never block
   selected validation.
3. Validate performs zero secret-provider calls, environment-variable/file-secret reads, source
   data enumeration/stat, driver discovery/health, destination/state opens, network calls, or
   writes, proven with counters/fault sentinels rather than claims.
4. Static checks cover root/config grammar, resource placement/tokens/UTF-8/bounds, SQL
   syntax/envelope, configured-source and semantic references, pure closed driver option schemas,
   secret-reference syntax, and integrity/status of locally present generated authority.
5. Missing lock/compiled authority is reported as missing status but does not fail an otherwise
   valid first-use project. Static invalidity exits nonzero after attempting all selected resources.
6. Human and JSON output come from one typed aggregate report and agree on selector resolution,
   environment/source/resource/valid/warning/error/current/stale/missing counts, ordered
   diagnostics, and skipped operational checks.
7. CLI help/completions/man/reference artifacts contain selectors and no `validate --deep`; no
   alias, fallback parser, retired deep fixture, or rejection-only compatibility test remains.
8. Focused behavioral tests, affected-package checks, strict affected Clippy, explicit cognitive-
   complexity diagnostic, formatting, generated-artifact checks, and `git diff --check` pass.

## References

- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/resource-selector-batch-commands.md`
- `.10x/decisions/static-validation-operational-readiness-boundary.md`
- `.10x/specs/project-cli-observability-security.md` (superseded only where the decision states)
- `.10x/specs/data-onramp-source-experience-cli.md` (superseded only where the decision states)
- `.10x/knowledge/cli-report-authority.md`
- `.agents/skills/audit-cli-report-authority/SKILL.md`

## Assumptions

- User-ratified: selector grammar/batch law and strictly static/offline validate behavior.
- Record-backed: root TOML syntax and path/config identity must be known before resource selection;
  pure driver JSON-schema admission is not external observation.
- Record-backed: missing generated authority is a normal first-use state because plan/run own
  preparation in the active model.

## Journal

- 2026-08-04: Opened after user confirmation. Current source inspection proves validate loads a
  whole `ProjectContext`, resolves secrets, and uses `--deep` to reach source and destination
  authorities. The child intentionally establishes one shared selector seam but wires it only to
  validate so later commands reuse evidence-backed behavior rather than a speculative framework.

## Blockers

None after U0 closes.

## Evidence

Pending execution.

## Review

Pending the U1+U2 authority-foundation review barrier.

## Retrospective

Pending execution.
