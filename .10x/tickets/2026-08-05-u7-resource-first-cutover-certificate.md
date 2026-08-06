Status: active
Created: 2026-08-05
Updated: 2026-08-06
Parent: `.10x/tickets/2026-08-04-resource-first-cli-experience-program.md`
Depends-On: `.10x/tickets/done/2026-08-06-u6b-default-live-telemetry.md`

# U7 resource-first cutover certificate

## Scope

Freeze and certify the complete resource-first CLI tranche. Remove the remaining superseded
product language, fixtures, helper names, generated references, and dead command implementation;
reconcile completed child records; run one broad workspace behavioral suite and the repository
quality gates; exercise the supplied sandbox across static, discovery, planning, portable-plan,
resource-run, exact-package, interrupted-run, and scoped-doctor authority; then perform one fresh
adversarial review of the complete U0-U6b change range.

The user explicitly prohibited subagents for this execution. The primary agent therefore performs
the final red-team pass with a fresh diff/spec reading after implementation and testing, and records
findings before judging closure. The review range includes U6b.

## Non-goals

- new CLI behavior, compatibility aliases, migration readers, or artifact formats;
- broad refactors, performance tuning, or cleanup unrelated to a concrete cutover finding;
- repeating focused tests already evidenced by children unless the broad suite or review exposes a
  specific failure;
- changing the other foundation program's orthogonal source/runtime work.

## Acceptance Criteria

1. Shipped CLI grammar, help, completions, manuals, operator docs, `VISION.md`, conformance
   transcripts, and current tests contain no top-level replay/resume, compile-refresh,
   schema-pin/discover, validate-deep, or no-pin surface. Internal replay/recovery terminology
   remains only where it names durable package/idempotency mechanics rather than a removed command.
2. Generated CLI artifacts and docs are fresh; current help clearly exposes selectors, `plan
   --out`, `run --plan`, `run --package`, `run --resume`, discovery scopes, and doctor scopes.
3. One workspace-level behavioral suite passes with the required developer DuckDB linkage. Doc
   tests run separately when the selected suite does not cover them. Failures are fixed only when
   owned by this tranche; orthogonal failures are evidenced precisely rather than hidden.
4. Formatter, workspace all-target/all-feature check, strict workspace Clippy, generated-reference
   checks, scoped duplication analysis, dependency/module cleanup check, and diff check pass.
   Cognitive-complexity findings are reviewed against changed production functions.
5. The release binary proves the supplied sandbox journeys: static validate; selected discovery;
   unchanged create-or-verify add/generation; selected plan plus `--out`; `run --plan`; selected
   resource run; exact-package run without source/compiler work; bare interrupted-run no-work; and
   explicitly scoped doctor. Writes are limited to the command's reported effect ceiling and test
   artifacts use explicit disposable paths where practical.
6. A fresh red-team review attempts to falsify selector isolation, all-selected preflight,
   lock/schema fencing, portable-plan no-repair validation, package/recovery authority isolation,
   static/operational boundaries, report redaction/effects parity, progress liveness, and current-
   only deletion. No critical or significant finding remains unresolved.
7. Child statuses, paths, references, evidence, and the parent record agree; the parent closes only
   after every criterion above is mapped to evidence and the retrospective is distilled.

## References

- `.10x/tickets/2026-08-04-resource-first-cli-experience-program.md`
- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/resource-selector-batch-commands.md`
- `.10x/specs/portable-plan-artifact.md`
- `.10x/specs/source-discovery-resource-generation.md`
- `.10x/specs/cli-command-intent-and-effects.md`
- `.10x/decisions/static-validation-operational-readiness-boundary.md`
- `.10x/knowledge/cli-report-authority.md`
- `.10x/knowledge/error-ownership-taxonomy.md`
- `QUALITY.md`

## Assumptions

- User-ratified: one broad suite and quality pass plus one final red-team review are sufficient for
  this tranche; intermediate child evidence is not repeated for reassurance.
- User-ratified: the primary agent implements and reviews without subagents.
- Record-backed: U0-U6 implementation is on `main` through `b486c1d0`; the cutover review range
  begins with U0 implementation commit `6a7bc875`.
- User-ratified: `/Users/alexanderbut/code_projects/cdf_sandbox` is available for bounded
  integration testing, and release mode is the meaningful runtime certificate.

## Journal

- 2026-08-05: Shaped after U6 passed its release FineWeb execution certificate and was pushed.
  Initial sweep found stale removed-command examples in `VISION.md` and the conformance acceptance
  transcript, plus terminal U1/U2 records still stored under the active ticket directory.
- 2026-08-06: Re-gated behind U6b after the user required useful default phase telemetry before the
  final cutover certificate.
- 2026-08-06: U6b closed after focused quality checks and a bundled-release FineWeb certificate;
  U7 is now unblocked. Its broad suite and final review remain intentionally deferred to this
  ticket.
- 2026-08-06: Activated for the current-only sweep, one broad behavioral/quality certificate,
  release sandbox journeys, and the single final review. Unrelated untracked evidence, personal
  Codex configuration, and distribution archives remain protected outside the ticket diff.
- 2026-08-06: Current-only sweep removed the last advertised/invoked top-level resume/replay
  surfaces from VISION, the MVP acceptance demo, and the MongoDB run-matrix public CLI cell. The
  demo now consumes the multi-resource plan envelope and passes its focused end-to-end test.
  Generated CLI snapshots and command/error docs are fresh. Terminal U1/U2 records and their
  dependency references now live under `tickets/done/`.
- 2026-08-06: Help audit found contextual description leakage from the generic command-name table:
  `inspect run` described execution, while doctor scopes used unrelated or generic descriptions.
  Added scope-specific inspect/doctor help and regenerated help, manpage, completion, and command
  reference artifacts.

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending final fresh red-team pass.

## Retrospective

Pending execution.
