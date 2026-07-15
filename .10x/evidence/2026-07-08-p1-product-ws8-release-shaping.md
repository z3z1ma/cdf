Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md, .10x/specs/versioning-lts-release-policy.md

# P1 WS8 release engineering shaping evidence

## What was observed

The P1 directive requires release engineering and distribution work, including CI workflows, release artifacts, changelog convention, versioning/LTS policy, install channel, generated completions, generated man pages, and the DataFusion git-pin publication constraint.

Existing authority already covered several required semantics:

- `.10x/specs/conformance-governance-roadmap.md` requires semver, serialized artifact migrations, dependency tuple cadence, and no crates.io publication while DataFusion is a git dependency.
- `.10x/knowledge/datafusion-cratesio-arrow59-tripwire.md` defines the trigger and migration shape for removing the temporary DataFusion git pin.
- `.10x/knowledge/quality-gate-execution.md` requires parallel quality checks and reusable CodeQL Rust databases.

## Procedure

Inspected:

- P1 directive attachment and objective file content.
- `.10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md`.
- `.10x/specs/conformance-governance-roadmap.md`.
- `.10x/knowledge/datafusion-cratesio-arrow59-tripwire.md`.
- `.10x/knowledge/quality-gate-execution.md`.
- `.10x/knowledge/vision-coverage-matrix.md` P1 row.

Created:

- `.10x/specs/versioning-lts-release-policy.md`.
- `.10x/tickets/done/2026-07-08-p1-product-ws8a-ci-quality-workflows.md`.
- `.10x/tickets/done/2026-07-08-p1-product-ws8b-release-artifact-workflow.md`.
- `.10x/tickets/done/2026-07-08-p1-product-ws8c-changelog-installer-channel.md`.

Updated:

- `.10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md`.

Checks:

- `git diff --check -- .10x/specs/versioning-lts-release-policy.md .10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md .10x/tickets/done/2026-07-08-p1-product-ws8a-ci-quality-workflows.md .10x/tickets/done/2026-07-08-p1-product-ws8b-release-artifact-workflow.md .10x/tickets/done/2026-07-08-p1-product-ws8c-changelog-installer-channel.md` passed.
- `rg -n "[Kk]iller[ _-]?[Dd]emo" . --hidden` found no matches.

## What this supports or challenges

This supports that WS8 is no longer a broad unsplit parent and that release work has a focused governing spec plus bounded executable children.

It also supports that crates.io publication remains explicitly blocked while the DataFusion git pin remains active.

## Limits

No CI workflow, release workflow, changelog, or installer implementation was performed in this shaping slice. The new WS8 children remain open for subagent execution.
