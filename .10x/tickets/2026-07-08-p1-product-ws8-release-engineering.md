Status: open
Created: 2026-07-08
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p1-product-experience-program.md
Depends-On: QUALITY.md, .10x/specs/conformance-governance-roadmap.md, .10x/knowledge/datafusion-cratesio-arrow59-tripwire.md

# P1 product WS8: Release engineering and distribution

## Scope

Add the production pipeline: CI workflows, release workflow, changelog, versioning/LTS policy, reproducible checksummed binaries, install channel, and generated completions/man pages in artifacts.

## Required outcomes

- CI has fast gates per push and scheduled/manual slow gates that follow the relevant `QUALITY.md` Deep Loop.
- Release workflow produces reproducible checksummed binaries for mainstream targets.
- `CHANGELOG.md` follows a ratified convention.
- Versioning/LTS policy covers artifact-spec versions, migration fixtures, dependency tuple cadence, support windows, and the crates.io publication constraint caused by the temporary DataFusion git pin.
- At least one install channel is scripted and smoke-tested; other channels are ticketed.
- Completions and man pages ship as release artifacts.

## Acceptance criteria

- Green pipeline runs are recorded as evidence.
- A versioned pre-release is cut end to end.
- Installer smoke test passes on a clean target or documented local equivalent.
- The LTS/versioning spec is active and referenced by release jobs.
- Supply-chain gates from `QUALITY.md` are wired into the appropriate fast/slow phases without recreating reusable CodeQL databases unnecessarily.

## Evidence expectations

Record CI run URLs or local workflow output, release artifact checksums, installer smoke output, generated artifact proof, LTS spec review, and supply-chain gate output.

## Explicit exclusions

No claim of crates.io publication while the DataFusion git pin remains. No unsupported target promises. No manual-only release steps unless explicitly recorded as temporary blockers.

## Progress and notes

- 2026-07-08: Opened from P1 product directive. This lane may begin immediately and must respect the existing reusable CodeQL database policy.
- 2026-07-08: Ratified `.10x/specs/versioning-lts-release-policy.md` from P1 plus existing governance records. Split execution into `.10x/tickets/done/2026-07-08-p1-product-ws8a-ci-quality-workflows.md`, `.10x/tickets/2026-07-08-p1-product-ws8b-release-artifact-workflow.md`, and `.10x/tickets/done/2026-07-08-p1-product-ws8c-changelog-installer-channel.md`.
- 2026-07-08: WS8B release artifact workflow closed at `.10x/tickets/done/2026-07-08-p1-product-ws8b-release-artifact-workflow.md`. It added the GitHub Actions release workflow, fail-closed metadata/artifact packaging scripts, local host artifact smoke evidence, and checksum verification. Parent remains open for actual hosted release-run evidence and generated completions/man pages once WS2D closes.
- 2026-07-08: WS8B reproducibility blocker repaired. Packaging now uses deterministic tar/gzip writing through `tools/write-reproducible-targz.py`, and WS8B evidence includes a two-package identical-input SHA-256 and byte-identity smoke proof.

## Blockers

None for shaping. Actual crates.io publication remains blocked until the DataFusion Arrow 59 tuple is available on crates.io and the git pin is removed.
