Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md
Depends-On: .10x/specs/versioning-lts-release-policy.md

# P1 product WS8B: Release artifact workflow

## Scope

Add a release workflow that builds reproducible, checksummed `cdf` binary artifacts for the mainstream targets in `.10x/specs/versioning-lts-release-policy.md`.

## Acceptance criteria

- Workflow can be run for a versioned pre-release without crates.io publication.
- Artifact archives include the `cdf` binary, license, changelog excerpt, SHA-256 checksums, and generated completions/man pages when WS2D has supplied the generator.
- Unsupported target gaps are recorded with evidence rather than silently skipped.
- Release workflow fails closed if checksums are missing or if release metadata is inconsistent with the versioning policy.
- Publication to crates.io remains disabled while the DataFusion git pin remains active.

## Evidence expectations

Workflow file, local build/package smoke where possible, checksum output, and a pre-release dry-run or documented local equivalent.

## Explicit exclusions

No shell installer implementation. No brew tap. No signing key infrastructure unless a later child ticket scopes it. No crates.io publication.

## Blockers

None for WS8B implementation. Generated completions and man pages still depend on `.10x/tickets/2026-07-08-p1-product-ws2d-completions-manpages-help.md`; the release package records that dependency and includes those artifacts when the generator supplies them.

## Progress and notes

- 2026-07-08: Activated for implementation. Read `VISION.md`, `QUALITY.md`, this ticket, `.10x/specs/versioning-lts-release-policy.md`, WS8A and WS8C done tickets, `.10x/knowledge/datafusion-cratesio-arrow59-tripwire.md`, and related WS8 evidence/reviews before editing.
- 2026-07-08: Added `.github/workflows/release-artifacts.yml`. It validates release metadata, builds `cdf` binary artifacts for the five mainstream targets, verifies per-target archives and checksums, emits an aggregate `SHA256SUMS`, uploads workflow artifacts, and can upload a GitHub prerelease without any crates.io publication step.
- 2026-07-08: Added `tools/verify-release-metadata.sh`, `tools/package-release-artifact.sh`, `tools/verify-release-artifacts.sh`, and `tools/test-release-artifacts.sh`. The scripts enforce workspace/changelog/license consistency, DataFusion-git publication disablement, archive contents, and checksum verification.
- 2026-07-08: Added the root `LICENSE` file so release artifacts can include the Apache-2.0 license text instead of relying only on Cargo metadata.
- 2026-07-08: Release packaging handles generated completions and man pages conditionally. When `target/generated/completions` or `target/generated/man` exists, those directories are included; otherwise `generated/ARTIFACTS.txt` records the WS2D dependency.
- 2026-07-08: Local host build/package smoke produced and verified `target/quality/ws8b-local-dist/cdf-0.1.0-aarch64-apple-darwin.tar.gz` with adjacent `.sha256`, then installed it through `tools/install-cdf.sh`.
- 2026-07-08: Recorded evidence in `.10x/evidence/2026-07-08-p1-product-ws8b-release-artifact-workflow.md` and review in `.10x/reviews/2026-07-08-p1-product-ws8b-release-artifact-workflow-review.md`.
- 2026-07-08: Addressed blocking review finding that the first packager used ordinary tar/gzip metadata and therefore produced checksummed but not reproducible archives. Added `tools/write-reproducible-targz.py`, switched packaging to deterministic tar/gzip metadata, and added smoke coverage proving two packages from identical staged inputs produce identical archive bytes and SHA-256 values.
