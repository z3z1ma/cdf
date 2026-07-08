Status: open
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

Generated completions and man pages depend on `.10x/tickets/2026-07-08-p1-product-ws2d-completions-manpages-help.md`; the release workflow may package them conditionally until WS2D closes.
