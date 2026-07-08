Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md
Depends-On: .10x/specs/versioning-lts-release-policy.md

# P1 product WS8C: Changelog and shell installer channel

## Scope

Add `CHANGELOG.md` under the ratified convention and implement the first install channel as a checksum-verifying shell installer.

## Acceptance criteria

- `CHANGELOG.md` has an `Unreleased` section and dated version section format matching `.10x/specs/versioning-lts-release-policy.md`.
- The installer downloads or consumes a release artifact, verifies SHA-256 checksums before mutation, supports dry-run or equivalent inspection, installs into a user-selected prefix, and prints the installed version.
- Unsupported OS/architecture, missing checksum, failed download, and checksum mismatch fail before writing the target binary.
- Installer smoke tests cover success, dry-run, and checksum mismatch using local fixtures or a documented local equivalent.
- Follow-up channels such as brew, package-manager feeds, signed updates, and cargo-install are ticketed or explicitly recorded as deferred.

## Evidence expectations

Changelog diff, installer tests/smoke output, checksum mismatch proof, and review of privilege/escalation behavior.

## Explicit exclusions

No release workflow implementation. No signing infrastructure. No brew tap. No crates.io publication.

## Blockers

None.

## Progress and notes

- 2026-07-08: Activated for implementation. Read the owning ticket, parent WS8 ticket, active versioning/LTS release policy, `QUALITY.md`, existing GitHub Actions workflows, `tools/codeql-rust-quality.sh`, `docs/operators/release-install.md`, existing `tools/` layout, and WS8A evidence/review before editing.
- 2026-07-08: Added `CHANGELOG.md` with `Unreleased` and `[0.1.0] - 2026-07-08` sections under the active changelog convention.
- 2026-07-08: Added `tools/install-cdf.sh`, a checksum-verifying shell installer for `cdf-<version>-<target>.tar.gz` artifacts with adjacent `.sha256` files. The installer supports dry-run, user-selected prefixes, local-path or URL artifacts/checksums, Darwin/Linux target detection, unsupported-target rejection, and no privilege escalation.
- 2026-07-08: Added `tools/test-install-cdf.sh`, a local fixture smoke harness covering successful install, dry-run, default version URL resolution, checksum mismatch, missing checksum, failed artifact download, and unsupported target.
- 2026-07-08: Explicitly deferred brew, cargo-install, OS package-manager feeds, signed updates, and auto-update channels to future scoped tickets or decisions. WS8C implements only the shell installer.
- 2026-07-08: Parent review verified the GitHub release URL against the configured `origin` remote and replaced the artifact binary lookup with `find ... -print -quit` to avoid a shell `pipefail` edge.
- 2026-07-08: Recorded evidence in `.10x/evidence/2026-07-08-p1-product-ws8c-changelog-installer-channel.md` and closure review in `.10x/reviews/2026-07-08-p1-product-ws8c-changelog-installer-channel-review.md`.
