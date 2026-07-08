Status: open
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
