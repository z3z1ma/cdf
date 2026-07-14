Status: done
Created: 2026-07-07
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-05-cli-surface.md
Depends-On: .10x/specs/project-cli-observability-security.md, .10x/specs/checkpoint-state-commit-gate.md, .10x/specs/conformance-governance-roadmap.md

# Implement state migrate/recover commands

## Scope

Implement the operational `cdf state migrate` and `cdf state recover` surfaces.

Owns:

- `crates/cdf-cli/src/state_command.rs` and focused CLI tests.
- `cdf-state-sqlite` migration runner/reporting APIs and committed migration fixtures where missing.
- Destination mirror recovery APIs required for `state recover` without bypassing `CheckpointStore::commit`.

## Acceptance criteria

- `cdf state migrate` reports current schema version, applies required local SQLite state migrations, and is idempotent when already current.
- Migration behavior is backed by committed fixtures for prior supported schema versions.
- `cdf state recover` can recover state from verified destination/package facts according to active specs, with explicit failure when evidence is missing or inconsistent.
- Neither command advances a checkpoint head without a verified receipt and `CheckpointStore::commit`.
- JSON output is stable for automation and includes migration/recovery decisions.

## Evidence expectations

Run focused CLI state migrate/recover tests, state-store migration fixture tests, recovery inconsistency tests, checkpoint-store conformance tests if touched, fmt/clippy/check/diff checks, and appropriate security/complexity scans.

## Explicit exclusions

No remote state store, no distributed leases, no destructive state deletion, no destination introspection beyond ratified mirror/receipt verification, and no migration of package manifests unless split separately.

## Blockers

None. If a migration fixture format or recovery precedence rule is missing, self-ratify it before source edits.

## Progress and notes

- 2026-07-07: Split from `.10x/tickets/done/2026-07-07-cli-remaining-command-planners.md`. Current `state show/history/rewind` work; `state migrate` and `state recover` return not-supported.
- 2026-07-08: Implemented SQLite state migration reporting API and CLI wiring, plus package-receipt `state recover` through the package replay destination resolver and lower `recover_package_from_artifacts` path. Added focused migration/recovery tests; ticket intentionally remains open pending requested external evidence/review closure work.
- 2026-07-08: Closed after recording API-shape decision `.10x/decisions/superseded/state-migrate-recover-package-receipt.md`, evidence `.10x/evidence/2026-07-08-cli-state-migrate-recover.md`, and review `.10x/reviews/2026-07-08-cli-state-migrate-recover-review.md`. `cdf state migrate` then reported component schema versions and idempotent actions; `cdf state recover` performed verified package-receipt recovery without destination row writes and without bypassing `CheckpointStore::commit`. The migration half was later superseded by the pre-production current-format-only policy; this terminal ticket remains historical evidence of its 2026-07-08 closure.

## References

- `.10x/decisions/superseded/state-migrate-recover-package-receipt.md`
- `.10x/evidence/2026-07-08-cli-state-migrate-recover.md`
- `.10x/reviews/2026-07-08-cli-state-migrate-recover-review.md`
