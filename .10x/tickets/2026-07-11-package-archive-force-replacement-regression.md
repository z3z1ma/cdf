Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/specs/package-lifecycle-determinism.md

# Package archive force-replacement regression

## Scope

Restore the ratified `force` archive behavior when persisted Parquet archive sidecars are tampered or missing. Determine why the force path verifies and returns the stale archive failure instead of replacing the archive, fix the owning archive layer, and preserve strict failure without force.

## Acceptance criteria

- `persisted_archive_default_fails_on_tamper_and_force_replaces` passes.
- `force_archive_reports_replaced_when_manifest_metadata_survives_missing_tree` passes.
- Default mode still rejects tampered or incomplete archive sidecars.
- Force mode replaces atomically and the rebuilt archive verifies.
- The fix does not weaken canonical package identity or archive verification.

## Evidence expectations

Focused reproductions, package archive suite, diff inspection, and adversarial review of default-versus-force failure routing.

## Explicit exclusions

No archive format expansion, compatibility shim, or change to canonical package identity.

## Blockers

None. The failure reproduces independently in the current tree.

## References

- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/tickets/done/2026-07-06-package-archive-contract-ratification.md`

## Progress and notes

- 2026-07-11: Discovered while verifying the unrelated FX1 payload-retention refactor. Both tests fail independently; the error is stale archive verification surfacing from a force invocation. No FX1 retention code participates in archive replacement selection.
