Status: open
Created: 2026-07-31
Updated: 2026-07-31
Parent: `.10x/tickets/2026-07-31-connector-mode-readiness-program.md`
Depends-On: `.10x/tickets/2026-07-31-cmr2-model-based-core-falsifiers.md`

# Add connector certification and a core-change budget

## Scope

Provide one repository-owned connector certification entry point that selects the existing source
or destination catalog laws and enforces an explicit connector-only changed-file budget. A
connector that needs generic-core edits must acknowledge core impact and receive the broader
quality profile rather than silently normalizing the change.

## Non-goals

- No connector scaffolding framework, dynamic plugin ABI, SDK mega-trait, or generated adapter
  implementation.
- No new connector and no live cloud credential requirement.
- No prohibition on justified core repairs; the budget makes them explicit and test-backed.

## Acceptance criteria

- One documented command accepts a source or destination connector identity and produces a
  machine-readable certification report.
- Certification covers catalog enrollment, capability truthfulness, applicable conformance/product
  laws, replay/duplicate behavior, and required static extension-boundary checks.
- The changed-file classifier accepts the documented connector leaf/catalog/fixture/manifest/docs
  surface and fails precisely when generic core ownership is touched without explicit core-impact
  acknowledgement.
- Core-impact acknowledgement activates the broader verification profile and is visible in the
  report; it is not a bypass.
- Nebula and Quasar fixtures prove both certification directions without production connector
  changes.
- Documentation gives a cold author the shortest correct path and explicitly forbids copying
  package, receipt, checkpoint, concurrency, or retry lifecycles into an adapter.

## References

- `.10x/tickets/2026-07-31-connector-mode-readiness-program.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/destination-extension-runtime-contract.md`
- `.10x/decisions/builtin-driver-catalog-composition.md`
- `QUALITY.md`

## Assumptions

- User-ratified: ordinary connector work should be able to proceed without generic-core edits;
  real core gaps remain repairable through an explicit, evidence-bearing path.
- Record-backed: Nebula and Quasar are synthetic authoring proofs and remain test-only; they are
  suitable certification fixtures but not production connectors.
- Mechanical: the entry point may be a repository script or Rust binary, whichever reuses current
  test/catalog authority with less new machinery.

## Journal

- 2026-07-31: Shaped after the quality and model workstreams so certification can consume a
  reliable bounded gate rather than introduce a second test authority.

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending program review.

## Retrospective

Pending execution.
