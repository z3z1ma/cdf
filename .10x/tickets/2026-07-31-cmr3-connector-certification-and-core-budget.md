Status: active
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
- 2026-07-31: Activated after the model falsifiers reached a pushed, focused green checkpoint.
  The selected implementation is a standard-library Python entry point around existing Cargo
  laws. The only conformance addition is a destination-oriented selector over the same registered
  source-by-destination matrix cells already used by scheduled source shards.
- 2026-07-31: Implemented a Git merge-base classifier with no caller-supplied changed-file
  override. Connector leaf, catalog, matching fixture, manifest, docs, ticket, and evidence paths
  remain in the connector-only profile; every other file is listed as generic core. Explicit
  `--core-impact` retains the connector checks and adds the engine/runtime/project/CLI regression
  suite plus strict workspace all-feature Clippy. Child logs stay on stderr while stdout and the
  optional report path receive one versioned JSON result.

## Blockers

None.

## Evidence

- Seven standard-library unit laws passed for source and destination allowed surfaces, generic
  core rejection, underscore-to-crate-name mapping, direction-specific check selection, and the
  non-bypass core-impact profile.
- The registered source-shard/catalog coverage law passed after adding the destination selector,
  proving the existing scheduled source matrix remained unchanged.
- The selected Quasar destination matrix completed all 15 catalog-derived cells in 10.30 seconds:
  append executed and passed for file, Python, REST, SQL, and Nebula; replace and merge were
  explicitly excluded for each source by Quasar's declared `supported_dispositions=[Append]`.
  Every executed cell asserted plan honesty, package verification, durable receipt verification,
  checkpoint-after-receipt gating, artifact replay identity, and no-op duplicate behavior.
- Strict all-target/all-feature `cdf-conformance` Clippy, formatting, and diff checks passed.

## Review

Pending program review.

## Retrospective

Pending execution.
