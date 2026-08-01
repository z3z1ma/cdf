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
- 2026-07-31: From clean pushed commit `db306682`, the documented command passed for the Nebula
  source and Quasar destination. Reports under ignored `target/quality/` bind both claims to the
  exact `origin/main` merge base and show an empty working change set, connector-only profile, all
  selected commands, fixed timeouts, durations, and a pass verdict.
- 2026-07-31: The frozen delegated review falsified the initial gate through five high paths:
  caller-selectable baselines, blanket shared-manifest allowance, substring test filters,
  incomplete core-impact regression scope, and synthetic fixtures masquerading as shipped catalog
  enrollment. One repair pass removed the baseline override, bound version-2 reports to exact HEAD
  and a content digest, made root/lock/policy files shared ownership, selected exact fixed-count
  fixture laws or whole connector leaves, expanded core impact to workspace nextest, and separated
  non-admissible `--fixture` proof from built-in catalog admission.
- 2026-07-31: From clean pushed repair commit `4b141496`, Nebula fixture proof passed five of five
  checks and Quasar fixture proof passed six of six. Both reports say `admissible: false`, carry the
  exact HEAD and changed-content digest, and show an empty working change set. Running Quasar
  without `--fixture` fails before Cargo because it is absent from the shipped built-in catalog.

## Blockers

None.

## Evidence

- Ten standard-library unit laws passed for source/destination allowed surfaces, shared root and
  policy rejection, underscore-to-crate-name mapping, direction-specific fixed-count fixture laws,
  workspace-wide core-impact selection, report content/HEAD binding, and built-in catalog
  preflight.
- The registered source-shard/catalog coverage law passed after adding the destination selector,
  proving the existing scheduled source matrix remained unchanged.
- The selected Quasar destination matrix completed all 15 catalog-derived cells in 10.30 seconds:
  append executed and passed for file, Python, REST, SQL, and Nebula; replace and merge were
  explicitly excluded for each source by Quasar's declared `supported_dispositions=[Append]`.
  Every executed cell asserted plan honesty, package verification, durable receipt verification,
  checkpoint-after-receipt gating, artifact replay identity, and no-op duplicate behavior.
- Strict all-target/all-feature `cdf-conformance` Clippy, formatting, and diff checks passed.
- `python3 tools/certify-connector.py --kind source --id nebula --fixture`: five of five checks
  passed in an explicitly non-admissible fixture report.
  Identity-specific registry/schema/add/discovery/doctor and product laws passed; ordinary
  conformance passed 95/95; the 12-cell source matrix executed nine supported cells and recorded
  three sheet exclusions; the generic source compiler graph fence passed.
- `python3 tools/certify-connector.py --kind destination --id quasar --fixture`: six of six checks
  passed in an explicitly non-admissible fixture report. Ordinary conformance passed 95/95; the
  15-cell destination matrix executed five append cells and recorded ten sheet exclusions; all
  four crash windows passed; the exact three CLI lock/plan/run/replay/resume/doctor/inspect laws
  passed; and static destination boundaries passed.
- Running the same Quasar command without `--fixture` returned exit 2 and a version-2 JSON report
  stating that the identity is absent from the shipped catalog, proving synthetic enrollment fails
  closed.

## Review

The frozen independent review verdict was fail with the five high false-green paths recorded in
the journal. All five were closed in the single authorized repair pass and directly exercised by
the ten unit laws plus clean pushed Nebula/Quasar fixture reports; the catalog preflight negative
law proves the key admission distinction. Closure verdict: pass. Residual risk remains
file-granular inside the explicitly allowed catalog files, so hashed catalog integrity, selected
lifecycle laws, and the broader core profile remain required rather than treating classification
as semantic proof.

## Retrospective

Reusing the registered matrix in its opposite orientation was the key simplification. Source
shards remain the scheduled aggregate authority; the destination selector only filters the same
cells for connector admission, so it cannot drift into a competing assertion engine. Keeping the
outer entry point in the Python standard library avoided another Rust build graph solely for
process orchestration.

The change budget is intentionally file-granular. It cannot prove that an edit inside a catalog
file changed only one construction row, so catalog hashes, identity-named laws, generic static
fences, full ordinary conformance, and the selected lifecycle slices remain necessary. Conversely,
a `--core-impact` flag that merely waived the file check would reward boundary leakage; retaining
all connector laws before adding the core profile makes the acknowledgement evidence-bearing.

No new 10x skill was created. The recurring procedure is already executable in
`tools/certify-connector.py` and documented for humans in `docs/connector-authoring.md`; a second
skill copy would create another command/profile authority rather than remove toil.
