Status: active
Created: 2026-07-31
Updated: 2026-07-31

# Connector-mode readiness program

## Scope

Turn the completed pre-wave hardening evidence into a finite, continuously enforced certificate
for connector development. Make deep CI independently observable, make the complete
source-by-destination matrix bounded and diagnosable, add model-based falsifiers for the two core
irreversibility boundaries, and provide one connector certification entry point with an explicit
core-change budget.

This is a parent plan and prioritization authority, not an executable implementation ticket.

## Workstreams and sequence

1. `.10x/tickets/2026-07-31-cmr1-reliable-deep-quality-certificate.md`
2. `.10x/tickets/2026-07-31-cmr2-model-based-core-falsifiers.md`
3. `.10x/tickets/2026-07-31-cmr3-connector-certification-and-core-budget.md`

CMR1 establishes a trustworthy current-HEAD certificate and bounded conformance execution.
CMR2 adds high-value generative evidence for deterministic packaging and receipt-gated
settlement. CMR3 turns the existing extension laws into the ordinary connector admission path
and prevents connector work from silently expanding the generic core.

## Acceptance criteria

- The slow-quality workflow exposes independent compile/lint, test/conformance, generated/API,
  and security/supply-chain outcomes; an early failure does not hide every unrelated gate.
- Every source-by-destination matrix cell is individually attributable, bounded, and represented
  in a durable machine-readable test/report surface.
- Deterministic package identity is falsified across generated batch/partition/scheduling shapes,
  and receipt/checkpoint settlement is falsified across generated recovery sequences against an
  explicit reference model.
- One repository command certifies a source or destination through catalog, conformance, product,
  and change-surface laws without inventing another connector runtime abstraction.
- Connector-only changes that cross into generic core ownership fail with precise remediation or
  carry an explicit core-impact acknowledgement and the broader verification it activates.
- Current HEAD passes the affected focused gates and the complete bounded readiness certificate.
- One frozen delegated adversarial review finds no unresolved critical/high issue; review does not
  become a serial polish loop.
- Retrospectives are distilled and the active ticket graph returns to zero before connector work
  begins.

## Non-goals

- No new source, destination, format, registry ABI, SDK mega-trait, or plugin system.
- No broad repository re-review and no reopening terminal hardening work without a reproducing
  failing law.
- No live cloud dependency in pull-request fast gates. Credentialed Glue/Iceberg and later cloud
  cells belong to the connector certification wave as scheduled/manual provider evidence.
- No one-TiB stress, performance-default change, or production data-path optimization.

## References

- `.10x/evidence/2026-07-28-prewave-architecture-hardening-closure.md`
- `.10x/knowledge/product-integration-and-closure-gate.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `QUALITY.md`

## Assumptions

- User-ratified: execute the finite connector-mode readiness recommendation and commit/push
  coherent checkpoints as work proceeds.
- Record-backed: the pre-wave program proved the neutral extension architecture; this program
  improves repeatable falsification and admission rather than redesigning that architecture.
- Source-backed: the existing conformance matrix contains five source archetypes, four
  destination fixtures, and three dispositions, but executes as one test that reports only after
  the full matrix completes.
- Source-backed: the current slow-quality workflow is one serial job, so an early compile failure
  prevents later independent gates from producing evidence.

## Journal

- 2026-07-31: Activated from the user's explicit request to proceed with the ranked readiness
  recommendation. The program is deliberately limited to three bounded workstreams and one final
  review batch; live provider breadth moves into connector certification rather than extending
  this core program indefinitely.
- 2026-07-31: All three implementation workstreams reached pushed commits. The model falsifier
  found and repaired Arrow bitmap-padding identity drift; the connector certificate passed from
  clean HEAD for both Nebula and Quasar. Remaining closure is bounded to hosted run `30675543293`,
  one frozen delegated review, record/status reconciliation, and final push.

## Blockers

None.

## Evidence

Pending child execution.

## Review

Pending aggregate closure.

## Retrospective

Pending closure.
