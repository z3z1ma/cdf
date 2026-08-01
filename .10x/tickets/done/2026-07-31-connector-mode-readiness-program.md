Status: done
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

1. `.10x/tickets/done/2026-07-31-cmr1-reliable-deep-quality-certificate.md`
2. `.10x/tickets/done/2026-07-31-cmr2-model-based-core-falsifiers.md`
3. `.10x/tickets/done/2026-07-31-cmr3-connector-certification-and-core-budget.md`

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
- 2026-07-31: The one frozen delegated batch is complete. Quality automation passed; core identity
  and connector admission reported six total high findings, all resolved in one repair commit
  `4b141496`. Revised synthetic reports pass while explicitly non-admissible, and final hosted run
  `30676262226` at that exact code commit is the only remaining technical closure gate.
- 2026-07-31: The superseded hosted run exposed one overconstrained concurrent-publication test,
  not a production invariant failure. Commit `4dfca286` expresses both permitted winner/duplicate
  schedules and passed 50/50 stress iterations. Final hosted run `30676480811` at that exact code
  commit is now the only remaining technical closure gate.
- 2026-07-31: Run `30676480811` then passed 19 jobs and exposed one metrics-only environment gap:
  llvm-cov executes Postgres-required conformance tests but its job lacked the established service.
  Commit `41421d93` supplies that authority. Final hosted run `30677283399` at that exact commit is
  the only remaining technical closure gate.
- 2026-07-31: Before accepting that run, an exact local REST-shard reproduction converted its
  Parquet stall into a bounded core correction: truthful REST minimum-memory admission plus a
  generic fail-before-source fence. The formerly hanging shard now completes in 13.43 seconds;
  the pushed correction and one replacement hosted certificate are the remaining closure steps.

## Blockers

None.

## Evidence

All child acceptance criteria map to their journaled evidence. Aggregate closure is recorded at
`.10x/evidence/2026-07-31-connector-mode-readiness-closure.md`; final hosted run `30678125873`
passed 21/21 jobs at exact code commit `161d10ff`.

## Review

One frozen OCR-selected delegated batch reviewed the complete program range. Quality automation
passed; identity and admission reported six high findings. The single repair tranche closed all
six with reproducing laws and focused verification. No critical/high issue remains. Aggregate
verdict: pass. Residual limits are recorded in the closure evidence rather than reopened as review.

## Retrospective

The original piecemeal failure pattern came from one serial workflow hiding independent dormant
gate defects. Splitting the certificate exposed the full frontier concurrently, but closure still
needed discipline: each later failure was inspected once and classified as runtime correctness,
overconstrained test semantics, or missing gate authority. The only mission-critical runtime stall
was a truthful-memory problem: REST's declared minimum did not cover its irreducible decode
reservation after the destination's resident lease. Making that bound explicit and failing before
work removed the closed wait without raising production budgets.

The program compounded into focused knowledge on Arrow bitmap identity, concurrent receipt
outcomes, hosted gate execution, and connector admission. No new procedural skill was warranted:
the recurring connector workflow is already executable in `tools/certify-connector.py`, while the
hosted fixes belong in workflow/knowledge authority rather than a duplicate runbook.
