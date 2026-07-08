Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-07-p0-workstream-e-contract-depth-program.md

# P1 contract-depth program

## Scope

Implement contract-governed live movement: compiled row verdicts, quarantine routing, deterministic dedup, variant capture, trust-ring ledger events, and drift-quarantine conformance.

This parent is a plan. Child tickets own executable implementation. Children must execute in order unless a later active record proves a dependency can be safely parallelized.

## Governing records

- `VISION.md` Chapter 11.
- `.10x/specs/types-contracts-normalization.md`.
- `.10x/specs/package-lifecycle-determinism.md`.
- `.10x/specs/destination-receipts-guarantees.md`.
- `.10x/specs/run-orchestration-ledger.md`.
- `.10x/decisions/contract-live-verdict-execution-semantics.md`.
- `.10x/knowledge/runtime-conformance-throughput-rule.md`.

## Child tickets

- `.10x/tickets/done/2026-07-08-p1-e1-row-level-verdicts-live-chain.md`
- `.10x/tickets/done/2026-07-08-p1-e2-quarantine-routing-redaction.md`
- `.10x/tickets/done/2026-07-08-p1-e3-merge-dedup-live-path.md`
- `.10x/tickets/done/2026-07-08-p1-e4-variant-capture-evolution-event.md`
- `.10x/tickets/done/2026-07-08-p1-e5-trust-ring-ledger-events.md`
- `.10x/tickets/done/2026-07-08-p1-e6-drift-quarantine-conformance.md`

## Acceptance criteria

- Live runs execute the compiled validation program for row-level rules rather than schema/column coverage only.
- Accepted rows continue through normalization, package writing, destination session commit, receipt verification, and checkpoint gating.
- Rejected rows route to package quarantine artifacts with rule id, error code, source position, and redaction-by-tag.
- Destination `_cdf_quarantine` mirrors are populated only where the destination sheet supports quarantine tables.
- Merge dedup is deterministic under package redelivery and runs before destination mutation.
- Variant capture and trust-ring promotion/demotion are recorded as package/run evidence.
- Drift-quarantine conformance proves frozen drift is quarantined while accepted rows continue.
- Contract throughput benchmarks record type/null/domain performance on 100k-row batches against the greater-than-1 GB/s spike target where the environment supports a stable measurement.

## Evidence expectations

Each child records focused evidence and adversarial review. Parent closure requires aggregate evidence mapping every Workstream E acceptance criterion, quality gates from `QUALITY.md` including jscpd and `rust-code-analysis-cli`, relevant conformance output, throughput benchmark output, quarantine/redaction adversarial checks, and final review.

## Explicit exclusions

No trust UI, no schema-on-read replacement for packages, no DataFusion multi-output-plan fork, no public performance claim, no destination quarantine mirror where the sheet declares unsupported, and no post-load modeling.

## Progress and notes

- 2026-07-08: Opened from P0 Workstream E after Workstreams A-C closed and the A-C stop-line lifted. `.10x/decisions/contract-live-verdict-execution-semantics.md` ratifies the live evaluator API and execution semantics required before implementation.
- 2026-07-08: E1 closed with compiled row-level verdict programs, pure Arrow evaluator, live `ContractExec` filtering before normalization, freshness `observed_at_ms` context, focused tests, local/non-public 100k-row benchmarkable path, evidence `.10x/evidence/2026-07-08-p1-e1-row-level-verdicts-live-chain.md`, and review `.10x/reviews/2026-07-08-p1-e1-row-level-verdicts-live-chain-review.md`.
- 2026-07-08: E2 closed with identity-participating package quarantine Parquet artifacts, redacted observed values, accepted-row continuation, unsupported mirror outcome artifacts, Postgres `_cdf_quarantine` mirrors, evidence `.10x/evidence/2026-07-08-p1-e2-quarantine-routing-redaction.md`, and review `.10x/reviews/2026-07-08-p1-e2-quarantine-routing-redaction-review.md`.
- 2026-07-08: E3 closed with deterministic pre-merge dedup over accepted package-order rows, `keep = first|last|fail` coverage, package identity `stats/dedup-summary.json`, live run replay/redrive identity coverage, legacy `EnginePlan` JSON compatibility repair, evidence `.10x/evidence/2026-07-08-p1-e3-merge-dedup-live-path.md`, and review `.10x/reviews/2026-07-08-p1-e3-merge-dedup-live-path-review.md`.
- 2026-07-08: E4 closed with live `_cdf_variant` capture for Struct/List/Map nested fields, semantic `json` schema evidence, deterministic `schema/contract-evolution.json` with zero implicit promotions, package verify/replay evidence, conformance compiler coverage, evidence `.10x/evidence/2026-07-08-p1-e4-variant-capture-evolution-event.md`, and review `.10x/reviews/2026-07-08-p1-e4-variant-capture-evolution-event-review.md`.
- 2026-07-08: E5 closed with trust-ring promotion/demotion ledger events for first-contact, clean-stable promotion, drift demotion, quarantine demotion, and explicit anomaly-fact demotion. Anomaly semantics are ratified by `.10x/decisions/contract-anomaly-signal-demotion-policy.md`; closure evidence is `.10x/evidence/2026-07-08-p1-e5-trust-ledger-events.md` and review is `.10x/reviews/2026-07-08-p1-e5-trust-ledger-events-review.md`. E6 drift-quarantine conformance remains open.
- 2026-07-08: E6 is partially implemented but blocked. Live packages now write verdict/quarantine summary artifacts and the conformance harness covers row-rule/domain drift quarantine with accepted-row progress. Literal source scalar type drift still fails in the decoder before `ContractExec`; `.10x/decisions/source-decode-type-drift-quarantine.md` ratifies the required seam, and `.10x/tickets/done/2026-07-08-source-decode-type-drift-quarantine-seam.md` owns implementation.
- 2026-07-08: Closed after E6 and the source-decode type-drift seam closed. P1 now covers row verdicts, package/destination quarantine routing, dedup, variant capture, trust ledger events, literal drift-quarantine conformance, and the required quality evidence. Rollup evidence: `.10x/evidence/2026-07-08-p0-structural-debt-program-exit.md`; review: `.10x/reviews/2026-07-08-p0-structural-debt-program-exit-review.md`.

## Blockers

None.
