Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l3-macro-roofline-runners.md

# P3 WS-L4: regression gates and envelope generation

## Scope

Generate the human envelope document from machine reports, add same-host median/variance comparison and greater-than-10% regression failure, wire smoke versus slow performance tiers into `QUALITY.md`/CI, and link README claims only to generated evidence.

## Acceptance criteria

- Baseline comparison refuses unlike hosts, modes, fixture identities, schema versions, or reference versions.
- A comparable regression over 10% fails; variance/incomparability is explicit and cannot silently pass as an improvement.
- Baseline replacement requires a named evidence record and preserves prior reports.
- Generated envelope includes host, absolute/ratio targets, observations, overhead, memory, bias, unavailable cells, and profile links.
- Ordinary CI remains bounded; long/stress cases are scheduled/manual slow-tier gates.
- Generated artifact freshness is tested.

## Evidence expectations

Comparator unit tests at threshold/variance boundaries, generated-document golden, workflow validation, and adversarial baseline-reset review.

## Explicit exclusions

No claim that the envelope is green until L5 executes it.

## Blockers

Depends on L3 report-producing runners.

## Progress and notes

- 2026-07-11: Activated after L3 closure. Ordinary CI will receive only schema/freshness/comparator smoke coverage; macro/stress execution remains scheduled/manual and will not be polled as part of implementation.
- 2026-07-11: Ratified the robust comparison boundary in `.10x/decisions/performance-comparison-variance-and-baseline-policy-v2.md`: CDF revision is the intentional delta, all other authority is exact, MAD above 10% is inconclusive, and regression strictly above 10% fails.
- 2026-07-11: Implemented exact summary/work-authority validation, comparison reports/exit status, content-addressed evidence-backed baseline history with tamper checks, deterministic envelope generation and committed freshness golden, and the pre-baseline no-claim docs surface.
- 2026-07-11: Removed benchmark execution from the broad slow-quality workflow and isolated it in scheduled/manual `performance-lab.yml`; ordinary fast CI remains unchanged and lean. Closure evidence is `.10x/evidence/2026-07-11-p3-l4-regression-envelope.md`; adversarial review is `.10x/reviews/2026-07-11-p3-l4-regression-envelope-review.md` (pass).
