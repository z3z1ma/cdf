Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p3-ws-l-performance-lab.md
Depends-On: .10x/tickets/2026-07-10-p3-ws-l3-macro-roofline-runners.md

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
