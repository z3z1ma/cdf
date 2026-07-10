Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/tickets/2026-07-09-p2-ws-a10c-exhaustive-local-binary-discovery.md, .10x/tickets/done/2026-07-09-p2-ws-b5-validation-program-coercion-evidence.md

# P2 WS-A10d effective schema runtime and per-file evidence

## Scope

Execute compatible multi-file resources against an immutable baseline pin and a plan-time effective schema. Materialize missing nullable fields, allow distinct verified per-file coercion plans targeting one effective schema, and stamp baseline/effective/manifest/per-file evidence into plans and packages.

## Acceptance criteria

- Plans distinguish baseline snapshot, effective schema, and discovery manifest hashes; package identity includes all three and their verified references.
- `evolve` admits compatible additions/widenings into the effective schema without modifying `cdf.lock` or the baseline snapshot.
- `freeze` keeps the baseline effective; incompatible disposition is deferred only to A10e, not misclassified or crashed.
- Readers materialize typed null arrays for compatible missing fields and preserve per-file physical provenance.
- Different preserved/widened/missing decisions may coexist across files when they target the exact effective schema; malformed or spoofed per-file evidence fails closed.
- Validation/package artifacts serialize deterministic per-file coercion/verdict evidence rather than collapsing it into one plan.
- Destination planning/normalization consumes the effective schema uniformly and replay needs no source contact.
- Legacy plan/package/snapshot deserialization remains compatible through additive omitted defaults.

## Evidence expectations

Compatible multi-file package runs, widening/missing/null/provenance inspection, package verification/replay, tamper/legacy fixtures, destination plan checks, semver/golden gates, and adversarial review.

## Explicit exclusions

No terminal file quarantine, quarantine-only package, processed-file checkpoint advancement, remote enumeration, or final S2/S6/S8 promotion.

## Progress and notes

- 2026-07-09: Opened after ratification of the immutable-baseline/effective-schema split.

## Blockers

Depends on A10c.
