Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-l-performance-lab.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l1-catalog-report-schema.md

# P3 L1 follow-up: small and startup catalog cells

## Scope

Add explicit deterministic small/startup datasets and workloads to the P3 catalog so fixed process/startup/package overhead is measured separately from throughput. Preserve the existing schema version if the current vocabulary is sufficient and update fixed canonical hashes intentionally.

## Acceptance criteria

- At least one tiny startup/end-to-end cell and one medium fixture-throughput cell are distinct catalog datasets/workloads.
- Timed-region inclusions/exclusions and logical/physical byte authorities are exact.
- Recipes remain deterministic, bounded, generated rather than committed as large data, and consumable by the L3 isolated runner.
- Catalog validation and canonical hash tests cover the new entries.

## Explicit exclusions

No benchmark execution, runtime optimization, report comparison, or baseline claim.

## Evidence expectations

Updated catalog fixture/hash tests and focused review against `.10x/specs/performance-lab-and-envelope.md`.

## Blockers

None. This is a discovered closure gap in L1 and must close before L5 records the baseline.
