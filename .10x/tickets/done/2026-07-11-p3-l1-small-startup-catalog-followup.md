Status: done
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md
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

## Progress and notes

- 2026-07-11: Activated. The existing recipe vocabulary cannot truthfully identify the deterministic legacy Arrow/file fixture generator, so catalog schema v2 will add a bounded benchmark-fixture recipe rather than overload the synthetic stream recipe.
- 2026-07-11: Added catalog schema v2 `benchmark_fixture` authority tied directly to baseline fixture catalog v1 and generator version, with exact tiny/medium rows, batch sizes, and byte ceilings. Added separate startup end-to-end and prepared medium NDJSON/package workloads with exact timed-region and byte-counter semantics.
- 2026-07-11: Aligned the report fixture's medium dataset/workload identities to the catalog. New canonical hashes are recorded in `.10x/evidence/2026-07-11-p3-l1-small-startup-catalog.md`; focused review `.10x/reviews/2026-07-11-p3-l1-small-startup-catalog-review.md` passes.
