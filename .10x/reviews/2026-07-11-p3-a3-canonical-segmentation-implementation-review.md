Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md
Verdict: pass

# P3 A3 canonical segmentation implementation review

## Target

The compiled `canonical-segmentation-v1` policy, adaptive nonidentity controller, typed position algebra, production assembler/writer integration, and full Arrow logical-byte estimator through commit `4811e29b` plus closure tests.

## Findings

The adversarial review found two significant defects during execution: the serialized byte target was inert, and position joining could attach a later batch cursor to an earlier size-flushed segment. Both are fixed and permanently tested.

No critical or significant A3-scoped defect remains:

- Row and byte targets are plan data and the assembler consults both; pressure only changes nonidentity microbatch telemetry.
- IDs derive solely from partition and segment ordinals. The production golden is fixed and source rechunking produces identical segments/package identity.
- Flat, nullable, nested, dictionary, view, list-view, map, and union byte estimates use logical slices rather than backing-buffer capacity. Dictionary values are cached by index.
- Positioned oversize inputs fail without exact slice authority. Unsupported position joins force a boundary rather than inventing cursor meaning.
- Package-global dedup executes before canonical persistence and its first/last/fail laws remain green in the full engine suite.
- The common byte-prefix case evaluates the full prefix once; binary search is reserved for an actual byte split.
- The release fixed-cost benchmark measures a 13.45x package-build speedup for canonical 64k coalescing versus 64 legacy 1,024-row segments on the same 65,536 rows.

## Verdict

Pass. A3 is complete and may unblock A5/B2/C2.

## Residual risk

Physical retained-buffer ownership is still represented by compatibility `RecordBatch` values; A5 already owns accounted-envelope ownership transfer. Actual concurrent jobs invariance and reorder pressure are C2 acceptance, not a hidden A3 exception. Future changes to logical-byte estimation alter segment boundaries and therefore require a new policy version/golden gate.
