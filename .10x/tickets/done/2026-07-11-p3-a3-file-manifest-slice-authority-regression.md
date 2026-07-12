Status: done
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md
Depends-On: None

# P3 A3 regression: file-manifest slice authority

## Scope

Allow canonical segmentation to split an oversized decoded file batch without losing or inventing source-position authority. Keep cursor and other row-relative positions fail-closed.

## Acceptance criteria

- File-manifest positioned batches split deterministically at canonical bounds and every emitted segment retains the same terminal file manifest.
- Cursor-positioned oversized batches remain rejected without exact source-provided slice positions.
- The public TLC Parquet run succeeds with 65,536-row decode batches.

## Evidence

- `.10x/evidence/2026-07-11-http-parquet-sequential-spool-and-positioned-slicing.md`
- `.10x/reviews/2026-07-11-http-parquet-sequential-spool-review.md`

## Retrospective

Terminal partition positions and row-relative positions are different algebraic authorities. The kernel now exposes that distinction narrowly through `SourcePosition::is_batch_slice_invariant`; future adapters must not infer cursor slice positions from a terminal batch value.
