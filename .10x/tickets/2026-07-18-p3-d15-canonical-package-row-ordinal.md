Status: open
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md

# P3 D15: canonical package row ordinal

## Scope

Implement `.10x/specs/canonical-package-row-ordinal.md` as the one destination-neutral row-sequence authority in canonical segments, manifests, verification/replay, and first-party destination bulk paths. Remove superseded DuckDB/Postgres destination-local enumeration code once equivalent conformance and performance evidence is green.

## Non-goals

- No public provenance-address change.
- No destination-visible ordinal column by default.
- No old package reader, migration, or compatibility shim.
- No unmeasured default-path retention.

## Acceptance Criteria

- Canonical segment storage and manifest evidence satisfy every assignment/continuity/tamper scenario in the governing spec.
- The shared memory ledger accounts the generated ordinal buffer and constant-memory tests remain green.
- DuckDB nanoarrow derives `_cdf_row_key` from the persisted ordinal with no `rowid`, window, sequence, or file-order premise.
- Postgres binary COPY derives row keys from the persisted ordinal and removes its generated row-index path.
- Parquet destination strips the internal field from visible data while preserving manifest provenance.
- Jobs-invariance and cross-destination logical-address conformance remain green.
- Controlled EC2 evidence records package overhead and DuckDB/Postgres/Parquet end-to-end impact; no slower default is retained.
- Superseded enumeration code and tests are deleted, and D14 resumes against the current ordinal-bearing package format.

## References

- `.10x/decisions/canonical-package-row-ordinal.md`
- `.10x/specs/canonical-package-row-ordinal.md`
- `.10x/decisions/compact-lossless-destination-row-provenance.md`
- `.10x/specs/canonical-segmentation-adaptive-batching.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/tickets/2026-07-18-p3-d14-duckdb-nanoarrow-080-lz4-revalidation.md`

## Assumptions

- User-ratified: shift deterministic row enumeration to the canonical post-verdict Arrow stream and keep destination-global keys transaction-owned.
- Record-backed: one package-global ordinal permits every relational destination to use `allocated_start + persisted_ordinal` while retaining the same public segment-local row address.
- User-ratified: performance and correctness are joint first priority; the extra column must be benchmarked rather than assumed free.

## Journal

- 2026-07-18: Opened after D14 proved direct nanoarrow 0.8.0 LZ4 ingestion at 4.56 seconds but destination-side enumeration alternatives cost 4.50–36.76 seconds and introduced adapter-specific ordering premises. The package-global form is selected because a segment-local ordinal would still require per-file constants, while a dense package ordinal makes the destination key one vectorized addition and keeps segment-local logical provenance derivable from manifest starts.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.

