Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md

# P3 B2: ranged row-group-parallel Parquet codec

## Scope

Move Parquet behind the format driver, implement bounded footer/page metadata, projection/predicate pushdown, deterministic row-group units and parallel ranged decode for local/remote sources, and remove collected `FormatRead` batches.

## Acceptance criteria

- Multi-file and row-group jobs preserve file/row-group/row order and whole-file manifest completion.
- Projection/predicate fidelity and schema/physical provenance are exact and conformance-tested.
- Remote decode uses overlapping bounded ranges without full download when the server supports ranges; fallback is explicit spool.
- Parquet reaches the ratified envelope and jobs-invariance hashes match.

## Evidence expectations

Raw arrow-rs roofline, TLC/nested/wide datasets, range trace, malformed footer/page fuzzing, pushdown equivalence, memory/cancellation, and local/remote profiles.

## Explicit exclusions

No Parquet destination writer.

## Blockers

Depends on L5, FX1, segmentation, and the execution host.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`

## Progress and notes

- 2026-07-11: The extracted driver is now the production CLI Parquet path for local files and verified remote spools. It emits accounted 64k physical batches through the injected structured-I/O stream and shared reconciliation. Full TLC correctness passed, but a three-run comparison recorded median wall/CPU of 1.63/1.80 seconds versus the recent 1.53/1.62 control. B2 remains open to remove local range allocation/copy/open/attestation overhead without exposing filesystem handles to the codec. Evidence: `.10x/evidence/2026-07-11-p0-fx1-production-parquet-registry-stream.md`.

- 2026-07-11: FX1 extracted the native codec into `cdf-format-parquet` behind the neutral `FormatDriver`/`ByteSource` boundary. The driver already provides footer discovery, row-group unit plans, exact projection, capability-bounded parallel ranges, incremental Arrow output, full physical-schema drift checks, and owner-backed zero-copy source accounting. B2 still owns production migration measurement, predicate/page-index pushdown, adaptive byte-target control, jobs scaling, malformed/fuzz coverage, and deletion of the superseded monolithic Parquet reader. Evidence: `.10x/evidence/2026-07-11-p0-fx1-parquet-driver-extraction.md`.

## Progress and notes

- 2026-07-11: Corrected the urgent full-scan policy: execution no longer routes through the unconditional serialized `RangeChunkReader`; discovery retains bounded footer ranges, while full/unknown coverage uses one generation-bound sequential spool. Removed the superseded range-execution exports and raised native read batches from 1,024 to 65,536 rows. The public January TLC file loaded 2,964,624 rows successfully in 43.85 seconds in an unoptimized debug end-to-end run. Streaming decoded publication, row-group units, projection/predicate pushdown, and release roofline remain open. Evidence: `.10x/evidence/2026-07-11-http-parquet-sequential-spool-and-positioned-slicing.md`.
- 2026-07-11: Replaced collected Parquet execution with an incremental `BatchStream` behind the generic format-stream boundary. Release profiling measured 113.9 ms Arrow decode and 0.2 ms reconciliation/envelope work for all 2,964,624 TLC rows, approximately 0.87x the median raw arrow-rs reference. Source execution no longer branches on Parquet. Row-group units, pushdown, parallel decode, and the final envelope remain open. Evidence: `.10x/evidence/2026-07-11-p3-parquet-stream-byte-first-segments.md`.
