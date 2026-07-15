Status: active
Created: 2026-07-11
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-10-p3-ws-f-constant-memory-guarantee.md
Depends-On: .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md, .10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md, .10x/tickets/2026-07-11-p3-b13-native-format-matrix.md, .10x/tickets/done/2026-07-11-p3-d5-bulk-path-matrix.md, .10x/tickets/2026-07-11-p3-e4-package-io-envelope.md, .10x/tickets/2026-07-11-p3-g3-codec-download-decode-overlap.md

# P3 F2: production materialization and allocation-owner closure

## Scope

Generate/audit every production allocation/materialization across source/format/transport/engine/contract/package/destination/interop, remove residual whole-input/package/cardinality collections in scope, and map every native/child/metadata class to ledger/headroom/external staging evidence.

## Acceptance criteria

- No production input/package/listing/segment/provenance collection scales outside ledger/spill.
- Static architecture gates reject known eager APIs in production runtime paths.
- Every allocation class has one owner/classification and measured bound.
- Mock source/format/destination/child additions must declare memory behavior through conformance.

## Evidence expectations

Generated owner matrix, static scans/dependency graph, runtime owner telemetry, focused residual fixes, high-cardinality tests, and adversarial “hidden Vec/native allocation” review.

## Explicit exclusions

No unrelated product feature or performance tuning beyond closure blockers.

## Blockers

Depends on the runtime/codec/destination/package/remote materialization owners.

## References

- `.10x/specs/constant-memory-proof.md`

## Journal

- 2026-07-14: A 2.15 GB FineWeb isolation matrix falsified the prior assumption that DuckDB's bounded two-segment input window also bounded its native residency. The same CDF source/package path peaked near 909 MB with the Parquet destination and 3.25 GB with DuckDB; raw Arrow decode at the actual ~1k-row row-group grain peaked near 58 MB. The package-long DuckDB transaction was the dominant unaccounted owner.
- 2026-07-14: Added an adapter-owned DuckDB native envelope rather than a runtime destination branch. Read/write connections now share bounded settings; the native memory limit is one quarter of the host managed pool clamped to 256 MiB–1 GiB, internal parallelism is the one thread already declared by the path, insertion-order retention is disabled because exact CDF row keys carry order authority, and DuckDB temporary scratch is capped at 1 GiB. Generic destination resolution injects execution services, and the adapter reserves the complete scratch ceiling from the shared spill coordinator before source execution.
- 2026-07-14: Final local FineWeb evidence measured 7.27 seconds and 1,385,006,712 bytes peak footprint versus the earlier uncapped 6.24 seconds and 3,248,835,536 bytes. Remote HTTPS-to-DuckDB measured 18.54 seconds versus an immediate 15.31-second curl floor and stayed near 1.62 GB peak footprint. Full adapter semantics passed 27 tests (one benchmark ignored); focused settings/reservation/failure tests and the product catalog fixture passed. Evidence: `.10x/evidence/2026-07-14-p3-f2-duckdb-native-resource-envelope.md`.

## Evidence

- DuckDB native transaction owner and bounded replacement: `.10x/evidence/2026-07-14-p3-f2-duckdb-native-resource-envelope.md`.
- This is partial F2 evidence only. The ticket remains active because its cross-codebase owner matrix, static architecture gates, direct-construction audit, metadata-cardinality closure, and geometric stress proof are not complete.

## Review

Verdict: concerns for F2 closure; pass for the bounded DuckDB slice.

- The retained implementation keeps all DuckDB policy inside `cdf-dest-duckdb`; runtime selection remains capability-driven and no destination id/path appears in orchestration.
- Native memory and scratch disk now have hard bounds; scratch capacity joins the shared spill authority before a production registry run and fails before source/destination mutation when unavailable.
- Residual significant F2 work remains: direct `DuckDbDestination::new` construction uses the same hard native/temp bounds but does not join a host-wide spill coordinator; F1 has not yet made process native-headroom authority available through `ExecutionServices`; other native allocation owners remain unaudited. These are recorded closure blockers, not waived claims.

## Retrospective

The misleading signal was a bounded CDF queue beside an unbounded native transaction. Queue capacity proves only CDF-owned residency; every database/parser/compression boundary needs its own measured native envelope. A single wide-text fixture exposed the gap immediately, while the TLC-shaped benchmark had been too narrow to falsify it. Future destination closeout must include both narrow numeric and wide variable-length schemas, and must compare the identical source/package path across destinations before assigning memory to the decoder.
