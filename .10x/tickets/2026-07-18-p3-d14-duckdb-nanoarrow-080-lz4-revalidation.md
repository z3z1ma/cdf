Status: active
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-18-p3-d11-duckdb-arrow-ipc-handoff-falsification.md

# P3 D14: DuckDB nanoarrow 0.8.0 LZ4 revalidation

## Scope

Correct D11's overbroad LZ4 conclusion by building the DuckDB nanoarrow extension against the signed Apache nanoarrow `0.8.0` release with `NANOARROW_IPC_WITH_LZ4=ON`, then re-run the direct current-CDF-segment and full-year TLC Arrow IPC ingestion probes on the controlled EC2 benchmark host. Determine whether direct consumption of CDF's canonical LZ4 Arrow IPC segments removes the redundant destination handoff and materially closes the DuckDB G4 envelope gap.

## Non-goals

- No package artifact-format change.
- No uncompressed duplicate handoff.
- No generic runtime branch for DuckDB, nanoarrow, Arrow IPC paths, or extension installation.
- No product default change before full-CDF EC2 evidence meets the retained performance and memory thresholds.
- No unpinned extension source, nanoarrow source, build flag, or binary artifact.
- No hard-coded performance cap without a configuration authority.

## Acceptance Criteria

- The investigation records the exact DuckDB extension revision, nanoarrow `0.8.0` tag/commit, archive digest, and `NANOARROW_IPC_WITH_LZ4=ON` build configuration.
- The pinned extension loads under CDF's bundled DuckDB `v1.5.4`, reports its version, and successfully reads a generated LZ4 Arrow IPC fixture.
- The pinned extension successfully reads at least one current CDF package segment directly, or records the precise remaining incompatibility with evidence.
- EC2 evidence measures full-year TLC-shaped direct LZ4 Arrow IPC ingestion through `read_arrow(list_of_files)` with 41,169,720 rows, wall time, throughput, peak RSS, and cgroup peak.
- If the direct LZ4 path clears the generated retention threshold, a destination-owned product implementation is retained only after a full-CDF EC2 run proves it faster than the current appender baseline without duplicating package bytes or leaking destination identity into orchestration.
- D11, D12, G4, and WS-D references are corrected so the backlog reflects the new evidence and no stale claim says nanoarrow lacks LZ4 support.

## References

- `.10x/tickets/done/2026-07-18-p3-d11-duckdb-arrow-ipc-handoff-falsification.md`
- `.10x/tickets/cancelled/2026-07-18-p3-d12-duckdb-arrow-ipc-handoff-ingress.md`
- `.10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/decisions/destination-runtime-composition-boundary.md`
- `https://github.com/apache/arrow-nanoarrow/pull/819`
- `https://github.com/apache/arrow-nanoarrow/releases/tag/apache-arrow-nanoarrow-0.8.0`

## Assumptions

- Record-backed: Apache nanoarrow `0.8.0` includes LZ4 IPC decompression from PR 819, gated by the build option `NANOARROW_IPC_WITH_LZ4`.
- Record-backed: the DuckDB community extension artifact tested by D11 was extension revision `42e4199a67c4cd0789087562a025e87e7130fdc3`, which vendors pre-LZ4 nanoarrow revision `4bf5a9322626e95e3717e43de7616c0a256179eb` and does not enable the LZ4 build option.
- Record-backed: D11's observed failure is valid only for that old extension artifact; it does not establish that nanoarrow or `read_arrow(...)` cannot consume CDF LZ4 IPC.
- User-ratified: use nanoarrow `0.8.0`, investigate thoroughly, and prioritize this correction before unrelated architecture work.
- User-ratified: performance and correctness are joint first priority; retain no potentially slower default without controlled EC2 evidence.

## Journal

- 2026-07-18: Opened after upstream verification invalidated D11's general LZ4 conclusion. Apache PR 819 merged LZ4 IPC decompression on 2025-10-27 and nanoarrow `0.8.0` released it behind `NANOARROW_IPC_WITH_LZ4`. The installed DuckDB community extension's `.info` file identifies extension revision `42e4199`; that revision fetches nanoarrow `4bf5a932` from 2025-01-09 and therefore produced the observed `Compression type with value 1 not supported by this build of nanoarrow` error.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
