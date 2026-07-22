Status: active
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md

# P3 D18A: DuckDB wide roofline and profile

## Scope

Create a reproducible controlled-host workload for the exact finalized 3,513,266-row,
2,052-column package and measure CDF's current stock scanner against the closest semantics-labeled
raw DuckDB materialization. Capture operator timings, CPU, rows, logical/physical bytes, process and
cgroup memory, DuckDB peak buffer memory, peak temp-directory size, and spill.

## Non-goals

No product tuning, path change, source re-extraction, or conclusion from a laptop-only sample.

## Acceptance Criteria

- The retained package and schema/statistics identities are recorded without committing payload.
- The lab has a repeatable raw reference and full-CDF replay cell with explicit semantic bias.
- Median-of-N controlled EC2 evidence attributes scanner conversion, DuckDB sink/storage,
  checkpoint/receipt, peak buffer memory, peak temp storage, and process/cgroup memory.
- The profile names the dominant wide-schema cost and establishes comparison keys for D18B-E.
- Existing full-year TLC control is rerun on the same clean revision/host class.

## References

- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/tickets/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md`
- `.10x/tickets/done/2026-07-21-p0-duckdb-wide-ingest-memory.md`

## Assumptions

- Record-backed: the finalized local package under `/Users/alexanderbut/code_projects/tmp/.cdf/packages/`
  is reusable benchmark input after manifest verification; no FQ12 source contact is required.
- User-ratified: performance claims require real end-to-end and EC2 evidence rather than intuition.

## Journal

- 2026-07-21: Began execution from the retained exact 3,513,266-row package. The existing lab
  already owns an independent stock-public-C-API canonical-segment scanner and the lean measured
  command runner already owns full `cdf replay` phase/RSS/cgroup evidence. The missing authority is
  DuckDB's native operator profile. The retained design adds opt-in profiling around only the
  materialization query in the destination and the same opt-in to the independent comparator; the
  default path, artifact identity, and performance configuration remain unchanged.
- 2026-07-21: Verified the retained finalized package before transfer: package id
  `pkg-flolake-transactions-92680-1784668000407799000`, package hash
  `sha256:69183c567f1b15bdf2cf6eafcfb3669d83ee1a3f3a29dd39f785a68a331d43c4`, 3,513,266 rows,
  231 canonical segments, 1,291,273,686 segment bytes, and 2,053 persisted fields (2,052 user
  fields plus `_cdf_package_row_ord`). The shape has 1,247 Arrow batches, 15,208 average rows per
  segment, and 2,817 average rows per batch; this is the exact artifact D18A must explain rather
  than a synthetic wide-table approximation.
- 2026-07-21: Added destination-owned, opt-in native DuckDB profiling around only the canonical
  segment `INSERT ... SELECT` materialization statement. `CDF_DUCKDB_PROFILE_DIRECTORY` is absent
  by default, so ordinary commits retain their configuration and path; enabled captures use unique
  filenames and always disable profiling before returning, including failed capture and OOM-retry
  paths. Added the same capture to the independent stock-public-C-API comparator and separated its
  global DuckDB threads from scanner threads so the raw and product cells can use the same admitted
  wide-schema scanner width without artificially suppressing the sink.
- 2026-07-21: Added a versioned DuckDB JSON-profile normalizer to `cdf-p3-lab` and a benchmark-host
  `sync-package` command that verifies the finalized package before and after rsync. The affected
  full tests pass (benchmark lab 19 unit + 7 fixture + 6 policy + 11 runner, one deliberate live
  PostgreSQL ignore; DuckDB 47 tests), including real product/reference profiles and failed-capture
  cleanup. Strict affected-crate Clippy passes. The benchmark catalog test exposed a stale DuckDB
  `max_in_flight_bytes` fixture left by the earlier 256 MiB segment-envelope change; the fixture is
  realigned to the runtime authority rather than weakening the test.
- 2026-07-21: The first live `sync-package` attempt verified all 246 package files, then macOS's
  system rsync rejected GNU-only `--info=progress2` before transferring payload. Replaced the
  display-only flag with portable `--stats`; the failed attempt left only an empty remote target
  directory and produced no measurement or package mutation.
- 2026-07-21: Controlled-host profiled replay on revision `bc8e737d` took 205.224 s for 3,513,266
  rows. DuckDB attributed 194.152 s of query latency and 369.570 aggregate CPU-seconds to the
  materialization: 324.963 aggregate seconds in `INSERT`, 43.980 in the canonical table scan, and
  0.627 in projections. Peak DuckDB buffer memory was 4,961,632,256 bytes and peak temp-directory
  storage was 7,564,656,640 bytes. The 16 GiB cgroup recorded no pressure or OOM event; child peak
  RSS was 4,134,821,888 bytes. This identifies DuckDB wide-table storage/encoding as the dominant
  cost, not CDF verification or canonical IPC decoding.
- 2026-07-21: The first exact raw-reference probe failed at bind time before reading payload because
  the intentionally independent benchmark table-function mapper supported primitives/decimals but
  not the package's `List<Utf8>` fields. The product adapter already supports that type. Added
  generic recursive list binding to the benchmark-only mapper rather than altering product code or
  special-casing field names; the failed observation is retained as evidence and makes no
  performance claim.
- 2026-07-21: The corrected independent raw-reference profile completed all 3,513,266 rows from all
  231 canonical segments on revision `2c61cf73`. Its native DuckDB query took 217.364 s versus
  194.152 s for the product query. DuckDB attributed 324.875 aggregate seconds to
  `CREATE_TABLE_AS`, 90.103 to the independent canonical scanner, and 0.330 to projection. Peak
  buffer memory was 8,544,073,728 bytes and peak temp-directory storage was 6,949,044,224 bytes;
  child peak RSS was 4,581,232,640 bytes and the cgroup recorded no pressure or OOM event. The raw
  reference therefore does not justify replacing the product scanner: under the same two-thread
  scan admission it consumed more scanner CPU and more DuckDB buffer memory while finishing 11.9%
  slower. The warm median-of-three remains the comparison authority; this single profiled sample
  establishes the operator-level hypothesis to test.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
