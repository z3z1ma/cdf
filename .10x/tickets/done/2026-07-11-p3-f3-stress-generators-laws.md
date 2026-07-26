Status: done
Created: 2026-07-11
Updated: 2026-07-25
Parent: .10x/tickets/done/2026-07-10-p3-ws-f-constant-memory-guarantee.md
Depends-On: .10x/tickets/done/2026-07-11-p3-f1-budget-enforcement-headroom.md, .10x/tickets/done/2026-07-11-p3-f2-materialization-closure-audit.md

# P3 F3: constant-memory generators and stress laws

## Scope

Build bounded deterministic generators and execute geometric-size, 100 GB/2 GiB, too-small, spill-full, metadata, compression, dedup, quarantine, slow destination, remote, and foreign-child stress cases with semantic assertions.

## Acceptance criteria

- Generator/setup memory is separate and bounded.
- 100 GB completes under enforced 2 GiB process-tree RSS budget with no OOM event; a separately
  forced spill case observes spill and clean reclamation.
- Geometric inputs show no memory slope; repeated runs show no leak/fragmentation drift.
- Geometric file/segment cardinality shows no open-file-descriptor slope; retained handles are
  bounded by admitted concurrency rather than total work units.
- Below-minimum and spill-full cases fail cleanly with exact remediation.
- Every case verifies package/receipt/checkpoint semantics where applicable.

## Evidence expectations

Machine reports/raw high-water/cgroup/ledger/spill data, package verification, failure-mode output, soak curves, host labels, and adversarial workload review.

## Explicit exclusions

No committed giant datasets.

## Blockers

Depends on F1/F2.

## Journal

- 2026-07-21: Activated the file-descriptor stress slice after a real
  `cdf run flolake.transactions` failed at 231 canonical package segments with `EMFILE`. Control
  flow tracing found `DuckDbStagedIngressSession` retained one already-opened segment file per
  accepted segment until final binding; package cleanup then also failed because no descriptor
  remained to open the data directory. The repair preserves package-root capability access and
  exact content identity while deferring each segment open to the DuckDB scan worker. The generic
  access boundary verifies the manifest byte count and SHA-256 on the newly opened handle, rewinds
  that same handle, and bounds live segment descriptors by scanner concurrency rather than package
  cardinality. Raising the process descriptor limit is explicitly not the product fix.
- 2026-07-21: The clean release smoke regenerated all 231 segments and crossed final package
  binding without `EMFILE`; while the one-worker diagnostic was actively scanning, `lsof` observed
  36 total process descriptors, exactly one `.arrow` segment descriptor, and three constant
  package-root directory capabilities. A 512-segment destination regression independently proved
  that staging opens zero segment files before final scanning. The smoke then reproduced the
  already-recorded wide-table DuckDB memory residual owned historically by the cancelled P0/D17
  investigations; no unmeasured thread or buffering default is bundled into this descriptor fix.
- 2026-07-25: Replaced the catalogue-only 100 GiB placeholder with an executable, deterministic
  Parquet stress generator and product-shaped runner. The generator creates one bounded base file
  and hard-links it into the requested file cardinality, so setup cost is independent of represented
  input size while CDF still decodes and commits every selected partition. Its Arrow batches and
  Parquet row groups share the same explicit bound; a fresh-hat review caught and removed the
  `ArrowWriter` default row-group retention that would otherwise have invalidated the 64 MiB setup
  claim. Generator RSS is measured separately from the timed product run. The runner performs a
  real governed files -> canonical package -> Parquet destination run, verifies the package,
  receipt, and checkpoint, and rejects managed-memory or process-RSS ceiling violations.
- 2026-07-25: The corrected local four-partition smoke represented 320,870,976 logical bytes and
  1,572,864 rows. Generator peak RSS was 51,953,664 bytes with a 13,369,624-byte peak Arrow batch.
  CDF completed in 638 ms, wrote all rows across four canonical segments, verified the package and
  committed checkpoint, peaked at 447,873,024 process bytes and 761,528,610 managed bytes under the
  respective 2 GiB and 1.5 GiB authorities, and required no spill. The no-spill observation is
  recorded honestly: it proves the bounded steady-state pipeline did not need disk spill for this
  case, not that the independent spill failure laws are satisfied.
- 2026-07-25: The first repaired EC2 run at commit `980b8312` crossed canonical construction under
  `MemoryMax=2G` and exposed the next independent frontier: the already-selected Parquet staged
  path had not reserved its 16 MiB writer floor before upstream work occupied the ledger. That P0
  is closed by `.10x/tickets/done/2026-07-25-p0-staged-writer-memory-headroom.md`; F3 remains the
  integration acceptance owner rather than absorbing its implementation.
- 2026-07-25: The untuned optimized binary completed the exact 100 GiB law under an enforced
  `MemoryMax=2G` and `MemorySwapMax=0` cgroup. It processed 530,841,600 rows and
  108,293,954,400 represented bytes into 500 segments in 263.493 seconds. Peak process RSS was
  1,657,630,720 bytes, peak managed memory was 1,610,598,707 bytes, generator peak RSS was
  185,831,424 bytes, and package, receipt, and checkpoint verification all passed. No spill
  occurred; this closes the 100 GiB/RSS/OOM portion but not F3's independent spill and geometric
  laws.
- 2026-07-25: Closed the remaining matrix with a compact falsification set rather than redundant
  giant cross-products. On the same release host, 5, 20, and 100 GiB inputs peaked between 1.658
  and 1.701 GiB RSS while managed peak remained effectively flat at 1.5 GiB. A repeated 5 GiB
  execution differed by 26.6 MiB. Existing 512-segment and 231-segment observations bound live
  file descriptors by scan concurrency rather than segment cardinality. Focused laws passed for
  forced spill, spill exhaustion/cleanup, exact-row dedup, slow-consumer backpressure, compressed
  remote streaming, bounded remote metadata, staged-writer progress, and impossible-budget
  rejection.

## Evidence

- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-runtime -p cdf-package -p
  cdf-engine -p cdf-project -p cdf-dest-duckdb`: pass; 36 DuckDB, 193 engine, 81 package, 214
  project, 148 runtime, seven build-graph, and all doc tests passed (13 intentional slow/performance
  ignores across these suites).
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-runtime -p cdf-package -p
  cdf-engine -p cdf-project -p cdf-dest-duckdb --all-targets -- -D warnings`: pass.
- `cargo fmt --all -- --check` and `git diff --check`: pass.
- Direct execution of
  `cdf_dest_duckdb tests::staged_ingress_retains_no_segment_count_file_handles --exact`
  under `ulimit -n 64`: pass while staging 512 segment capabilities; the test observed zero
  segment-file opens before final scanning.
- Clean copied-workspace release `cdf run flolake.transactions --to
  duckdb://.cdf/fd-smoke.duckdb` under `ulimit -n 64`: all 231 canonical segments (1.29 GB) were
  published and admitted without descriptor exhaustion, reaching DuckDB materialization in 37.00
  seconds before the independently recorded 3.3 GiB wide-table memory ceiling stopped the run. The
  observation proves the reported cardinality no longer controls descriptor count; it does not
  claim a successful destination commit or close the ticket's independent 100 GB/RSS stress matrix.
- `CDF_STRESS_LAB=target/debug/cdf-p3-lab tools/run-constant-memory-stress.sh <empty-root> 4
  67108864 2GiB`: pass on the local macOS host. `summary.json`, generator timing, process timing,
  CDF's JSON report, package verification, and destination output were retained in the ephemeral
  smoke root. This proves the executable law and its assertions at 0.32 GiB; the required enforced
  100 GiB release-host observation is recorded below.
- Exact EC2 100 GiB / 2 GiB product law: pass in 263.493 seconds with 1,657,630,720 bytes peak RSS,
  1,610,598,707 bytes managed peak, 530,841,600 rows, 500 segments, and verified package
  `sha256:5ea1a0a9dfef85d274cde51a0711a3a42a6b60cb0bf9e6b47b43e905afdfd33e`,
  destination receipt, and checkpoint. This proves constant RSS at the required scale; it does not
  prove the still-independent spill and geometric-series cases.
- `.10x/evidence/2026-07-25-p3-f3-constant-memory-matrix.md` records the raw 5/20/100 GiB
  summaries, repeated-run comparison, exact 64 MiB typed failure, focused spill/backpressure/
  compression/metadata laws, procedures, and limits.
- The geometric series produced 25, 100, and 500 canonical segments with peak RSS of
  1,701,265,408, 1,670,701,056, and 1,657,630,720 bytes respectively. The largest-minus-smallest
  spread is 43,634,688 bytes (2.6% of the smallest observation) and slopes downward rather than
  with input size.
- The impossible-budget product run exited 5 with a Data error before creating `.cdf`; the message
  named the 64 MiB request, 64 MiB minimum working set, 512 MiB native headroom, and both
  corrections.

## References

- `.10x/specs/constant-memory-proof.md`
- `.10x/specs/performance-lab-and-envelope.md`

## Review

Closure review passes. The generator is bounded and deterministic; product execution—not a mock
operator—performs every governed stage and independently verifies package, receipt, checkpoint,
row count, managed peak, and process RSS. The geometric observations share one host, binary,
dataset recipe, cgroup authority, and timed-region policy. Semantic specialty cases use focused
tests rather than weakening the main workload or forcing performance-degrading spill into a path
that does not require it. F4 retains the distinct 1 TiB, device-saturation, and permanent-schedule
acceptance.

## Retrospective

The first 100 GiB failure was not one monolithic memory bug. Exact evidence exposed successive
boundaries: double-accounted canonical inputs, late destination working-set admission, an omitted
staged handoff, and finally allocator arena retention outside the live-object ledger. Closing each
as a bounded owner kept the stress ticket from becoming the implementation dumping ground. Future
scale laws should always assert both managed ownership and process RSS, and should force spill in a
dedicated semantic case rather than penalizing an ordinary streaming path that can stay in memory.
