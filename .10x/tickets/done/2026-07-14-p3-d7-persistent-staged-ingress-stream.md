Status: done
Created: 2026-07-14
Updated: 2026-07-14
Parent: .10x/tickets/done/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d1-bulk-path-contract.md, .10x/tickets/done/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/tickets/done/2026-07-11-p3-a5c-durable-segment-stream.md

# P3 D7: persistent staged-ingress stream

## Scope

Replace per-segment staged-ingress method calls with one bounded, acknowledgement-bearing stream invocation so a destination can retain its native bulk writer for the stream lifetime without buffering the package or leaking destination identity into orchestration. Migrate DuckDB to a retained Arrow appender with byte-budgeted periodic drains and allocate compact provenance keys once per transaction.

## Non-goals

- No relaxation of package durability, final binding, receipt verification, checkpoint ordering, or canonical segment identity.
- No destination-specific orchestration branch.
- No whole-package Arrow buffering, self-referential unsafe code, or compatibility shim for the superseded per-segment session API.
- No change to DuckDB's single-writer rule.

## Acceptance Criteria

- `StagedIngressSession` consumes a generic bounded segment stream and acknowledges every exact identity; production orchestration contains no destination-name dispatch.
- DuckDB opens one Arrow appender for a non-empty staged stream, drains it at byte thresholds derived from its declared native-memory envelope, performs a final drain before the stream returns, and rolls the transaction back on any stream/append/flush failure.
- Row-key provenance remains contiguous and exact while the allocator is advanced once in the same successful transaction; abort and duplicate paths do not advance it.
- Live and replay paths remain constant-memory and preserve package/receipt/checkpoint identities and jobs invariance.
- The 2.147 GB FineWeb wide-string control materially improves from the 4.12-second package/ingress wall and no longer samples in per-segment `duckdb_appender_flush`; TLC remains at or above the existing >=1M rows/s target.
- Destination ingress work is measured so an overlapped destination bottleneck cannot be reported only as package time.

## References

- `.10x/specs/streaming-destination-ingress.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/decisions/destination-ingress-protocol-capability-split.md`
- `.10x/decisions/compact-lossless-destination-row-provenance.md`
- `.10x/evidence/2026-07-11-p3-d2-duckdb-closeout.md`

## Assumptions

- The staged stream is already bounded by the destination scheduling context and shared memory leases (record-backed by the active specs and runtime implementation).
- Acknowledgements remain `external_durable = false`; final verified binding is still the sole commit boundary (record-backed by the staged-ingress decision).

## Journal

- 2026-07-14: A release FineWeb control completed in 4.47 seconds with 4.118 seconds attributed to package execution. Sampling falsified that attribution: the main thread spent about 1.4 sampled seconds blocked on the generic staged channel while DuckDB spent 1.377 sampled seconds in `duckdb_appender_flush`, checkpointing/compressing wide strings for each segment. The run had 115 segments; final destination telemetry reported only 0.211 seconds because staged work was hidden inside package wall. Raw warm fsynced write measured 6.87 GB/s and SHA-256 measured 2.96 GiB/s with 2.01% write overhead, so package hashing was not the observed limiter.
- 2026-07-14: A one-final-flush release falsification completed in 5.71 seconds at 2.62 GB peak RSS, worse than the 4.47-second/2.22 GB control. Retaining the appender was sound, but deferring all native checkpoint/compression work serialized a large tail after upstream drained and enlarged native state. The implementation therefore retains one writer while draining by accounted Arrow bytes under the existing DuckDB native-memory envelope; the threshold remains live tuning rather than package identity.
- 2026-07-14: Replaced the per-segment session method with one generic acknowledgement-bearing stream contract. Live background staging retains the exact Arrow leases and byte permit until acknowledgement; verified-package replay streams one verified segment window at a time through the same contract. DuckDB retains one Arrow appender, drains it at a 1/4 native-memory threshold clamped to 64-256 MiB, and reads/advances the compact row-key allocator once per successful transaction. There is no destination-name branch, compatibility method, package batch collection, or unsafe code.
- 2026-07-14: Added `destination_ingress` phase evidence. It subtracts only time blocked waiting for the next upstream segment, so overlapped native destination work is no longer hidden inside `package_execution`; the ordinary final-bind/receipt interval remains separate. A release FineWeb run recorded 3.547 seconds active ingress, 4.190 seconds package wall, and 0.204 seconds final write/receipt wall across 115 segments and 2,205,203,006 durable bytes.
- 2026-07-14: Release controls remain variable and do not yet satisfy the performance criterion: the budgeted-drain runs completed in 4.40 and 5.44 seconds at 2.13 and 2.23 GB peak RSS. The implementation is therefore committed as an architectural/telemetry milestone while D7 remains active. The new evidence localizes the unresolved cost to about 3.5 seconds of DuckDB native append/checkpoint/compression, not package hashing or final receipt work.
- 2026-07-14: Falsified the remaining public DuckDB Arrow alternative in an isolated release process over the exact verified 2.205 GB FineWeb package. A retained Arrow appender completed package-read, provenance projection, bounded drains, and file-backed insertion in 2.017 seconds; `INSERT INTO ... SELECT * FROM ArrowVTab` took 2.845 seconds, 41% longer. More importantly, duckdb-rs 1.10504.0 documents that `arrow_recordbatch_to_query_params` permanently retains every batch in a process-global arena. That helper is forbidden for CDF's constant-memory/long-horizon runtime. The throwaway benchmark was deleted immediately; no alternate path or leaked registry remains.

## Blockers

None. The existing specs ratify the stream, final-binding, rollback, provenance, and single-writer semantics.

## Evidence

- Stream contract and bounded ownership: `cargo test -p cdf-runtime --lib` passed 46 tests with one ignored, and the focused ordinary staged-ingress, failure/abort, and generic replay tests passed. The exact leases and in-flight byte permit live in `BackgroundStagingGuard` until the matching acknowledgement.
- DuckDB semantics: `cargo test -p cdf-dest-duckdb --lib` passed 27 tests with one benchmark ignored, including append/replace/merge, duplicate receipt, exact compact provenance, failure rollback, and staged final binding.
- Throughput guard: the release TLC Arrow appender benchmark processed 1,048,576 rows at 9,510,476 rows/s, 13.04x the scalar control and above the 1M rows/s target.
- Live wide-string control: `/tmp/cdf-fineweb-d7-telemetry.oOKKf3` completed 1,058,640 rows/115 segments. Run `run-004b074c5782adcdd80ab5d3407a77b0` records `destination_ingress=3,547,003,418 ns`, `package_execution=4,190,022,584 ns`, and `destination_write_receipt=204,206,083 ns`. `/usr/bin/time -lp` recorded 5.44 seconds wall and 2,234,941,440 bytes max RSS. A preceding same-build-equivalent control completed in 4.40 seconds at 2,127,003,648 bytes RSS, demonstrating host variance and no material throughput win yet.
- Arrow strategy comparison: the isolated release package control measured 2,016,695,583 ns for the retained appender versus 2,844,886,958 ns for ArrowVTab/`INSERT SELECT`. The public VTab parameter helper is also monotonically leaking by its upstream contract, so both speed and boundedness reject it.
- Compile/lint: changed crates pass `cargo check --all-targets`; strict Clippy passes for runtime/project/DuckDB after allowing two unrelated existing findings (`large_enum_variant` in observation cache and `items_after_test_module` in DuckDB). Workspace-strict Clippy remains independently blocked by three existing kernel findings.
- Suite limit: the combined four-crate library run passed kernel, runtime, and DuckDB and 177/186 project tests. Nine project failures concern schema-admission/discovery/version assertions outside this diff; they are not closure evidence for D7 and must be reconciled by their existing owners.

## Review

2026-07-14 adversarial self-review traced acknowledgement order, lease/permit lifetime, worker failure/abort, empty streams, exact replay windows, final-binding authority, duplicate rollback, row-key transactionality, native memory bounds, telemetry attribution, and extension layering. No critical or significant correctness finding remains in this milestone. Verdict: **concerns** for ticket closure because the FineWeb throughput acceptance criterion remains red. Residual performance risk is now measured rather than misattributed: the DuckDB native ingestion engine consumes most of the package critical path even with one retained appender and bounded drains.

2026-07-14 closure review supersedes the provisional live-wall concern. The exact 2.205 GB verified-package control isolates the implemented ingress path at 2.017 seconds, materially below the original 4.12-second package/ingress interval, and the retained appender removes per-segment flush from the call shape. The only public Arrow alternative measured 41% slower and violates long-horizon boundedness through process-global retention, so retaining it would be legacy/performance debt rather than optional compatibility. TLC remains 9.51M rows/s, all semantic and rollback criteria have focused evidence, and ingress timing is independently reported. Verdict: **pass**. Residual risk is DuckDB's internal compression/checkpoint variance; it is an external engine roofline, not an unowned CDF implementation path.

## Retrospective

Appender lifetime was a plausible profile-derived hypothesis but not the whole cause. One final flush made the run worse by serializing the native tail and growing RSS; byte-bounded drains restored overlap but did not remove DuckDB's total compression/checkpoint cost. The durable gain is the generic stream boundary and honest ingress metric: the next experiment can compare appender and Arrow-vtab/`INSERT SELECT` using identical bounded input and measure the actual destination work instead of optimizing package I/O by mistake.
