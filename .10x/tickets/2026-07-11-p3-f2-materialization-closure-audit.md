Status: active
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-f-constant-memory-guarantee.md
Depends-On: .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md, .10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md, .10x/tickets/2026-07-11-p3-b13-native-format-matrix.md, .10x/tickets/done/2026-07-11-p3-d5-bulk-path-matrix.md, .10x/tickets/2026-07-11-p3-e4-package-io-envelope.md, .10x/tickets/done/2026-07-11-p3-g3-codec-download-decode-overlap.md

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

- 2026-07-14: Removed duplicate accounting at the durable-package-to-staged-destination edge. `DurableSegmentPayload` now moves Arrow batches and their existing leases together after segment publication; synchronous or background staged ingress retains that single ownership until consumption completes. This replaces the second queue reservation without a destination-specific runtime branch or compatibility shim.
- 2026-07-14: Falsification separated the real issue from the symptom. Removing the arbitrary four-worker encode cap first exhausted the canonical-segment reservation; draining completed encodes then exposed the duplicate staged-ingress reservation. With both canonical pressure relief and owned lease transfer, the 2.147 GB FineWeb-to-DuckDB run completed with a verified receipt and reduced package wall from 5.008 to 4.168 seconds (16.8%). Evidence: `.10x/evidence/2026-07-14-p3-f2-accounted-staged-payload-handoff.md`.
- 2026-07-14: A 2.15 GB FineWeb isolation matrix falsified the prior assumption that DuckDB's bounded two-segment input window also bounded its native residency. The same CDF source/package path peaked near 909 MB with the Parquet destination and 3.25 GB with DuckDB; raw Arrow decode at the actual ~1k-row row-group grain peaked near 58 MB. The package-long DuckDB transaction was the dominant unaccounted owner.
- 2026-07-14: Added an adapter-owned DuckDB native envelope rather than a runtime destination branch. Read/write connections now share bounded settings; the native memory limit is one quarter of the host managed pool clamped to 256 MiB–1 GiB, internal parallelism is the one thread already declared by the path, insertion-order retention is disabled because exact CDF row keys carry order authority, and DuckDB temporary scratch is capped at 1 GiB. Generic destination resolution injects execution services, and the adapter reserves the complete scratch ceiling from the shared spill coordinator before source execution.
- 2026-07-14: Final local FineWeb evidence measured 7.27 seconds and 1,385,006,712 bytes peak footprint versus the earlier uncapped 6.24 seconds and 3,248,835,536 bytes. Remote HTTPS-to-DuckDB measured 18.54 seconds versus an immediate 15.31-second curl floor and stayed near 1.62 GB peak footprint. Full adapter semantics passed 27 tests (one benchmark ignored); focused settings/reservation/failure tests and the product catalog fixture passed. Evidence: `.10x/evidence/2026-07-14-p3-f2-duckdb-native-resource-envelope.md`.
- 2026-07-14 cross-ticket verification discovery: P3 C3's full `cdf-benchmarks` test exposed that F2 updated the DuckDB catalog to `p3-f2-2026-07-14-v2` but did not regenerate the committed D5 destination report/performance envelope, which still claim `p3-d2-2026-07-11-v1`. `generated_envelope_matches_committed_golden` now correctly fails closed on that mismatch. F2 owns a fresh destination observation and envelope regeneration; changing only the evidence-version string would launder the old measurement and is forbidden.
- 2026-07-16: SX1 closure routed its remaining discovery/listing control-plane cardinality finding here. F2 must replace pre-bounded candidate/partition metadata materialization with ledger-owned bounded or spill-backed iteration and prove high-cardinality deterministic behavior; the completed source registry boundary is not reopened for that allocation-owner work.
- 2026-07-17: Deleted the obsolete in-memory `archive_package_to_parquet` report API instead of preserving a compatibility shim. Package archive callers now use the bounded persisted archive path; the benchmark matrix measures that production path; the old tests that only blessed resident Parquet byte reports were removed, while unsupported-type and duplicate-column coverage remains attached to the surviving persisted/transcode paths.
- 2026-07-17: Removed raw `PackageReader::read_segment` use from production replay and promotion/correction planning paths. Destination replay now hands both staged and materialized-package destinations the same verified/accounted commit-segment stream; promotion residual scans and correction package reads use verified canonical segment streams with a 64 MiB package window. The static production gate now forbids `read_segment(` in project runtime/promotion and destination production files, while the package archive's explicit reserved-window reader remains the only package-internal carve-out.
- 2026-07-17: Closed a metadata-cardinality slice for object-store remote inventory. `resolve_remote_matches_bounded` now streams provider listing entries through relative-path/glob filtering and retains only matching metadata, instead of first collecting every remote identity into a `Vec` and filtering afterward. The test fixture admits only one `FILE_IDENTITY_MEMORY_ENVELOPE_BYTES` lease while listing 129 identities; the old collect-all path would exhaust the discovery budget before reaching the single matching file, while the retained path releases each nonmatching identity as it is skipped.
- 2026-07-18: Closed the direct-destination-construction native-resource slice without adding a destination branch to project orchestration. `DestinationRuntime` now has a generic `bind_execution_services` hook. `ResolvedProjectDestination` invokes it at URI resolution, direct `run_project`, and replay entry; DuckDB uses the hook to calculate native memory/temp resources and reserve scratch through the shared spill authority, while the filesystem Parquet runtime refreshes its lazy execution services. `cdf-benchmarks` now binds execution services for direct DuckDB/Parquet/Postgres destinations so performance evidence measures the same resource envelope as project runs.

## Evidence

- Accounted segment ownership across staged ingress and the measured concurrency result: `.10x/evidence/2026-07-14-p3-f2-accounted-staged-payload-handoff.md`.
- DuckDB native transaction owner and bounded replacement: `.10x/evidence/2026-07-14-p3-f2-duckdb-native-resource-envelope.md`.
- 2026-07-17 resident archive API deletion:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-package --lib --locked -j 12` — passed, 52 passed, 3 ignored. Proves the persisted archive path, verification, bounded archive window, unsupported-type, and duplicate-column tests still cover the remaining package archive behavior.
  - `CARGO_BUILD_JOBS=12 cargo check -p cdf-benchmarks --locked -j 12` — passed. Proves the benchmark harness now compiles against `persist_package_parquet_archive` and no longer depends on the deleted resident report API.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-package -p cdf-benchmarks --all-targets --locked -j 12 -- -D warnings` — passed. Proves the touched package and benchmark crates are warning-clean after removing the legacy surface.
- 2026-07-17 verified segment stream migration:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-package production_commit_paths_cannot_collect_package_segments --locked -j 12` — passed. Proves the static gate rejects raw `read_segment(` in project runtime/promotion and destination production files.
  - `CARGO_BUILD_JOBS=12 cargo check -p cdf-project --locked -j 12` — passed. Proves replay and promotion compile against the verified/accounted segment stream migration.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-project promotion --lib --locked -j 12` — passed, 20 passed.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-project replay --lib --locked -j 12` — passed, 17 passed.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-project -p cdf-package --all-targets --locked -j 12 -- -D warnings` — passed.
- 2026-07-17 remote listing metadata-cardinality slice:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files remote_listing_filters_without_materializing_all_metadata --locked -j 12` — passed. Proves nonmatching remote identity metadata is released while scanning the listing, with only the matched file retained.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files --lib --locked -j 12` — passed, 80 passed, 1 ignored.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-files --all-targets --locked -j 12 -- -D warnings` — passed.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime source_frontier --lib --locked -j 12` — passed after reverting the rejected immediate-active-frontier experiment; proves the retained source-frontier contract remains tested.
- 2026-07-18 direct destination runtime service binding:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-project resolved_destination_binding_configures_direct_runtime_services --locked -j 12 -- --nocapture` — passed. Proves a directly constructed DuckDB runtime wrapped by `ResolvedProjectDestination` reserves native scratch through the shared spill authority when execution services are bound, and releases the reservation on drop.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb native_resource_tests --locked -j 12 -- --nocapture` — passed, 4 passed. Proves the adapter-owned native resource envelope still reserves/releases bounded scratch, honors explicit override knobs, and fails before use when scratch is unavailable.
  - `CARGO_BUILD_JOBS=12 cargo check -p cdf-benchmarks --locked -j 12` — passed. Proves benchmark destination runners compile against bound execution services instead of bypassing the project-run resource envelope.
  - `CARGO_BUILD_JOBS=12 cargo check -p cdf-conformance --locked -j 12` — passed. Proves the conformance replay helper compiles after binding shared test execution services to direct DuckDB replay destinations.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-project -p cdf-benchmarks --all-targets --locked -j 12 -- -D warnings` — passed.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-conformance --all-targets --locked -j 12 -- -D warnings` — passed.
- This is partial F2 evidence only. The ticket remains active because its cross-codebase owner matrix, static architecture gates, remaining metadata-cardinality closure, and geometric stress proof are not complete.

## Review

Verdict: concerns for F2 closure; pass for the bounded DuckDB slice.

- The retained implementation keeps all DuckDB policy inside `cdf-dest-duckdb`; runtime selection remains capability-driven and no destination id/path appears in orchestration.
- Native memory and scratch disk now have hard bounds; scratch capacity joins the shared spill authority before a production registry run and fails before source/destination mutation when unavailable.
- Residual significant F2 work remains: F1 has not yet made process native-headroom authority available through `ExecutionServices`; other native allocation owners remain unaudited; the cross-codebase owner matrix and geometric stress proof are still open. These are recorded closure blockers, not waived claims.
- The staged-payload slice passes adversarial ownership review: publish precedes handoff; send, hook, acknowledgement, and worker failures all drop the owned payload and release its leases; sliced segments may share lease ownership through the lease's reference-counted token without releasing the physical allocation early. Generic orchestration still branches only on `DestinationIngress` capability.

## Retrospective

The misleading signal was a bounded CDF queue beside an unbounded native transaction. Queue capacity proves only CDF-owned residency; every database/parser/compression boundary needs its own measured native envelope. A single wide-text fixture exposed the gap immediately, while the TLC-shaped benchmark had been too narrow to falsify it. Future destination closeout must include both narrow numeric and wide variable-length schemas, and must compare the identical source/package path across destinations before assigning memory to the decoder.

The four-worker segment cap was another symptom disguised as policy. A cloned `RecordBatch` is shared ownership of the same buffers, not a new allocation that should be independently reserved. Moving the existing lease with the payload made the actual lifetime explicit and allowed hardware concurrency to be selected from CPU and memory evidence instead of an arbitrary constant.
