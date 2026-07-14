Status: active
Created: 2026-07-13
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md
Depends-On: .10x/tickets/done/2026-07-13-p0-sa0-cold-discovery-final-plan-lifecycle.md, .10x/tickets/done/2026-07-13-p0-sa1-compiled-stream-admission-plan.md, .10x/tickets/done/2026-07-13-p0-sa2-metadata-inventory-observation-cache.md

# P0 SA3: fused codec observation and extraction

## Scope

Fuse format detection, physical schema observation, reconciliation, and retained first-window extraction for registered file/REST codecs under one accounted source stream; hand any same-command discovery payload spool or retained batches into final-plan execution.

## Non-goals

No dynamic-language producer lifecycle or same-run typed promotion.

## Acceptance criteria

- JSON/NDJSON/CSV open once, retain exact first data, and continue the same stream.
- Full-scan remote Parquet transfers data pages once; selective ranges remain generation-bound and plan-recorded.
- Full-content cold discovery and transformed/seekable discovery reuse one live stream, retained batches, or exact spool; small unspooled bounded probes may be reread only within recorded limits.
- Drift produces compiled residual/quarantine/fail verdicts and never silently mutates the pin.

## References

- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/tickets/2026-07-11-p3-b5-json-codecs.md`
- `.10x/tickets/2026-07-11-p3-g3-codec-download-decode-overlap.md`

## Assumptions

SA1 owns verdict semantics; SA2 owns inventory/cache identity.

## Journal

- 2026-07-14: Activated after SA2 closed. Source inspection confirms the governing defect remains exact and bounded: transformed/seekable local and remote discovery create `AccountedSpool`, discover from it, and immediately drop it; `PreparedDiscoveredResource` carries only compiled schema/discovery facts, so final execution cannot consume the already materialized generation. Row-oriented drivers already have incremental accounted streams, providing the retained-window seam without reintroducing format branches into project orchestration.
- 2026-07-14: Landed the source-neutral, driver-keyed prepared-payload handoff through schema preparation and source resolution. File discovery now retains fully materialized transformed/seekable spools, or a disk-accounted bounded row window plus the still-open continuation; final-plan execution consumes the handoff exactly once. The handoff payload is type-erased at the runtime boundary so REST can reuse the same facility without teaching orchestration about file or REST payload shapes.
- 2026-07-14: Adversarial implementation pass found and fixed two boundary defects before commit: the discovery wrapper advertised upstream `reopenable` even though it is single-invocation, and the internal local replay spool inherited a caller chunk preference outside `LocalByteSource` bounds. A focused stream law now consumes one discovery chunk, drops the discovery reader at the plan barrier, replays the exact bytes, continues the same source stream, and proves one underlying open.
- 2026-07-14: REST discovery and execution now share the same driver-keyed prepared-payload store without exposing REST payload shapes to project or CLI orchestration. Discovery retains the exact first `HttpResponse` under a source-memory lease; execution consumes it once, preserves rate-limit/pagination response semantics, and contacts the transport only for subsequent pages. The superseded raw-transport discovery API was deleted rather than retained as a shim; discovery/runtime dependencies remain phase-specific and share only the neutral handoff.
- 2026-07-14: Adversarial review found that full-content coverage existed in schema evidence but not in the format-driver capability vocabulary. Added `FormatDiscoveryKind::FullContent` and routed sequential bounded/full-content drivers through the same retained-window/live-continuation handoff. SA3 owns this source-crossing capability; P3 B5 owns the still-open constant-memory JSON inference engine and operator configuration. The existing bounded collector is intentionally not widened to object size because that would falsely satisfy full-content semantics by materializing giant inputs.

## Blockers

None. SA0-SA2 are done.

## Evidence

- Partial file-codec slice: `CARGO_BUILD_JOBS=6 cargo test -p cdf-source-files --lib --no-fail-fast` passed 38/38, including retained sequential replay/continuation, CSV/JSON exact-spool reuse, transformed Parquet reuse, and remote Parquet sequential-spool laws.
- Partial project integration: focused `cdf-project` tests `unversioned_http_parquet_runs_and_commits_terminal_content_identity` and `object_store_gzip_ndjson_discovers_pins_and_executes_through_one_transport` passed. The former proves one full weak-HTTP transfer across cold discovery and execution; the latter proves retained transformed row payload execution.
- Compile boundary: `CARGO_BUILD_JOBS=6 cargo check -p cdf-runtime -p cdf-source-files -p cdf-project -p cdf-cli --all-targets` passed.
- REST handoff: `generic_discover_prepare_autopins_rest_snapshot` proves one recorded transport request across cold discovery, snapshot compilation, and execution, then zero pending payloads and zero retained source memory. CLI test `cold_rest_run_reuses_the_discovery_page_without_a_second_request` passes against a server that can answer only one request.
- REST regression surface: all 5 `cdf-source-rest` unit tests, the focused declarative discovery test, both project REST discovery tests, and the four-crate all-target check passed.
- Full-content capability seam: focused `bounded_and_full_content_drivers_share_the_retained_stream_handoff` passed, and the all-target runtime/source/project check passed. This proves future streaming full-content drivers inherit the same single-invocation handoff without a project/orchestration format branch; P3 B5 still owns the JSON engine and operator-facing configuration.
- Limits: final ticket closure review remains pending.

## Review

Pending.

## Retrospective

Pending.
