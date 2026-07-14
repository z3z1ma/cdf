Status: done
Created: 2026-07-13
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md
Depends-On: .10x/tickets/done/2026-07-13-p0-sa0-cold-discovery-final-plan-lifecycle.md, .10x/tickets/done/2026-07-13-p0-sa1-compiled-stream-admission-plan.md, .10x/tickets/done/2026-07-13-p0-sa2-metadata-inventory-observation-cache.md

# P0 SA3: fused codec observation and extraction

## Scope

Fuse format detection, physical schema observation, reconciliation, and retained first-window extraction for registered file/REST codecs under one accounted source stream; hand any same-command discovery payload spool or retained batches into final-plan execution.

## Non-goals

No dynamic-language producer lifecycle, same-run typed promotion, selective Parquet pushdown/range-policy implementation, or format-specific full-content inference engine/operator configuration. P3 B2/G2 and B5 own those engines; SA3 owns the neutral retained-source handoff they consume.

## Acceptance criteria

- Sequential JSON/NDJSON/CSV discovery opens one source invocation, retains exact first data, and continues that same stream into execution.
- Full-scan remote Parquet transfers data pages once per command; transformed/seekable discovery that materializes a payload hands the exact spool to execution.
- A sequential driver declaring bounded-content or full-content discovery receives the same live-stream/retained-window handoff; small unspooled bounded probes may be reread only within recorded limits.
- Drift produces compiled residual/quarantine/fail verdicts and never silently mutates the pin.

## References

- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/tickets/2026-07-11-p3-b5-json-codecs.md`
- `.10x/tickets/2026-07-11-p3-b2-parquet-codec.md`
- `.10x/tickets/2026-07-11-p3-g2-range-readahead-spool-controller.md`
- `.10x/tickets/2026-07-11-p3-g3-codec-download-decode-overlap.md`

## Assumptions

SA1 owns verdict semantics; SA2 owns inventory/cache identity.

## Journal

- 2026-07-14: Activated after SA2 closed. Source inspection confirms the governing defect remains exact and bounded: transformed/seekable local and remote discovery create `AccountedSpool`, discover from it, and immediately drop it; `PreparedDiscoveredResource` carries only compiled schema/discovery facts, so final execution cannot consume the already materialized generation. Row-oriented drivers already have incremental accounted streams, providing the retained-window seam without reintroducing format branches into project orchestration.
- 2026-07-14: Landed the source-neutral, driver-keyed prepared-payload handoff through schema preparation and source resolution. File discovery now retains fully materialized transformed/seekable spools, or a disk-accounted bounded row window plus the still-open continuation; final-plan execution consumes the handoff exactly once. The handoff payload is type-erased at the runtime boundary so REST can reuse the same facility without teaching orchestration about file or REST payload shapes.
- 2026-07-14: Adversarial implementation pass found and fixed two boundary defects before commit: the discovery wrapper advertised upstream `reopenable` even though it is single-invocation, and the internal local replay spool inherited a caller chunk preference outside `LocalByteSource` bounds. A focused stream law now consumes one discovery chunk, drops the discovery reader at the plan barrier, replays the exact bytes, continues the same source stream, and proves one underlying open.
- 2026-07-14: REST discovery and execution now share the same driver-keyed prepared-payload store without exposing REST payload shapes to project or CLI orchestration. Discovery retains the exact first `HttpResponse` under a source-memory lease; execution consumes it once, preserves rate-limit/pagination response semantics, and contacts the transport only for subsequent pages. The superseded raw-transport discovery API was deleted rather than retained as a shim; discovery/runtime dependencies remain phase-specific and share only the neutral handoff.
- 2026-07-14: Adversarial review found that full-content coverage existed in schema evidence but not in the format-driver capability vocabulary. Added `FormatDiscoveryKind::FullContent` and routed sequential bounded/full-content drivers through the same retained-window/live-continuation handoff. SA3 owns this source-crossing capability; P3 B5 owns the still-open constant-memory JSON inference engine and operator configuration. The existing bounded collector is intentionally not widened to object size because that would falsely satisfy full-content semantics by materializing giant inputs.
- 2026-07-14: Closure audit corrected the ticket seam before review. Selective Parquet pushdown/range selection was already explicit open B2/G2 scope, and actual full-content JSON inference was already explicit B5 scope; neither is an SA3 closure claim. SA3's acceptance now names the neutral one-invocation/spool handoff it implemented, while the parent spec's end-to-end selective/full-content requirements remain open under those executable tickets and SA5 conformance. Added the previously missing explicit NDJSON retained-stream regression rather than inferring it from the generic stream law.

## Blockers

None. SA0-SA2 are done.

## Evidence

- Sequential row codecs: `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files --lib --locked --no-fail-fast` passed 42/42. `local_csv_discovers_and_streams_through_registered_driver`, `local_json_document_discovers_and_streams_through_registered_driver`, and `local_ndjson_discovery_replays_and_continues_the_same_source` delete the source path after discovery, then consume all rows from the retained exact window/continuation and leave zero pending payloads/source memory. `retained_sequential_window_replays_then_continues_one_source_invocation` separately asserts one underlying open across the plan barrier.
- Parquet/spool transfer: focused `cdf-project` tests `http_parquet_auto_pin_plan_preview_and_run_use_file_runtime` and `unversioned_http_parquet_runs_and_commits_terminal_content_identity` each passed 1/1. The former proves bounded footer discovery plus one sequential full-scan GET for each independent preview/run command and zero pinned pre-probe; the latter proves one weak-HTTP full transfer total across cold discovery and same-command execution. Source tests `gzip_parquet_composes_transform_spool_with_registered_format_driver` and `remote_parquet_full_scan_uses_verified_sequential_spool` pass, including delete-before-execution exact transformed-spool reuse.
- Generic full-content handoff: `bounded_and_full_content_drivers_share_the_retained_stream_handoff` passed inside the 42-test source suite. It proves any sequential driver declaring `BoundedContent` or `FullContent` enters the same neutral retained-stream path; P3 B5 remains the owner of the actual constant-memory JSON inference engine/configuration.
- REST handoff: focused project `generic_discover_prepare_autopins_rest_snapshot` and CLI `cold_rest_run_reuses_the_discovery_page_without_a_second_request` each passed 1/1. The project counter proves one request across discovery, snapshot compilation, and execution, then zero pending payloads/source memory; the CLI fixture can answer only one request.
- Immutable admission: focused CLI `pinned_multi_file_parquet_keeps_fixed_schema_and_admits_new_physical_schemas_in_stream` and `sampled_discovery_renders_every_cli_path_and_routes_unseen_drift_to_package_quarantine` each passed 1/1. They prove lock/snapshot bytes remain unchanged, extra values become residuals, incompatible files quarantine under the compiled rule, package admission evidence is replayable, and no current-file schema pre-scan mutates the pin.
- Codec boundary: `CARGO_BUILD_JOBS=12 cargo test -p cdf-format-parquet --lib --locked --no-fail-fast` passed 2/2. `CARGO_BUILD_JOBS=12 cargo check -p cdf-runtime -p cdf-source-files -p cdf-project --all-targets --locked` passed after the full-content capability addition.
- Limits: selective Parquet pushdown/range planning is still open under P3 B2/G2; full-content JSON inference/configuration and async REST decoding remain open under P3 B5; weak object-store provider body framing remains open under G1. None is claimed by this ticket, and all retain active owners.

## Review

Fresh adversarial closure review covered the implementation commits `eebcc9ca`, `49fd0ac0`, `ab97f72c`, the runtime/source/REST boundaries, exact assertions above, and ownership against the governing spec.

Findings:

- **Significant, resolved:** a provider-controlled object-store body could exceed CDF's pre-admitted window. The strong-generation path was repaired under G1 in `740b1e9a` to issue exact preconditioned CDF-sized windows; review then found and fixed the zero-byte generation-attestation hole before that repair committed. Weak-provider framing remains explicitly owned by G1.
- **Significant, resolved:** SA3's original wording could have falsely closed selective Parquet range planning and the full-content JSON inference engine. Those requirements were already executable B2/G2/B5 scope. The ticket seam and references now distinguish SA3's neutral retained-source capability from those still-open engines; the parent spec and SA5 retain end-to-end conformance authority.
- **Significant, resolved:** CSV and JSON had explicit same-source tests, but NDJSON relied on the generic retained-stream law. Added an explicit NDJSON delete-before-execution regression and included it in the complete source suite.
- **Minor, accepted with owner:** the REST discovery response is retained under the shared ledger after the synchronous transport has materialized it. B5 already owns replacing that transport/DOM boundary with async streamed/tape decoding, so no second compatibility path was added here.

Verdict: **pass**. No critical or significant SA3 finding remains. Residual risks are named above with active executable owners.

## Retrospective

The duplicate-transfer defect was not codec-specific; orchestration discarded live source authority at the discovery/final-plan barrier. A source-neutral, driver-keyed, single-use prepared-payload store fixed files and REST without adding format/source identity branches. The important guard is that payload shape remains private to its source crate while orchestration carries only type-erased ownership and retention.

The expensive review lesson was to audit ticket ownership as aggressively as code. A neutral seam can be complete while consumers that exploit it remain open; leaving the original broad prose would have made the backlog lie. Future closure reviews should map each clause to both an observed assertion and the exact ticket that owns any deferred engine behavior before running tests. Provider body framing was the other surprise: requested chunk size is not an allocation guarantee, so transport conformance must test adversarial provider frames, including empty and oversized frames, rather than only well-shaped loopback bodies.
