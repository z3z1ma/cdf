Status: active
Created: 2026-07-13
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md
Depends-On: .10x/tickets/2026-07-13-p0-sa1-compiled-stream-admission-plan.md

# P0 SA0: cold discovery to final-plan lifecycle

## Scope

Make an unpinned package-producing command consume its bounded discovery result exactly once, freeze the persistent or run-local snapshot, and compile the final execution plan directly from that result. Delete the post-auto-pin re-entry into ordinary pinned preparation.

## Non-goals

No observation cache, payload-spool handoff, decoder-loop fusion, dynamic producer lifecycle, or destination behavior.

## Acceptance criteria

- Cold `run|plan|preview` with persistent auto-pin performs one discovery lifecycle and compiles from its returned normalized schema/evidence.
- `--no-pin` freezes the identical run-local schema/plan authority without project writes.
- Snapshot/lock persistence does not trigger a second current-file discovery or alter the already compiled observation evidence.
- Ordinary pinned preparation loads/verifies the snapshot without source payload probes; current physical observations are not required to finalize the plan.
- Transport counters and regression tests replace the current behavior that calls pinned preparation after auto-pin.

## References

- `.10x/decisions/fixed-schema-discovery-and-stream-admission.md`
- `.10x/specs/schema-discovery-and-stream-admission.md`
- `.10x/specs/data-onramp-schema-intelligence.md`

## Assumptions

The user ratified fixed schema before final plan, direct cold-result reuse, and no current-schema pre-scan on pinned runs in the 2026-07-13 discovery-lifecycle correction.

## Journal

- 2026-07-13: Inspection identifies two concrete regressions: `prepare_resource_schema_for_cli` writes a new pin and immediately calls pinned effective-schema preparation, while the ordinary pinned branch calls current-file discovery before extraction. This ticket owns removing those lifecycle re-entries without weakening final-plan schema authority.
- 2026-07-14: Execution began from clean commit `53f6a45a`. The call-graph audit found the duplicate lifecycle in both the CLI (`prepare_resource_schema_for_cli`) and the project helper (`prepare_discover_resource_with_file_dependencies` through `attach_pinned_file_runtime`). Pinned preparation currently requires secret/transport/format dependencies solely to rediscover physical schemas; the replacement will hydrate and verify the snapshot plus linked discovery manifest from project artifacts only.
- 2026-07-14: The duplicate probe was removed and cold discovery's already-held physical schema catalog was compiled directly into the final cold plan. The HTTP Parquet lifecycle test then passed with the auto-pin request trace byte-for-byte equal to one standalone discovery trace. A pinned rerun correctly performs no preparation probe, but normalized source fields now require SA1's source-neutral admission program because the old per-file coercion evidence was manufactured by the prohibited pre-scan. This is a real dependency, not permission to restore discovery I/O.
- 2026-07-14: SA1 supplied the missing admission authority. The cold command now consumes its first discovery result directly; ordinary pinned preparation verifies only the snapshot and linked discovery manifest; and the registered-format execution fallback that called `driver.discover(...)` before decode was deleted. The HTTP Parquet regression now exercises a fresh pinned resource and records zero preparation requests, one sequential extraction GET, and zero ranged schema probes.
- 2026-07-14: Rebound cold discovery's effective identity to the final linked snapshot hash before final planning. This removes the former first-run-only schema identity: cold auto-pin, the committed checkpoint, and every later pinned command now name the same fixed schema epoch.
- 2026-07-14: The first independent review rejected closure because the integrated package evidence did not yet bind every execution outcome strongly enough for exact replay. No discovery pre-scan was restored. The repair instead carried the compiler's physical observations and the stream's exact positions through SA1's generic evidence model, including schema-only codec output for empty columnar inputs.

## Blockers

None. SA1's compiled admission operation is integrated and the pinned rerun is green.

## Evidence

- Cold single-lifecycle: `CARGO_BUILD_JOBS=12 cargo test -p cdf-project tests::http_parquet_auto_pin_plan_preview_and_run_use_file_runtime -- --exact --nocapture` passed. It compares cold auto-pin requests with one standalone discovery trace, then proves a fresh pinned preparation performs no transport calls and pinned execution performs one sequential GET with no range request.
- Artifact-only pin hydration: both exact `pinned_schema_preparation_*` project tests passed; one removes the source directory before preparation.
- CLI authority reporting: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli plan_local_parquet_discover_autopins_snapshot_and_reports_hash -- --nocapture` passed.
- Cold/pinned identity stability is covered by the local/HTTP ad-hoc, plan auto-pin, and run auto-pin CLI regressions; they now assert equality between the execution schema and the linked pinned snapshot rather than preserving the superseded schema-only identity.
- The repaired pinned multi-file lifecycle remains green: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli --lib pinned_multi_file_parquet_keeps_fixed_schema_and_admits_new_physical_schemas_in_stream -- --nocapture` passed with three files, including a zero-row Parquet file, and no preparation discovery pass.

## Review

Pending fresh adversarial review of the integrated SA0/SA1 batch.

## Retrospective

The duplicated work was not one bad transport call; it was an identity/lifecycle split. Reusing the first discovery result while retaining its pre-link schema-only hash still made the cold plan differ from the next pinned plan. The final linked snapshot must be minted before final plan compilation and is the epoch identity for both paths.
