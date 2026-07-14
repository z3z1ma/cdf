Status: active
Created: 2026-07-13
Updated: 2026-07-13
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

## Blockers

None. SA0-SA2 are done.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
