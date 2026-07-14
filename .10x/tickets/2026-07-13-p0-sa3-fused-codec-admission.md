Status: open
Created: 2026-07-13
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-13-p0-single-crossing-schema-admission.md
Depends-On: .10x/tickets/2026-07-13-p0-sa1-deferred-admission-plan-ir.md, .10x/tickets/2026-07-13-p0-sa2-metadata-inventory-observation-cache.md

# P0 SA3: fused codec observation and extraction

## Scope

Fuse format detection, physical schema observation, reconciliation, and retained first-window extraction for registered file/REST codecs under one accounted source stream.

## Non-goals

No dynamic-language producer lifecycle or same-run typed promotion.

## Acceptance criteria

- JSON/NDJSON/CSV open once, retain exact first data, and continue the same stream.
- Full-scan remote Parquet transfers data pages once; selective ranges remain generation-bound and plan-recorded.
- Exhaustive run-time observation reuses one live stream or exact spool.
- Drift produces compiled residual/quarantine/fail verdicts and never silently mutates the pin.

## References

- `.10x/specs/single-crossing-schema-admission.md`
- `.10x/tickets/2026-07-11-p3-b5-json-codecs.md`
- `.10x/tickets/2026-07-11-p3-g3-codec-download-decode-overlap.md`

## Assumptions

SA1 owns verdict semantics; SA2 owns inventory/cache identity.

## Journal

Pending.

## Blockers

Depends on SA1 and SA2.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.

