Status: open
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md

# Iceberg F2: Arrow 59 dependency and source-crate foundation

## Scope

Select and admit an Apache Iceberg Rust revision aligned to CDF Arrow/Parquet 59, establish the dependency feature/pin policy and first-party `cdf-source-iceberg` crate boundary, and prove no Arrow-major conversion or second DataFusion tuple enters the graph.

## Non-goals

No catalog network calls, scans, object-access implementation, Glue external tables, source-position change, or product registration.

## Acceptance Criteria

- The admitted Iceberg core/catalog dependency resolves on Arrow/Parquet 59 with no `iceberg-datafusion` hot-path dependency.
- Supply-chain, advisory, license, MSRV, build-time/size, feature, and pin evidence is recorded; any fork/revision has an explicit upstream/removal trigger.
- `cargo tree` proves one first-party Arrow/Parquet major and no unintended DataFusion tuple.
- The new source crate exposes no Iceberg/AWS types to kernel/runtime/project/engine and has descriptor/config-schema golden foundations.
- Clean and incremental build impact is measured rather than assumed.

## References

- `.10x/decisions/iceberg-glue-source-boundaries.md`
- `.10x/decisions/arrow-datafusion-tuple-policy.md`
- `.10x/decisions/datafusion-git-pin-arrow59-tuple.md`
- `.10x/specs/iceberg-source.md`

## Assumptions

- User-ratified 2026-07-19: a tightly pinned Apache revision/fork is acceptable when upstream has no Arrow 59 release; permanent conversion is not.

## Journal

None yet.

## Blockers

None for local investigation. Creating or pushing an external fork requires explicit external-write confirmation.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
