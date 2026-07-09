Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-d2-file-manifest-run-aggregation.md, .10x/decisions/data-onramp-file-source-transport-manifest.md, .10x/specs/data-onramp-file-sources-transports.md

# P2 WS-D3 file manifest incrementality and no-op reruns

## Scope

Implement default `FileManifest` incrementality for append file resources: a run whose committed checkpoint already contains unchanged files MUST plan no duplicate extraction, and a run with only new or changed files MUST extract only those files.

This child is local-file first and MUST preserve the existing one-partition-per-modest-file shape from D1 and resource-level manifest aggregation from D2.

## Acceptance criteria

- A first local multi-file append run records a resource-level `SourcePosition::FileManifest` listing every loaded file with stable path, size, and checksum/ETag evidence.
- A second run over unchanged files is a fast explicit no-op: no package-producing extraction, no destination rows duplicated, and the run report/rendered output says no changed files were planned.
- Adding a new matching file makes the next run plan and load only that new file while committing an updated resource-level manifest containing old and new entries.
- Changing an existing file's identity makes the next run plan that file as changed.
- Replace-disposition file resources continue to plan all matched files unless a later ticket ratifies a different replacement policy.
- Checkpoint gating, package identity, replay determinism, and per-file source-position evidence remain intact.

## Evidence expectations

Focused `cdf-declarative` partition-filtering tests, `cdf-project` runtime tests proving first-run/unchanged-rerun/new-file behavior, CLI/rendered no-op evidence if surfaced through the report, `FileManifest` state assertions, and the normal quality gate set for touched Rust code.

## Explicit exclusions

This ticket does not implement remote manifest identity, HTTP template enumeration, cloud transports, compression, large-N coalescing, or S2 conformance closure.

## Progress and notes

- 2026-07-09: Opened as the next WS-D child after D2. This is the critical S2 state step before public HTTPS globs and `cdf add` can honestly claim incremental forever.

## Blockers

None for local append file resources.
