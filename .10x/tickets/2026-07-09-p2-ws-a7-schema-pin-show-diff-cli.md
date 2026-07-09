Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-a-discovery-compiler-stage.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-a6-rest-sample-discovery-autopin.md, .10x/specs/data-onramp-schema-intelligence.md

# P2 WS-A7 schema pin/show/diff CLI

## Scope

Complete the operator schema command surface started by A3-A6: `cdf schema pin <resource>`, `cdf schema show <resource>`, and `cdf schema diff <resource>` with P1 rendering and additive JSON.

## Acceptance criteria

- `cdf schema pin <resource>` runs the generic discovery dispatcher, writes or refreshes `.cdf/schemas/<resource>@<hash>.json`, updates the lockfile/project reference where the current project model supports it, and reports whether the pinned snapshot was added, unchanged, or changed.
- `cdf schema show <resource>` prints the currently pinned declared/discovered schema snapshot or a clear source-experience error when no pinned snapshot exists.
- `cdf schema diff <resource>` compares the currently pinned snapshot against a fresh probe and reports added, removed, type-changed, nullable-changed, and metadata-changed fields without package/destination/checkpoint writes.
- Commands support local Parquet, declarative Postgres table, and REST sample discovery through the generic dispatcher; unsupported archetypes fail with exact unsupported-slice remediation.
- Human output uses the renderer; JSON output is additive and stable; secret values are not leaked.

## Evidence expectations

CLI parser/help snapshots, schema command tests per supported archetype, no-write assertions, lockfile/snapshot fixture diffs where applicable, redaction tests, and normal quality gates.

## Explicit exclusions

This ticket does not implement `cdf add`, remote file discovery, Python/WASM discovery, or conformance golden paths.

## Progress and notes

- 2026-07-09: Opened because WS-A still lacks the explicit `pin/show/diff` CLI surface required by the P2 directive.

## Blockers

Project lockfile mutation support may need a narrow implementation inside this ticket; if the current lockfile model cannot store the refreshed snapshot reference without semantic churn, record the unsupported piece explicitly and still land show/diff plus snapshot writes.
