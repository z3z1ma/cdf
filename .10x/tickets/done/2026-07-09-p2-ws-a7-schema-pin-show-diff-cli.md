Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-a-discovery-compiler-stage.md
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
- 2026-07-09: Worker implementation added parser/help support and schema command handling for `cdf schema pin`, `cdf schema show`, and `cdf schema diff`; reused the generic dispatcher for local Parquet, Postgres catalog, and REST sample discovery; added snapshot show/diff reports, no-write diff behavior, pin snapshot writes, and an existing-lockfile resource update helper. Focused CLI tests were added for parser help, local Parquet pin/show/diff, unsupported no-lock pin reporting, REST changed diff/no-write/redaction, and Postgres pin/redaction when the live harness is available.
- 2026-07-09: Parent integration fixed the concurrent file-runtime compile blocker, reran focused CLI schema tests, duplication checks, complexity checks, Rust quality gates, and security/supply-chain checks. Closure evidence is `.10x/evidence/2026-07-09-p2-ws-a7-d3-i2-batch.md`; closure review is `.10x/reviews/2026-07-09-p2-ws-a7-d3-i2-batch-review.md`.

## Blockers

None. Creating a brand-new lockfile remains explicitly excluded from this child; `cdf schema pin` writes the snapshot and reports the unsupported lockfile-reference piece when no existing lockfile can be updated safely.
