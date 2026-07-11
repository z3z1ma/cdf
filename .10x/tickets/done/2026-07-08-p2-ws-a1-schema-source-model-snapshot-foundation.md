Status: done
Created: 2026-07-08
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-a-discovery-compiler-stage.md
Depends-On: .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md

# P2 WS-A1 schema source model and snapshot foundation

## Scope

Create the first schema-discovery foundation without implementing every source probe: separate unpinned discover intent from pinned snapshot evidence, add a project-owned schema snapshot artifact model/store, and remove wording that treats discover mode itself as unsupported when a concrete snapshot hash is available.

Owned write scope:

- `crates/cdf-kernel/src/resource.rs`
- `crates/cdf-project/src/**` for snapshot artifact and lockfile model plumbing
- `crates/cdf-declarative/src/compiled.rs` only for schema-source construction compatibility
- focused tests in the same crates
- this ticket's evidence/review records

## Acceptance criteria

- Kernel/project models can represent:
  - declared schema with concrete hash;
  - discover intent before pinning;
  - pinned discovered snapshot with concrete hash and snapshot metadata;
  - hints mode, even if hint application is a later child.
- Schema snapshot artifact JSON can serialize an Arrow schema plus metadata and deterministic hash input.
- Snapshot files use the `.cdf/schemas/<resource>@<hash>.json` path convention.
- `cdf-project` validation no longer rejects every discovered schema source solely because the variant is discovered; it may still fail closed when no pinned hash exists.
- Existing package/run behavior remains deterministic and existing tests either pass or are updated only for the model split.

## Evidence expectations

Record focused evidence for:

- `cargo test -p cdf-kernel -p cdf-project --locked`
- focused schema-source/snapshot tests by name
- `cargo clippy -p cdf-kernel -p cdf-project --all-targets --locked -- -D warnings`
- `cargo fmt --all -- --check`
- `git diff --check`
- jscpd scoped to touched Rust files

## Explicit exclusions

This ticket does not implement Parquet/REST/SQL discovery probes, first-use auto-pin, schema CLI commands, or full lockfile diff rendering.

## Progress and notes

- 2026-07-08: Opened after inspection found `SchemaSource::Discovered { schema_hash: Option<SchemaHash> }` in `cdf-kernel` and fail-closed discovered-schema checks in `crates/cdf-project/src/runtime/validation.rs`.
- 2026-07-09: Implemented the schema-source split, recursive project-owned schema snapshot artifact/store, lockfile snapshot reference plumbing, and pinned-schema validation path. Closure evidence is `.10x/evidence/2026-07-09-p2-ws-a1-schema-source-model-snapshot-foundation.md`; review is `.10x/reviews/2026-07-09-p2-ws-a1-schema-source-model-snapshot-foundation-review.md`.
- 2026-07-09: Parent integration added cross-crate compatibility updates for existing descriptor/test constructors, hardened snapshot reference path validation against traversal, reduced the schema snapshot Arrow type conversion complexity hotspot, regenerated the live-run golden expected hashes for the ratified model change, and reran the full workspace plus QUALITY scanner gates recorded in the evidence.

## Blockers

None.
