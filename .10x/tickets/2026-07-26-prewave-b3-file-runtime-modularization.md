Status: open
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

# Modularize the file-source runtime

## Scope

Split `cdf-source-files/src/runtime.rs` along existing ownership boundaries—discovery,
task authority, planning/inventory, input preparation/spooling/cache, decode/validation, and
glob/format/compression resolution—without changing public behavior or inserting compatibility
re-exports.

## Non-goals

- No new source abstraction, transport policy, format behavior, or performance knob.
- No arbitrary line-count split or cyclic internal module graph.
- No change to file inventory, plan, schema, task, package, or receipt identity.

## Acceptance criteria

- The 9,138-line monolith is replaced by cohesive internal modules with one-way dependencies.
- Public exports and `FileSourceDriver` construction remain minimal and documented.
- No module becomes a miscellaneous helper bucket or exceeds the monolith's concern count.
- Local/HTTP/object-store, discovery/pinned, multi-file, compression, preview/run, retry, and
  payload-cache conformance pass unchanged.
- Focused TLC/FineWeb performance evidence remains within ordinary variance of the current floor.

## References

- `.10x/specs/data-onramp-file-sources-transports.md`
- `.10x/decisions/native-format-driver-and-byte-source-boundary.md`
- `.10x/knowledge/source-destination-extension-invariant.md`

## Assumptions

- Source-backed: the monolith already has separable type/function clusters and existing lower
  transport/format/task-store boundaries.

## Journal

- 2026-07-26: Function/type inventory identified six existing concerns; this ticket is a
  behavior-preserving topology change.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
