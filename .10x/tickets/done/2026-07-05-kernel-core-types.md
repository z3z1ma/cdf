Status: done
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-bootstrap-rust-workspace.md

# Implement kernel core types

## Scope

Implement `cdf-kernel` types and traits for resources, descriptors, capabilities, batches, Arrow metadata helpers, contracts as kernel values, package/receipt identities, destination abstractions, positions, scopes, state deltas, errors, and result types. Owns `crates/cdf-kernel/**`.

## Acceptance criteria

- Public kernel APIs expose arrow-rs and standard/runtime-neutral types only.
- `ResourceStream`, `QueryableResource`, `ResourceDescriptor`, `ResourceCapabilities`, `Batch`, `SourcePosition`, `Receipt`, `StateDelta`, typed positions, and scope keys exist with serde support where artifacts require it.
- Source-name, semantic, and null-origin metadata helpers are available and tested.
- Error taxonomy includes `Transient`, `RateLimited`, `Auth`, `Contract`, `Data`, `Destination`, and `Internal`.

## Evidence expectations

Record kernel unit tests, serde round-trip tests, and dependency-boundary evidence.

## Explicit exclusions

No SQLite, DataFusion, package file I/O, destination drivers, CLI, or Python.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-05: Assigned to worker subagent `019f34b6-36cf-7f30-b36b-e29b741018f2` for kernel core types and dependency-boundary implementation.
- 2026-07-05: Worker 2 implemented `cdf-kernel` core types, runtime-neutral resource/destination traits, Arrow metadata helpers, serde artifact values, and focused unit tests. Evidence recorded in `.10x/evidence/2026-07-05-kernel-core-types.md`.
- 2026-07-05: Repaired parent review issues by deriving `PartitioningCapabilities::default`, making `ResourceStream` the public resource trait, and renaming the pinned batch stream alias to `BatchStream`. Full locked check/clippy/test matrix passed and was appended to evidence.
- 2026-07-05: Added mutation-focused tests for `CdfError` display output, `SourcePosition::version()` variants, and negative `Receipt::covers_state_delta` cases. The requested kernel test/clippy/mutation commands passed with zero missed mutants; evidence appended.
- 2026-07-06: Parent verified the implementation with `QUALITY.md` gates, recorded `.10x/evidence/2026-07-06-kernel-quality-gates.md`, reviewed `.10x/reviews/2026-07-06-kernel-core-types-review.md`, and closed the ticket.

## Blockers

None.
