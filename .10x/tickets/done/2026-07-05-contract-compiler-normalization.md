Status: done
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md

# Implement contract compiler and normalization rules

## Scope

Implement contract policy models, trust presets, validation-program compiler, verdict lattice, type fidelity checks, identifier normalizer `namecase-v1`, nested/variant policy, transform descriptions, promotion/demotion event models, and serialization. Owns `crates/firn-contract/**` and narrowly required kernel additions.

## Acceptance criteria

- Validation program is serializable and total over supported rule outcomes.
- Decimal and timestamp fidelity rules reject silent lossy behavior.
- Identifier normalizer preserves source names in metadata and hard-errors post-normalization collisions.
- Trust presets compile to the policies in `.10x/specs/types-contracts-normalization.md`.
- PII redaction decisions are available to quarantine/package code.

## Evidence expectations

Record unit/property tests for verdict totality, decimal/timezone fidelity, normalizer collisions, nested/variant policy, and trust preset expansion.

## Explicit exclusions

No DataFusion `ExecutionPlan` implementation beyond data structures needed by the engine ticket.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-06: Assigned to contract worker in parallel with the package worker. Worker owns `crates/firn-contract/**` and may propose minimal `firn-kernel` additions only when required by `.10x/specs/types-contracts-normalization.md`; leave unrelated dirty `.gitignore` changes untouched.
- 2026-07-06: Implemented the contract policy/program model, trust presets, type fidelity checks, `namecase-v1`, nested/variant decisions, transform descriptions, promotion/demotion events, type-mapping decisions, and PII redaction decisions in `crates/firn-contract`. No `firn-kernel` additions were required.
- 2026-07-06: Evidence recorded in `.10x/evidence/2026-07-06-contract-compiler-normalization.md`. Contract tests, contract clippy, contract formatting, and `git diff --check` passed. Required all-workspace `cargo fmt --all -- --check` was run and failed only on out-of-scope dirty `crates/firn-http/src/lib.rs` and `crates/firn-package/src/lib.rs`; those files were left untouched.
- 2026-07-06: Parent review found and fixed an exact duplicate-source-name collision gap in `namecase-v1`; duplicate output names now hard-error even when the source names are identical. Closure evidence recorded in `.10x/evidence/2026-07-06-package-contract-http-quality-gates.md`; closure review recorded in `.10x/reviews/2026-07-06-contract-compiler-normalization-review.md`.

## Blockers

None.
