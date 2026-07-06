Status: open
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

## Blockers

None.

