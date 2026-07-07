Status: done
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/done/2026-07-05-http-toolkit.md, .10x/tickets/done/2026-07-05-contract-compiler-normalization.md

# Implement declarative resources

## Scope

Implement Tier 0 TOML/YAML declarative resource parsing, JSON Schema generation, REST/SQL/files resource compilers, semantic validation, cursor/partition mapping, and escape-hatch references. Owns `crates/cdf-declarative/**`.

## Acceptance criteria

- REST examples from the book parse and compile into `QueryableResource` descriptors.
- JSON Schema validates editor/project files and is emitted as an artifact.
- REST cursor filters negotiate as `Inexact` unless configured/proven otherwise.
- Semantic validation detects missing cursor/key fields in sample or declared schema.
- SQL and file resource declarations compile to resource descriptors at MVP level.

## Evidence expectations

Record parser tests, JSON Schema snapshot tests, REST planning tests, and semantic validation fixture tests.

## Explicit exclusions

No CLI command implementation except APIs consumed by project/CLI tickets.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-06: Assigned to declarative worker after HTTP toolkit and contract compiler closure. Worker owns `crates/cdf-declarative/**` and may propose minimal shared type additions only when required by active specs; leave unrelated dirty `.gitignore` untouched.
- 2026-07-06: Implemented the MVP declarative parser/compiler surface in `crates/cdf-declarative`: TOML/YAML serde models, JSON Schema artifact generation, REST/SQL/file descriptor compilation, REST cursor pushdown planning with `Inexact` default, cursor/key semantic validation, and focused tests. No shared type additions were required. Evidence recorded in `.10x/evidence/2026-07-06-declarative-resources.md`.
- 2026-07-06: Closure review recorded in `.10x/reviews/2026-07-06-declarative-resources-review.md`; acceptance criteria are satisfied and this ticket is closed.
- 2026-07-06: Workspace quality gates for the engine/declarative/formats batch are recorded in `.10x/evidence/2026-07-06-engine-declarative-formats-quality-gates.md`.

## Blockers

None.
