Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-05-declarative-resources.md
Verdict: pass

# Declarative resources review

## Target

Review of the MVP `cdf-declarative` implementation in `crates/cdf-declarative`.

## Assumptions tested

- REST examples from the book should parse into resource descriptors and plan-visible HTTP concepts.
- REST cursor predicate pushdown must default to `Inexact` unless exact behavior is explicitly declared.
- Required cursor/key fields must be validated against declared schemas and samples.
- SQL and file declarations should compile into resource descriptors without performing I/O.
- The declarative crate should reuse `cdf-kernel` and `cdf-http` concepts rather than inventing parallel descriptor or HTTP models.

## Findings

None.

## Verdict

Pass. Focused tests cover book-style REST TOML, explicit exact cursor override, YAML SQL/file descriptors, schema artifact generation, semantic validation failures, and exact SQL filter negotiation.

## Residual risk

Concrete REST/SQL/file execution is intentionally outside this compiler/planning crate and remains owned by downstream runtime/project/CLI tickets. No blocker remains for this child ticket.
