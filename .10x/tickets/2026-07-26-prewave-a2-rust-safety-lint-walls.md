Status: open
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

# Enforce Rust safety and panic lint walls

## Scope

Install workspace lint inheritance, explicit unsafe-code exceptions for the three production FFI
owners, and non-test unwrap/expect denial for foundational and extension-contract crates. Repair
the finite production violations needed to activate the walls.

## Non-goals

- No denial of unwrap/expect in test modules.
- No enablement of the complete Clippy pedantic catalog.
- No redesign of measured FFI paths or weakening of their performance.

## Acceptance criteria

- Every crate explicitly inherits the workspace policy or declares a named FFI exception.
- Unsafe production code exists only in narrow modules under `cdf-dest-duckdb`, `cdf-python`, and
  `cdf-subprocess`; benchmark exceptions are measurement-only.
- Every unsafe block has a safety rationale and governing record reference.
- Named foundational crates compile with non-test unwrap/expect denied.
- Architecture tests enumerate the exception set and fail on a new exception.
- Focused tests, formatting, and strict workspace Clippy pass with the same release features used
  by CI.

## References

- `.10x/decisions/compiler-enforced-rust-safety-walls.md`
- `QUALITY.md`

## Assumptions

- Source-backed: the current unsafe owner set is finite and already isolated by module.
- Record-backed: no performance regression is acceptable solely for lint aesthetics.

## Journal

- 2026-07-26: Source inventory found production unsafe only in DuckDB segment scan/envelope,
  Python Arrow capsule, and subprocess runner; workspace lint inheritance is absent.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
