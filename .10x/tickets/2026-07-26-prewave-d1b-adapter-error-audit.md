Status: open
Created: 2026-07-26
Updated: 2026-07-27
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-d1-environment-error-taxonomy.md`

# Audit adapter environment and internal errors

## Scope

Semantically audit every internal-error construction site in source, format, transform,
transport, destination, Python, subprocess, and foreign-stream adapter crates. Reclassify host,
filesystem, process, SDK, resource-limit, and local I/O failures as Environment while preserving
real CDF invariants as Internal.

## Non-goals

- No adapter behavior, retry policy, type mapping, or transport semantic change.
- No keyword-based mass replacement or generic remediation.
- No error masking at FFI boundaries.

## Acceptance criteria

- Every internal-error site in the named adapter families is classified and reviewable.
- OS/file-descriptor/temp-path/executable/native-library failures become Environment with concrete
  context and remediation.
- Decoder, identity, ownership, and impossible-state invariants remain Internal.
- Source/destination/format/foreign conformance preserves retry/auth/data/destination kinds and
  redaction.
- TTY/headless/JSON representative snapshots and focused adapter tests pass.

## References

- `.10x/specs/cli-report-authority-and-environment-errors.md`
- `.10x/specs/cli-error-experience-catalog.md`
- `.10x/tickets/done/2026-07-26-prewave-d1-environment-error-taxonomy.md`

## Assumptions

- Record-backed: the shared Environment kind and stable exit mapping land in D1 first.

## Journal

- 2026-07-26: Split from the 1,094-site audit so adapter failure semantics receive focused review.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
