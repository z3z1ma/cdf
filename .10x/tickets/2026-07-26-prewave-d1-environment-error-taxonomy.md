Status: open
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

# Add environment errors and audit internal failures

## Scope

Add `ErrorKind::Environment`, catalog mappings, generated docs, and exact remediation. Audit the
foundational construction sites in kernel, memory, runtime, engine, package/package-contract,
task-store, object-access, and HTTP, reclassifying host/filesystem/process/resource-limit failures
without changing genuine invariant failures.

## Non-goals

- No change to existing stable exit codes; Environment uses 70 in this program.
- No blanket text replacement based on message keywords.
- No hiding program defects as environmental failures.

## Acceptance criteria

- `Environment` is serialized, cataloged, rendered, documented, redacted, and exhaustively
  matched.
- Every current internal-error site in the named foundational crates is classified by ownership;
  migrated sites carry relevant context/remediation.
- Missing current directory, temp directory, executable, file descriptors, and local I/O have
  focused tests.
- Poisoned invariants, impossible state, and internal serialization/authority failures remain
  `Internal`.
- Generated error reference and TTY/headless/JSON snapshots are fresh.

## References

- `.10x/specs/cli-report-authority-and-environment-errors.md`
- `.10x/specs/cli-error-experience-catalog.md`

## Assumptions

- Record-backed: stable exit codes are preserved; classification/remediation is the intended
  behavior change.

## Journal

- 2026-07-26: Source inventory counted 1,094 internal construction sites. This first bounded slice
  owns roughly 470 foundational sites; adapter and product slices have separate owners.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
