Status: open
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/2026-07-26-prewave-d1b-adapter-error-audit.md`

# Audit product and governance environment errors

## Scope

Complete the internal-error semantic audit in project, CLI/CLI-core, state, contract,
declarative, conformance, benchmark, and other product/governance crates. Regenerate the complete
error catalog and prove no unaudited `CdfError::internal` owner remains.

## Non-goals

- No CLI visual redesign; D3 owns presentation after taxonomy/report authority.
- No change to domain-specific non-environment kinds or stable exit codes.
- No weakening of benchmark/conformance failures that identify CDF defects.

## Acceptance criteria

- Every remaining internal-error construction site is classified and the repository-wide audit
  has no unowned site.
- Project path/environment/state-store failures receive exact Environment remediation.
- Benchmark host/setup failures are distinct from measured product failures.
- Genuine configuration/contract, data, destination, transient, auth, and invariant failures keep
  their authoritative kinds.
- Generated docs, all error-kind exhaustiveness tests, and product error snapshots pass.

## References

- `.10x/specs/cli-report-authority-and-environment-errors.md`
- `.10x/specs/cli-error-experience-catalog.md`
- `.10x/tickets/2026-07-26-prewave-d1-environment-error-taxonomy.md`
- `.10x/tickets/2026-07-26-prewave-d1b-adapter-error-audit.md`

## Assumptions

- Record-backed: D1/D1b establish the taxonomy and adapter semantics before this aggregate sweep.

## Journal

- 2026-07-26: Split from the original monolithic error ticket to give product/remediation
  semantics an independent closure review.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
