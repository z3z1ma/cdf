Status: open
Created: 2026-07-27
Updated: 2026-07-27

# Make cdf add publication recoverable across process loss

## Scope

Reproduce and close the pre-existing process-loss window in multi-file `cdf add` publication where
`cdf.toml` can become visible before the final `cdf.lock` commit point. Ensure a subsequent run
cannot execute an incompletely published resource and that retry deterministically recovers or
rolls back the transaction.

## Non-goals

- No claim of cross-filesystem atomicity.
- No redesign of source compilation or lock identity.
- No expansion of D1c error-taxonomy scope.

## Acceptance criteria

- A process-level failpoint after `cdf.toml` installation and before `cdf.lock` publication
  reproduces the current exposure.
- Project load/run fails closed or completes recovery before the new resource can execute.
- Retry converges without overwriting unrelated concurrent authority.
- The stale fail-closed statement in
  `.10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md` receives a correcting durable
  reference without rewriting terminal history.

## References

- `.10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

## Assumptions

- Source-backed: `publish_project_files_transactionally` installs content paths before its final
  lock path and has in-process rollback but no crash-recovery journal.
- Review-backed: `ProjectContext::load` can compile the replaced `cdf.toml` while a stale lock lacks
  the resource; not every run path requires the lock before execution.

## Journal

- 2026-07-27: D1c review found the window and independently confirmed it predates D1c, including in
  current `HEAD`. The old SX1 no-action rationale overstates the final-lock fence, so this bounded
  ticket owns correction rather than expanding the error audit.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
