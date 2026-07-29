Status: done
Created: 2026-07-27
Updated: 2026-07-28

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
- 2026-07-27: Activated after D3 closure because aggregate Z1 requires zero known correctness
  windows. `graphify query` could not run because the executable remains unavailable; direct
  source inspection confirmed that multi-file publication prepared and synced temporary files,
  installed `cdf.toml` before `cdf.lock`, and had only in-process rollback.
- 2026-07-27: Implemented a hash-only, owner-private, generation-stamped pending/committed marker.
  Writers publish and sync the pending marker before installing any target; recovery completes
  only prior-or-new paths under the existing mutation guard and refuses unrelated authority.
  Project loading samples/reconciles the generation before and after compilation so a transaction
  that starts, commits, or rolls back during the read forces a stable retry.
- 2026-07-28: The single bounded delegated OCR pass found two material design errors in the first
  checkpoint: automatic load recovery violated plan/preview no-write contracts, and destructive
  rollback after the durable pending marker could strand recovery or overwrite a non-cooperating
  editor. Repaired both at the state-machine boundary. Ordinary loads now observe a stable
  committed generation and fail closed without mutation; only a real, non-dry-run `cdf add`
  explicitly completes recovery. Once pending is durable, publication has one forward-recovery
  decision and never destructively rolls targets back. Private marker/temporary corruption is
  `Internal`, host failures remain `Environment`, and unrelated target authority remains
  `Contract`.

## Blockers

None.

## Evidence

- `.10x/evidence/2026-07-28-project-publication-crash-recovery.md` maps the process-exit
  reproduction, read-only no-write behavior, explicit add recovery, concurrent-authority
  preservation, private-state error ownership, 18 focused project tests, 300 CLI tests, and strict
  affected-root lint.
- The broad `cdf-project` limit is explicit: one pre-existing nondeterministic scheduler-overlap
  assertion reproduced independently, and no clean full-project claim is made.

## Review

The single frozen delegated OCR batch returned two high-severity design findings and one medium
error-ownership finding. The highs shared one root: destructive rollback after a durable pending
marker was not a crash-safe terminal decision, and automatic load recovery violated read-only
command contracts. Commit `03f3f359` replaced rollback with forward-only recovery, added explicit
real-add recovery, made ordinary loads fail closed without mutation, and corrected private-state
ownership. Permanent regressions falsify both reviewer scenarios. No critical or significant
residual remains; physical power-loss and non-cooperating-actor CAS remain documented platform
limits rather than waived correctness claims.

## Retrospective

The first implementation optimized for preserving the old in-process rollback behavior after
introducing a durable journal. Five whys reduced both high findings to that mixed terminal model:
rollback consumed the new material, a second crash could preserve only the pending decision, and a
non-cooperating editor was outside the rollback's authority. The smallest complete repair was not
more rollback machinery; it was one durable decision after pending: forward recovery.

The second lesson is that project loading is not semantically read-only when it performs recovery.
Recovery policy must be explicit at the command boundary. Those lessons are distilled into
`.10x/knowledge/project-file-publication-recovery.md` and the canonical/mirrored
`audit-project-file-publication` skill.
