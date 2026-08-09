Status: open
Created: 2026-08-08
Updated: 2026-08-08

# Reject checkpoint-ineligible partial snapshot plans before effects

## Scope

Make plan/run authority surface checkpoint ineligibility for a global execution limit before
source contact or package creation. Preserve plan's useful terminal introspection and portable
artifact output, but prevent a portable-plan run from learning only after extraction/package work
that the canonical source frontier is incomplete.

## Non-goals

- Changing exact global-limit membership or allowing partial source evidence to advance state.
- Treating an adapter-native bounded input, such as a complete MongoDB pipeline ending in `$limit`,
  as a partial generic execution.
- Deleting or collecting existing package artifacts; retention remains owned by G1.

## Acceptance Criteria

- A plan whose generic global limit makes source execution checkpoint-ineligible records that fact
  in typed plan authority and human output.
- `cdf run --plan` rejects that plan before source contact, package creation, destination mutation,
  or checkpoint mutation, with guidance to use preview or produce a complete executable plan.
- Intrinsically complete bounded source inputs continue through package, receipt, and checkpoint.
- Focused tests prove zero source opens/effects and that retry is not blocked by a deterministic
  leftover package directory.

## References

- `.10x/decisions/canonical-frontier-parallel-scheduling.md`
- `.10x/specs/portable-plan-artifact.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/cli-command-intent-and-effects.md`

## Assumptions

- Record-backed: rows beyond a global limit are nonprocessed and cannot enter checkpoint state.
- Record-backed: plan remains a useful no-effect introspection surface even when its artifact is not
  executable under the checkpoint contract.

## Journal

- 2026-08-08: A release MongoDB sandbox plan with `--limit 1000` passed portable preflight, opened
  the source, read an 8,192-row wire batch, packaged the selected 1,000 rows, and only then failed
  because partial execution could not advance state. The partial immutable package then occupied
  the deterministic CLI package path and blocked a corrected complete run until preserved under a
  different diagnostic name. The checkpoint refusal is correct; its timing and retry ergonomics
  are not.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
