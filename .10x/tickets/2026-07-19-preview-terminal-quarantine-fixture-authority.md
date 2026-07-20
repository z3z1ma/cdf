Status: open
Created: 2026-07-19
Updated: 2026-07-19

# Repair terminal-quarantine preview fixture authority

## Scope

Restore the engine terminal-quarantine preview conformance fixture so its effective-schema discovery manifest and pinned schema snapshot carry one valid compiler authority, then prove the intended no-payload attestation behavior.

## Non-goals

No change to preview selection, external task execution, or effective-schema product semantics merely to weaken the failing assertion.

## Acceptance Criteria

- `preview_terminal_quarantine_uses_run_attestation_without_opening_payloads` reaches preview execution and passes with its intended attestation/open-count assertions.
- The fixture uses the same production authority constructors as the governing effective-schema path; no hash or manifest field is patched after binding.
- Focused warnings-denied Clippy remains green for `cdf-engine`.

## References

- `.10x/specs/schema-discovery-and-stream-admission.md`

## Assumptions

- Record-backed 2026-07-19: the failure occurs in `Planner::plan_tier_b` with `effective schema discovery manifest does not match its pinned schema snapshot`, before the I2 preview changes execute.

## Journal

- 2026-07-19: Discovered while running the broad `preview_` engine filter during Iceberg I2. Seven other preview tests passed; the focused large-plan and traversal tests also passed. Kept separate because this is existing effective-schema fixture authority, not external-task preview behavior.

## Blockers

None.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine preview_ --lib --locked -j 12`: 7 passed, 1 failed at `crates/cdf-engine/src/tests.rs:1312` before preview execution with the authority mismatch above.

## Review

Pending.

## Retrospective

Pending.
