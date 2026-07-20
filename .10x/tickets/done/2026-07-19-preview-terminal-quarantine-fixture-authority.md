Status: done
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
- 2026-07-19: Repaired the shared engine fixture composition rather than patching the failing test's hashes. Effective-schema evidence now binds its discovery manifest into the pinned snapshot through `SchemaSnapshotReference::with_discovery_manifest`; installing that runtime into `MockResource` installs the same pinned baseline and observation catalog into the resource descriptor; compiled mock source plans consume that one catalog rather than inventing an empty copy. All effective-schema fixtures now cross the same constructor and validation seams as production.

## Blockers

None. Focused behavior, adjacent preview/effective-schema laws, strict Clippy, format, and diff checks pass.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine preview_ --lib --locked -j 12`: 7 passed, 1 failed at `crates/cdf-engine/src/tests.rs:1312` before preview execution with the authority mismatch above.
- Post-repair `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine preview_ --lib --locked -j 12` passed all 8 preview laws. The terminal-quarantine case observed zero payload opens, two attestations, two terminal quarantines, and zero output rows.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine effective_schema_ --lib --locked -j 12` passed both adjacent runtime-authority laws.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-engine --all-targets --locked -j 12 -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed.

## Review

Verdict: pass. The repair removes competing fixture authority rather than weakening production validation. Discovery-manifest identity is created once, bound into the pinned snapshot before `EffectiveSchemaEvidence` construction, and propagated unchanged into the resource and compiled source plan. No preview, source, schema-admission, or product behavior changed.

## Retrospective

The failing preview assertion was downstream of a fixture that independently constructed four supposedly identical authorities: descriptor snapshot, effective runtime, baseline schema catalog, and compiled source plan. Production validation correctly rejected the first disagreement. A shared fixture constructor is safer and smaller than teaching every test which hashes to patch, and it improves all effective-schema tests rather than exempting the terminal-quarantine case.
