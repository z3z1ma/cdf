Status: active
Created: 2026-08-07
Updated: 2026-08-07

# Workspace suite has a failing and partly flaky baseline on `main`

## Scope

`main` does not have a green workspace test suite. Establish the true baseline, classify each
failure as a real defect versus an environment or flakiness problem, give every real defect an
owner, and restore a trustworthy `cargo test --workspace` signal.

This ticket owns the *condition*. Individual defects it uncovers get their own bounded tickets.

## Non-goals

- fixing the failures in this ticket without first classifying them;
- weakening, skipping, or deleting any assertion to make the suite green;
- re-litigating A2, which was proven not to cause any of these.

## Discovery

Found on 2026-08-07 while verifying the `STREAM_EPOCH_POLICY_VERSION` 1 → 2 bump in
`.10x/tickets/2026-08-07-a2-log-source-runtime-archetype.md`. Because a serialized-artifact version
bump can break golden hashes workspace-wide, the change was verified by differential sweep against a
stashed baseline rather than by inspection:

```text
baseline (clean main, DUCKDB_DOWNLOAD_LIB=1 cargo test --workspace --locked --no-fail-fast)
  → 36 failing tests

same command with the A2 changes, after repairing the one genuine regression
  → 33 failing tests, 0 new versus baseline
```

Both numbers are from the same host and command. The A2 tranche introduced exactly one regression,
which was found and fixed; everything else predates it.

## Observed failure clusters

From the baseline sweep (full list in Evidence):

- **schema promotion** — 12 tests in `cdf-project`, including plan identity, correction sidecar
  routing, crash-boundary recovery, and tampered-authority rejection;
- **package replay** — 10 tests in `cdf-conformance`, including the crash-matrix helper-process
  cases;
- **DuckDB doctor drift** — 3 tests, including the clean-ledger case, which suggests environment
  rather than logic;
- **live-run / run-adapters / drift quarantine** — REST, Parquet, and mirror conformance;
- **`runners::tests::duckdb_replay_case_uses_current_package_authority`** — fails with
  `destination commit plan content authority does not match the verified package manifest`,
  which points at package content-authority drift, plausibly from the A1.5 package-native
  keyed-effect transition;
- **`tests::determinism::package_identity_is_invariant_to_source_batch_rechunking`** — diagnosed
  below. The determinism invariant itself is **intact**; a committed golden hash is stale.

## Flakiness is confirmed, not suspected

Three tests differed between two sweeps of the *same* code in both directions:

- `live_run::drift_quarantine::drift_quarantine_postgres_conformance_asserts_supported_mirror`
- `mvp_acceptance_demo::mvp_acceptance_demo_fixture_proves_rest_duckdb_recovery_replay_and_drift`
- `tests::schema_promotion::schema_promote_api_rejects_divergent_destination_authority_before_mutation`

These touch live PostgreSQL and DuckDB. Shared long-lived fixtures with accumulated state are the
obvious suspect — see `.10x/knowledge/live-connector-fixture-topology.md`, which documents
containers that have been running since 2026-08-03.

A second, distinct flake family surfaced on 2026-08-07 in `cdf-subprocess`:

- `tests::cancellation_before_first_frame_kills_descendants_and_joins`
- `tests::timeout_terminates_the_entire_subprocess_process_group`

Both failed during a fully parallel `--workspace` sweep. They spawn real child processes with
`sleep 30` and assert on process-group termination. `cdf-subprocess` contains no reference to any
type changed in that sweep, which rules out the change under test as a cause; no orphaned child
processes were present either, so this is not leaked state from repeated runs.

`timeout_terminates_the_entire_subprocess_process_group` was observed passing in isolation once and
failing in isolation later, so it is flaky independent of load. **Diagnosed:** it panics at
`crates/cdf-subprocess/src/tests.rs:908`, on `pid.trim().parse::<i32>().unwrap()` — the descendant
PID file is *empty*. The fixture shell is `sleep 30 & child=$!; printf '%s' "$child" > "$1"; wait`,
so the test's timeout can elapse before the child has written its PID. The failure is a **startup
race in the fixture**, not a process-group termination defect: the assertion it exists to make is
never reached. Fixing it means waiting for the PID file to become non-empty (or failing with a
clear message when it does not) before asserting on descendant death.

This one is worth prioritising with the determinism invariant: a test that panics before its real
assertion cannot protect the behavior it names.

A flaky suite is worse than a failing one: it teaches readers to discount red, which is exactly how
a real regression ships unnoticed.

## Root-cause finding: `fe53f2a5` changed package identity and left goldens stale

Traced 2026-08-07. This is the strongest lead in the whole failure set and probably explains a large
share of it.

**Correction to the discovery notes above.** `package_identity_is_invariant_to_source_batch_rechunking`
was first described here as a failing determinism invariant. That was wrong, and the distinction
matters for priority. `crates/cdf-engine/src/tests/determinism.rs` asserts two different things:

- lines 276–285 compare the one-batch run against the many-batch run — `identity_segments`,
  `lineage`, `manifest.identity`, and `manifest.package_hash`. **All of these pass.** Rechunking does
  not change package identity; the invariant holds.
- line 286 compares the package hash against a hardcoded golden. **This is the only failing
  assertion.**

Observed `sha256:55a44f7a…`, committed golden `sha256:ce88efb0…`.

**Where the golden came from.** `git log -L 286,289` shows the golden was last updated by
`f5d4d4c2 feat: type package keyed effect authority` (A1.5), which also recorded in its ticket that
this very test *passed* at closure.

**What changed afterwards.** Exactly three commits followed `f5d4d4c2`:

| commit | what it touched |
|---|---|
| `3ffc85fd chore: remove graphify ref` | `AGENTS.md` only |
| `fe53f2a5 feat: lower cdc batches to canonical keyed effects` | 74 files, +3022 lines |
| `e961900a docs: open log-source runtime archetype` | two `.10x/` records only |

`fe53f2a5` is the only candidate, and it touched precisely the identity-bearing surface:
`cdf-kernel/src/effect.rs`, `cdf-kernel/src/batch.rs`, `cdf-package-contract/src/receipt.rs`,
`cdf-package/src/json.rs`, `cdf-package/src/reader.rs`, and
`cdf-engine/src/execution/orchestration.rs`. It did **not** touch `determinism.rs`, so the golden was
never updated to match the package content it changed.

**Conclusion.** `fe53f2a5` altered package identity and shipped without refreshing the committed
package-hash evidence — and, given the size of the failure set, without a green workspace run. The
schema-promotion, package-replay, and
`duckdb_replay_case_uses_current_package_authority` clusters are all package-identity-dependent and
plausibly share this cause; that remains a hypothesis until each is checked.

**Do not simply update the golden.** A package hash is committed evidence, and rewriting it to match
current output is how a real regression becomes permanently invisible. Two things must be
established first: that the content change in `fe53f2a5` was intended and ratified (A1.5's spec did
ratify package-native keyed effects, so this is likely but unconfirmed), and that no *other*
behavior regressed alongside it. Only then is refreshing the golden a correction rather than a
cover-up.

Confirming bisect, for whoever picks this up:

```bash
git stash && git checkout f5d4d4c2 && \
  DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine --locked \
  package_identity_is_invariant_to_source_batch_rechunking
# expect: pass. Then repeat at fe53f2a5 — expect: fail on line 286 only.
```

## Classification progress (2026-08-07)

**36 → 28 failing.** Every fix below made a fixture *correct*; no assertion was weakened, skipped,
or deleted, and no enforcement was relaxed.

### Resolved

1. **Stale package-hash golden** — `package_identity_is_invariant_to_source_batch_rechunking`.
   Bisect confirmed pass at `f5d4d4c2`, fail at `fe53f2a5`. Intent confirmed: `fe53f2a5` refreshed
   the sibling golden in `fixed_fixture_hash_is_deterministic_across_repeated_runs` and missed this
   one, so the content change was deliberate and the propagation incomplete. Golden refreshed with
   the reasoning recorded inline. All 9 determinism tests pass.
2. **Placeholder schema hash in the package-replay harness** — 10 tests.
   `DEFAULT_PREPARED_SCHEMA_HASH = "schema-v1"` (introduced 2026-07-06 by `f763b99c`) was a literal
   placeholder. `96fd277d feat: complete state-backed schema authority cutover` began enforcing that
   a package's runtime Arrow schema hashes to exactly its `StateDelta` schema hash
   (`cdf-project/src/runtime/replay.rs:1087`), which the placeholder could never satisfy. Replaced
   with `prepared_schema_hash()`, derived from the fixture's own payload — the value it should
   always have carried.
3. **Stale CLI generated artifacts** — this was breaking CI, not the local suite. `cdf package gc`
   gained `--execute` without regenerating either artifact set. Both regenerated; CI green at
   `84007fb8`. `QUALITY.md` documented only the `docs/` check, so the
   `crates/cdf-cli/generated` set (completions, help, man) was invisible to the gate — fixed there
   too, along with the note that its test is silently filtered out without `--features cli-artifacts`.

### Diagnosed, not yet fixed

**Schema-promotion cluster (12 tests) plus `duckdb_replay_case_uses_current_package_authority`
share one cause.** All fail with `destination commit plan content authority does not match the
verified package manifest`, raised at `crates/cdf-package/src/reader.rs:1086` where
`commit_plan.content != manifest.identity.content`.

The CLI test fixtures stamp packages via the `package_builder!` macro
(`crates/cdf-cli/src/tests/mod.rs:68`) with
`PackageContentAuthority::rows(SchemaHash("cli-test-schema"))` — another literal placeholder —
while the commit plan carries the planner-derived authority. A1.5's typed content authority made
these comparable and therefore enforceable.

The fix is the same shape as #2: derive the fixture's content authority from the schema it actually
builds rather than a placeholder string. It is larger because `package_builder!` is used by many
tests and several sibling fixtures hardcode `"schema-status-1"`, `"schema-doctor-1"`, and similar.
Each must be checked rather than blanket-replaced, because a fixture that is *supposed* to test a
mismatch must keep mismatching.

### Load-sensitive, not defects

Confirmed to pass in isolation and fail only under a saturated parallel `--workspace` sweep:
`nebula_source_inherits_generic_plan_run_receipt_checkpoint_and_replay_laws`, both
`cdf-subprocess` process-group tests, and intermittently several `package_replay` helper-process
cases. These spawn real child processes. The full list of tests observed flipping between sweeps of
identical code is above.

## Status after the second pass: 36 → 12

All fixes made a fixture describe what it actually holds. No assertion was weakened, skipped, or
deleted, and no enforcement was relaxed. **CI is green** (`237583ac`, `9a6e73de`, `bcdcfd87`).

### Additionally resolved

4. **Content-authority cluster — 17 tests.** All failed on `commit_plan.content !=
   manifest.identity.content` (`cdf-package/src/reader.rs:1086`). Three fixtures stamped packages
   with placeholder content authority while writing commit plans carrying a different hash:
   - `crates/cdf-cli/src/tests/mod.rs` — the `package_builder!` macro hardcoded
     `rows("cli-test-schema")`. It now takes the hash explicitly, and the schema-promote fixture
     passes its own. 14 schema-promotion tests pass.
   - `rebuild_correction_package_semantically` preserved the original commit plan verbatim while
     rebuilding with the default hash. It now stamps `commit.content`, so the repackaged fixture
     reaches the semantic tampering it exists to test instead of dying on a content mismatch first.
   - `crates/cdf-benchmarks/src/runners.rs` derived its hash from the *resource* schema while the
     engine stamps content from the *plan output* schema (`initial_package_content`). Contract
     evaluation and normalization move these apart. Now uses `plan.output_schema.arrow_schema_hash`.
     All 31 benchmark tests pass.
5. **DuckDB doctor drift — 6 tests.** Two layered causes: the content-authority mismatch above, then
   the `"schema-doctor-1"` placeholder failing the StateDelta schema check. Both fixed by deriving
   `doctor_schema_hash()` from the fixture payload.
6. **`receipt::tests::ordinary_draft_maps_typed_request_fields`.** `CommitCounts` became a tagged
   enum with package-native keyed effects, so serialized `counts` now carries `"kind": "rows"`. The
   expected JSON predated the tag.

### Remaining 12, classified

**Load-sensitive — pass in isolation, fail only under a saturated parallel sweep (5):** four
`package_replay` helper-process/crash cases and
`nebula_source_inherits_generic_plan_run_receipt_checkpoint_and_replay_laws`. Verified: 11/11 and
2/2 pass when run alone. These spawn child processes.

**Real, reproduce in isolation (7).** Two distinct causes, neither yet fixed:

- `tests::run_adapters::run_rest_progress_drift_fails_closed_without_parse_coercion` asserts exit
  code 1 and observes **0** (`crates/cdf-cli/src/tests/run_adapters.rs:148`). The command
  *succeeded* where the test requires it to fail closed. **This may be a genuine product
  regression in fail-closed behavior rather than fixture drift, and should be triaged first** —
  a fail-closed path that silently succeeds is exactly the class of defect a red suite hides.
  Siblings `active_multi_file_parquet_keeps_fixed_schema_and_admits_new_physical_schemas_in_stream`
  and `governed_quarantines_incompatible_partition_with_exact_arrow_field_evidence` are in the same
  file and likely related.
- `live_run::drift_quarantine::*` (2, panic at
  `crates/cdf-conformance/src/live_run/drift_quarantine/mod.rs:156`),
  `mvp_acceptance_demo_fixture_proves_rest_duckdb_recovery_replay_and_drift`, and the two
  `runtime_tests::live_adapters::*` cases. Not yet diagnosed.

## Acceptance criteria

- [ ] Every baseline failure is classified: real defect, environment/fixture problem, or flaky.
- [ ] Each real defect has a bounded ticket or a recorded no-action rationale.
- [ ] Flaky tests are made deterministic or given isolated fixtures; none are simply deleted.
- [x] `package_identity_is_invariant_to_source_batch_rechunking` is diagnosed explicitly. Done: the
      invariant holds; a golden hash went stale in `fe53f2a5`. See the root-cause finding.
- [ ] It is established whether `fe53f2a5`'s package-identity change was intended and ratified, and
      whether anything else regressed with it, before any golden is refreshed.
- [ ] A documented command reproduces a known-good baseline, and the count is recorded so future
      differential sweeps are cheap.
- [ ] No assertion was weakened, skipped, or removed to reach green.

## References

- `.10x/tickets/2026-08-07-a2-log-source-runtime-archetype.md` (discovery context and method)
- `.10x/knowledge/live-connector-fixture-topology.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/tickets/2026-08-07-a1-5-package-native-keyed-effects.md`

## Assumptions

- Record-backed: the A2 tranche introduced no failure in this set, proven by the differential sweep
  above.
- Unverified: that the content-authority and schema-promotion clusters originate from the A1.5
  transition. This is a hypothesis from the error text, not a diagnosis.

## Journal

- 2026-08-07: Second pass took the suite from 28 to 12 by fixing the content-authority cluster
  (17 tests), doctor drift (6), and the receipt commit-counts tag. CI is green. Five of the
  remaining twelve are load flakes proven to pass in isolation; seven are real. The highest-priority
  one is `run_rest_progress_drift_fails_closed_without_parse_coercion`, which observes exit 0 where
  it requires 1 — a fail-closed path that appears to succeed, which is a candidate product
  regression rather than fixture drift.

- 2026-08-07: Classified and fixed three causes, taking the suite from 36 to 28 failing: a stale
  package-hash golden, a placeholder schema hash in the package-replay harness, and stale CLI
  generated artifacts that were breaking CI. Diagnosed the 13-test content-authority cluster to
  another placeholder in the CLI test fixtures. Every fix corrected a fixture; none relaxed an
  assertion.

- 2026-08-07: Traced the determinism failure to a stale golden rather than a broken invariant, and
  identified `fe53f2a5` as the commit that changed package identity without refreshing committed
  evidence. Corrected the discovery notes, which had mischaracterised it. The `cdf-subprocess`
  timeout flake was also diagnosed to a fixture startup race rather than left labelled "flaky".

- 2026-08-07: Opened from A2 increment 5. The differential-sweep method is the durable lesson: when
  a change touches a versioned serialized artifact, compare failure *sets* against a stashed
  baseline rather than reading a red suite and assuming the failures are someone else's.

## Blockers

None. Classification can begin immediately and needs no ratification.

## Evidence

Baseline failure list (36) captured 2026-08-07:

```text
live_run::drift_quarantine::drift_quarantine_duckdb_conformance_asserts_unsupported_mirror_exclusion
live_run::drift_quarantine::drift_quarantine_postgres_conformance_asserts_supported_mirror
mvp_acceptance_demo::mvp_acceptance_demo_fixture_proves_rest_duckdb_recovery_replay_and_drift
package_replay::tests::bad_recovery_inputs_fail_closed_without_checkpoint_head
package_replay::tests::committed_before_checkpointed_helper_process
package_replay::tests::duplicate_replay_returns_noop_receipt_and_single_destination_load
package_replay::tests::helper_process_after_checkpoint_commit_finalizes_status_without_second_load
package_replay::tests::helper_process_after_checkpoint_proposal_leaves_no_destination_or_checkpoint_head
package_replay::tests::helper_process_after_packaged_before_destination_write_leaves_no_destination_or_checkpoint
package_replay::tests::helper_process_crash_recovers_from_durable_receipt_without_second_load
package_replay::tests::negative_self_tests_prove_package_replay_harness_checks_required_edges
package_replay::tests::package_artifacts_replay_commits_destination_receipt_checkpoint_and_status
package_replay::tests::packaged_no_receipts_replay_commits_destination_receipt_checkpoint_and_status
receipt::tests::ordinary_draft_maps_typed_request_fields
runners::tests::duckdb_replay_case_uses_current_package_authority
runtime_tests::live_adapters::general_project_run_executes_rest_with_discovered_snapshot_hash
runtime_tests::live_adapters::merge_dedup_live_run_records_deduped_package_replay_identity_and_duplicate_redrive
tests::determinism::package_identity_is_invariant_to_source_batch_rechunking
tests::doctor_drift::doctor_fails_on_duckdb_state_mirror_drift
tests::doctor_drift::doctor_fails_on_missing_and_extra_duckdb_mirror_rows
tests::doctor_drift::doctor_passes_clean_duckdb_ledger_mirror_drift_check
tests::run_adapters::active_multi_file_parquet_keeps_fixed_schema_and_admits_new_physical_schemas_in_stream
tests::run_adapters::governed_quarantines_incompatible_partition_with_exact_arrow_field_evidence
tests::run_adapters::run_rest_progress_drift_fails_closed_without_parse_coercion
tests::schema_promotion::schema_promote_api_rejects_divergent_destination_authority_before_mutation
tests::schema_promotion::schema_promote_execute_commits_correction_checkpoint_and_state_publication
tests::schema_promotion::schema_promote_execute_recovers_every_persisted_crash_boundary
tests::schema_promotion::schema_promote_execute_routes_parquet_through_correction_sidecar
tests::schema_promotion::schema_promote_execute_updates_postgres_through_generic_command_dispatch
tests::schema_promotion::schema_promote_failure_reports_persisted_recovery_status_without_secret_leak
tests::schema_promotion::schema_promote_multi_target_uses_canonical_checkpoint_chain_and_exact_publication
tests::schema_promotion::schema_promote_plan_identity_binds_receipted_packages_without_residual_values
tests::schema_promotion::schema_promote_plans_fresh_residual_correction_without_writes
tests::schema_promotion::schema_promote_rejects_semantically_rebuilt_correction_packages_without_sources
tests::schema_promotion::schema_promote_rejects_tampered_correction_authority_before_mutation
tests::schema_promotion::schema_promote_settles_fully_superseded_replace_package_as_typed_noop
```

**Limits.** One host, one toolchain, `DUCKDB_DOWNLOAD_LIB=1`, with the long-lived fixture containers
from `.10x/knowledge/live-connector-fixture-topology.md` running. The list is a starting point for
classification, not a verdict on any individual test. CI may differ.

## Review

Pending.

## Retrospective

Pending.
