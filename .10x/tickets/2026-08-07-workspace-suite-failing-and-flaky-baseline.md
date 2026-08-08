Status: open
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
- **`tests::determinism::package_identity_is_invariant_to_source_batch_rechunking`** — a determinism
  invariant is currently failing. This one deserves priority: it asserts a property the CDC work
  depends on.

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

## Acceptance criteria

- [ ] Every baseline failure is classified: real defect, environment/fixture problem, or flaky.
- [ ] Each real defect has a bounded ticket or a recorded no-action rationale.
- [ ] Flaky tests are made deterministic or given isolated fixtures; none are simply deleted.
- [ ] `package_identity_is_invariant_to_source_batch_rechunking` is diagnosed explicitly, since a
      determinism invariant failing silently undermines package identity claims elsewhere.
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
