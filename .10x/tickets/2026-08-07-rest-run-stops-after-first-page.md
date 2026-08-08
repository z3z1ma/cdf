Status: open
Created: 2026-08-07
Updated: 2026-08-07

# REST run completes after one page, bypassing progress-drift fail-closed

## Scope

Determine why a REST resource run consumes only its first page and reports success, and restore
either the pagination advance or the fail-closed behavior the contract requires. Then confirm the
three `run_adapters` tests assert the behavior they name.

## Non-goals

- relaxing, skipping, or rewriting the assertions to match current output;
- broader REST source rework beyond restoring the contracted behavior.

## Observation

`tests::run_adapters::run_rest_progress_drift_fails_closed_without_parse_coercion`
(`crates/cdf-cli/src/tests/run_adapters.rs:148`) requires exit code **1** and observes **0**.

The fixture serves two pages. Page one types `updated_at` as a number; page two types it as the
string `"20"`. The run must fail closed rather than coerce across that drift.

What actually happens — the run reports complete success:

```json
"counts": { "blocked": 0, "completed": 1, "failed": 0, "selected": 1 }
"admission": { "accepted_main_rows": 2, "quarantined_rows": 0, "failed_resource_count": 0 }
"checkpoint": { "committed": true, "is_head": true, "status": "committed" }
```

`accepted_main_rows: 2` is exactly page one. **Page two was never consumed**, so the type drift the
test exists to reject never entered the pipeline, and a checkpoint was committed on a partial read.

That reframes the failure: this is not a coercion bug. Either pagination/progress stopped advancing
after the first response, or the fixture no longer drives a second request. The fail-closed path is
untested either way, which is the dangerous part — the assertion that protects it cannot fire.

## Why this matters beyond one test

A committed checkpoint after a single page is a correctness concern in its own right if the source
had more data. Whether the source *should* have paged here is the first question to answer.

Two sibling tests in the same file fail in isolation and are likely related:

- `active_multi_file_parquet_keeps_fixed_schema_and_admits_new_physical_schemas_in_stream`
- `governed_quarantines_incompatible_partition_with_exact_arrow_field_evidence`

## Acceptance criteria

- [ ] Established whether the run *should* have requested page two, from the compiled REST
      pagination/progress contract rather than from test expectations.
- [ ] If pagination regressed: fixed, with a test proving a multi-page REST read consumes every page.
- [ ] If the harness stopped driving a second request: fixed so the drift is genuinely presented.
- [ ] The parse-drift run fails closed with the typed error the test asserts, and secrets stay
      redacted.
- [ ] The two sibling `run_adapters` failures are diagnosed and owned.
- [ ] No assertion weakened to reach green.

## References

- `.10x/tickets/2026-08-07-workspace-suite-failing-and-flaky-baseline.md` (discovery)
- `crates/cdf-cli/src/tests/run_adapters.rs`
- `.10x/specs/data-onramp-file-sources-transports.md`

## Assumptions

- Record-backed: the failure reproduces in isolation, so it is not the load-sensitive flakiness
  affecting other tests in the same suite.
- Unverified: that pagination rather than the fixture is at fault. The evidence shows only that one
  page was consumed, not which side stopped.

## Journal

- 2026-08-07: Split out of the workspace-baseline classification. Initially looked like a coercion
  fail-closed regression; reading the actual run output showed the drift never reached the pipeline
  because only page one was consumed. Recorded before diagnosing further so the reframing is not
  lost.

## Blockers

None.

## Evidence

Reproduced in isolation:

```text
DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli --locked \
  run_rest_progress_drift_fails_closed_without_parse_coercion
→ assertion `left == right` failed  left: 0  right: 1
```

Full run JSON above. **Limit:** one host, one run; the output proves what the run reported, not
which component stopped paging.

## Review

Pending.

## Retrospective

Pending.
