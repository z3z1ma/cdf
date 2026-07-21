Status: done
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-18-p0-post-iceberg-integration-stabilization.md

# P0: isolate engine invocation state

## Scope

Separate reusable engine execution configuration from cancellation and retry evidence owned by one invocation, so cloning or reusing policy can never poison or contaminate another run.

## Non-goals

- Changing retry semantics, scheduling policy, or cancellation propagation.
- Introducing process-global invocation registries.

## Acceptance Criteria

- Reusable execution configuration contains no cancellation token or retry journal.
- Every engine execution entry point requires a non-cloneable invocation created explicitly from configuration.
- Two invocations from one configuration have independent cancellation and retry evidence.
- Engine, project, source, benchmark, and workspace static checks compile against the exclusive API.

## References

- `.10x/tickets/2026-07-18-p0-post-iceberg-integration-stabilization.md`
- `.10x/specs/deterministic-parallel-scheduler.md`

## Assumptions

- Record-backed: the full-tranche audit proved that cloned execution options shared `Arc`-backed cancellation and retry state.
- User-ratified: invocation-local authority leaks are P0 stabilization defects.

## Journal

- 2026-07-18: Replaced cloneable `EngineExecutionOptions` with reusable `EngineExecutionConfig` and non-cloneable `EngineExecutionInvocation`. Execution entry points now consume one invocation; project orchestration retains only a read-only retry-evidence view before moving it.
- 2026-07-18: The focused isolation regression passed, the workspace compiled across all targets, and the complete `cdf-project` product suite reached 207/208 before the one unrelated observation-cache fixture ownership failure. The two affected remote-file lifecycle tests then passed together after giving each independent discovery invocation its own transient prepared-payload store while retaining same-command discovery-to-execution reuse.
- 2026-07-18: The exact workspace barrier completed with 1,771/1,771 tests green. Strict workspace all-target Clippy also passed with warnings denied.

## Blockers

None.

## Evidence

- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo check --workspace --all-targets --locked -j 12` passed after migrating every engine/project/source/benchmark call site to explicit invocation construction.
- `reusable_engine_execution_config_creates_isolated_invocation_state` passed and proves two invocations created from one configuration do not share cancellation or retry evidence.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo nextest run --locked -j 12 -p cdf-project --no-fail-fast` ran 208 tests: 207 passed; the only failure was an unrelated test-fixture reuse of invocation-local prepared payload state. The corrected focused cache/reuse pair then passed 2/2.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo nextest run --workspace --locked -j 12 --no-fail-fast` ran 1,771 tests: 1,771 passed, 40 explicitly skipped.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo clippy --workspace --all-targets --locked -j 12 -- -D warnings` passed.

## Review

Verdict: pass. A fresh-hat sequential review traced every field on `EngineExecutionConfig` and `EngineExecutionInvocation`, all public execution entry points, and the isolation regression. Reusable policy contains only read-only services and tuning; every cancellation token and retry journal is minted by explicit invocation construction. No compatibility shim or implicit invocation path remains.

Residual risk: none within scope. The collaboration thread limit prevented commissioning a new independent agent without reusing an old reviewer, so this is explicitly a sequential self-review rather than independent review.

## Retrospective

Cloneability was the architectural smell, not the individual cancellation bug. Splitting reusable policy from one-shot invocation state made wrong sharing unrepresentable and exposed tests that had themselves been reusing transient invocation state. Future run-local evidence should enter through the invocation value, never the configuration object.
