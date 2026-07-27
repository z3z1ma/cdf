Status: done
Created: 2026-07-27
Updated: 2026-07-27
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-c1-receipt-clock-authority.md`

# Bind receipt clock authority in schema promotion

## Scope

Bind the request's `ExecutionServices` into every destination used by direct schema-promotion
settlement and recovery, preserving the C1 host-clock authority when the product path bypasses the
ordinary run wrapper.

## Non-goals

- No receipt schema, timestamp semantics, promotion lifecycle, or destination physical behavior
  change.
- No process-wall-clock fallback.
- No broad destination construction refactor.

## Acceptance criteria

- Direct single- and multi-target schema-promotion settlement binds the injected host clock before
  destination correction execution.
- Every persisted promotion crash boundary resumes with the same receipt-clock authority.
- The two reproducing CLI product tests pass in isolation and in the relevant product gate.
- No destination receipt/correction path gains a direct `SystemTime::now` fallback.

## References

- `.10x/tickets/done/2026-07-26-prewave-c1-receipt-clock-authority.md`
- `.10x/knowledge/destination-receipt-authority.md`
- `.10x/specs/destination-common-services.md`
- `.10x/specs/execution-host-structured-runtime.md`

## Assumptions

- Record-backed: C1 requires destination receipt time to come from injected
  `ExecutionServices`.
- Source-backed: `SchemaPromotionExecutionRequest` already owns those services; the direct
  settlement path fails before its configured failpoint only because the resolved destination
  never receives them.

## Journal

- 2026-07-27: D1's broader CLI gate passed 270 of 272 tests and reproduced
  `DuckDB commit execution requires injected ExecutionServices for receipt time` in
  `schema_promote_execute_recovers_every_persisted_crash_boundary`; the multi-target test failed
  before its expected schema-promotion failpoint for the same direct-settlement path.
- 2026-07-27: Bound every resolved promotion destination to the request-owned
  `ExecutionServices` after semantic request validation and before lease acquisition or staged
  mutation. This makes direct callers and recovery use the same host-clock authority as ordinary
  orchestration while keeping invalid dry plans contact-free.
- 2026-07-27: The first delegated review rejected unconditional rebinding because a resolved
  DuckDB runtime can already own the full shared spill budget. A second review rejected wrapper
  metadata as proof of runtime binding and found that direct Parquet correction retained its
  constructor clock. Removed the wrapper-only setter, added exact cloned-authority comparison,
  implemented Parquet rebinding, and limited promotion binding to dry-plan-selected targets.
- 2026-07-27: The final binding audit found the built-in drivers and project facade both binding
  during resolution, plus a stale-capability hazard when DuckDB derived native parallelism after
  lanes were installed. Centralized one neutral bind operation: drivers return unbound runtimes;
  binding happens before post-bind capability validation and lane installation; the project
  facade records only a successfully proven binding. The same operation now governs direct
  facade binding as well as registry resolution.
- 2026-07-27: Added a 64 MiB exact-spill production-resolver regression, a post-bind lane
  revalidation, and a fixed-clock Parquet correction lifecycle. The broader project run also
  exposed a direct Postgres lane-registration failure; routing direct facade binding through the
  same neutral operation repaired it without adapter-name branching.
- 2026-07-27: Adversarial failure injection showed that a runtime bind can succeed before
  post-bind lane installation fails, leaving facade metadata stale. The facade now invalidates its
  cached authority before a non-identical attempt and records it only after the complete operation.
  Regressions cover failure before adapter mutation, failure after adapter mutation, and restoration
  of the original authority.
- 2026-07-27: Two further delegated review rounds falsified DuckDB's resource reuse first for
  derived services on the same host and then for decorated hosts that share spill through distinct
  host and memory objects. DuckDB now transfers its existing scratch reservation by actual spill
  coordinator identity, recomputes incoming memory/CPU/native settings, replaces the complete
  services handle, and rejects a changed post-bind scratch-size environment setting. Exact-host,
  decorated-clock, and shared-spill/different-memory regressions all preserve reservation counts.
- 2026-07-27: `graphify update .` remained unavailable (`command not found`); no graph refresh is
  claimed.

## Blockers

None.

## Evidence

- Direct and crash-safe promotion: with the cached DuckDB runtime exposed through
  `DUCKDB_LIB_DIR` and `DYLD_LIBRARY_PATH`,
  `cargo test -p cdf-cli schema_promote -- --test-threads=1` passed all 10 selected tests. This
  includes single-target execution, multi-target canonical settlement, Parquet and Postgres
  correction dispatch, and every persisted crash-boundary recovery. It proves the product
  promotion paths receive bound services within those scenarios; it does not simulate a process
  restart under a different host implementation.
- Product regression gate: under the same native-library environment,
  `cargo test -p cdf-cli -- --test-threads=1` passed all 272 library tests, the one
  `doctor_env` integration test, and doc tests. This includes the two failures that originally
  opened C1b and all non-promotion CLI behavior in the current product crate.
- Adapter clock and resource binding: `cargo test -p cdf-dest-duckdb -- --test-threads=1` passed
  57 tests. The added regressions observe fixed receipt time after a decorated-host rebind, exact
  spill current/failure counters, and recomputed memory limits under a distinct memory authority
  sharing the same spill coordinator. The Parquet fixed-clock correction test also passed.
- Facade failure/recovery: the four focused `cdf-project` tests
  `repeated_destination_binding_reuses_the_exact_execution_authority`,
  `failed_destination_rebind_invalidates_cached_execution_authority`,
  `failed_destination_bind_stage_can_restore_the_original_execution_authority`, and
  `same_host_destination_rebind_reuses_initialized_native_resources` each passed. They cover exact
  idempotence and failures before and after adapter mutation; they do not make the trait-level bind
  operation itself transactional.
- Neutral registry: `cargo test -p cdf-runtime registry` passed 12 selected runtime tests and
  `cargo test -p cdf-dest-parquet
  runtime_rebinding_replaces_the_parquet_correction_receipt_clock` passed its selected lifecycle.
- Static gates: strict all-target/all-feature Clippy passed for `cdf-runtime`, `cdf-project`,
  DuckDB, Postgres, Parquet, conformance, and CLI with `-D warnings`; `cargo fmt --all -- --check`
  and `git diff --check` passed. A production scan found no receipt-path `SystemTime::now`;
  DuckDB's remaining direct call is monotonic profiling telemetry.

## Review

Delegated adversarial review initially failed the implementation for unbound direct Parquet
correction, unused destination rebinding, driver/facade double binding, pre-bind capability
derivation, stale facade authority after partial failure, DuckDB exact-host double reservation,
and decorated/shared-spill host wrappers. Each finding received a production fix and a focused
regression. Both independent final reviews pass the corrected diff with no findings.

Residual risks are bounded. The explicit rejection of a post-bind scratch-size environment change
lacks a dedicated rollback test; the setting is process-start configuration and the failure is
pre-mutation. Reuse also depends on `ExecutionHost::spill` returning the shared coordinator `Arc`;
a future proxy-producing host would conservatively attempt a new reservation and could fail rather
than silently sharing an unproven ledger. Current production and decorated hosts return the actual
shared authority. These residuals are accepted for C1b.

## Retrospective

The apparent missing-clock bug crossed four ownership boundaries: product promotion bypassed the
ordinary wrapper, drivers and registries both believed they owned binding, capability installation
observed pre-bind state, and native adapters retained resource reservations across rebinds. The
useful technique was to make one neutral operation own bind → post-bind capability validation →
lane installation, then attack every failure boundary with exact-capacity resources. Wrapper state
is a cache of completed proof, never proof itself.

The repeated friction came from treating a fallible mutating trait method as if `Result` implied
rollback. It does not. Future adapters must either make binding transactional internally or remain
recoverably rebindable, and facade caches must be pessimistically invalidated. Actual coordinator
identity—not host wrapper identity—is the correct resource-transfer boundary.

The durable rules were distilled into
`.10x/knowledge/destination-receipt-authority.md` and
`.10x/knowledge/source-destination-extension-invariant.md`. No new skill is warranted: the
existing delegated-review procedure produced the necessary independent, iterative falsification;
the lesson is architectural judgment rather than repeatable operator toil.
