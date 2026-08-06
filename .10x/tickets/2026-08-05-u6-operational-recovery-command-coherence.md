Status: active
Created: 2026-08-05
Updated: 2026-08-05
Parent: `.10x/tickets/2026-08-04-resource-first-cli-experience-program.md`
Depends-On: `.10x/tickets/done/2026-08-05-u5-source-resource-discovery-generation-add.md`

# U6 operational and recovery command coherence

## Scope

Make operational readiness and recovery follow the same explicit-scope model as preparation.
Implement `doctor` scopes; fold exact-package execution and interrupted-run recovery into mutually
exclusive `run` input modes; remove the separate replay/resume command grammar and implementation
entry points. Audit preview, backfill, status, and inspect so each loads only its named authority,
reports its actual effect ceiling, and emits remediation owned by the failing boundary.

Repair the concrete sandbox run path so a resource that successfully prepares does not later fail
relational input-schema identity, and make meaningful execution progress visible on stderr during
long-running work. Use a release build for the end-to-end sandbox check.

## Non-goals

- changing package, receipt, checkpoint, or run-ledger durability semantics;
- adding a new recovery verb or compatibility alias for removed replay/resume grammar;
- schema promotion, cross-resource dependencies, or hidden preparation from package authority;
- the combined U-tranche test/review/cutover certificate, which belongs to U7.

## Acceptance Criteria

1. `cdf run <selector>...`, `cdf run --plan <path>`, `cdf run --package <dir> --to <destination>`,
   and `cdf run --resume [<run-id>]` are mutually exclusive input modes with precise help and usage
   errors; top-level replay and resume commands are absent without aliases or shims.
2. Package mode creates a new run from exact package authority and does not inventory resources,
   parse or compile authored SQL, resolve source credentials, contact a source, or rediscover a
   schema; broken unrelated authored state cannot block it.
3. Resume mode selects an explicit run exactly; bare resume reports clean no-work for zero
   recoverable runs, resumes exactly one, and reports all candidate ids without mutation when
   several exist. Finalized-package recovery performs no source/compiler work.
4. One typed run result authority names `resource_set`, `portable_plan`, `package`, or
   `interrupted_run`; human and JSON output retain redaction parity and established proof/effects
   vocabulary.
5. Bare `cdf doctor` equals `doctor runtime`; `doctor resource`, `doctor source`,
   `doctor destination`, and `doctor all` contact only their declared reachable authorities.
   Reports count attempted/skipped/passed/warned/failed probes, disclose external authorities
   contacted, and write nothing.
6. Preview remains bounded observe-only; backfill selects only its named resource and applies an
   all-selected preparation barrier before execution; status and inspect load only their named
   local/durable authority. Their diagnostics do not recommend generic compile/validate actions.
7. The three executable PostgreSQL backfill schema-coercion regressions observed in U4 pass under
   the current relational input authority.
8. A release-mode sandbox `fineweb.documents` run consumes the exact prepared relational input
   schema without a compile-again mismatch and emits live, useful stderr progress while work is in
   flight.
9. Focused behavioral tests, affected-package check, strict Clippy, formatter, generated CLI
   artifacts, and diff check pass; broader closure checks remain deferred to U7.

## References

- `.10x/specs/cli-command-intent-and-effects.md`
- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/resource-selector-batch-commands.md`
- `.10x/specs/portable-plan-artifact.md`
- `.10x/decisions/static-validation-operational-readiness-boundary.md`
- `.10x/knowledge/cli-report-authority.md`
- `.10x/knowledge/error-ownership-taxonomy.md`
- `.10x/knowledge/project-file-publication-recovery.md`
- `.agents/skills/audit-cli-report-authority/SKILL.md`
- `.agents/skills/audit-error-ownership/SKILL.md`

## Assumptions

- User-ratified: one execution verb, exact package and interrupted-run input modes, scoped doctor,
  static validate, resource selectors, and no separate replay/resume verbs.
- Record-backed: current package replay and run-ledger recovery implementations own the durable
  mechanics and are to be moved behind `run`, not reimplemented.
- Record-backed: U3 canonicalizes admitted physical batches to compiled logical metadata and U4
  carries exact prepared authority; U6 must prove and repair remaining runtime paths rather than
  restoring compile prerequisites.
- User-ratified: progress belongs on the established live terminal channel and release-mode E2E is
  the useful performance certificate for the sandbox.

## Journal

- 2026-08-05: Activated after U5 completed and was pushed. Source inspection confirmed that CLI
  grammar still exposes top-level `resume` and `replay package`, bare doctor still loads the whole
  project operational context, and run has only resource-set and portable-plan modes.
- 2026-08-05: Folded exact-package and interrupted-run authority into mutually exclusive `run`
  modes, removed the top-level replay/resume grammar and modules, introduced project-operational
  loading for package/recovery/inspect paths, and split doctor into runtime, resource, source,
  destination, and all scopes.
- 2026-08-05: Reproduced the three live PostgreSQL executable-backfill failures. The coercion
  evidence validator treated a preserved field's source-native `cdf:physical_type` (`bigint`) as
  though it were the pre-coercion Arrow type (`Int64`). Preserved fields now validate against their
  unchanged Arrow type while non-preserved decisions still require physical Arrow provenance.
- 2026-08-05: Traced the apparent post-package execution wedge to the interactive progress worker.
  A destination-start event received inside the redraw throttle window was retained in state but
  never rendered unless another event arrived. The worker now performs bounded timed refreshes for
  deferred phases, so a long destination commit displays `Loaded` without waiting for completion.
- 2026-08-05: Audited preview, backfill, status, and inspect authority loading. Preview/backfill and
  singular resource inspection use selected read-only compilation; status reads lock/state
  authority; package and run inspection use operational or artifact authority; only explicit
  resource inventory keeps whole-project loading.

## Blockers

None.

## Evidence

- `cargo test -p cdf-contract --lib
  preserved_schema_coercion_accepts_source_native_physical_type_metadata --locked` passed 1/1.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli --lib backfill_execute_ --locked --
  --test-threads=1` passed the three live PostgreSQL execution cells, including window commit,
  multi-slice progress, and failed-repeat recovery guidance.
- `cargo test -p cdf-cli-core --lib --locked` passed 54/54 parser, report, and progress tests.
- Focused `cdf-cli` module runs passed: inspect 9/9, status 2/2, recovery 18/18, package execution
  15/15, planning/backfill 23/23, and the preview observe-only regression 1/1.
- Affected-package `cargo check --all-targets` and strict `cargo clippy --all-targets -- -D
  warnings` passed for `cdf-cli-core`, `cdf-cli`, `cdf-contract`, and `cdf-runtime` with the required
  developer DuckDB linkage setting. The explicit cognitive-complexity diagnostic reported only
  existing functions outside this change set.
- Generated CLI artifact and documentation checks, `cargo fmt --all -- --check`, and `git diff
  --check` passed.

## Review

Deferred by user instruction to the single combined U-tranche review in U7.

## Retrospective

Pending implementation.
