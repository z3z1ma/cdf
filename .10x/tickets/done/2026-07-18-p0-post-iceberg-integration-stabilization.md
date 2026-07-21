Status: done
Created: 2026-07-18
Updated: 2026-07-18

# P0: post-Iceberg integration stabilization barrier

## Scope

Restore product-level trust after the Iceberg/Glue tranche by closing the authority representation defects and requiring one real smoke barrier before further feature closure. This parent owns sequencing and closure only; executable work lives in its children.

## Children

1. `.10x/tickets/done/2026-07-18-p0-external-partition-authority.md`
2. `.10x/tickets/done/2026-07-18-p0-typed-compiled-source-identities.md`
3. `.10x/tickets/done/2026-07-18-p0-source-io-accounting-separation.md`
4. `.10x/tickets/done/2026-07-18-p0-product-smoke-matrix-gate.md`
5. `.10x/tickets/done/2026-07-18-p0-engine-invocation-state-isolation.md`
6. `.10x/tickets/done/2026-07-18-p0-destination-settlement-crash-evidence.md`
7. `.10x/tickets/done/2026-07-18-p0-portable-partition-ordinals-u64.md`
8. `.10x/tickets/done/2026-07-18-p0-source-planning-authority-closure.md`

## Acceptance Criteria

- The core kernel/runtime/project/CLI/conformance integration suite is green rather than classified around.
- The product smoke matrix passes local Parquet, HTTPS Parquet, multi-file incremental/no-op, FQ12 Iceberg, package verify/replay, preview/run parity, and Parquet destination paths.
- No invalid inline/external partition state or interchangeable string identity remains in the compiled source authority path.
- Planned work estimates and actual source I/O remain distinct typed facts.
- Intersecting ticket closures are corrected or reopened with durable evidence.

## References

- `.10x/tickets/done/2026-07-18-p0-file-inventory-discovery-identity-regression.md`
- `.10x/tickets/done/2026-07-19-iceberg-f4-externalized-scan-tasks.md`
- `.10x/tickets/done/2026-07-19-iceberg-i3-incremental-product-conformance.md`

## Assumptions

- User-ratified: the post-Iceberg/Glue tranche is provisionally untrusted until a product-level barrier passes.
- User-ratified: structural authority defects must be repaired now rather than hidden by configuration workarounds or weakened validation.

## Journal

- 2026-07-18: Opened from the full-tranche audit and the first compact integration barrier. `cargo nextest` across kernel/runtime/source-files/source-rest/engine/project/conformance/CLI ran 1,045 tests: 970 passed, 75 failed, 10 skipped. The failures cluster around incomplete external-partition migration, non-atomic package/fixture authority migration, source-lifecycle identity divergence, and stale CLI/error wiring.
- 2026-07-18: Closed the invalid two-field partition representation after 325/325 authority-owner tests and 11/11 cross-layer parity cases passed. A later broad barrier improved to 1,101 tests with 1,032 passed, 69 failed, and 10 skipped; the failures are still real sibling stabilization work, but direct inline/external authority leakage is no longer one of their causes.
- 2026-07-18: Closed the discovery-to-pinned file-inventory regression. Stable discovery identity now owns reusable inventory while complete plan identity still owns executable partition tasks; real HTTP and CLI Parquet lifecycles passed without a second inventory traversal.
- 2026-07-18: Repaired the first complete CLI product barrier rather than classifying its remaining failures away. REST admission now observes the already-retained page and preserves compiled schema authority while quarantining row-local parse drift; Postgres validates deferred execution dependencies before package/state/destination mutation; ad-hoc no-op and staged-failure reports preserve current schema and recoverable package evidence; stale doctor and product assertions were reconciled to the current fail-closed behavior. `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli` passed all 271 library tests, and the separately compiled child-process environment test passed after asserting secret resolution independently from deliberately failing live source probes.
- 2026-07-18: Incorporated the independent full-tranche audit. It confirms remaining P0 owners for invocation-local cancellation/retry state, compiler/runtime schema authority exclusivity, source-authored observation bindings, destination settlement crash evidence and cleanup failures, actual source-I/O accounting including partial terminal outcomes, and portable `u64` task cardinality. These are stabilization blockers, not residual polish; the product smoke child remains blocked until they close.
- 2026-07-18: Ran the first exact broad failure manifest with `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo nextest run --workspace --locked -j 12 --no-fail-fast`: 1,767 tests ran, 1,752 passed, 15 failed, and 40 explicit skips remained. The failures are: three live-run destination goldens plus their source-position recovery assertion; MVP REST discovery/execution; P2 friction-registry ownership; SQL empty-secret lifecycle evidence; REST inexact cursor closure; trust-ring anomaly rerun setup; zero-segment file-hash fixture; non-Postgres dialect diagnostic; HTTP discovery egress/auth isolation; plaintext-secret diagnostic; remote observation-cache payload reuse; and unversioned HTTP terminal content identity. Workspace all-target check and strict all-target Clippy passed before this run. Every failure is now named and must be resolved or durably invalidated before closure.
- 2026-07-18: The complete `cdf-project` suite now reaches 207/208 green after restoring weak-HTTP terminal SHA authority without repeating its same-command discovery transfer. The remaining failure was not product behavior: one cache test incorrectly reused invocation-local prepared payload state across three independent discovery invocations. Giving each invocation a fresh transient store made both that cache test and the same-command unversioned HTTP reuse test pass together.
- 2026-07-18: Reconciled the independent audit's remaining findings into explicit children rather than leaving them in chat. Portable partition/task ordinals and cardinalities have their own `u64` migration child. Public post-construction partition-authority mutation, untyped observation binding, external drain-epoch manifest evidence, and the high-cardinality source-SDK planning trap are owned by the source-planning authority child and the existing typed-identity child.
- 2026-07-18: Ran the exact barrier after repairing the six remaining failures rather than classifying them around. The Postgres cold-discovery path now binds observations to the source target rather than the compiled resource id; file-manifest recovery asserts the ratified resource-scoped `file:*` key; three live-run goldens were regenerated only after auditing their already-committed schema/statistics/lineage representation changes; and the benchmark isolation test no longer mistakes whole-workspace contention for the timeout behavior it does not own. `cargo nextest run --workspace --locked -j 12 --no-fail-fast` passed 1,771/1,771 with 40 explicit skips. Strict workspace all-target Clippy passed with warnings denied.
- 2026-07-18: Closed the portable cardinality ceiling. Canonical partition and decode-unit ordinals now remain `u64` from external task authority through scheduler admission, retry and worker artifacts, engine segmentation, and drain/replay; scheduler resolution no longer narrows total external cardinality to process address space. The complete workspace gate passed 1,774/1,774 tests with 40 explicit skips, including jobs-invariance and external-task suites, and strict all-target Clippy passed.
- 2026-07-18: Closed source-planning authority seams. Source adapters now choose one closed inline/external authority through the explicit constructor and can only rebind by consuming and returning a complete plan; zero-task external sources retain their representation; external file drain summaries use typed cardinality without enumeration. Strict Clippy and the final 1,777/1,777 workspace gate passed. The only remaining child is the product smoke matrix.
- 2026-07-18: Closed the final product barrier after deterministic local/recorded coverage and fresh
  release runs across public HTTP Parquet, FQ12 Glue/Iceberg, DuckDB, Parquet destination, package
  verification, and artifact-only replay. The barrier found and repaired two adapter-local defects
  that package suites had missed: discarded file physical-schema attestations and sorted Iceberg
  field ids. The final workspace gate passed 1,777/1,777 tests and strict all-feature Clippy.

## Blockers

None.

## Evidence

- Every child is terminal under `.10x/tickets/done/` with criterion-mapped evidence.
- Closed partition authority, typed source identities, source-I/O accounting, invocation-local
  state, settlement crash evidence, portable `u64` ordinals, and source-planning authority together
  eliminate the invalid or interchangeable authority states named at program opening.
- `.10x/tickets/done/2026-07-18-p0-product-smoke-matrix-gate.md` records the complete deterministic
  and live product matrix, including exact row/segment counts, package hashes, and provider limits.
- Final integration gates: 1,777/1,777 workspace tests passed with 40 explicit skips; formatting and
  strict workspace all-target/all-feature Clippy passed.

## Review

Fresh closure review checked the parent against every child and the original audit. Compiled source
partition authority is a closed representation; semantically distinct identities are typed;
planned estimates and observed physical I/O are separate; adapters own their planning and schema
semantics; and the required product matrix now crosses the exact lifecycle that isolated tests had
missed. No critical or significant finding remains. Verdict: pass. Temporal provider availability
is retained as explicit residual risk and cannot replace the deterministic gate.

## Retrospective

The tranche became untrusted because individually plausible migrations changed several adjacent
authorities without one cross-lifecycle release smoke. The repair succeeded by closing invalid
representations first, then running real products rather than classifying integration failures as
pre-existing. Future core tranches must pass the small product matrix before closure; live provider
cells are added only when their adapter or transport changed, keeping fast checks lean.
