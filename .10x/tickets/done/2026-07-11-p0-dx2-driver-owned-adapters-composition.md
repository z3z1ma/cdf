Status: done
Created: 2026-07-11
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-11-p0-destination-extension-boundary.md
Depends-On: .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md

# P0 DX2: driver-owned adapters and composition root

## Scope

Move DuckDB, Parquet, and Postgres runtime driver adapters into their destination crates; build one explicit CLI first-party registry; inject it into project entry points; remove builtin registration and production convenience constructors from shared runtime code.

## Acceptance criteria

- Destination-specific planning/private types remain inside destination crates.
- `cdf-project` removes all `cdf-dest-*` Cargo dependencies and imports.
- CLI composition is one auditable registration list; generic commands receive registry authority.
- Existing run/replay/resume/promotion artifacts and receipts remain stable.

## Blockers

None. DX1 is complete.

## Evidence

| Acceptance criterion | Existing evidence | Limits |
| --- | --- | --- |
| Destination-specific planning/private types remain inside destination crates. | The 2026-07-11 execution note records the DuckDB, Parquet, and Postgres runtime adapters moving into their owning crates, with their unchanged destination suites passing 21, 27, and 40 tests respectively. The 2026-07-12 adversarial review independently inspected the runtime adapters and found the destination planning/runtime private types in those crates while production project runtime used only neutral `cdf_runtime` traits and sheet/commit/runtime types. | This proves the three current first-party adapters and the reviewed production boundary; it does not claim a dynamic plugin ABI or automatic ownership for future crates. |
| `cdf-project` removes all `cdf-dest-*` Cargo dependencies and imports. | `.10x/evidence/2026-07-11-p0-dx2-project-build-graph.md` records no `cdf-dest-*` crate in `cdf-project`'s normal dependency tree and a passing manifest-law test that rejects any such normal dependency. The adversarial review found no production concrete import or production API dependency. | First-party destination crates intentionally remain as dev-dependencies for `#[cfg(test)]` generic orchestration fixtures. The review accepted this as outside the normal library graph; this evidence does not claim their removal from test-only dependency resolution. |
| CLI composition is one auditable registration list; generic commands receive registry authority. | The execution notes record `cdf-cli/src/destination_registry.rs` as the single first-party registry and the later conversion of lock, run, replay, standard doctor, inspection, target parsing, policy validation, and health execution to injected `DestinationRegistry`/driver authority. The adversarial review inspected the composition and generic paths and found exactly DuckDB, Parquet, and Postgres registered in that one module, with the architecture assertion guarding concrete imports. | `doctor_drift.rs` retains the explicitly ratified DuckDB mirror diagnostic exception from `.10x/decisions/destination-runtime-composition-boundary.md`; it is not generic doctor orchestration. |
| Existing run/replay/resume/promotion artifacts and receipts remain stable. | The ticket records the unchanged destination suites, focused project/CLI contract and schema-lock tests, and the repaired 273-test CLI regression suite. The adversarial review found package/receipt/checkpoint hash and lifecycle consistency assertions plus generic replay/recovery assertions for receipt coverage, classification, checkpoint equality, stage order, and duplicate-mutation prevention. `.10x/evidence/2026-07-11-p0-dx4-conformance-catalog-milestone.md` additionally records four passing deterministic package-golden tests through the same adapters. | The evidence proves the asserted semantic identities, invariants, and unchanged regression behavior. There is no dedicated pre-/post-DX2 byte-for-byte receipt golden, so byte-level receipt-format identity beyond those assertions is not claimed. |

## Progress and notes

- 2026-07-11: Added driver-owned runtime adapters to DuckDB, Parquet, and Postgres crates against `cdf-runtime`, including typed no-mutation inspection, package-aware preparation, correction/replay behavior, secret/policy resolution, and explicit current bulk/ingress/concurrency declarations. The existing destination suites passed 21 DuckDB, 27 Parquet, and 40 Postgres tests. Project-side compatibility adapters remain active until the next tranche injects the CLI registry and removes concrete project dependencies.
- 2026-07-11: Deleted all project-owned runtime adapter modules and production convenience constructors. `cdf-cli/src/destination_registry.rs` is now the single first-party composition list, project resolution requires injected registry authority, and run/replay callers preserve prior redaction/not-supported behavior. The 273-test CLI suite reached 270 passes; its two registry-wording regressions and one pre-existing ledger-v5 expectation were repaired and all three focused reruns passed. DX3 still owns lock/doctor/replay residual branches before this ticket can close its full dependency criterion.
- 2026-07-11: Lock creation and schema pinning now receive driver-inspected `DestinationSheetArtifact` values; the project lock module's destination URI match tree and direct DuckDB/Parquet/Postgres sheet construction were deleted. DuckDB and Parquet moved out of the normal `cdf-project` build graph (remaining as test fixtures); the remaining normal Postgres edge is source catalog discovery owned by the source-extension boundary, while DX3 owns doctor/replay product residuals. Focused project and CLI contract/schema-lock tests passed.
- 2026-07-11: Generic driver declarations now own replay target requirements, target parsing, policy keys/allowed values, and health execution. CLI replay removed its Postgres URI/type branches; standard doctor and inspect destination paths consume one generic inspection/health view. DuckDB ICU behavior and typed JSON details remain driver-owned and focused doctor plus Postgres replay error-order/target/policy regressions pass. `doctor_drift.rs` remains the explicitly adapter-specific DuckDB mirror diagnostic allowed by the composition decision, not generic doctor orchestration.
- 2026-07-11: Removed the last concrete destination from `cdf-project`'s normal dependency graph by moving the PostgreSQL destination fixture to dev-dependencies. Production SQL discovery already resolves through the neutral declarative/source boundary. Added a manifest-law test that rejects any future `cdf-dest-*` normal dependency. `cargo tree -p cdf-project -e normal` now contains no concrete destination crate; focused project compilation and the law test pass. Evidence: `.10x/evidence/2026-07-11-p0-dx2-project-build-graph.md`.

## Journal

- 2026-07-12 (adversarial review): Read the governing destination runtime spec and composition decision, DX1 ticket/evidence/review, DX2 build-graph evidence, current manifests, runtime adapters, registry/doctor/replay sources, architecture assertions, regression assertions, and the normal `cdf-project` dependency tree. Used read-only inspection only; no implementation or external state was changed and no verification already journaled by the executors was repeated.
- 2026-07-12 (closure): Mapped every acceptance criterion to the existing execution, evidence, and pass-review observations with their stated limits. DX1 is done, no DX2 blocker remains, and the review found no implementation repair or follow-up defect warranted. Closed DX2 without repeating verification.

## Review

### Findings

- No critical or significant implementation finding. DuckDB, Parquet, and Postgres planning/runtime private types are implemented in their destination crates. The remaining concrete references in `cdf-project` are dev-dependency fixtures and `#[cfg(test)]` convenience constructors; they are not part of the normal library graph or production API. `cdf-project`'s production runtime modules resolve through `cdf_runtime` traits and carry only neutral sheet/commit/runtime types.
- No dependency-boundary finding. `crates/cdf-project/Cargo.toml` contains the three destination crates only under `[dev-dependencies]`; `cargo tree -p cdf-project -e normal --prefix none | rg '^cdf-dest-'` returned no match. The manifest-law assertion inspects `[dependencies]` and rejects any key beginning `cdf-dest-`, so it is not limited to the current three drivers.
- No composition finding. `crates/cdf-cli/src/destination_registry.rs` is the sole first-party registration list and registers exactly DuckDB, Parquet, and Postgres. Production concrete imports outside it are confined to `doctor_drift.rs`, the named DuckDB mirror diagnostic explicitly allowed by the composition decision. Generic lock inspection, run resolution, replay target parsing/policy validation, standard doctor inspection/health, and project resolution all consume `DestinationRegistry`/driver authority rather than destination-name branches. The conformance architecture assertion independently scans project runtime and CLI source for the current concrete imports, with only the composition module, diagnostic, tests, and test-only project fixture file exempted.
- No artifact/receipt regression was found in the relevant assertions. Project run tests assert package, receipt, and checkpoint package-hash identity and lifecycle consistency; generic mock replay/recovery asserts receipt coverage, receipt-source classification, checkpoint equality, stage order, and no duplicate mutation. The ticket journals the unchanged destination suites and the 273-test CLI regression suite, and the later DX4 evidence records four deterministic package-golden passes through the same adapters. The DX2 commits did not introduce a new serialized receipt or project artifact contract.
- Minor residual architecture risk, not a DX2 closure finding: project configuration still has the ratified, destination-specific `DestinationPolicy.postgres` shape and its provider match. This is active product-config authority under `.10x/specs/project-cli-observability-security.md`, not a leaked destination planning/private implementation type, but a future driver needing new project policy keys would require a separately governed generic config evolution.

### Evidence gaps

- There is no dedicated pre-DX2/post-DX2 byte-for-byte receipt golden. The available evidence proves semantic identity/invariants and unchanged regression behavior, while the later package goldens prove deterministic package artifacts. A byte-level receipt-format claim beyond those assertions remains unsupported.
- DX2's ticket body does not yet contain an acceptance-criterion-to-evidence table or an executor retrospective. Those are record-completeness gates for final status movement even though the reviewed implementation criteria are supported.

### Verdict

Pass. The review did not falsify any DX2 acceptance criterion. Residual risk is limited to the explicit receipt-byte evidence gap and the separately governed typed project-policy shape.

### Closure recommendation

The implementation is eligible for DX2 closure. Before moving the ticket to `done`, the orchestrator should add the required acceptance-criterion evidence mapping and distill the executor retrospective; no implementation repair or new defect ticket is warranted from this review.

## Retrospective

- The adapter migration required a staged compatibility handoff: driver-owned adapters landed first while project-side adapters remained active, then the CLI registry was injected and the project-owned adapters and production convenience constructors were removed. That sequencing kept each recorded tranche testable while reversing the dependency direction.
- The broad CLI regression run initially reached 270 of 273 passes. Two registry-wording regressions and one pre-existing ledger-v5 expectation were repaired, and their focused reruns passed. The recorded lesson is that composition-boundary work can expose both boundary-specific output regressions and unrelated stale expectations; each needs to be classified before repair.
- The apparent remaining Postgres production edge proved to be a test fixture after production SQL discovery was traced to the neutral declarative/source boundary. Moving the fixture to dev-dependencies and adding a manifest-law test produced a durable production build-graph boundary instead of relying on the current dependency list.
- Driver declarations were sufficient to remove concrete replay target, policy, health, standard doctor, and lock-sheet branches. Keeping these semantics as inspected driver data made the generic paths auditable while preserving the separately authorized `doctor_drift.rs` diagnostic exception.
- The recurring friction came from concrete adapter construction living in a shared crate: it created dependency fan-out, convenience constructors, and repeated scheme-specific branches. Moving ownership to destination crates, injecting one explicit registry, and enforcing the normal dependency law addresses that cause at the boundary. The remaining typed Postgres project-policy shape is governed product configuration and is residual architecture risk for a future separately specified config evolution, not unfinished DX2 work.
