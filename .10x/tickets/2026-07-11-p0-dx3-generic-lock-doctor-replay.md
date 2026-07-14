Status: blocked
Created: 2026-07-11
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-11-p0-destination-extension-boundary.md
Depends-On: .10x/tickets/done/2026-07-11-p0-dx2-driver-owned-adapters-composition.md, .10x/tickets/2026-07-12-p0-dx3a-cli-destination-registry-authority.md

# P0 DX3: generic lock, doctor, replay, and product surfaces

## Scope

Replace lockfile URI matches, CLI destination runtime enum, doctor/replay target parsing, and legacy concrete orchestration helpers with registry inspection/runtime capabilities and typed probes. Preserve command output semantics additively.

## Acceptance criteria

- Lock generation gets sheet artifacts from driver inspection and contains no destination scheme match.
- Doctor and replay generic modules contain no concrete destination imports or branches.
- A mock fourth driver works through lock, inspect/doctor, plan, replay, and resume.
- Secret redaction and no-mutation inspection are proven.

## Blockers

- Remaining focused report, doctor, and project reruns are pending the orchestrator-owned Cargo slot. A single focused `cdf-runtime` metadata-validation test passed after the earlier unrelated kernel compile failure cleared; no additional Cargo command was started after the orchestrator reserved the slot for other work. DX3 did not edit or repair the unrelated kernel files.
- DX3A has implemented and focused-tested the real public CLI `resume` path with the injected fourth driver, resolving this ticket's original architectural blocker. DX3A itself is now blocked only by its explicit full-CLI-suite gate (272/291); this parent remains blocked until that child moves terminal, after which its fourth-driver evidence can be consumed without repetition.

## Journal

- 2026-07-12: Began execution after confirming DX2 is done and the only pre-existing dirty path is the unrelated active E3 ticket `.10x/tickets/2026-07-11-p3-e3-streaming-verification-replay-io.md`. Read the DX3 ticket, governing runtime contract and composition decision, DX2 ticket/build-graph evidence, and started source-authority inspection. No overlapping DX3 file was dirty at entry.
- 2026-07-12: Removed DuckDB/Parquet/Postgres matches from generic CLI destination and receipt reports. `DestinationDescription` now carries driver-owned optional location-field and receipt-source metadata; DuckDB and filesystem Parquet declare their existing compatibility field names, and DuckDB declares the existing `duck_db_commit` label. Generic rendering flattens those fields without naming a destination and preserves URI redaction.
- 2026-07-12: Extended the existing registered mock destination regression. One fourth driver now yields a lock-ready sheet artifact, typed non-mutating inspection/health, a redacted secret contract, a generic commit plan, package replay, and receipt-based recovery without a destination-specific shared-code branch. Inspection/health assertions run before resolution and assert zero destination writes.
- 2026-07-12: A focused CLI replay filter exposed pre-existing active-E3 drift: staged-ingress replay in `crates/cdf-project/src/runtime/replay.rs` reports `DestinationCommitReceiptOnly`, while five legacy CLI tests still expect DuckDB duplicate/progress classification. `git blame` places the staged path before DX3, and the active E3 ticket owns staged replay I/O. DX3 neither changed that semantic nor edited E3's dirty ticket; this is recorded as a no-action rationale because it already has a durable owner.
- 2026-07-12: After successful DX3 compilation/tests, unrelated dirty kernel statistics work appeared and made subsequent Cargo invocations fail in `cdf-kernel` before reaching DX3. Stopped rather than overlapping or repairing another executor's files.
- 2026-07-12 (review repair): Moved driver label redaction to `RunDestinationReport::from_project`, so the same redacted value feeds flattened JSON and human output, and added structured JSON assertions. Standard doctor now recursively redacts URI userinfo in driver health messages/details before `DoctorCheck` serialization; its regression asserts both JSON and human output.
- 2026-07-12 (review repair): Added destination-neutral `DestinationDescription` validation at registry inspection and resolution. Driver-provided location fields must be non-empty snake_case and cannot collide with `kind`, `destination_id`, or `target`; receipt-source values must be non-empty snake_case. Registry tests cover rejection on both inspection and resolution.
- 2026-07-12 (review repair): Strengthened the fourth-driver regression to pass the inspected sheet artifact through `generate_lockfile_with_destination_artifacts`, use `ResolvedProjectDestination::plan_resource_commit`, retain staged project replay assertions, and use the artifact recovery orchestration called by resume. Removed the prior vacuous secret-free health serialization claim. A separate standard-doctor regression now supplies secret-bearing health metadata and exercises actual `destination_checks` plus `DoctorReport` JSON/human rendering. The CLI `ResumeAttempt` path remains the explicit blocker above rather than being represented by artifact recovery.
- 2026-07-12 (review repair verification): `cargo test -p cdf-runtime registry_rejects_product_metadata_that_cannot_compose_with_stable_reports --locked` passed 1/1 with 35 filtered. The report, doctor, and strengthened project regressions were not run because the orchestrator reserved the Cargo slot; source formatting and `git diff --check` passed for the scoped repair files.

## Evidence

| Acceptance criterion | Observation | Limits |
| --- | --- | --- |
| Lock generation gets sheet artifacts with no destination scheme match. | The fourth-driver regression calls `DestinationRegistry::inspect`, validates the returned `DestinationSheetArtifact`, and proves inspection performs zero writes. Static `rg` over `cdf-project/src/lockfile.rs` and generic CLI lock/doctor/replay/report modules found no concrete destination import or equality/match branch. | Existing first-party lock wiring was completed and evidenced by DX2; this ticket adds the fourth-driver/no-mutation proof rather than repeating all lock tests. |
| Doctor and replay generic modules contain no concrete destination imports or branches. | `rg -n 'cdf_dest_(duckdb|parquet|postgres)|cdf-dest-(duckdb|parquet|postgres)'` and the concrete equality/match search returned no output for `reports.rs`, `replay_command.rs`, `doctor_command.rs`, and `lockfile.rs`. Generic reports now consume driver description metadata. | `doctor_drift.rs` remains the explicitly ratified adapter-specific diagnostic exception. Fixed user-facing examples/hints are not resolution branches and were not changed. |
| A mock fourth driver works through lock, inspect/doctor, plan, replay, and resume. | Source-level repair now routes the inspected artifact through lock generation, the resolved driver through the project planning facade, project replay, and artifact recovery; standard doctor rendering is exercised separately with secret-bearing driver health metadata. | Fresh execution is pending the orchestrator-owned Cargo slot. CLI `ResumeAttempt` is not exercised; see Blockers. Artifact recovery is not claimed as resume orchestration. |
| Secret redaction and no-mutation inspection are proven. | Source-level assertions now cover both JSON and human run reports, plus both JSON and human standard-doctor reports with secret-bearing URI userinfo in driver health messages/details. The fourth-driver regression asserts zero writes through inspection, lock generation, health, and planning. | Fresh execution is pending the orchestrator-owned Cargo slot. Redaction covers URI userinfo at these product boundaries; arbitrary non-URI driver secrets remain a driver redaction obligation unless supplied through runtime secret-redaction authority. |

Additional verification:

- `cargo check -p cdf-cli --tests --locked`: passed after the implementation.
- `cargo fmt --all -- --check`: passed after formatting.
- `git diff --check -- <DX3 paths>`: passed.
- Focused `cargo test -p cdf-cli --lib replay_package_ --locked`: 12 passed, 5 failed. The failures expose the already-owned staged-ingress/legacy-expectation drift described above; they do not falsify the fourth-driver branch removal. No broad workspace or release build was run.

## Review

### Findings

- **Significant — structured run/replay output bypasses the redaction asserted by this slice.** `RunDestinationReport::from_project` copies `DestinationDescription::label` verbatim into `product_fields`, and `#[serde(flatten)]` serializes that map directly into JSON. Only the human `summary()` path calls `safe_display_value`. The new regression deliberately supplies `postgres://user:secret-value@localhost/db`, but asserts only the rendered human document; serializing the same `RunCliReport` retains `secret-value` in `destination.database_path`. This falsifies the ticket's secret-redaction criterion for the structured product surface and conflicts with `.10x/specs/project-cli-observability-security.md`'s requirement that resolved secrets not appear in output. The test must cover the serialized report as well as the human renderer, and the compatibility field value must be redacted before serialization (without changing non-secret first-party path compatibility).
- **Significant — the fourth-driver regression does not execute the lock, doctor, or resume product paths claimed by the acceptance criterion.** The test calls `DestinationRegistry::inspect` and checks the returned sheet artifact, but never passes that artifact through lock construction/generation; it calls `DestinationRegistry::health` and serializes the driver's already-secret-free result, but never passes it through `destination_checks`/`DoctorReport`; and it calls `recover_package_with_runtime`, not resume orchestration. The health assertion is therefore vacuous as a redaction proof because the mock health result never contains the credential or an error containing it. The test does prove generic protocol planning, project package replay, receipt-based recovery, and no destination writes during direct inspection/health. It does not prove the full lock/inspect-doctor/plan/replay/resume acceptance scenario stated in this ticket and the governing runtime contract.
- **Significant — driver-controlled compatibility field names can corrupt the stable structured destination schema.** `product_location_field` is an arbitrary unvalidated string in the lower `cdf-runtime` description, and the CLI flattens it alongside reserved fields `kind`, `destination_id`, and `target`. A fourth driver can therefore select a reserved name and cause duplicate/conflicting JSON keys; registration/inspection performs no validation or collision rejection. `product_receipt_source` is likewise an arbitrary unvalidated product token. This makes product-output compatibility an unchecked driver obligation and does not provide a safe generic fourth-driver composition law. Use a typed compatibility description or validate a closed set of reserved/non-empty field and source identifiers before rendering.

### Confirmed boundaries

- No concrete DuckDB, Parquet, or Postgres import or destination-name resolution branch was found in the scoped generic `reports.rs`, `replay_command.rs`, `doctor_command.rs`, or `lockfile.rs` audit. The first-party driver declarations preserve the existing `database_path`, `root`, and `duck_db_commit` spellings.
- The project regression does assert zero destination writes after direct inspection and health, and its replay/recovery assertions cover stage order, receipt coverage, checkpoint equality, and absence of a second destination write during receipt recovery.
- Broad Cargo verification was not repeated. The executor's recorded later compile blocker is outside DX3, and this review used source inspection rather than allowing the unrelated J0 kernel drift to contaminate the findings.

### Verdict

**Fail.** The review falsified the secret-redaction acceptance criterion on structured output and found that the fourth-driver test does not exercise three named product paths. The arbitrary flattened compatibility key also leaves stable JSON composition unsafe for a fourth driver.

### Residual risk

Even after repairing the JSON redaction and metadata validation, closure still requires direct evidence that one registered mock driver reaches lock generation, doctor rendering, and resume orchestration without shared destination branches. The unrelated J0 compile drift limits fresh executable evidence but does not explain the source-level redaction leak or the missing assertions.

### Fresh repair re-review (2026-07-12)

#### Findings

- **No remaining finding on prior finding 1.** `RunDestinationReport::from_project` now redacts URI userinfo before the driver label enters either `display_label` or the flattened `product_fields` map. The same redacted value therefore feeds structured JSON and human rendering, and the report regression asserts both forms. Standard doctor applies the same destination-neutral URI-userinfo redaction to every health message and recursively to string values nested in JSON arrays/objects before constructing the serializable `DoctorCheck`; its regression passes a credential-bearing driver result through `destination_checks` and asserts both `DoctorReport` JSON and human rendering. The regression's detail fixture is shallow, so fresh executable evidence of a nested fixture is not claimed, but source inspection confirms recursive traversal rather than a top-level-only rewrite. Inspection-error redaction and arbitrary non-URI secrets remain driver/runtime-contract obligations and were not newly claimed by this repair.
- **No remaining finding on prior finding 3.** `DestinationDescription::validate` rejects empty or non-snake-case location/source identifiers and rejects the flattened destination report's reserved `kind`, `destination_id`, and `target` location keys. `DestinationRegistry::inspect` and `DestinationRegistry::resolve` both validate the driver-produced description before a generic product renderer can consume it. The focused runtime regression exercises the reserved-key rejection through both registry paths and separately covers an invalid receipt-source token. This is validation at the neutral authority boundary rather than a CLI destination list.
- **No finding on the strengthened implemented fourth-driver path.** The registered mock driver's inspected `DestinationSheetArtifact` is passed to `generate_lockfile_with_destination_artifacts` and recovered from the resulting lock; its resolved runtime is passed through `ResolvedProjectDestination::plan_resource_commit`; package replay executes the real project replay stages through destination commit, receipt, checkpoint, and package-status update; and `recover_package_from_artifacts`, the artifact-recovery orchestration used by resume after selection, proves receipt recovery without a duplicate destination write. Inspection, lock generation, health, and planning retain the zero-write assertions. Standard doctor rendering is exercised by a separate focused fourth-destination fixture rather than falsely inferred from a secret-free runtime health serialization.
- **Significant closure blocker — real CLI resume still cannot accept the fourth-driver registry.** `ResumeAttempt::selected_destination_or_report` calls `SelectedDestination::from_context`; that calls `resolve_selected_destination_with_services`; and that helper unconditionally constructs `builtin_destination_registry()`, whose composition contains only DuckDB, Parquet, and Postgres. Neither `ResumeAttempt`, `SelectedDestination`, `ProjectContext`, nor the helper accepts injected registry authority. Consequently the recovery helper assertion is valid evidence for the implemented artifact-recovery surface but is not evidence for the acceptance criterion's CLI resume path. Repairing this requires a separately shaped, bounded CLI composition/test-injection ticket because it changes registry ownership and propagation across shared run/replay/resume destination selection; widening the project regression or disguising recovery as resume would not satisfy the contract. DX3 must remain active and depend on that ticket (or the acceptance criterion must be explicitly superseded) before closure.

#### Confirmed boundaries and evidence limits

- Static searches found no concrete DuckDB, Parquet, or Postgres crate import or destination-name resolution branch in the scoped generic report, replay, standard-doctor, and lock modules. `git diff --check` passed for the DX3 paths.
- This fresh agent did not repeat broad Cargo verification or the executor's focused runtime test. The orchestrator owns the shared Cargo slot; this review is based on the current diff, source call-chain inspection, the executor's journaled focused pass, and narrowly scoped static checks.
- The requested `5.6-sol/high` model/reasoning selection could not be passed to this fresh review agent because the collaboration API exposes no model or reasoning fields.

#### Verdict

**Pass for the repaired implemented surface; not eligible for DX3 closure.** The fresh review did not falsify the structured/human redaction repair, recursive doctor value redaction, neutral metadata validation, real lock generation, project planning, replay, or artifact-recovery assertions. However, the ticket-level verdict remains **concerns/blocked for closure** because the named fourth-driver `resume` acceptance criterion is still unsupported by the real CLI registry call chain. Artifact recovery must not be relabeled as resume evidence.

#### Residual risk

Until registry authority can be injected through CLI destination selection, fourth-driver behavior proven below that seam can drift from the builtin-only resume product path. The separate architecture ticket should preserve the single explicit production composition root while adding a narrow injection seam and a real `ResumeAttempt` fourth-driver acceptance test; DX3 should link that owner and consume its evidence rather than absorb the refactor ad hoc.

### Final batch re-review (2026-07-12)

#### Findings

- **No critical or significant finding in the current implemented slice.** Fresh source and diff inspection found no weakening of production semantics in the report, replay, doctor, runtime-description, registry-validation, or project facade changes. First-party compatibility remains data-driven: DuckDB declares `database_path` and `duck_db_commit`, filesystem Parquet declares `root`, and Postgres retains the neutral defaults. The generic report and replay paths no longer infer those values from destination names.
- **No critical or significant fixture finding after the final repairs.** The fourth-driver regression uses a syntactically valid project resource mapping, a declarative SQL resource solely to avoid an unresolved local-file runtime during plan construction, the identifier policy derived from the mock destination sheet, and project target `orders`, which matches the built package's destination-commit target. Lock generation receives the inspected destination artifact; planning uses `resolve_project_run_destination` and `ResolvedProjectDestination::plan_resource_commit`; recovery uses the public `recover_package_from_artifacts` facade. The obsolete test-only `recover_package_with_runtime` re-export was removed rather than retained to support the regression.
- **No critical or significant redaction or metadata-validation finding.** `RunDestinationReport::from_project` redacts URI userinfo before the driver label reaches either flattened JSON or human rendering. Standard doctor redacts URI userinfo in every health message and recursively in JSON string values before constructing serializable checks. `DestinationDescription::validate` rejects invalid product identifiers and collisions with the stable flattened report fields, and the neutral registry applies that validation on both inspection and resolution. The focused regressions exercise structured and human report/doctor output plus both registry validation paths.
- **Significant ticket-level closure blocker remains unchanged — real CLI resume still uses builtin-only registry authority.** DX3A (`.10x/tickets/2026-07-12-p0-dx3a-cli-destination-registry-authority.md`) remains the bounded owner for propagating one registry through the real public CLI resume path. Artifact recovery is valid evidence for the implemented lower facade but is not resume evidence, so DX3 cannot close.

#### Evidence limits

- This was a fresh adversarial source/diff review. No build or test was run and the orchestrator's passing focused evidence was not repeated.
- `git diff --check` passed for the scoped DX3 paths, and static searches found no concrete first-party destination import or destination-name branch in generic report, replay, standard-doctor, or lock modules.
- The requested `5.6-sol/high` selection was unavailable because the collaboration interface exposes no model or reasoning selector.

#### Verdict

**Pass for the current implemented batch; blocked for ticket closure.** No critical or significant implementation issue remains in the reviewed DX3 slice. DX3 MUST remain active until DX3A supplies real public-CLI fourth-driver resume evidence or the governing acceptance criterion is explicitly superseded.

#### Residual risk

The URI-userinfo scrubber is intentionally narrow and driver-provided non-URI secret text remains governed by the runtime's explicit secret-redaction authority. The material open risk is unchanged: below-seam fourth-driver recovery can drift from builtin-only CLI resume until DX3A lands.

## Retrospective

- Most lock/doctor/replay resolution work had already landed during DX2. The remaining extension leak was output compatibility: generic reports still inferred field names and receipt labels from schemes. Moving that descriptive data onto the driver boundary removed the branch while preserving first-party JSON.
- Extending the existing full mock replay/recovery test was smaller and stronger than inventing a second synthetic harness: the same registry instance now proves inspection, health, planning, replay, and recovery with observable mutation counters.
- Focused filters can still exercise active neighboring semantics. The replay filter usefully exposed stale E3-era expectations, but DX3 should not repair another active ticket's staged-ingress classification. Recording the existing owner and limits keeps that failure from being mistaken for DX3 evidence.
- Concurrent dirty-source arrival can invalidate later reruns after earlier evidence passed. Exact command timing and limits matter; stopping at the foreign kernel compile failure avoided turning a verification blocker into an overlapping implementation change.
- Review repair exposed that a direct runtime helper can look like orchestration evidence while skipping product decisions. Strong tests must call the named lock/planning/doctor boundaries, and a missing registry-injection seam in CLI resume must remain a visible blocker rather than be disguised as equivalent artifact recovery.
