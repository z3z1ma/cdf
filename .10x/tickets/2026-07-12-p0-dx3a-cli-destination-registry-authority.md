Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-11-p0-dx3-generic-lock-doctor-replay.md
Depends-On: .10x/tickets/done/2026-07-11-p0-dx2-driver-owned-adapters-composition.md

# P0 DX3A: inject one CLI destination registry authority

## Scope

Make one caller-supplied `cdf_runtime::DestinationRegistry` the authority for every generic destination operation within a CLI invocation. The separately authorized DuckDB-only `doctor_drift.rs` diagnostic remains outside this generic authority. Add a public `cdf_cli::invoke_with_destination_registry(args, &registry)` entry point for tests and future custom CLI composition. Keep `cdf_cli::invoke` as the production entry point: after successful argument parsing it constructs the builtin registry once from `crates/cdf-cli/src/destination_registry.rs` and delegates to the same parsed-command dispatch path as the injected entry point; neither entry point parses its arguments twice.

Thread the borrowed registry explicitly through command dispatch and every current generic destination consumer. Source inspection fixes the bounded propagation set at:

- resolution for plan/explain/preview, run, executable backfill, schema promotion, deep validation, and resume; dry-run backfill has no destination consumer and remains unchanged;
- inspection and health for `inspect destinations` and standard doctor;
- target parsing, inspection, policy validation, and resolution for `replay package` and `state recover`;
- destination sheet-artifact inspection used by add, contract freeze, plan-time discovery pinning, `schema pin` lock updates, and the other lock-producing helpers reached through those commands.

`ResumeAttempt` and `SelectedDestination` MUST receive and use that same borrowed authority so a registered fourth driver reaches the real CLI replay/recovery decision path. `ProjectContext` remains parsed project/state authority and MUST NOT own, construct, or discover destination registries.

## Non-goals

- Dynamic plugins, linker inventory, global registration, a service locator, or a registry factory/provider trait.
- Moving the builtin DuckDB/Parquet/Postgres registration list or adding a fourth production driver.
- Generalizing source, format, or byte-transform composition.
- Changing destination semantics, project configuration, command grammar, output schemas, error codes, redaction, receipt/checkpoint behavior, or the adapter-specific `doctor_drift.rs` exception.
- Repairing the separately owned staged-ingress replay expectation drift recorded in the DX3 journal.

## Acceptance criteria

- `cdf_cli::invoke_with_destination_registry` accepts a borrowed `DestinationRegistry` and uses that exact instance unchanged for parsed-command dispatch. `cdf_cli::invoke` preserves the normal binary API, returns parser failures before registry construction, constructs `builtin_destination_registry()` once per successfully parsed production invocation, and delegates to the same internal parsed-command dispatch path. Registry construction failures use the existing generic lower-layer `CliError`/`InvocationResult` boundary; valid builtin parser/help/version output remains unchanged, and neither entry point reparses arguments.
- Outside the builtin composition function and the single production call from the top-level invocation root, non-test CLI source contains no call to `builtin_destination_registry()`. Destination helpers accept `&DestinationRegistry`; they do not reconstruct, clone, cache globally, or obtain it through `ProjectContext`.
- Plan/run/replay/state-recover/resume/doctor/inspect and every lock-producing path named in Scope consume the registry passed at invocation. Existing destination-consuming deep-validation, schema, preview, and backfill paths use the same authority rather than retaining a builtin-only side path.
- A CLI-local fourth-driver fixture is registered onto a caller-owned registry and is accepted through the public injection entry point by lock generation, plan, run, replay, doctor, and the real `resume` command. Resume evidence executes the public invocation path through `ResumeAttempt`, not `resume_run` or a project recovery helper directly. One finalized-package/no-receipt case asserts `source_contact == false` and proves one destination commit, a durable receipt, checkpoint commit, and terminal package status without an unsupported-builtin fallback. A separate durable-receipt/proposed-checkpoint case proves resume verifies the receipt and commits the checkpoint/package status without another destination commit. Direct `recover_package_from_artifacts` is not resume evidence.
- The fourth-driver regression uses observable counters to prove inspection/health/plan and lock construction do not mutate the destination; run and replay commit only at their expected commit gates; replay duplicate handling and durable-receipt resume recovery do not duplicate a destination commit. Secret-bearing URI userinfo remains absent from JSON, human output, and errors.
- A permanent architecture assertion pins the composition law: in non-test CLI source the three concrete first-party driver imports remain confined to `destination_registry.rs` (plus the already-authorized adapter diagnostic), and builtin destination registry construction cannot reappear below the invocation root. CLI-local fixture code may import or define test-only drivers without becoming production composition.
- Existing first-party CLI tests and focused fourth-driver tests pass without changing established structured/human output, error codes, redaction, package identity, receipts, checkpoints, or lifecycle assertions.

## References

- `.10x/specs/destination-extension-runtime-contract.md`
- `.10x/decisions/destination-runtime-composition-boundary.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/tickets/2026-07-11-p0-dx3-generic-lock-doctor-replay.md`
- `.10x/tickets/done/2026-07-11-p0-dx2-driver-owned-adapters-composition.md`
- `.10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md`

## Assumptions

- **Record-backed:** The CLI is the one explicit first-party composition root; generic plan/run/replay/resume/doctor/lock behavior consumes registry authority, and hidden/global auto-registration is excluded by `.10x/specs/destination-extension-runtime-contract.md` and `.10x/decisions/destination-runtime-composition-boundary.md`.
- **Record-backed:** `cdf-project` already requires an injected neutral registry and has no normal concrete destination dependency; this ticket changes CLI propagation, not the lower runtime contract, per DX1/DX2 and `.10x/evidence/2026-07-11-p0-dx2-project-build-graph.md`.
- **Record-backed:** Existing command output, error, redaction, resume no-source-contact, receipt, and checkpoint semantics remain authoritative under `.10x/specs/project-cli-observability-security.md` and the DX3 review; no semantic default is introduced here.
- **User-ratified:** The injection seam is production-usable for tests and future external/custom composition while preserving one explicit builtin production composition root.
- **Mechanical and reversible:** The public entry-point name is `invoke_with_destination_registry`, and explicit borrowed parameters are used instead of a single-implementation context/provider abstraction.

## Journal

- 2026-07-12 (shaping): Fished active and terminal tickets; no existing owner covers CLI destination-registry propagation. Read the active DX3 and parent tickets, terminal DX1/DX2 records and evidence, the active destination extension contract, composition decision, CLI security spec, and the current CLI/project/runtime composition source.
- 2026-07-12 (shaping): Source authority shows `builtin_destination_registry()` is rebuilt inside `destination_uri.rs`, `replay_command.rs`, and `destination_registry.rs` inspection helpers. `ProjectContext::destination_runtime` reaches the latter; `ResumeAttempt -> SelectedDestination -> resolve_selected_destination_with_services` reaches the former. Consequently a caller-registered fourth driver cannot reach real CLI resume even though project artifact recovery is generic.
- 2026-07-12 (shaping): Selected explicit top-down borrowing as the smallest complete seam. Storing the registry in `ProjectContext` would mix configuration/state with runtime composition; a service locator/global would violate the active decision; a provider/factory trait would add a single-implementation abstraction and permit per-helper reconstruction. No product-semantic blocker remains.
- 2026-07-12 (adversarial shaping review): Traced the active contract/decision through `lib.rs -> commands.rs` and all current consumers. Confirmed hidden builtin reconstruction in `destination_uri.rs`, `replay_command.rs`, and both artifact/runtime helpers in `destination_registry.rs`; confirmed `state recover` reuses replay destination construction and `ResumeAttempt -> SelectedDestination` reaches the builtin-only resolver. Tightened the ticket to name the complete propagation set, preserve the authorized `doctor_drift.rs` exception, forbid double parsing, and require separate real-CLI resume proofs for the no-receipt replay and durable-receipt recovery branches.

## Blockers

None. Scope, authority, behavior-preservation constraints, and acceptance evidence are established by the referenced active records and current source.

## Evidence

Pending execution. Each acceptance criterion requires journaled command/output evidence with stated limits before closure.

## Review

### Findings

- **Significant, repaired in shaping — the original “every destination operation” scope contradicted its own `doctor_drift.rs` non-goal.** The active composition decision explicitly permits that adapter-specific diagnostic outside the generic registry contract. Scope and the architecture criterion now say “every generic destination operation” and retain exactly that named exception.
- **Significant, repaired in shaping — the propagation and crash-matrix evidence were underspecified for a cold executor.** `state recover` calls `build_replay_destination`, while plan-time discovery, add, contract freeze, and schema pin each reach destination sheet inspection. The source-backed list now names those consumers. The prior resume wording could be satisfied by only the finalized/no-receipt replay branch even though its duplicate-after-durable-receipt claim requires a different branch; acceptance now requires both real public-invocation cases and excludes direct helper evidence.
- **Minor, repaired in shaping — the entry-point wording permitted an accidental second parse and did not distinguish production parser failure from registry construction.** Acceptance now requires parser failures before builtin construction, one parse per entry point, shared parsed-command dispatch, and the existing generic lower-layer CLI error mapping.
- **No service-locator or test-only-abstraction finding.** The ticket requires an explicit borrowed `&DestinationRegistry` from invocation dispatch through helpers, forbids storing it in `ProjectContext`, and rejects provider/factory/global alternatives. `DestinationRegistry` is already a public neutral runtime type, so the public injection seam adds no new abstraction or concrete driver exposure.
- **No unresolved semantic assumption found.** Registry ownership, the public injection seam, behavior preservation, redaction, no-source-contact resume, and the first-party composition boundary are record-backed or user-ratified in Assumptions. The edits above clarify source-backed reachability and evidence without choosing new product behavior.
- **Execution coordination risk, not an executability blocker.** DX3 has uncommitted owned changes in `doctor_command.rs`, `replay_command.rs`, `reports.rs`, `cdf-project/runtime_tests.rs`, and runtime/destination support files, while unrelated E3/J0/WASM work also dirties the worktree. DX3A is the explicit child that unblocks and completes the parent’s real-resume criterion, so its executor must preserve and extend the parent-owned diff and avoid unrelated paths; it must not treat the dirty tree as authority to rewrite or discard changes.

### Verdict

Pass after shaping repair. Scope, authority lifetime, production composition, public API visibility, consumer propagation, evidence obligations, dependency relationship, and semantic provenance are concrete enough for a cold-start executor. Residual risk is integration with the parent’s uncommitted DX3 diff; the ticket’s narrow ownership and review/evidence gates make that coordination auditable.

## Retrospective

Pending execution.
