Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-general-run-orchestrator.md
Depends-On: .10x/specs/run-orchestration-ledger.md, .10x/specs/package-lifecycle-determinism.md

# Add non-file resource streams to the general run orchestrator

## Scope

Extend `cdf-project` general run orchestration beyond declarative local file resources so REST resources and table-backed Postgres SQL resources can be run safely when deterministic runtime dependencies are supplied.

Owns:

- A project-run resource input shape that can accept supported `ResourceStream` implementations with their required runtime dependencies.
- State-delta construction for non-file `SourcePosition` values.
- Fail-closed validation for unsupported source/runtime combinations before package, destination, or checkpoint mutation.
- Tests for deterministic REST and table-backed Postgres SQL resource streams where existing lower-layer harnesses make them safe.

## Acceptance criteria

- The orchestrator can execute a supported REST `ResourceStream` without using `CompiledResource::open` for REST, which currently fails by design.
- The orchestrator can execute a supported table-backed Postgres SQL `ResourceStream` with explicit runtime dependencies.
- State-delta artifacts and checkpoint commits use ratified source-position semantics for each supported source kind.
- Recovery after package finalization or durable receipt does not contact the source.
- Unsupported or missing source runtime dependencies fail before mutation.

## Blockers

None for this bounded slice. The project-run API must accept only local-file compiled resources or concrete runtime wrappers that own their dependencies (`RestResource`, `SqlResource`). Non-file checkpoint construction is limited to one coherent emitted `SourcePosition` across all package segments; divergent per-segment source positions must fail closed instead of aggregating cursors, page tokens, or windows.

## Explicit exclusions

No live external HTTP credentials, no arbitrary SQL query execution, no scheduler/resident streaming, no CLI parsing, and no cursor/page-token aggregation across divergent segment positions. Multi-segment cursor window-close semantics remain outside this ticket unless separately specified.

## Evidence expectations

Run focused `cdf-project` tests, existing REST/SQL resource conformance tests where deterministic, `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`, and workspace check.

## Progress and notes

- 2026-07-07: Blocked during `.10x/tickets/2026-07-07-general-run-orchestrator.md` continuation. Inspection found safe lower-layer `RestResource` and `SqlResource` wrappers exist, but the general project-run request and checkpoint artifact semantics do not yet carry the needed runtime dependencies or non-file source-position contract.
- 2026-07-07: Reactivated for a conservative dependency-bearing stream slice. Governing records now support typed cursor/file/source positions (`.10x/specs/checkpoint-state-commit-gate.md`), execution through `ResourceStream` (`.10x/specs/run-orchestration-ledger.md`), and concrete REST/SQL runtime wrappers in source. The worker must keep raw REST/SQL `CompiledResource::open` out of the project run path, validate missing runtime dependencies before package/destination/state mutation, and fail closed on divergent segment source positions rather than inventing cursor aggregation semantics.
- 2026-07-07: Implemented the bounded dependency-bearing stream slice in `cdf-project` and `cdf-declarative`: `ProjectRunRequest` now accepts only local-file compiled resources or concrete `RestResource`/`SqlResource` wrappers, runtime execution uses the selected `ResourceStream`, REST/SQL missing secret-provider dependencies fail before package/destination/state writes, and state-delta preimages accept one coherent non-file `SourcePosition` while preserving file-manifest normalization. Added focused REST, raw compiled REST rejection, missing dependency, divergent position, and live/local Postgres SQL run coverage. Verification passed: `cargo fmt --check`; `cargo check -p cdf-project --tests --locked`; `cargo test -p cdf-project general_project_run_ --locked -- --nocapture` (9 passed; local Postgres test executed); `cargo test -p cdf-project state_delta_rejects_divergent_segment_source_positions --locked`; `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`; `cargo check --workspace --locked`.
- 2026-07-07: Parent review found that accepting inexact REST cursors or missing cursor declarations would violate active window-close semantics and that dependency preflight needed to resolve actual secret values, not just provider presence. Repaired by requiring exact zero-lag cursors for non-file project runs in this slice, resolving REST/SQL secrets in preflight, rejecting missing/empty secrets before writes, and adding project/declarative tests for those cases.
- 2026-07-07: Closed with evidence `.10x/evidence/2026-07-07-general-run-non-file-resource-streams.md` and review `.10x/reviews/2026-07-07-general-run-non-file-resource-streams-review.md`. Final gates passed on the current tree: fmt check, diff check, workspace check, workspace clippy, touched package tests, final Semgrep Rust/security-audit scans, final source-only gitleaks, and direct unsafe scan. Earlier full workspace tests, Nextest, feature-matrix checks, docs, deny/vet/audit/OSV, reusable CodeQL analysis, semver, machete, and metrics are recorded in the evidence record. Broader non-file window-close/page-token semantics are split to `.10x/tickets/2026-07-07-non-file-window-close-checkpoint-semantics.md`.
