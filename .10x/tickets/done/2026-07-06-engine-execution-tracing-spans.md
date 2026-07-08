Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-observability-doctor-status-sql.md
Depends-On: .10x/tickets/done/2026-07-05-datafusion-engine-planner.md, .10x/tickets/done/2026-07-05-package-builder-reader.md

# Add engine execution tracing spans

## Scope

Implement the first bounded tracing-field slice for engine package execution. Add an explicit caller-supplied `RunId` tracing entry point around `cdf-engine` package execution so future run orchestration can emit spans with the book-required run, resource, partition, and package identifiers without the engine inventing run identity.

Own `crates/cdf-engine/**`, `Cargo.lock`, `crates/cdf-engine/Cargo.toml`, this ticket's evidence/review records, and parent progress notes. Keep `crates/cdf-engine/src/lib.rs` thin; if a new type is needed, put it in an existing focused module or a new focused module rather than expanding the crate root.

The preferred implementation is additive: preserve the existing public `execute_to_package` API for compatibility and introduce an explicit-run-id execution entry point, such as `execute_to_package_with_run_id` or an equivalent context-bearing API, that delegates to the same package execution logic. Do not synthesize a `RunId` from package id, resource id, timestamps, UUIDs, paths, or tests.

## Acceptance criteria

- `cdf-engine` exposes an explicit package execution entry point that requires a caller-supplied `RunId`.
- The explicit-run-id execution path emits a top-level execution span containing exact `run_id`, `resource_id`, and `package_id` fields.
- The explicit-run-id execution path emits per-partition spans containing exact `run_id`, `resource_id`, `package_id`, and `partition_id` fields.
- Tests capture tracing output for at least one package execution and one partition and assert the exact field names and values.
- Tests prove tracing does not change package identity/hash-relevant artifacts for an otherwise identical execution, or otherwise compare the manifest hash before/after a traced run against the untraced path.
- No secret-bearing plan filters, config values, auth material, URLs with credentials, or environment-derived secret values are added as span fields.
- The existing `execute_to_package` API remains available unless source inspection proves compatibility is impossible; any need to break that public API must be recorded as a blocker before editing.

## Evidence expectations

Record focused `cargo fmt --all -- --check`, `git diff --check`, `cargo test -p cdf-engine --locked --no-fail-fast`, and `cargo clippy -p cdf-engine --all-targets --locked -- -D warnings`.

Because this adds a dependency and public API, also record `cargo check --workspace --all-targets --locked`, `cargo test --workspace --all-targets --locked --no-fail-fast`, `cargo semver-checks --workspace --baseline-rev HEAD`, `cargo machete`, `cargo audit`, `cargo deny check`, `cargo vet`, direct unsafe/FFI/raw-pointer source scan over `crates/`, and the reused `tools/codeql-rust-quality.sh` result. Reuse `target/quality/codeql-db-rust` and do not recreate the CodeQL database unless the wrapper decides the fingerprint is stale.

Run mutation testing only if the tracing field assertions are not already strict enough to fail on omitted or renamed fields. If mutation tooling is skipped, record why the tracing assertions are sufficient for this slice.

## Explicit exclusions

No `inspect run`, no CLI parser or command changes, no run ledger, no run id generation/minting policy, no run-to-package/checkpoint persistence, no OTLP exporter, no global subscriber initialization, no package `trace.jsonl` changes, no package manifest field changes, no checkpoint schema changes, no live `cdf run` orchestration, and no removal of existing untraced execution API unless recorded as a blocker.

`inspect run` remains blocked because current records and source do not ratify where run ids are minted, how runs map to packages/checkpoints/receipts, how multi-resource or multi-package runs are bounded, where transition ordering is stored, or which artifact owns verdict summaries.

## References

- `VISION.md` Chapter 15 observability and Chapter 17 CLI.
- `.10x/specs/project-cli-observability-security.md`
- `.10x/tickets/2026-07-05-observability-doctor-status-sql.md`
- `.10x/tickets/done/2026-07-05-cli-surface.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/quality-gate-execution.md`

## Progress and notes

- 2026-07-06: Explorer Hegel performed read-only inspection and found `inspect run` is not executable without inventing unratified run-ledger semantics. The safe next observability slice is engine execution tracing with an explicit caller-supplied `RunId`. Current `cdf-engine::execute_to_package` has resource, partition, and package context, and `RunId` already exists as a kernel id type, so a first tracing entry point can be added without solving run orchestration.
- 2026-07-06: Do not implement in the ticket-creation turn. Assign to a worker in a later turn with this ticket and the referenced records.
- 2026-07-06: Implemented additive `execute_to_package_with_run_id` in `cdf-engine`, preserving `execute_to_package`. The traced path emits exact package and partition span fields for caller-supplied `RunId`, resource id, package id, and partition id.
- 2026-07-06: Added exact span-capture tests, traced/untraced manifest identity tests, and mutation-driven hardening for residual limit depletion, profile byte accounting, and validation-program source/output-name coverage.
- 2026-07-06: Closure evidence recorded in `.10x/evidence/2026-07-06-engine-execution-tracing-spans.md`; closure review recorded in `.10x/reviews/2026-07-06-engine-execution-tracing-spans-review.md`. Final bounded mutation testing over `crates/cdf-engine/src/execution.rs` reported 29 mutants tested, 18 caught, 11 unviable, and 0 missed.

## Blockers

None.
