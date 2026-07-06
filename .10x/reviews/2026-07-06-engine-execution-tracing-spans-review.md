Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-engine-execution-tracing-spans.md
Verdict: pass

# Engine Execution Tracing Spans Review

## Target

Review of the `firn-engine` explicit-run-id package execution tracing slice and its closure evidence.

## Findings

No blocking findings.

Mutation testing initially exposed surviving mutants in existing engine execution assertions: profile byte accounting, residual limit depletion, and validation-program source/output-name coverage. The final change adds focused tests for those exact behaviors, and the final bounded mutation run reports 29 mutants tested with 18 caught, 11 unviable, and 0 missed.

The new tracing API is additive: `execute_to_package` remains exported, while `execute_to_package_with_run_id` requires a caller-supplied `RunId`. The implementation does not mint, infer, persist, or serialize run IDs. The crate root remains a thin module/export root, consistent with `.10x/knowledge/rust-crate-organization.md`.

The emitted span fields are limited to ratified identifiers: `run_id`, `resource_id`, `package_id`, and `partition_id`. Tests assert exact field maps, so omitted, renamed, or extra tracing fields fail. No plan filters, auth values, URLs, environment values, or secret-bearing configuration are recorded as span fields.

Package identity risk is covered by traced/untraced manifest identity, package hash, and signature comparison. The tracing instrumentation wraps execution with spans but does not write package artifacts or alter package manifest contents.

## Verdict

Pass. Acceptance criteria are met, focused and workspace quality gates passed, and mutation testing is clean for `crates/firn-engine/src/execution.rs`.

## Residual Risk

CodeQL was not rerun after final test-only mutation hardening to avoid another persistent database refresh; the production implementation and dependency changes were covered by the CodeQL run with 0 SARIF findings, and final tree checks covered tests, clippy, nextest, Semgrep, Gitleaks, unsafe source scan, Miri, `cargo careful`, coverage, and mutation testing. The Rust extractor still has known macro-expansion diagnostic limits documented in `.10x/knowledge/quality-gate-execution.md`.

`inspect run` remains intentionally out of scope and blocked on unratified run-ledger/run-to-artifact semantics owned by the observability parent ticket.
