Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-python-doctor-interpreter-probe.md
Verdict: pass

# Python doctor interpreter probe review

## Target

Review of the `firn doctor` Python interpreter probe implemented in `crates/firn-cli/src/commands.rs` with tests in `crates/firn-cli/src/tests.rs`.

## Findings

No blocking findings remain.

Earlier adversarial checks found actionable gaps in the first implementation: the fixed invocation contract did not yet prove project Python resource code was excluded, the required-free-threaded success case was missing, mutation testing exposed weak assertions for failure details and inconsistent probe metadata, and clippy flagged an over-broad test helper signature. Those were resolved before this review record by adding focused tests and refactoring the helper to named fields.

## Assumptions tested

- The doctor does not silently skip Python checks when `python://` resources exist and `python.interpreter` is absent.
- The interpreter process receives only `-I -c <fixed inspection snippet>`, not resource paths, function names, or `python://` URIs.
- Probe stdout/stderr is not echoed on unsuccessful execution or invalid JSON, preventing accidental secret leaks from interpreter output.
- `python.require_free_threaded = true` requires both `free_threaded_build = true` and `gil_enabled = false`.
- Probe JSON cannot claim contradictory version or GIL/free-threading metadata without failing the doctor check.
- Existing DuckDB drift doctor behavior remains read-only and does not create missing state or DuckDB files.

## Verdict

Pass. The implementation satisfies the child ticket acceptance criteria with focused CLI tests, mutation hardening for the actionable branches, workspace quality gates, security scans, secret scans, and CodeQL analysis recorded in `.10x/evidence/2026-07-06-python-doctor-interpreter-probe.md`.

## Residual risk

The remaining mutation miss is the `#[cfg(not(unix))]` `is_executable` fallback branch, which is not compiled on this macOS run. This is a platform coverage limit, not a defect in the Unix path covered by the current environment.

The CodeQL Rust extractor still reports known macro diagnostics while producing 0 SARIF findings; `.10x/knowledge/quality-gate-execution.md` and `.10x/evidence/2026-07-06-codeql-rust-extractor-coverage.md` describe that local tooling limit.

Full `cargo deny check` and `cargo vet` remain repository-level policy blockers owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`, not this implementation slice.
