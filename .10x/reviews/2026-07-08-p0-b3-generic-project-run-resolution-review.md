Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-07-p0-b3-generic-project-run-resolution.md
Verdict: pass

# P0 B3 Generic Project Run Resolution Review

## Target

Ticket `.10x/tickets/done/2026-07-07-p0-b3-generic-project-run-resolution.md`, evidence `.10x/evidence/2026-07-08-p0-b3-generic-project-run-resolution.md`, and the B3 source changes in `crates/cdf-project`, `crates/cdf-cli`, and `crates/cdf-conformance`.

## Findings

- Minor, accepted: `cargo semver-checks` reports removal of public `ProjectRunResource` and `ProjectRunDestination`. This is the intended B3 break: the old public closed enums were the architecture debt, and the repo is still pre-1.0.
- Minor, accepted: `jscpd` still reports 7.97% duplication in the scoped runtime/CLI/conformance surface. Inspection and the metric trend show the B3 split reduced the inherited duplication; remaining duplication is mostly existing test and harness shape and does not block this ticket.
- Minor, owned by B4: destination-specialized package replay/recovery wrapper families remain in the public API and in CLI/conformance callers. This is explicitly excluded from B3 and owned by `.10x/tickets/done/2026-07-07-p0-b4-caller-migration-wrapper-deletion.md`.

## Assumptions Tested

- The run path no longer depends on the removed closed enum names: `rg` over Rust source found no `ProjectRunDestination` or `ProjectRunResource`.
- CLI `run` no longer duplicates destination URI parsing for the built-in destination set; it uses `resolve_project_run_destination`.
- Resource-specific validation still occurs for local file, REST, and SQL resources before orchestration.
- Lazy filesystem Parquet destination resolution preserves validation-before-write behavior for unsupported dispositions.
- Trait-level destination receipt verification remains in the generic replay path.

## Verdict

Pass. B3 removes the closed run resource/destination API, routes project runs through the generic destination runtime path, keeps focused tests green, and leaves the remaining wrapper-family debt to the already-open B4 ticket.

## Residual Risk

The main residual risk is Workstream B incompleteness, not B3 correctness: replay/resume wrappers and caller migration still need B4 closure before the stop-the-line can lift for Workstream B.
