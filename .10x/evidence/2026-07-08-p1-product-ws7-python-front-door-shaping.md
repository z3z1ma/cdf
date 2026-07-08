Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/2026-07-08-p1-product-ws7-python-front-door.md, .10x/specs/python-front-door-product-surface.md

# P1 WS7 Python front-door shaping evidence

## What was observed

The WS7 parent mixed four independent outcomes: product resource resolution/plan/preview, run-spine execution, interpreter CI matrix evidence, and dlt GA/gap ownership.

Existing records already establish the lower-level facts needed to split the work:

- `.10x/specs/resource-authoring-planning-batches.md` says Python is Tier 2 authoring/interchange only, downstream execution remains Rust, and GIL/free-threaded deterministic semantics must match.
- `.10x/specs/project-cli-observability-security.md` requires Python interpreter configuration, doctor checks, and no secret leakage.
- `.10x/tickets/done/2026-07-05-python-sdk-bridge.md` records PyO3/pyo3-arrow bridge, dict batching, typed SDK stubs, GIL/free-threaded semantics, watchdogs, and redaction-aware context APIs.
- `.10x/tickets/done/2026-07-05-dlt-shim-preview.md` records the preview dlt shim and its explicit exclusion of bug-for-bug dlt emulation and dlt destination delegation.
- `.10x/tickets/done/2026-07-06-python-doctor-interpreter-probe.md` records the fixed process-based doctor probe and the no-resource-code-execution guarantee.

Source inspection found:

- `crates/cdf-project/src/models.rs` already classifies `source` values beginning with `python://` as `ResourceSourceKind::Python`.
- `crates/cdf-cli/src/project_run_resource.rs` currently converts only compiled declarative `Files`, `Rest`, and `Sql` resources into `CliProjectRunSource`.
- `crates/cdf-project/src/runtime/resources.rs` exposes `ProjectRunSource` wrappers for local file, REST, SQL, and prevalidated trait resources.
- `crates/cdf-cli/src/run_command.rs` invokes `build_project_run_resource` before calling `run_project`.

## Procedure

Read the WS7 parent, governing specs, done Python/dlt/doctor tickets, relevant source owner files, and the P1 parent. Then wrote:

- `.10x/specs/python-front-door-product-surface.md`
- `.10x/tickets/2026-07-08-p1-product-ws7a-python-resource-resolution-plan-preview.md`
- `.10x/tickets/2026-07-08-p1-product-ws7b-python-run-spine.md`
- `.10x/tickets/2026-07-08-p1-product-ws7c-python-interpreter-ci-matrix.md`
- `.10x/tickets/2026-07-08-p1-product-ws7d-dlt-ga-gap-integration.md`

## What this supports or challenges

This supports making WS7 executable without implementing on an unbounded premise. It preserves the existing rule that Python is an authoring/interchange tier and routes product behavior through the general resource/run surfaces.

## Limits

No runtime implementation, tests, CI workflow changes, or quality gates were run for this shaping slice. Child tickets own execution evidence.
