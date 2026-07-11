Status: done
Created: 2026-07-08
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p1-product-ws7-python-front-door.md
Depends-On: .10x/specs/python-front-door-product-surface.md, .10x/tickets/done/2026-07-05-python-sdk-bridge.md, .10x/tickets/done/2026-07-06-python-doctor-interpreter-probe.md, .10x/tickets/done/2026-07-07-cli-preview-resource-breadth.md

# P1 product WS7A: Python resource resolution, plan, and preview

## Scope

Wire `python://` project resources into the product resolution path for `cdf plan` and `cdf preview`.

Primary write scope is `crates/cdf-project/src/**`, `crates/cdf-cli/src/project_run_resource.rs`, `crates/cdf-cli/src/scan_command.rs`, focused tests, and this ticket's records. Touch `crates/cdf-python/**` only if the bridge is missing a narrow metadata or preview API required by this ticket.

## Acceptance criteria

- A project resource with `source = "python://..."` resolves by resource id through the same `ProjectContext` path used by existing resources.
- `cdf plan <python-resource>` returns honest descriptor/schema/partition/destination-planning facts and creates no package, destination, checkpoint, or run-ledger state.
- `cdf preview <python-resource>` opens one deterministic planned partition and emits one preview batch through the Python bridge, creating no package, destination, checkpoint, or run-ledger state.
- Missing, non-executable, unhealthy, missing-free-threaded, missing resource target, ambiguous target, and path-escaping URI cases fail closed with redacted errors that route interpreter remediation to `cdf doctor`.
- Doctor probes still execute only the fixed interpreter-inspection snippet and never import or execute project Python resource code.
- Existing declarative file, REST, and SQL plan/preview behavior is unchanged.

## Evidence expectations

Record focused `cdf-cli`, `cdf-project`, and `cdf-python` tests as applicable; no-write plan/preview filesystem assertions; interpreter-remediation snapshots; redaction checks; and the `QUALITY.md` profile selected for the touched source.

## Explicit exclusions

Do not implement package-producing `cdf run` for Python resources. Do not add dlt GA behavior. Do not add release workflows, docs, renderer changes, completions, or man pages.

## Progress and notes

- 2026-07-08: Split from WS7 parent after the Python front-door product spec was created. Current source recognizes `ResourceSourceKind::Python`, but the CLI run/plan/preview builders are still centered on compiled declarative resources.
- 2026-07-10: Closed by `edc8468e` and the trait-boundary repair `fa1b8092`. Inspect/plan resolve descriptor and schema metadata without invoking the row callable; preview emits one batch and asserts no package, destination, checkpoint, or ledger writes. Missing interpreter and path-escape paths use `CDF-PYTHON-RESOURCE` and `cdf doctor`. Evidence: `.10x/evidence/2026-07-10-p1-python-front-door-closure.md`; review: `.10x/reviews/2026-07-10-p1-python-front-door-closure-review.md`.

## Blockers

None.
