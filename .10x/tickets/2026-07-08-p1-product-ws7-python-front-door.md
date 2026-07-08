Status: active
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-experience-program.md
Depends-On: .10x/tickets/done/2026-07-05-python-sdk-bridge.md, .10x/specs/resource-authoring-planning-batches.md, .10x/specs/python-front-door-product-surface.md

# P1 product WS7: Python through the front door

## Scope

Make `python://` resources runnable through the product surface, not only through the lower-level bridge.

## Required outcomes

- Wire the existing PyO3 bridge into project resource resolution and the run, plan, and preview paths.
- `cdf run my.python_resource` works end to end through the general run spine.
- Plan/run interpreter-missing errors route users through `cdf doctor` remediation.
- Add free-threaded 3.14t and GIL interpreter CI matrix coverage required by the standing goal's Python criteria.
- Move the dlt shim from preview toward GA behind a real dlt-source integration test, or ratify the remaining slice order if full parity needs sequencing.

## Child tickets

- `.10x/tickets/2026-07-08-p1-product-ws7a-python-resource-resolution-plan-preview.md`
- `.10x/tickets/2026-07-08-p1-product-ws7b-python-run-spine.md`
- `.10x/tickets/2026-07-08-p1-product-ws7c-python-interpreter-ci-matrix.md`
- `.10x/tickets/2026-07-08-p1-product-ws7d-dlt-ga-gap-integration.md`

## Acceptance criteria

- End-to-end run evidence exists for a Python resource through package creation, destination commit, receipt verification, checkpoint gating, and replay/resume-compatible artifacts.
- Plan and preview work for the same resource identity without executing untrusted project code during doctor probes.
- GIL and free-threaded CI matrix output has identical output hashes where semantics require identity.
- The dlt shim GA gap list is explicit and owned.

## Evidence expectations

Record end-to-end run evidence, plan/preview evidence, CI matrix output, interpreter-remediation snapshots, and dlt integration or gap evidence.

## Explicit exclusions

No use of Python as the execution substrate. No weakening of secret redaction or doctor no-resource-code-execution guarantees. No WASM or subprocess authoring work.

## Progress and notes

- 2026-07-08: Opened from P1 product directive. The Python bridge exists; this workstream opens the product doorway.
- 2026-07-08: Shaped the Python product surface in `.10x/specs/python-front-door-product-surface.md` and split implementation into WS7A-WS7D child tickets. Shaping evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws7-python-front-door-shaping.md`; shaping review recorded in `.10x/reviews/2026-07-08-p1-product-ws7-python-front-door-shaping-review.md`.

## Blockers

None for shaping. Implementation is owned by the child tickets.
