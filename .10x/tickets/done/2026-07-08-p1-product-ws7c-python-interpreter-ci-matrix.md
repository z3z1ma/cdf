Status: done
Created: 2026-07-08
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p1-product-ws7-python-front-door.md
Depends-On: .10x/specs/python-front-door-product-surface.md, .10x/specs/versioning-lts-release-policy.md, .10x/tickets/done/2026-07-05-python-sdk-bridge.md, .10x/tickets/done/2026-07-06-python-doctor-interpreter-probe.md

# P1 product WS7C: Python interpreter CI matrix

## Scope

Add CI coverage and local quality hooks proving deterministic Python bridge behavior across GIL and free-threaded interpreter configurations required by D-25.

Primary write scope is `.github/workflows/**`, targeted helper scripts if needed, Python bridge CI fixtures, focused tests, and this ticket's records. Coordinate with `.10x/tickets/done/2026-07-08-p1-product-ws8a-ci-quality-workflows.md` so quality workflow ownership stays coherent.

## Acceptance criteria

- CI has an explicit Python matrix covering a GIL build and a free-threaded 3.14t build where available in the runner ecosystem.
- Deterministic bridge fixtures produce identical package or fixture hashes across the matrix where the input and resource semantics are deterministic.
- Free-threaded-unavailable environments report an explicit skipped or allowed-failure state with a revisit trigger, not a silent pass.
- `python.require_free_threaded = true` remains enforced by doctor and is represented in CI.
- The workflow does not leak secrets and does not weaken selected `QUALITY.md` verification, including `--locked` Cargo commands, supply-chain gates, or CodeQL when those profiles apply.

## Evidence expectations

Record workflow validation, local smoke where possible, CI matrix output or a documented local substitute when GitHub output is unavailable, identical-hash evidence, and any free-threaded availability limits. If CI workflow changes overlap WS8A, cross-link the evidence and avoid duplicate workflows.

## Explicit exclusions

Do not implement Python resource run/plan/preview product behavior. Do not change dependency tuple policy except by opening or updating the appropriate dependency decision/ticket.

## Progress and notes

- 2026-07-08: Split from WS7 parent. This is a CI/evidence lane and can proceed in parallel with WS7A/WS7B if workflow ownership is coordinated with WS8A.
- 2026-07-10: Closed by `daff44b6`. The strict 3.14/3.14t workflow runs bridge, product, dlt, and doctor gates, uploads deterministic fixture hashes, and requires byte identity in a dependent job. Local GIL substitute evidence and the hosted-run limit are recorded in `.10x/evidence/2026-07-10-p1-python-front-door-closure.md`.

## Blockers

None. If free-threaded 3.14t is not available on hosted runners, record the availability limit and a revisit trigger.
