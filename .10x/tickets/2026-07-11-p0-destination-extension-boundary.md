Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/decisions/destination-runtime-composition-boundary.md, .10x/specs/destination-extension-runtime-contract.md

# P0 destination extension boundary completion

## Scope

Complete the dependency inversion required for one-step destination additions before P3 streaming/bulk changes amplify concrete driver wiring. Extract the neutral runtime contract, move adapters into destination crates, inject registry authority through project/product surfaces, and make conformance data-driven.

This parent is a plan. Its children are the executable units.

## Child tickets

- `.10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md`
- `.10x/tickets/2026-07-11-p0-dx2-driver-owned-adapters-composition.md`
- `.10x/tickets/2026-07-11-p0-dx3-generic-lock-doctor-replay.md`
- `.10x/tickets/2026-07-11-p0-dx4-conformance-extension-law.md`

## Acceptance criteria

- A fourth mock/external destination satisfies the active extension-contract scenario without editing shared project/CLI/conformance-engine code.
- `cdf-project` has no concrete destination dependencies.
- CLI concrete destination imports are restricted to one composition module or explicitly owned adapter-only diagnostics.
- Lockfile, doctor, replay, correction, and runtime planning consume driver inspection/runtime traits.
- P3 streaming/bulk/memory declarations live in sheet/runtime data, never destination-name branches.
- Build-graph evidence records the reduced impact of editing one destination crate.

## Blockers

None after shaping. P3 WS-A and WS-D wait for the relevant children; WS-L remains unblocked.

## Progress and notes

- 2026-07-11: DX1 closed with neutral runtime registry/inspection/prepared-commit contracts and unchanged project runtime behavior. DX2 is unblocked and owns driver-crate adapter migration plus the single CLI composition root.
