Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p0-destination-extension-boundary.md
Depends-On: .10x/tickets/2026-07-11-p0-dx3-generic-lock-doctor-replay.md

# P0 DX4: destination extension conformance and build-graph law

## Scope

Replace repeated concrete conformance enums/factories with one data-driven adapter catalog, add the fourth-driver extension law and static dependency/import gates, and measure the Cargo rebuild graph for a destination-only edit.

## Acceptance criteria

- One fixture catalog entry enrolls a destination in every applicable shared law.
- Generic conformance assertions contain no destination-name match arms.
- Static tests prevent `cdf-project`/generic CLI imports of concrete destinations.
- Before/after `cargo build --timings` or equivalent evidence shows a destination-only edit no longer rebuilds destination-neutral runtime/project crates unnecessarily.

## Blockers

Depends on DX3.
