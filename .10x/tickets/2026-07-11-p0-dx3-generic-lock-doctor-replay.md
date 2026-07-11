Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p0-destination-extension-boundary.md
Depends-On: .10x/tickets/2026-07-11-p0-dx2-driver-owned-adapters-composition.md

# P0 DX3: generic lock, doctor, replay, and product surfaces

## Scope

Replace lockfile URI matches, CLI destination runtime enum, doctor/replay target parsing, and legacy concrete orchestration helpers with registry inspection/runtime capabilities and typed probes. Preserve command output semantics additively.

## Acceptance criteria

- Lock generation gets sheet artifacts from driver inspection and contains no destination scheme match.
- Doctor and replay generic modules contain no concrete destination imports or branches.
- A mock fourth driver works through lock, inspect/doctor, plan, replay, and resume.
- Secret redaction and no-mutation inspection are proven.

## Blockers

Depends on DX2.
