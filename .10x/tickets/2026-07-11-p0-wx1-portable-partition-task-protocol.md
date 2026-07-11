Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/tickets/2026-07-11-p0-bx1-kernel-stream-extent-artifacts.md, .10x/specs/portable-partition-task-protocol.md

# P0 WX1: portable partition task/result protocol

## Scope

Implement neutral canonical task, attempt/fence, typed artifact reference, worker result/attestation protocol values and validation/hash fixtures; integrate capability declarations without transport or a remote scheduler.

## Acceptance criteria

- Protocol has no engine/runtime/driver/CLI/store/path/secret-value implementation types.
- Canonical serialization/hash/version/tamper/stale-fence validation is fixture-backed.
- Task is sufficient for mock isolated reconstruction through registries/injected services.
- Package/receipt/checkpoint authority remains absent from worker results.

## Evidence expectations

API/dependency checks, golden/tamper/compatibility fixtures, mock resolution, secret/path scans, and adversarial protocol/authority review.

## Explicit exclusions

No RPC, worker daemon, framework adapter, remote store, or placement.

## Blockers

Depends on SX1, DX1, and BX1 artifact ownership.

## References

- `.10x/decisions/portable-partition-task-capsule.md`
- `.10x/research/2026-07-11-portable-partition-task-audit.md`
- `.10x/specs/portable-partition-task-protocol.md`
