Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-g-remote-io-overlap.md
Depends-On: .10x/tickets/2026-07-11-p3-g1-streaming-transport-byte-sources.md, .10x/tickets/2026-07-11-p3-c1-scheduler-admission-contract.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md

# P3 G2: per-origin range, readahead, spool, and cache controller

## Scope

Implement accounted per-origin connection/range admission, low-gain BDP feedback, coalescing/readahead, exact range retries, bounded spool with early decode where safe, and opt-in cryptographic cache promotion/revalidation.

## Acceptance criteria

- High-BDP fixtures saturate without unbounded buffers; throttle/pressure reduces concurrency cleanly.
- Coalescing/prefetch never changes logical bytes/order and reports waste.
- Spool/cache disk/security/identity/cleanup laws pass; cache state cannot change packages.
- Fixed/auto/manual settings are observable and outside package identity.

## Evidence expectations

Controller simulations, high-BDP/throttle benchmarks, memory/disk/cancel chaos, cache/spool identity/security tests, and network profiles.

## Explicit exclusions

No format-specific range planning.

## Blockers

Depends on G1, C1, and memory ledger.

## References

- `.10x/specs/remote-local-io-overlap.md`
