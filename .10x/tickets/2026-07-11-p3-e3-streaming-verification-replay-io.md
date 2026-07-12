Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-e-hashing-package-io.md
Depends-On: .10x/tickets/done/2026-07-11-p3-e2-streaming-manifest-durability.md, .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md

# P3 E3: bounded verification and replay I/O

## Scope

Stream manifest/segment verification, hash during consumer reads, bound/parallelize explicit verification, eliminate package-sized maps/vectors, and evaluate buffered/pread/mmap local replay with the unsafe gate closed by default.

## Acceptance criteria

- Run/replay destination reads verify exact segment bytes without a separate redundant pass.
- Explicit verify is bounded, parallel where beneficial, canonically reported, and detects all current tamper cases.
- Read-byte/syscall/copy/page-fault evidence selects the local strategy; mmap is absent unless separately ratified.
- High-cardinality and 1 TB replay remain within the memory budget.

## Evidence expectations

Tamper/golden parity, million-entry/RSS, buffered/pread/mmap evaluation, cold/warm profiles, cancellation/error cleanup, and destination-reader integration.

## Explicit exclusions

No unsafe/mmap without a new decision/fuzz target.

## Blockers

Depends on E2 and A5 bounded readers.

## References

- `.10x/specs/package-io-hashing-durability.md`

## Progress and notes

- 2026-07-11: Introduced a package-owned `VerifiedPackageReader` consumption authority and threaded it through generic replay, recovery, prepared commit, staged ingress, final binding, and destination planning. A commit operation now performs one full verification and reuses that unforgeable-in-API authority instead of calling whole-package verification at replay-input reconstruction, replay validation, final binding, and binding reconstruction. The fresh 104 MiB/2,964,624-row TLC staged run fell from the recent 1.89-second median to 1.64 seconds; three redundant 104 MiB scans were removed. Replay consumer-read hash fusion, bounded/parallel explicit verification, and high-cardinality closure remain open. Evidence: `.10x/evidence/2026-07-11-p3-e3-single-pass-consumption-verification.md`.
- 2026-07-11: Fresh package finalization now issues the same typed authority directly from its reconciled hash-while-write receipts after manifest publication. Ordinary run therefore performs zero package-content rereads between build and staged final binding; reopened replay still verifies independently. Three fresh TLC controls measured 1.55, 1.58, and 2.13 seconds (median 1.58), a further 3.7% versus the preceding 1.64-second observation and 16.4% versus the recent 1.89-second control median. Replay read/hash fusion and explicit verifier boundedness remain open.
