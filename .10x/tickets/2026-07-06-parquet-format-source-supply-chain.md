Status: blocked
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/done/2026-07-05-contract-compiler-normalization.md, .10x/tickets/done/2026-07-05-formats-and-subprocess.md, .10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md

# Implement Parquet file source without violating supply-chain gates

## Scope

Implement MVP Parquet file-source reads into `firn-kernel::Batch` values with observed schema and deterministic schema hash population, without introducing a supply-chain scanner failure.

## Acceptance criteria

- Parquet file sources produce resource descriptors and batches for MVP file sources.
- Arrow field metadata and batch shape are preserved to the extent supported by the chosen reader.
- `cargo deny check advisories`, `cargo audit`, and OSV scanning pass or the project has an active ratified policy exception for the exact dependency/advisory.
- The implementation has parser tests, malformed input tests, and package write/replay compatibility tests.

## Evidence expectations

Record the dependency path selected, advisory/scanner results, parser tests, malformed input tests, and package integration tests.

## Explicit exclusions

No destination writer or object-store destination behavior. Those remain owned by destination tickets.

## Progress and notes

- 2026-07-06: Split from `.10x/tickets/done/2026-07-05-formats-and-subprocess.md`. A direct `parquet = "59.0.0"` implementation worked locally, but `cargo deny check advisories` and OSV reported `RUSTSEC-2024-0436` because arrow-rs `parquet` depends unconditionally on `paste 1.0.15`, which RustSec marks unmaintained. Feature trimming cannot remove `paste`. The direct dependency was removed before committing the formats/subprocess core.
- 2026-07-06: Supply-chain policy is now ratified by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`. The policy keeps advisory checks enforced with no ignores, so this ticket remains blocked for the direct arrow-rs `parquet -> paste` path unless a later specific advisory exception is ratified or an alternative Parquet reader path avoids the advisory.

## Blockers

- Need either a ratified project policy exception for `RUSTSEC-2024-0436` on the arrow-rs `parquet -> paste` path, or an alternative Parquet reader that satisfies the MVP behavior without introducing advisory, license, or vet failures.
