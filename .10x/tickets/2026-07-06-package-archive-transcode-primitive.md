Status: open
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-singer-airbyte-and-package-archive.md
Depends-On: .10x/tickets/done/2026-07-05-package-builder-reader.md, .10x/tickets/done/2026-07-05-parquet-object-store-destination.md, .10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md

# Add supply-chain-safe package archive transcode primitive

## Scope

Implement the first executable `firn package archive` slice from the Singer/Airbyte/package-archive parent: load and verify package IPC segments, transcode them to Parquet bytes with a supply-chain-clean writer path, and return an in-memory fidelity report primitive that preserves the canonical IPC package identity.

Owns `crates/firn-package/**`. It may touch `crates/firn-dest-parquet/**` only to extract or reuse the existing DuckDB-backed Parquet writer without changing destination commit semantics. Do not add the direct arrow-rs `parquet` crate or any dependency path that reintroduces `RUSTSEC-2024-0436`.

## Acceptance criteria

- The archive primitive verifies the package before transcode work and returns an error before producing Parquet bytes when canonical package identity files fail verification.
- The primitive transcodes one Parquet byte vector per package segment using a supply-chain-clean writer path and records byte count, SHA-256, source IPC path/hash, row count, and segment id for each archived segment.
- The fidelity report explicitly states that Arrow IPC remains canonical, Parquet is an archive/interchange projection, and field metadata or other Arrow-only semantics are not promoted to canonical Parquet truth.
- The primitive does not write archive files, mutate `manifest.json`, change lifecycle status, change `manifest.identity`, change `manifest.package_hash`, or alter replay preference for IPC segments.
- Running the primitive twice against an unchanged package is deterministic: it reports the same package hash and per-segment archive hashes.
- `PackageReader::replay_view` and `PackageReader::read_segment` continue to use IPC segments after the primitive is exercised.
- Existing Parquet/object-store destination behavior remains unchanged if writer code is extracted or shared.

## Evidence expectations

Record focused tests for transcode report content, deterministic rerun behavior, replay-after-transcode behavior, tampered-package refusal before Parquet output, and Parquet destination regression when its writer path is touched. Record formatting, clippy, targeted package/destination tests, supply-chain checks, secret scans, and reused CodeQL evidence per `QUALITY.md`.

## Explicit exclusions

No `firn-cli` command surface, no archive file placement, no manifest archive metadata schema, no Parquet file source implementation, no package GC retention planner, no destination commit changes, no archive deletion/tombstone workflow, no signature population, no CI workflow changes, and no advisory ignore for `RUSTSEC-2024-0436`.

## References

- `firn-the-book-of-the-system.md` Chapter 11 and Decision D-4.
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/tickets/2026-07-05-singer-airbyte-and-package-archive.md`
- `.10x/tickets/2026-07-06-parquet-format-source-supply-chain.md`
- `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`

## Progress and notes

- 2026-07-06: Split from `.10x/tickets/2026-07-05-singer-airbyte-and-package-archive.md` after Singer/Airbyte protocol adapters closed. The next no-guess slice is the package archive transcode primitive and fidelity report data model. Existing Parquet destination code already writes Parquet through DuckDB rather than the blocked arrow-rs `parquet -> paste` path; implementation should reuse or extract that path rather than adding a vulnerable direct Parquet dependency. CLI command wiring, archive file placement, and manifest metadata mutation remain with the parent until this primitive is proven.

## Blockers

None.
