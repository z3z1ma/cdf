Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-05-parquet-object-store-destination.md
Verdict: pass

# Parquet/object-store destination review

## Target

Implementation in `crates/cdf-dest-parquet/**` plus direct dependency declarations in `crates/cdf-dest-parquet/Cargo.toml` and `Cargo.lock`.

## Findings

No blocking findings.

Resolved during parent review:

- The first implementation conflated state-segment byte counts with package IPC byte counts in object manifest/receipt fields. The final implementation stores state `byte_count`, original `package_byte_count`, and written `parquet_byte_count` separately, and tests assert the state byte count is what receipts acknowledge.
- The first writer depended on arrow-rs `parquet`, which pulled in `paste` and triggered `RUSTSEC-2024-0436`. The final writer uses DuckDB's Parquet export path; `Cargo.lock` no longer contains `parquet` or `paste`, and advisory scanners pass.
- Mutation testing initially missed duplicate replay, replace-pointer identity, manifest/receipt identity, duplicate column names, canonical JSON array separators, key derivation, root-prefix normalization, and timestamp plausibility. Focused tests now cover those paths, and the final mutation run has zero missed mutants.

## Assumptions tested

- Sheet truth: merge and CDC remain unsupported because this implementation only provides append and replace package-token materialization.
- Replay safety: an existing package-token manifest is verified before duplicate/no-op replay; if it is tampered or missing referenced objects, commit refuses to overwrite it.
- Replace ordering: data objects and package-token manifest are written before the current pointer is advanced, so the target-visible replace step is the final object write.
- Receipt teeth: verification checks manifest sha256, manifest etag when available, object sha256, object etags when available, object byte counts, segment counts, schema hash, and replace-pointer metadata.
- Dry-run behavior: planning loads and validates package metadata but does not write destination objects.
- Supply-chain posture: the final dependency graph avoids the known `parquet`/`paste` advisory path while preserving Parquet materialization through DuckDB.
- Crate shape: `lib.rs` remains a small module map; implementation logic is split by API, manifest, package loading, receipt verification, sheet metadata, storage keys, and writer mechanics.

## Verdict

Pass. The implementation satisfies the ticket scope at MVP fidelity and has parent-observed verification evidence in `.10x/evidence/2026-07-06-parquet-object-store-destination-verification.md`.

## Residual risk

Object-store replace is pointer-atomic, not a multi-object transaction; crash-matrix behavior across all object-store failure windows remains for the future conformance/chaos layer. Cloud-provider-specific etag/version behavior is represented when available but not exercised beyond local filesystem and in-memory object_store tests.

`jscpd` reported small deliberate duplication between DuckDB and Parquet destination helper/test patterns. No follow-up was opened: extracting shared Arrow-to-DuckDB conversion now would couple separate destination crates for two consumers; revisit only if a third consumer appears or the duplication becomes behaviorally inconsistent.

Full license policy and cargo-vet adoption remain outside this ticket and are already owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.
