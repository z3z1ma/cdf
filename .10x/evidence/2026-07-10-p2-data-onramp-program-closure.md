Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-08-p2-data-onramp-program.md

# P2 data-onramp program closure evidence

## Observation

P2 now provides one compiler/evidence path for local and remote file sets, SQL tables, and REST APIs. Declared, Hints, exhaustive Discover, and explicit sampled Discover compile into hash-addressed snapshots and immutable baseline authority. Observed schemas reconcile through one typed lattice; physical provenance, coercion, effective schema, residual capture, quarantine, and promotion remain serialized verdicts.

Local, HTTP, S3, GCS, and Azure file resources share one facade. Logical files remain independent partitions and `FileManifest` identities. Append runs skip unchanged files, load new/changed identities, and gate terminal quarantine advancement on verified receipts. Gzip/zstd, format confirmation, destination normalization, deep validation, file/Postgres/REST add, ad-hoc Parquet, explicit exact-row append dedup, and the canonical TLC documentation path are implemented without bypassing plan/package/receipt/checkpoint evidence.

The executable P2 registry marks S1-S8 covered. All eighteen field-test frictions name concrete tests and retain zero open P2 owners.

## Acceptance mapping

- S1/S2: production-path recorded HTTP fixtures prove add/footer pin/run and monthly manifest initial/no-op/new-file-only behavior. The live TLC session pinned 19 fields; the provider then returned 403 to both CDF and an independent full download.
- S3: recursive object-store gzip NDJSON discovers, previews, executes 10,000 rows, and preserves remote manifest identity.
- S4/S5: local Postgres add/catalog/pin/run and recorded REST sample/pin/run are standalone conformance scenarios.
- S6: incompatible observations complete with typed file/field/type/rule/remediation quarantine rendering.
- S7: append is keyless; merge fails once pre-contact; explicit exact-row append dedup is typed package evidence.
- S8: local multi-file, REST, SQL, dated HTTP, and compressed object-store archetypes share the bounded preview/run front end.
- Full Arrow declarative vocabulary, Hints, schema commands, normalizer metadata, residual promotion/correction, and coverage-matrix rows are recorded by their focused evidence and terminal workstreams.

## Procedure

- `cargo test -p cdf-cli`: 271 unit tests plus doctor integration/doc tests passed after final product changes.
- `cargo test -p cdf-conformance p2_ --locked`: 9/9 passed after the registry reached zero open owners.
- Exact source-owned S1, S2, and S6 tests passed in the documentation verification session.
- `cargo test -p cdf-kernel -p cdf-contract -p cdf-engine -p cdf-declarative`: 22 + 69 + 51 + 100 tests passed after exact-row semantics landed.
- `cargo check --workspace --all-targets`: passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed after final P2 code changes.
- A final `cargo nextest run --workspace` cold rebuild completed, but nextest stalled while asking multiple test binaries for `--list`; it was interrupted and is not counted as passing evidence. Earlier parent-observed integration evidence records 913/913 before the final bounded slices, which are covered by the focused/full-crate runs above.

## Live TLC session

On 2026-07-10, `cdf add` against the directive's January URL completed and pinned schema hash `sha256:916e8470a951fafa0c48851ef4ac1fca5d5312c6717fe1ee687cae59f3d245b9` with 19 normalized fields. `cdf run` then failed closed at source extraction with HTTP 403. Independent `curl` full download returned HTTP 403 and a 919-byte error body; bounded access had returned 206 earlier in the same session. No package receipt or checkpoint was falsely reported.

## Limits

External public-provider availability prevents claiming a successful live data-page download in this session. The deterministic ordinary-runtime S1/S2 fixtures are the durable demo asset and nightly live tiers must continue to distinguish provider denial from CDF regressions.

Channel-level constant-memory row decoding, executor packing, and remote overlap are P3 performance work constrained by P2's logical partition and evidence contracts.
