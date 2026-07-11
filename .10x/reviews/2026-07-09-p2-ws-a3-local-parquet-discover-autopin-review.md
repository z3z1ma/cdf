Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-a3-local-parquet-discover-autopin.md
Verdict: pass

# P2 WS-A3 local Parquet discover CLI and auto-pin review

## Target

The A3 implementation adds a local Parquet discovery helper in `cdf-project`, exposes `cdf schema discover <resource>` in `cdf-cli`, and routes `cdf plan`/`cdf run` through an auto-pinned clone when the original resource is `SchemaSource::Discover` and resolves to exactly one local Parquet file.

## Findings

- Pass: the non-mutating CLI doorway uses `discover_local_parquet_resource_schema`, not the auto-pin helper, so `cdf schema discover <resource>` reports the candidate snapshot without writing `.cdf/schemas`, `cdf.lock`, packages, destination state, or checkpoints. CLI tests assert all of those no-write properties.
- Pass: the package-producing paths use `prepare_local_parquet_discover_resource`, which writes the snapshot, replaces the descriptor with `SchemaSource::Discovered`, and feeds the normalized schema through planning and runtime opening. The run test verifies the committed checkpoint's schema hash matches the pinned snapshot hash.
- Pass: normalization happens before snapshot creation in the auto-pin path. Parent review moved `cdf:normalizer` metadata out of the generic Parquet footer snapshot helper and into the path that actually normalizes, avoiding an inaccurate metadata claim for raw footer handoff tests.
- Pass: non-Parquet discover resources and multi-file Parquet globs still fail closed before package/destination/checkpoint writes. The error names the unsupported discovery slice instead of silently choosing one file.
- Pass: the CLI grammar adds only `schema discover` in this child. `schema pin/show/diff`, lockfile updates, remote discovery, and source-archetype expansion remain excluded, which keeps this slice small and consistent with the P2 sequencing.
- Pass: A3 tests avoid the P2 WS-F anti-pattern of adding keys to append resources. Parent review removed an unnecessary test-only `primary_key` from the Parquet discover resource helper.
- Minor residual, accepted: broad touched-file `jscpd` still reports pre-existing duplication in the large `crates/cdf-cli/src/tests.rs` preview/resume scaffold area. The A3-introduced duplicate Parquet fixture blocks were removed, and the implementation-only scan is clean with 0 clones. Existing reviews already record the no-action rationale for the older CLI-test duplication surface.
- Minor residual, owned elsewhere: CodeQL reports three existing hard-coded cryptographic value findings in `crates/cdf-cli/src/tests.rs` backfill fixtures. They are outside A3 discovery code and are already owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.
- Minor residual, owned elsewhere: OSV reports the already-ratified `RUSTSEC-2024-0436` `paste` maintenance advisory. No dependency metadata changed in this slice.

## Verdict

Pass. The implementation satisfies the A3 acceptance criteria with focused and broad tests, scanner evidence, and explicit residual ownership. It creates the first operator-visible discovery doorway without pretending local Parquet solves the later SQL, REST, Python, WASM, remote, multi-file, or conformance parity work.

## Residual risk

The auto-pin helper is intentionally source-slice-specific: it only understands local single-file Parquet. Future WS-A children should preserve the product-wide discovery abstraction by adding source-archetype probes rather than branching more CLI command code by source type.
