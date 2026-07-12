Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-e2-streaming-manifest-durability.md

# P3 E2 million-entry canonical manifest milestone

## What was observed

The v1 manifest identity encoder hashes entries incrementally without constructing a whole `serde_json::Value` tree or canonical byte buffer. Package manifest publication writes the same canonical representation directly into the atomic hashing sink. Builder draft segment and artifact indexes are append-only temporary journals, and trace durability is batched at the package-finalization barrier.

The release-mode one-million-file identity benchmark serialized 1,000,000 entries in 225,134,083 ns (4,441,797 entries/s). Running the test binary directly under `/usr/bin/time -l` reported 175,800,320 bytes maximum RSS and peak memory footprint of 170,689,016 bytes, including the million owned path/hash strings, with zero page faults and zero swaps.

## Procedure

- `cargo test -p cdf-package --lib` — 45 passed; two explicit performance tests ignored.
- `cargo clippy -p cdf-package --all-targets -- -D warnings` — passed.
- `cargo test --release -p cdf-package million_entry_manifest_identity_streams_without_a_dom -- --ignored --nocapture` — 4,251,185 entries/s in the compile-invoking run.
- `/usr/bin/time -l target/release/deps/cdf_package-c92af45f1834037f million_entry_manifest_identity_streams_without_a_dom --ignored --nocapture` — isolated 4,441,797 entries/s and the RSS figures above.
- Existing fixed-fixture hash, archive-manifest, status-update, tamper, and package-layout tests passed against the streaming writer.

## What this supports

- Canonical v1 identity hashing and manifest publication no longer duplicate million-entry metadata into a DOM and a second whole-manifest byte buffer.
- The package hash and serialized fixture contract are unchanged.
- Runtime draft metadata is spill-backed instead of retained in builder collections.

## Limits

The public v1 `PackageManifest` still owns its final file and segment vectors, so base manifest metadata remains proportional to cardinality. Filesystem discovery still collects paths before reconciliation, archive metadata still materializes, and crash/syscall evidence remains required before E2 closure.
