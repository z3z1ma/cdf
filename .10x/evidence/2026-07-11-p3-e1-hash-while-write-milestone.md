Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-e1-hashing-artifact-sink.md, .10x/specs/package-io-hashing-durability.md

# P3 E1 hash-while-write milestone

## What was observed

The package IPC and small identity-artifact writers now hash exact bytes in their write call chain and return typed path/count/SHA-256/durability receipts only after flush, file sync, atomic rename, and containing-directory sync. Package finalization consumes registered receipts after a metadata size check and hashes only compatibility artifacts that lack a receipt; registered segment and identity content is not reopened for manifest metadata.

On host class `host-class-f4bf4d1c46a93156` (Apple M5 Pro, arm64 macOS, Rust 1.96.1), the release SHA-256 loop over 512 MiB measured:

- software backend: 0.541 GiB/s, 924,557,959 ns;
- `sha2 0.10.9` `asm` backend: 3.035 GiB/s, 164,725,459 ns;
- measured rate improvement: 5.61x.

The host reports ARM `FEAT_SHA256=1`. Enabling `asm` added `sha2-asm 0.6.4`; its no-std fixed-buffer FFI wrapper, build script, and architecture entry points were reviewed and a `safe-to-deploy` cargo-vet audit was recorded. `cargo vet --locked` no longer lists `sha2-asm`; it still reports the same eleven unrelated pre-existing audit gaps.

The seven-sample current-tree P3 lab report is `.10x/evidence/.storage/p3-e1-current-macos.json` with SHA-256 `ef4c53d725f908ca6ee70374752f8001c0192247b5d17650cc158836ecf96ba2`. Against the immutable pre-optimization report, median package-build wall time changed from 155,885,125 ns to 124,124,666 ns (-20.37%), and NDJSON-to-package changed from 164,721,125 ns to 128,548,167 ns (-21.96%). These are cumulative current-tree comparisons across intervening P3 work, not isolated E1 attribution.

## Procedure

1. Ran `cargo test -p cdf-package --lib`: 37 passed, one ignored performance test.
2. Ran `cargo clippy -p cdf-package --all-targets -- -D warnings`: passed.
3. Ran `cargo test -p cdf-package --release storage::tests::hashing_writer_sha256_rate -- --ignored --nocapture` before and after enabling `sha2/asm`.
4. Ran `cargo tree -p cdf-package -e features -i sha2`, `rustc -vV`, and the host SHA feature probe.
5. Inspected and certified `sha2-asm 0.6.4`, then ran `cargo vet --locked`.
6. Built `cdf-p3-lab` in release mode and ran the complete baseline matrix with seven samples.

## What this supports or challenges

This supports exact receipt hashing, golden compatibility, cleanup on cancelled or failed publication, hardware SHA activation, and removal of the migrated writers' content reread. The fixed-fixture package hash test remained green.

## Limits

This milestone does not close E1. Boundary-specific injected failures for encoder finish, file sync, directory sync, and panic remain to be proven before closure. Trace remains an intentionally unregistered compatibility artifact and is hashed once at finalization; E2 owns its bounded sink. The current receipt cache is memory-resident and E2 owns the bounded append/spill-backed draft index and million-entry RSS law. The tiny baseline remains dominated by fixed durability costs and cannot establish device-roofline throughput or hashing's large-file wall fraction.
