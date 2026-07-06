Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-parquet-object-store-destination.md

# Parquet/object-store destination verification

## What was observed

`firn-dest-parquet` now exposes a real local-filesystem/object_store Parquet destination with dry-run planning, append and replace support, package-token idempotency, object manifest receipts, package receipt recording, and receipt verification for manifest/object hashes, etags where available, counts, and schema hashes.

The implementation is split across ordinary Rust modules (`api`, `duckdb_writer`, `manifest`, `package`, `receipts`, `sheet`, `store`, and tests) rather than a monolithic `lib.rs`.

The final writer path uses DuckDB's built-in Parquet export instead of depending on arrow-rs `parquet`. `Cargo.lock` contains no `parquet` or `paste` package entries after the change, and `cargo deny check advisories` is clean.

Focused tests cover:

- Destination sheet truth: append and replace are declared; merge and CDC are unsupported.
- Filesystem-backed append materializes a readable Parquet object and records a package receipt.
- In-memory object_store duplicate replay returns duplicate/no-op behavior without rewriting the manifest.
- Replace writes a current pointer to the latest package-token manifest.
- Dry-run planning reports keys without writing destination objects.
- Receipt verification fails for tampered and missing Parquet objects.
- Requested-segment validation rejects mismatched segment metadata.
- Duplicate column names fail before writing destination objects.
- Replace duplicate replay verifies the current pointer still points to the package-token manifest identity.
- Receipt verification rejects replace-pointer and manifest identity mismatches.
- Object-store root prefixes are normalized and parent traversal is rejected.
- Canonical JSON array serialization preserves separators in order.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

```text
cargo fmt --all -- --check
cargo test -p firn-dest-parquet --locked --no-fail-fast
cargo clippy -p firn-dest-parquet --all-targets --locked -- -D warnings
cargo mutants --package firn-dest-parquet --file 'crates/firn-dest-parquet/src/*.rs' --no-shuffle --jobs 4 --timeout 120 --output target/quality/reports/mutants-parquet-destination
cargo test --workspace --all-targets --locked --no-fail-fast
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo nextest run --workspace --locked
cargo test --workspace --all-targets --all-features --locked --no-fail-fast
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo deny check advisories
cargo audit --json > target/quality/reports/cargo-audit-parquet-destination-final.json
semgrep scan --config p/rust --error --no-git-ignore --json --output target/quality/reports/semgrep-rust-parquet-destination-crate-final.json crates/firn-dest-parquet
semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-parquet-destination-staged-final.json .
tools/codeql-rust-quality.sh
tmpdir=$(mktemp -d); rsync -a --exclude .git --exclude target --exclude reports . "$tmpdir/firn"; gitleaks dir "$tmpdir/firn" --no-banner; rc=$?; rm -rf "$tmpdir"; exit $rc
git diff --check -- . ':(exclude).gitignore'
```

Final required verification results:

```text
cargo fmt --all -- --check
exit code: 0

cargo test -p firn-dest-parquet --locked --no-fail-fast
result: 14 passed; 0 failed; 0 ignored; doc-tests 0 passed; exit code 0

cargo clippy -p firn-dest-parquet --all-targets --locked -- -D warnings
exit code: 0

cargo mutants --package firn-dest-parquet ...
result: 158 mutants tested in 8m: 128 caught, 30 unviable, 0 missed; exit code 0

cargo test --workspace --all-targets --locked --no-fail-fast
result: 166 passed; 0 failed; exit code 0

cargo clippy --workspace --all-targets --locked -- -D warnings
exit code: 0

cargo nextest run --workspace --locked
result: 166 passed; 0 skipped; exit code 0

cargo test --workspace --all-targets --all-features --locked --no-fail-fast
result: 166 passed; 0 failed; exit code 0

cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
exit code: 0

cargo deny check advisories
result: advisories ok; exit code 0

cargo audit --json > target/quality/reports/cargo-audit-parquet-destination-final.json
exit code: 0

semgrep scan --config p/rust --error --no-git-ignore ... crates/firn-dest-parquet
result: 0 findings across 9 Rust targets; exit code 0

semgrep scan --config p/rust --error ... .
result: 0 findings across 134 tracked targets after staging new Parquet files; exit code 0

tools/codeql-rust-quality.sh
result: exit code 0; reused/refreshed target/quality/codeql-db-rust; CodeQL scanned 128 Rust files, extraction errors 0, extraction warnings 1517 unresolved macro calls

source-only gitleaks temp-copy scan
result: no leaks found; exit code 0

git diff --check -- . ':(exclude).gitignore'
exit code: 0
```

## What this supports or challenges

Supports the ticket acceptance criteria that package segments materialize as Parquet, receipt verification catches tampering/missing objects and manifest/pointer identity drift, duplicate replay is a no-op under package-token layout, the destination sheet is honest about unsupported semantics, and filesystem/object_store paths share the same object_store commit protocol.

Also supports the supply-chain requirement for this slice: the previous `parquet`/`paste` advisory path is absent from `Cargo.lock`, and advisory scanners pass for the final graph.

## Limits

Full `cargo deny check` and `cargo vet` are still blocked by the unratified repository supply-chain policy tracked in `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`; advisory-only deny and cargo-audit passed.

CodeQL completed successfully, but current Rust extractor coverage still reports unresolved macro expansion warnings. The local wrapper preserves the reusable database at `target/quality/codeql-db-rust` and records zero extraction errors for this run.

Verification does not exercise parent-plan integration, the future conformance harness, cloud-provider-specific object store behavior, multi-process crash injection, Iceberg/Delta, merge, CDC semantics, or Miri over the native DuckDB/libduckdb FFI path.
