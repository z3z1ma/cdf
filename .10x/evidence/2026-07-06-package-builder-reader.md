Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-package-builder-reader.md, .10x/specs/package-lifecycle-determinism.md

# Package builder and reader evidence

## What was observed

Implemented `cdf-package` package builder, reader, canonical manifest hashing, LZ4 Arrow IPC segment writing/reading, receipt storage hook, package verifier, lifecycle status updates, tombstoning, and replay views for the active package ticket.

The fixed package fixture produces golden package hash `sha256:87789e563e66acd0cec0f0edcb4b5f54052e7695440cdc66d5512b5007b24adf`.

Tests cover:

- required package layout: `manifest.json`, `plan/`, `schema/`, `data/`, `quarantine/`, `stats/`, `lineage/`, `state/`, `destination/`, and `trace.jsonl`
- manifest file entries with path, byte count, and SHA-256
- deterministic manifest identity hash across repeated fixture builds
- LZ4 Arrow IPC segment round-trip reading
- lifecycle status updates preserving package identity
- tamper detection for identity files
- receipt append/storage outside package identity
- tombstone removal of identity files while preserving manifest/hash records

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/cdf`:

```text
cargo fmt --all -- --check
```

Result: passed.

```text
cargo test -p cdf-package --locked --no-fail-fast
```

Result: passed. Seven unit tests passed; doc-tests ran zero tests and passed.

```text
cargo clippy -p cdf-package --all-targets --locked -- -D warnings
```

Result: passed.

```text
git diff --check
```

Result: passed.

## What this supports or challenges

Supports the package ticket acceptance criteria for layout, canonical identity hashing, crash-safe status update mechanism via atomic rename-over, deterministic fixed-fixture hashes, and tamper detection.

Supports the ticket's explicit exclusions: no destination-specific commit, no DataFusion execution, and no package archive implementation were added.

No `cdf-kernel` additions were required.

## Limits

Verification was scoped to `cdf-package`; broader conformance, chaos recovery, package GC policy, and destination replay semantics remain outside this child ticket.

`Cargo.lock` was refreshed so the requested `--locked` package checks pass with the current workspace state. Concurrent dirty workspace edits outside this child were left intact.
