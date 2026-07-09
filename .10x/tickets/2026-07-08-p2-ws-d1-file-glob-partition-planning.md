Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md
Depends-On: .10x/decisions/data-onramp-file-source-transport-manifest.md, .10x/specs/data-onramp-file-sources-transports.md

# P2 WS-D1 file glob partition planning

## Scope

Replace the single-file live-run gate with deterministic file partition planning for modest-N local file globs, while keeping manifest incrementality, remote transports, compression, and large-N coalescing for later children.

Owned write scope:

- `crates/cdf-declarative/src/file_runtime.rs`
- `crates/cdf-declarative/src/compiled.rs`
- `crates/cdf-declarative/src/tests.rs`
- `crates/cdf-conformance/**` only for focused preview/run parity or partition planning assertions
- this ticket's evidence/review records

## Acceptance criteria

- A glob matching multiple local files plans deterministic file partitions instead of failing with "narrow the glob to exactly one file".
- Each partition carries file-scoped metadata and scope sufficient for later `FileManifest` incrementality.
- Preview and run use the same deterministic file match ordering and file-resolution front end for local globs.
- Zero-match globs still fail with an actionable data error.
- Existing single-file behavior is preserved.
- Large-N coalescing remains explicitly deferred; modest-N tests must not encode a hidden threshold.

## Evidence expectations

Record focused evidence for:

- `cargo test -p cdf-declarative <new glob partition tests> --locked`
- `cargo test -p cdf-declarative --locked`
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`
- `cargo fmt --all -- --check`
- `git diff --check`
- jscpd scoped to touched Rust files

## Explicit exclusions

This ticket does not implement default manifest incrementality, compression, remote transports, schema variance policy, or no-op reruns.

## Progress and notes

- 2026-07-08: Opened after inspection found `resolve_single_file` and `resolve_preview_file` as divergent paths in `crates/cdf-declarative/src/file_runtime.rs`.

## Blockers

Coordinate with WS-B1/WS-C1/WS-F1 before implementation because they also touch `cdf-declarative`.
