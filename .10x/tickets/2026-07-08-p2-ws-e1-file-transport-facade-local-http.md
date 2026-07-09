Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p2-ws-e-remote-transports.md
Depends-On: .10x/decisions/data-onramp-file-source-transport-manifest.md, .10x/specs/data-onramp-file-sources-transports.md

# P2 WS-E1 file transport facade for local and HTTP

## Scope

Create the first file-source transport facade over local files and HTTP(S), including deterministic file metadata records and ranged-read support needed by future Parquet discovery.

Owned write scope:

- a new or existing transport module under `crates/cdf-declarative/src/**` or a more appropriate existing crate if source inspection proves ownership belongs elsewhere
- `crates/cdf-declarative/src/declarations.rs` only for adding URL/root shape needed by the facade
- focused tests in the same crate
- this ticket's evidence/review records

## Acceptance criteria

- Local file and HTTPS resources can be represented behind one facade trait/model with file identity metadata: URL/path, size when known, checksum or ETag when known, and modification time when available.
- HTTP(S) supports bounded ranged reads in tests without requiring a full download.
- HTTP(S) does not pretend arbitrary directory listing exists.
- Secret handling and egress allowlist hooks are represented in the API shape, but full secret-provider/doctor/cloud behavior may remain later WS-E children.
- The facade does not bypass plan/package/receipt/checkpoint evidence; it only supplies file bytes and metadata to the source runtime.

## Evidence expectations

Record focused evidence for:

- `cargo test -p cdf-declarative <new transport tests> --locked`
- `cargo test -p cdf-declarative --locked`
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`
- `cargo fmt --all -- --check`
- `git diff --check`
- jscpd scoped to touched Rust files

## Explicit exclusions

This ticket does not implement S3/GCS/Azure, credential resolution, doctor probes, HTTP template enumeration, compression, or full file-run integration.

## Progress and notes

- 2026-07-08: Opened after inspection found file sources currently use local `PathBuf` resolution only, while REST has existing HTTP URL validation that is not a file transport facade.

## Blockers

Coordinate with WS-D1 before integration into file runtime; the first child may remain a facade plus tests.
