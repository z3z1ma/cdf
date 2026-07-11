Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-ws-e3-cloud-object-stores-and-http-templates.md, .10x/decisions/object-store-credentials-and-http-enumeration.md

# P2 WS-E3 cloud object-store evidence

## What was observed

- The shared file facade lists, heads, and exact-range reads an injected S3-compatible object store.
- A recursive `2026/**/*.parquet` cloud glob resolves a stable sorted set and excludes files outside the prefix/year.
- Two Parquet objects with different compatible schemas reconcile into one normalized pinned snapshot and a two-candidate discovery manifest. Re-observation of the pinned resource uses the same remote facade and produces effective-schema runtime evidence.
- An HTTPS `{01..03}` template discovers and plans three independent files; unbounded HTTP wildcards fail with a finite-template remediation.
- Credential and egress failures occur before network use and do not expose the secret URI.

## Procedure

- `cargo test -p cdf-declarative --lib` — 93 passed.
- `cargo test -p cdf-project` — 165 passed, including live local Postgres tests.
- `cargo test -p cdf-project object_store_multi_file -- --nocapture` — passed.
- `cargo test -p cdf-project http_numeric_template -- --nocapture` — passed.
- `cargo test -p cdf-project http_parquet_schema_discovery -- --nocapture` — passed.
- `cargo check -p cdf-cli` — passed.
- `cargo clippy -p cdf-declarative -p cdf-project -p cdf-cli --all-targets -- -D warnings` — passed.

## What this supports

The cloud providers and HTTPS enumeration are transport adapters feeding the existing partition/discovery calculus, not provider-specific execution paths. Remote discovery is bounded and manifest-bearing for multiple files, and subsequent drift observation remains available after pinning.

## Limits

Provider network credentials were not available in this run. Nightly live-provider evidence remains owned by WS-I. Doctor rendering and streamed remote row-format decompression remain parent WS-E work, not this completed child.
