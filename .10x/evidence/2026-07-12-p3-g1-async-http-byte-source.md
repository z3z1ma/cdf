Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/done/2026-07-11-p3-g1-streaming-transport-byte-sources.md, .10x/tickets/done/2026-07-11-p3-g2-range-readahead-spool-controller.md, .10x/tickets/done/2026-07-11-p3-g3-codec-download-decode-overlap.md, .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md

# Async generation-bound HTTP byte source

## What was observed

The concrete Reqwest provider moved out of `cdf-cli` into dependency-isolated `cdf-transport-http`. The CLI now performs composition only. One pooled async client supplies `HttpByteSource`; legacy REST/file compatibility calls share a pooled blocking client whose creation and destruction are confined outside Tokio contexts.

Strong-ETag HTTP resources expose accounted sequential streaming and exact ranges. Every request sends `If-Match`; sequential responses attest status, ETag, Content-Length, total transferred bytes, cancellation, and per-chunk pre-admission. Range responses additionally require 206, exact `Content-Range`, exact body length, and the planned ETag. Redirect following is disabled so egress policy cannot be bypassed by a client-side redirect. Weak HTTP identity returns no direct byte source and therefore retains the single verified sequential spool/re-attestation path.

`FormatDriverDescriptor` now declares `FormatSourceAccess::{Sequential, Seekable, Adaptive}`. CSV, JSON, NDJSON, and Arrow IPC select direct remote sequential streams. Parquet is adaptive; because current execution has no propagated selective projection/predicate, remote full scans select the verified one-GET spool rather than pathological ranges. The decision is a driver capability join, not a format/provider-name branch.

## Procedure

- `cargo test -p cdf-transport-http` — a recorded loopback server observed exactly one full GET and one exact range, both with `If-Match`; streamed/ranged bytes matched and the ledger returned to zero. A weak-identity fixture selected spool fallback.
- `cargo test -p cdf-source-files` — 26 provider/runtime/format tests passed, including adaptive remote Parquet spool policy and direct sequential Arrow IPC.
- `cargo test -p cdf-runtime format_registry_is_deterministic_and_rejects_ambiguous_authority`.
- Targeted compile and strict Clippy commands are recorded in the associated review/commit validation.

## What this supports

HTTP body transport no longer requires blocking Reqwest or a disk spool for sequential formats, while full-scan Parquet retains the bandwidth-efficient single-transfer policy. Provider implementation and lifecycle no longer live in the CLI.

## Limits

This does not close G1/G2/G3. Selective scan evidence is not yet propagated into the access join; adaptive remote formats therefore always spool. HTTP auth resolution, typed retries/backoff, redirect revalidation, response telemetry, live/public envelope measurements, transform streaming, and early decode of growing spools remain.
