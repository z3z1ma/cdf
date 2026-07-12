Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-transport-http; crates/cdf-source-files/src/transport.rs; crates/cdf-source-files/src/runtime.rs; crates/cdf-runtime/src/format.rs
Verdict: concerns

# Async HTTP byte-source review

## Findings

- Critical policy risk, fixed before verdict: exposing HTTP ranges directly caused adaptive Parquet full scans to choose ranges. A typed source-access contract now keeps unselective adaptive scans on one verified spool; no format-name check was introduced.
- Significant lifecycle risk, fixed before verdict: Reqwest's blocking client panics when created/dropped inside Tokio. The provider confines those lifecycle operations to ordinary threads; file bodies use only the async client.
- Significant: CDF pre-admits the requested response chunk, but Reqwest controls actual chunk boundaries. Oversized chunks fail closed after arrival; live/high-BDP evidence and a provider-level bounded-body adapter remain before closure.
- Significant: selective projection/predicate facts are not yet propagated from `ScanRequest` to the access join, so adaptive formats cannot safely choose ranges even when they would save bytes.
- Significant: auth, typed retry/backoff, redirect revalidation, and transport telemetry remain incomplete. Redirects are currently rejected rather than followed, preserving egress correctness.
- Minor: the blocking compatibility client remains for REST/discovery paths. It is isolated in the transport leaf but must disappear when G1/B5 migrate those paths.

## Verdict

Concerns raised. The slice is safe and materially improves sequential HTTP formats without regressing Parquet full scans. G1/G2/G3 remain open for the significant items and measured envelope.

## Residual risk

The loopback fixture proves request/identity/accounting mechanics, not real CDN, HTTP/2, auth-refresh, disconnect, throttle, or provider behavior.
