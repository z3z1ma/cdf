Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-e-remote-transports.md
Depends-On: .10x/tickets/done/2026-07-08-p2-ws-e1-file-transport-facade-local-http.md, .10x/tickets/done/2026-07-09-p2-ws-a3-local-parquet-discover-autopin.md, .10x/decisions/data-onramp-file-source-transport-manifest.md, .10x/specs/data-onramp-file-sources-transports.md

# P2 WS-E2 HTTPS file runtime and remote Parquet discovery

## Scope

Wire the existing local/HTTP file transport facade into production file resources for direct HTTPS single files. `https://...parquet` resources MUST support bounded ranged footer discovery and package-producing run/preview without manual download.

## Acceptance criteria

- Declarative file sources with an HTTPS single-file URL compile to a file resource plan that uses the HTTP transport facade rather than a local `PathBuf`-only path.
- `cdf schema discover` for a single HTTPS Parquet resource reads only bounded ranges sufficient for footer/schema discovery through the facade and writes no runtime artifacts.
- `cdf plan`, `cdf preview`, and `cdf run` can auto-pin and execute a discover-mode HTTPS single-file Parquet resource without requiring an operator-side download.
- Egress allowlist and auth hooks fail closed before transport use and do not leak resolved secret values in debug, errors, JSON, or renderer output.
- Source-position evidence records URL, size, ETag/checksum where available, and bytes loaded.

## Evidence expectations

Deterministic HTTP fixture tests for ranged footer discovery and streaming run/preview, redaction tests, egress-denial tests, no full-download assertion for discovery, and normal quality gates. Live public TLC network evidence is optional here and required later by S1/S2 closure.

## Explicit exclusions

This ticket does not implement arbitrary HTTP directory listing, HTTP glob/template enumeration, S3/GCS/Azure, gzip/zstd, or multi-file remote manifest incrementality.

## Progress and notes

- 2026-07-09: Opened after E1 facade closure and A6 REST discovery closure. This child owns the first production remote-file path needed for S1.
- 2026-07-09: Implemented HTTPS single-file file-source runtime integration. `cdf-declarative` now exposes a `FileRuntimeDependencies` facade for local/HTTP file metadata, ranged reads, and reads; HTTP(S) single-file resources compile without local path assumptions; egress allowlist and auth checks fail before client use; `cdf-formats` gained range-backed Parquet discovery/read support; `cdf-project` discovery/prepare and run resources pass file runtime dependencies through plan/preview/run; and `cdf-cli` wires the reqwest-backed file transport into `schema discover`, `plan`, `preview`, and `run`.
- 2026-07-09: Focused tests cover bounded HTTPS Parquet footer discovery without runtime artifacts, auto-pin plan/preview/run over an HTTP Parquet fixture, egress/auth fail-closed behavior, HTTP range contract enforcement, and no secret-bearing debug output. Closure evidence: `.10x/evidence/2026-07-09-p2-e2-g1-b4-batch.md`. Review: `.10x/reviews/2026-07-09-p2-e2-g1-b4-batch-review.md`.

## Blockers

None for the scoped single-file HTTPS slice.
