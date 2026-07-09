Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-08-p2-ws-e1-file-transport-facade-local-http.md
Verdict: pass

# P2 WS-E1 file transport facade review

## Target

Reviewed the WS-E1 changes in:

- `crates/cdf-declarative/src/file_transport.rs`
- `crates/cdf-declarative/src/lib.rs`

## Assumptions tested

- The ticket is a facade plus tests slice, not runtime integration.
- Local and HTTP(S) resources should share one model and trait.
- HTTP(S) must support bounded range reads for future Parquet discovery without accepting a range-ignored full download.
- HTTP(S) must not invent arbitrary directory listing.
- Secret and allowlist hooks must be visible, while credential resolution remains excluded.
- The facade must not write or bypass plan/package/receipt/checkpoint evidence.

## Findings

No blocking findings.

The implementation keeps the scope bounded to `cdf-declarative`, exports the new facade model, computes local SHA-256 metadata, preserves HTTP response bytes through the file-specific HTTP seam, requires `206 Partial Content` for ranged HTTP reads, enforces egress allowlist checks before client use, and fails closed for auth because credential resolution is explicitly excluded.

Parent review found one security-adjacent gap: public debug surfaces needed explicit URL redaction because they may carry signed URLs, URL query parameters, or auth headers in tests or future traces. That repair is now in the implementation, using `cdf_http::Redactor`, with focused tests proving sensitive query/header values are suppressed for `HttpFileRequest`, `FileTransportResource`, and `FileIdentityMetadata`.

The review found one initially duplicated tiny builder pattern through `jscpd`; it was removed and the scoped duplicate check now reports zero clones.

## Verdict

Pass. The acceptance criteria for WS-E1 are supported by focused tests, the full `cdf-declarative` test suite, clippy, formatting, diff whitespace, scoped duplicate-check evidence, complexity output, Semgrep, scoped Gitleaks, supply-chain gates, OSV residual inspection, and reusable-DB CodeQL evidence recorded in `.10x/evidence/2026-07-09-p2-ws-e1-file-transport-facade-local-http.md`.

## Residual risk

No WS-E1 blocker remains. Production HTTP client wiring, remote/cloud transports, credential resolution, doctor probes, HTTP template enumeration, compression, and full file-run integration are intentionally excluded here and remain owned by the parent WS-E workstream or later children.

Current-tree scanner residuals remain outside this slice: CodeQL reports three existing P1 backfill test fixture findings in `crates/cdf-cli/src/tests.rs`, owned by `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`; OSV reports the already-ratified `paste` advisory `RUSTSEC-2024-0436`.
