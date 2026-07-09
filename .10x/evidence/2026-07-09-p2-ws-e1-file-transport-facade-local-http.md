Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-08-p2-ws-e1-file-transport-facade-local-http.md, .10x/specs/data-onramp-file-sources-transports.md, .10x/decisions/data-onramp-file-source-transport-manifest.md

# P2 WS-E1 file transport facade evidence

## What was observed

`cdf-declarative` now exposes a file transport facade from `crates/cdf-declarative/src/file_transport.rs` and `crates/cdf-declarative/src/lib.rs`.

The facade represents implicit local paths, `file://` URLs, and HTTP(S) URLs with one `FileTransportResource` model and one `FileTransport` trait. `FileIdentityMetadata` carries path or URL, size when known, SHA-256 checksum for local files, ETag for HTTP(S), and modification evidence when available. Metadata can be converted to `cdf_kernel::FilePosition` evidence without writing package, receipt, checkpoint, or plan artifacts.

HTTP(S) reads use a binary-preserving `HttpFileTransport` test/client seam. Ranged reads send a bounded `Range` header, require `206 Partial Content`, reject `200 OK` range-ignored responses, and reject responses larger than the requested bound. HTTP(S) `list` fails with an explicit contract error; arbitrary directory listing is not invented. Egress allowlist checks run before the HTTP client is called. Auth and secret-provider hooks are present in the API shape and fail closed because credential resolution is excluded from this child ticket.

Parent integration review added `Debug` implementations for the public transport/request metadata surfaces that can carry URLs. `HttpFileRequest` routes URLs and headers through `cdf_http::Redactor`; `FileTransportResource`, `FileTransportLocation`, and `FileIdentityMetadata` redact signed URL query values in debug output while preserving ordinary non-sensitive query evidence.

## Procedure

- `cargo test -p cdf-declarative file_transport --locked`
  - First run compiled and ran 5 focused tests; 4 passed and 1 failed because the test fixture expected the wrong SHA-256 literal.
  - After correcting the fixture digest, final run passed: 5 passed, 0 failed.
  - After the parent redaction repair, final parent-observed run passed: 7 passed, 0 failed.
- `cargo test -p cdf-declarative --locked`
  - Passed before the redaction repair: 70 unit tests and 0 doc tests.
  - Passed after the parent redaction repair: 72 unit tests and 0 doc tests.
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`
  - Passed with no warnings.
- `cargo fmt --all -- --check`
  - Passed.
- `git diff --check`
  - Passed.
- `jscpd crates/cdf-declarative/src/lib.rs crates/cdf-declarative/src/file_transport.rs --min-lines 5 --min-tokens 50`
  - First run found one small request/response builder clone.
  - After removing the unneeded request header builder, final run reported 0 clones and 0 duplicated lines/tokens.
- `jscpd --format rust --reporters console --no-colors --no-tips crates/cdf-declarative/src/lib.rs crates/cdf-declarative/src/file_transport.rs`
  - Parent-observed run reported 0 clones and 0 duplicated lines/tokens.
- `rust-code-analysis-cli -m -O json -p crates/cdf-declarative/src/file_transport.rs > target/quality/reports/rust-code-analysis-p2-e1.json`
  - Passed and wrote the complexity report.
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-p2-e1.json .`
  - Passed with 0 findings.
- `gitleaks detect --no-git --redact --source target/quality/gitleaks-src-p2-e1 --report-format json --report-path target/quality/reports/gitleaks-src-p2-e1.json --verbose`
  - Passed with no leaks over the scoped source snapshot for this slice.
- `rg -n "unsafe" crates/cdf-declarative/src/file_transport.rs crates/cdf-declarative/src/lib.rs`
  - Returned no matches in the touched Rust files.
- `cargo deny --locked check advisories licenses sources`
  - Passed.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`
  - Passed; the ignored advisory is the already-ratified `paste` advisory.
- `cargo vet --locked --no-minimize-exemptions`
  - Passed with existing exemptions.
- `osv-scanner scan source --lockfile Cargo.lock --format json > target/quality/reports/osv-p2-e1.json`
  - Exited non-zero only for the already-ratified `paste` advisory `RUSTSEC-2024-0436`; no E1 dependency change introduced a new advisory.
- `tools/codeql-rust-quality.sh 2>&1 | tee target/quality/reports/codeql-rust-p2-e1.log`
  - Used the reusable database path `target/quality/codeql-db-rust`; the database refreshed because Rust inputs changed, then analysis completed.
  - SARIF result count: 3, all pre-existing current-tree hardcoded-value findings in `crates/cdf-cli/src/tests.rs` lines 1313, 1403, and 1459. They are owned by `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md` and are outside WS-E1.

## What this supports

- Local file and HTTP(S) resources share one facade model and trait.
- Local file metadata records path, size, SHA-256, and modification time evidence.
- HTTP(S) metadata records URL, size from `Content-Length`, ETag, and `Last-Modified` when supplied.
- HTTP(S) ranged reads are bounded in tests and do not accept range-ignored full downloads.
- HTTP(S) directory listing remains explicitly unsupported.
- Secret handling and egress allowlist hooks are visible in the API; allowlists are enforced before transport use and credentials fail closed pending a later child.
- HTTP request debug rendering redacts sensitive URL and header values through the shared HTTP redactor.
- Public resource/location/metadata debug rendering redacts sensitive signed URL query values.
- The facade only returns metadata/bytes and does not integrate around plan, package, receipt, or checkpoint evidence.

## Limits

This evidence does not cover S3, GCS, Azure, credential resolution, doctor probes, HTTP template enumeration, compression, or full file-run integration. Those remain outside WS-E1 and are still owned by the active P2 WS-E remote transport workstream or later children.

The CodeQL and OSV runs are current-tree signals rather than pure WS-E1 signals. CodeQL still reports the three P1 backfill fixture findings in `crates/cdf-cli/src/tests.rs`; OSV still reports the accepted `paste` advisory. Both residuals have existing durable owners and did not originate in this facade slice.
