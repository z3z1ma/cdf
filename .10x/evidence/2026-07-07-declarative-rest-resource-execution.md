Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-declarative-rest-resource-execution.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md

# Declarative REST Resource Execution Evidence

## What was observed

Tier-0 declarative REST resources are openable through explicit runtime dependencies via `RestResource` and `RestRuntimeDependencies`. The default `CompiledResource::open` path still fails closed for REST without dependencies. REST execution is deterministic in tests through in-memory transports, enforces allowlists before transport use, resolves auth through `SecretProvider`, retries one configured auth refresh on `401`/`403`, uses the `cdf-http::Paginator` modes, decodes JSON selector outputs into Arrow batches, emits cursor source positions, and wires cursor pushdown metadata into first-request query params.

`cdf-http::HttpResponse` gained body access without changing its public field set; body text is stored behind a reserved response-page field and custom debug output redacts it.

## Procedure

- `cargo fmt --all -- --check` passed after final edits.
- `git diff --check -- . ':(exclude).gitignore'` passed after final edits.
- `cargo test -p cdf-declarative --locked --no-fail-fast` passed after final edits: 42 tests passed.
- `cargo test -p cdf-http --locked --no-fail-fast` passed before final test-only hardening: 6 tests passed.
- `cargo clippy -p cdf-declarative -p cdf-http --all-targets --locked -- -D warnings` passed after final edits.
- `cargo nextest run -p cdf-declarative -p cdf-http -p cdf-conformance --locked` passed after final edits: 88 tests passed, 0 skipped.
- `cargo check --workspace --all-targets --locked` passed before final test-only hardening.
- `cargo test --workspace --all-targets --locked --no-fail-fast` passed before final test-only hardening.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` passed before final test-only hardening.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked` passed before final test-only hardening.
- `cargo hack check --workspace --all-targets --each-feature --locked` passed before final test-only hardening.
- `cargo check --workspace --all-targets --all-features --locked` and `cargo check --workspace --all-targets --no-default-features --locked` passed before final test-only hardening.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` and `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings` passed before final test-only hardening.
- `cargo deny check` passed; duplicate dependency warnings remain policy-allowed.
- `cargo audit` passed with the ratified `RUSTSEC-2024-0436` `paste` warning.
- `cargo vet --locked` passed with the current exemption backlog.
- `osv-scanner --lockfile Cargo.lock` reported only the ratified `RUSTSEC-2024-0436` path.
- `semgrep scan --config auto --error crates/cdf-declarative/src crates/cdf-http/src` passed with 0 findings.
- `gitleaks dir --no-banner --redact --exit-code 1 --log-level error` passed for `crates/cdf-declarative`, `crates/cdf-http`, and the ticket record. A prior multi-path invocation was interrupted after hanging; the successful evidence used separate source-only scans.
- `cargo machete` passed with no unused dependencies.
- `cargo semver-checks check-release --workspace --exclude cdf-python --exclude cdf-wasm --exclude cdf-cli` could not use registry baselines because workspace crates are unpublished. Local baseline checks passed for `cdf-declarative` and `cdf-http` against `HEAD`.
- Direct unsafe/FFI/raw-pointer scan over `crates/cdf-declarative` and `crates/cdf-http` found no unsafe/FFI/raw pointer implementation hits; the only hit was explanatory text in `cdf-http/src/retry.rs`.
- `rust-code-analysis-cli` ran against `crates/cdf-declarative/src` and `crates/cdf-http/src` after pre-creating output directories.
- `cargo +nightly udeps --workspace --all-targets --locked` passed and reported all deps used.
- `jscpd crates/cdf-declarative/src crates/cdf-http/src --reporters console --threshold 10` passed with 5.40% duplicated lines.
- `cargo llvm-cov --workspace --all-features --locked --summary-only` passed. Touched rows included `cdf-declarative/src/rest_runtime.rs` at 82.51% line coverage and total workspace line coverage at 79.78%.
- Full bounded mutation over `crates/cdf-declarative/src/rest_runtime.rs` first exposed real missing tests. After hardening, a full run reported 268 caught, 26 unviable, and 3 missed mutants in `validate_http_url` and `days_from_civil`. The three missed mutants were then repaired with focused tests, and `cargo mutants --file crates/cdf-declarative/src/rest_runtime.rs --examine-re 'validate_http_url|days_from_civil' --baseline skip --all-features --cargo-arg --locked --jobs 2 --test-tool cargo --output reports/ai-quality/mutants-rest-runtime-repaired -- -p cdf-declarative` passed with 52 caught and 0 missed.
- CodeQL was intentionally skipped for this checkpoint per the active user instruction not to recreate the database.

## What this supports

The evidence supports the ticket acceptance criteria for deterministic explicit-dependency REST execution, fail-closed default REST open behavior, safe GET construction, allowlist enforcement, secret-backed auth, one refresh retry, paginator coverage, JSON-to-Arrow materialization, conformance batch headers, cursor source positions, cursor pushdown URL effects, and negative fail-closed behavior.

Mutation hardening specifically added coverage for predicate literal safety, selector boundary rejection, non-nullable fields, scalar coercions, cursor maxima, blank pagination markers, absolute-path origin joins, empty/whitespace hosts, RFC3339/date/leap-year parsing, and generated id sanitization.

## Limits

No live API or GitHub execution was run; live network behavior and CLI `cdf run` REST wiring are explicitly excluded from this ticket. SQL source execution, transform execution, OAuth, streaming, and destination/checkpoint lifecycle changes are outside this evidence. Generated reports under `reports/ai-quality` are ignored artifacts and are not committed.
