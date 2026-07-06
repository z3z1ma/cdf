Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-http-toolkit.md

# HTTP toolkit implementation evidence

## What was observed

`firn-http` now provides pure request/response toolkit primitives for pagination, rate limiting, retry classification/budgeting, auth session refresh, egress allowlist checks, redaction, and pagination auto-detection plan summaries.

The implementation uses the existing `firn-kernel` `ErrorKind` and `FirnError` taxonomy. No `crates/firn-kernel/**` changes were required.

## Procedure

- Read the active HTTP ticket and governing records before editing.
- Implemented `crates/firn-http/src/lib.rs` against mocked HTTP request/response primitives; no real network I/O was introduced.
- Added `firn-kernel` as the `firn-http` dependency for shared error taxonomy integration.
- Ran targeted HTTP tests: `cargo test -p firn-http --lib --locked --no-fail-fast`.
- Ran required final checks:
  - `cargo fmt --all -- --check`
  - `cargo test -p firn-http --locked --no-fail-fast`
  - `cargo clippy -p firn-http --all-targets --locked -- -D warnings`
  - `git diff --check`

## Results

- `cargo test -p firn-http --lib --locked --no-fail-fast`: passed; 6 unit tests passed.
- `cargo fmt --all -- --check`: passed after formatting `firn-http` with `cargo fmt --package firn-http`.
- `cargo test -p firn-http --locked --no-fail-fast`: passed; 6 unit tests passed and 0 doctests ran.
- `cargo clippy -p firn-http --all-targets --locked -- -D warnings`: passed.
- `git diff --check`: passed.

## What this supports or challenges

This supports the HTTP ticket acceptance criteria:

- Cursor, page, offset, link-header, and next-token pagination are covered by `paginators_cover_cursor_page_offset_link_and_next_token`.
- Plan-visible auto-detection is covered by `auto_detection_is_plan_visible`.
- `Retry-After` and configured quota headers are covered by `rate_limiter_respects_retry_after_and_quota_headers`.
- Retry taxonomy, safe-unit retry gating, and retry exhaustion are covered by `retry_budget_maps_taxonomy_and_retries_only_safe_units`.
- Auth refresh hooks and trace/debug redaction are covered by `auth_refresh_hooks_and_traces_do_not_format_secrets`.
- Egress allowlist denial before transport use is covered by `allowlist_denies_before_transport_send`.

## Limits

The toolkit intentionally remains pure and unit-testable. It defines a transport trait and policy wrapper but does not implement a concrete network client, file-backed `trace.jsonl` writer, declarative compiler integration, Python bindings, or WASM host integration.
