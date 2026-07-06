Status: open
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md

# Implement HTTP toolkit

## Scope

Implement `firn-http`: paginators, rate limiting, retry/backoff budget, auth session/refresh hooks, egress allowlist enforcement hooks, redacted request tracing, and plan-visible auto-detection results. Owns `crates/firn-http/**`.

## Acceptance criteria

- Cursor, page, offset, link-header, and next-token pagination are supported.
- Rate limiting respects `Retry-After` and configured quota headers.
- Retry budget maps failures into the shared taxonomy and retries only safe units.
- Secrets are never formatted into traces or errors.
- Egress allowlist blocks disallowed hosts before network use.

## Evidence expectations

Record unit tests with mocked HTTP responses for pagination, rate limits, auth refresh, retry exhaustion, redaction, and allowlist denial.

## Explicit exclusions

No declarative resource compiler; no Python SDK bindings.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.

