Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p2-ws-e-remote-transports.md
Depends-On: .10x/tickets/done/2026-07-10-p2-ws-e3-cloud-object-stores-and-http-templates.md, .10x/specs/data-onramp-file-sources-transports.md, .10x/specs/data-onramp-conformance.md

# P2 WS-E6 HTTP date globs and S1/S2 conformance

## Scope

Complete the bounded HTTP enumeration contract for canonical dated monthly globs such as `yellow_tripdata_2024-*.parquet`, using a transport-level optional metadata probe so absent candidates are not inferred from error strings. Add deterministic S1/S2/S8 conformance over the production HTTP path and record a live public TLC session when reachable.

## Acceptance criteria

- A single `*` in the month position of a year-qualified filename expands deterministically to months 01–12; other HTTP wildcards remain rejected as unbounded.
- HTTP 404 during finite candidate enumeration means absent candidate; auth, transient, rate-limit, and other data errors remain failures.
- Zero present candidates fails with the ordinary no-match diagnostic.
- Deterministic fixture conformance proves S1 add/run and S2 multi-file initial load, unchanged no-op, and newly-present-month incremental load.
- Preview and run resolve the same HTTP candidate set and schema path.
- The public TLC S1/S2 session is recorded with network/date limits, or a reproducible external-availability failure is recorded without weakening deterministic CI.

## Explicit exclusions

General HTTP directory scraping, arbitrary wildcard enumeration, HTML indexes, and guessing non-date domains.

## Evidence expectations

Facade-level 404 classification tests, bounded expansion tests, deterministic conformance with request capture, live terminal evidence, clippy, and focused adversarial review.

## Blockers

None for deterministic implementation. Public evidence depends on upstream availability.

## Progress and notes

- 2026-07-10: Opened from the remaining S1/S2 gap. The finite date heuristic is deliberately compiler-visible and transport-neutral: enumeration produces candidates; existence is decided by a typed optional metadata operation, not message matching.
- 2026-07-10: Implemented the typed optional metadata operation, finite year-month wildcard expansion, exact 404 absence handling, deterministic S1 add/run, and deterministic S2 preview/initial-load/no-op/new-month-only execution. Evidence is `.10x/evidence/2026-07-10-p2-e6-http-date-glob-s1-s2.md`; review is `.10x/reviews/2026-07-10-p2-e6-http-date-glob-s1-s2-review.md`.
- 2026-07-10: Public TLC `cdf add` succeeded through ranged footer discovery and inferred/pinned 19 fields in 0.3 seconds. The subsequent public GET failed because the upstream CloudFront endpoint returned HTTP 403 to both CDF and independent curl GET/range requests while cached HEAD remained 200; deterministic fixture conformance remains green and the external availability limit is recorded rather than hidden.
