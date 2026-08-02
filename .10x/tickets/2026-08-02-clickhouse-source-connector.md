Status: open
Created: 2026-08-02
Updated: 2026-08-02
Parent: .10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md
Depends-On: .10x/tickets/2026-08-02-sqlite-destination-connector.md

# ClickHouse source connector

## Scope

Implement and ship `cdf-source-clickhouse` on the official Rust client plus the Arrow 58
`clickhouse-ext-arrow` path. Add built-in enrollment, data-driven and live type/cursor fixtures,
operator documentation, and a release-mode direct-ArrowStream source roofline cell.

## Non-goals

Destination writes, CDC, arbitrary SQL, a private native/TCP protocol, string-inferred temporal
cursors, or cross-query snapshot claims.

## Acceptance Criteria

- Driver configuration, discovery, schema mapping, compile/portability, health, query generation,
  pushdown fidelity, cursor ordering, partial-stream failure, and execution implement
  `.10x/specs/clickhouse-table-source.md`.
- Supported ClickHouse types round-trip through Arrow 58; unsupported types fail during discovery
  with field/type remediation and no stringification.
- The source streams accounted `fetch_arrow()` batches through injected async host services with
  bounded buffering, cancellation, egress, and no private runtime/pool/retry authority.
- Built-in catalog integrity, generic source matrix, jobs invariance, package/replay/checkpoint
  laws, and `tools/certify-connector.py --kind source --id clickhouse --core-impact` pass against a
  digest-pinned security-supported ClickHouse image.
- The source macro benchmark reaches the 0.90 direct official ArrowStream roofline and records the
  server/client/compression/`max_threads` settings.
- Independent review passes after closure repair.

## References

- `.10x/specs/clickhouse-table-source.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/research/2026-08-02-sqlite-clickhouse-mongodb-connector-shaping.md`
- `docs/connector-authoring.md`

## Assumptions

- Finite snapshot/cursor behavior, current security-supported ClickHouse, official Arrow path, and
  the 90% roofline are user-ratified or record-backed.
- CDF Arrow 58.3.0 matches the official extension's current Arrow 58 matrix.

## Journal

- 2026-08-02: Ticket opened; execution waits for complete SQLite tranche closure.

## Blockers

None beyond the declared dependency.

## Evidence

Pending.

## Review

Pending independent red-team review.

## Retrospective

Pending executor handback.
