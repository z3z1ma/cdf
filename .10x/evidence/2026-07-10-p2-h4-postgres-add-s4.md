Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-ws-h4-postgres-add-s4.md

# P2 H4 Postgres add and S4 evidence

## Observation

`cdf add warehouse.orders postgres://.../database/schema.orders` parses the table from the terminal path segment, probes catalog metadata with process-local secret authority, writes only `secret://file/.cdf/secrets/sources/warehouse.dsn` to TOML, and creates the DSN file at mode 0600 only for non-dry-run execution. Reports redact DSN userinfo and expose serial/timestamp/date fields as cursor candidates explicitly labeled unselected.

The deterministic local-Postgres scenario proves dry-run writes no secret/config/lock, committed add pins discovery, generated configuration plans and previews, and an explicitly selected `updated_at` cursor then runs two rows through package/receipt/checkpoint into DuckDB.

## Procedure

- `cargo test -p cdf-cli`: 267 tests plus doctor environment integration passed.
- `cargo test -p cdf-conformance p2_`: all 9 P2 matrix/conformance tests passed with S1-S8 covered.
- `cargo clippy -p cdf-cli -p cdf-conformance --all-targets -- -D warnings`: passed after removing the now-dead `Pending` scenario state.

## Limits

Cursor suggestions are structural candidates, not uniqueness/order proofs. CDF intentionally requires explicit cursor selection before incremental SQL run.
