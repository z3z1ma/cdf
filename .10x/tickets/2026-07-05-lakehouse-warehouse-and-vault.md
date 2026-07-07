Status: open
Created: 2026-07-05
Updated: 2026-07-05
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-05-parquet-object-store-destination.md, .10x/tickets/done/2026-07-05-postgres-destination.md, .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md

# Implement lakehouse, warehouse, and vault-class integrations

## Scope

Implement first warehouse destination, vault-class secret providers, and lakehouse destinations `cdf-dest-iceberg` and `cdf-dest-delta` over the destination sheet/conformance protocol. Owns new destination/secret-provider crates or modules created for these integrations.

## Acceptance criteria

- Warehouse destination passes destination conformance and declares honest sheets.
- Vault/cloud secret providers resolve only by reference and preserve redaction guarantees.
- Iceberg and Delta are implemented as destinations, not package formats.
- Lakehouse receipts embed the table format's snapshot/transaction metadata alongside cdf package receipt data.
- Parquet/object-store destination remains the seam, not a competing metadata story.

## Evidence expectations

Record destination conformance output, secret provider redaction tests, lakehouse commit/replay tests, and receipt verification tests.

## Explicit exclusions

Vector-store destinations and UI remain out of scope unless later ratified by active decision.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.
