Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/2026-07-10-p2-rp2-residual-verdict-runtime-package.md, .10x/tickets/done/2026-07-10-p2-rp3-correction-capabilities-row-provenance.md

# P2 RP8 Parquet correction sidecars and materialization contract

## Scope

Implement the safe non-UPDATE strategy for the Parquet/object-store destination: immutable addressed correction sidecars with manifest/receipt evidence, plus an explicit versioned-rematerialization plan boundary where a consumer requires a fully materialized replacement.

## Acceptance criteria

- Correction sidecars contain original provenance address, promoted fields, residual-path operation, schema hashes, and correction package identity.
- Base Parquet objects are never silently rewritten; human/JSON output says the base target is unchanged.
- Sidecar manifests are atomic, idempotent, content-addressed, and independently verifiable through destination receipts.
- Destination sheet declares sidecar support and does not claim in-place update/readback it cannot prove.
- Versioned-rematerialization planning names required source packages, target version, pointer/manifest advance, and unsupported cases; implementation MUST NOT fake atomic pointer support.
- Replay of the same correction package produces no duplicate sidecar effect.

## Evidence expectations

Filesystem/object-store fixture sidecars, manifest hashes, receipt verification, interrupted manifest writes, duplicate replay, base-object immutability, and destination conformance.

## Explicit exclusions

No table-format merge engine, arbitrary object overwrite, Iceberg/Delta implementation, or generic promotion orchestration.

## Progress and notes

- 2026-07-10: Opened as Parquet's honest append-only correction strategy.

## Blockers

Depends on RP2/RP3.
