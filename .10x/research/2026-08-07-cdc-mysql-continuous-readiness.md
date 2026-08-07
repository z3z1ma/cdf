Status: done
Created: 2026-08-07
Updated: 2026-08-07

# CDC, MySQL, and continuous package-retention readiness

## Question

What remains between the completed source-position/compiler foundations and first-class MongoDB,
Postgres, and MySQL CDC plus ordinary MySQL reads, and which current package-lifecycle behavior
would make a forever-running resource grow without bound?

## Sources and methods

Inspected current `main` at `96fd277d`, the active CDC/keyed-effect/package-lifecycle/SQL-source
records, the existing Postgres/MongoDB/SQLite/ClickHouse source drivers, kernel batch/position
types, engine drain execution, destination disposition gates, project retention configuration, and
the CLI package-GC planner. Reused the official-protocol findings recorded four days earlier in
`.10x/research/2026-08-03-cdc-protocol-position-contract.md`; no protocol claim was broadened and no
server was contacted.

## Findings

1. A1 position authority is complete. Kernel/checkpoint/state/replay artifacts already carry typed
   PostgreSQL commit positions, MySQL commit positions, and scoped opaque MongoDB change-stream
   resume tokens.
2. `BatchHeader::cdc` currently carries only an operation-field name and source position. Engine
   code protects the operation field as control data, but packages still have one homogeneous row
   segment family and do not lower CDC events into package-native keyed upsert/delete effects.
3. Every built-in destination either rejects `cdc_apply` or reserves it for the CDC tranche. There
   is no end-to-end source/package/destination proof yet.
4. Postgres and MongoDB already have first-party source crates and runtime registrations. They
   should gain snapshot/CDC resource modes inside those crates. There is no MySQL crate or client
   dependency; one `cdf-source-mysql` should own both ordinary table reads and binlog CDC.
5. The relational sources repeat high-level table/projection/cursor validation but retain genuinely
   different catalog, consistency, SQL rendering, transport, decoding, and error behavior. The
   draft `cdf-source-sql` boundary remains appropriate if kept to typed relational intent and
   structural validation.
6. CDF already has finite drain epochs, rolling replay/spill, package settlement, receipts, and
   checkpoint gates. Resident execution is explicitly rejected today. The CDC runtime should
   first reuse finite settled epochs; resident supervision remains a thin later loop over that
   proven unit unless separately activated.
7. Project retention rules (`runs` or duration, with trust-specific overrides) are parsed but are
   not consumed by package GC. `cdf package gc` is dry-run only and classifies every committed
   checkpoint package and every package receipt as permanently protected. A long-running resource
   therefore retains all heavy canonical package data forever.
8. `cdf-package` already has crash-safe tombstoning that removes canonical identity files while
   preserving manifest hashes and receipts. The missing behavior is a retention-aware collector,
   automatic invocation after a successful checkpoint, and explicit promotion-availability
   reporting. Deleting before receipt/checkpoint settlement would violate existing authority.
9. The draft CDC contract has one unresolved database-log resource choice: the concrete hard byte
   limit for a single PostgreSQL/MySQL transaction. The resolved runtime spill budget already
   supplies a deterministic host-owned ceiling (8 GiB by default, CLI/environment overridable), so
   it is the smallest existing authority to bind rather than introducing another ambient default.

## Conclusions

- Execute package-native keyed effects before any CDC adapter.
- Make retention-aware package tombstoning part of ordinary post-checkpoint settlement so heavy
  package storage is bounded for repeated drain epochs; keep manual dry-run/execute parity.
- Extract only the relational intent/validation seam proven by current sources, then add one MySQL
  source crate for both bounded snapshot reads and CDC.
- Prove MySQL CDC first because ROW/FULL/GTID supplies complete images, then extend the existing
  Postgres and MongoDB source crates with their protocol-specific exactness prerequisites.
- At least Postgres and DuckDB should consume `cdc_apply` if local and production-like workflows
  are both expected; delete application must remain explicit and identity-bearing.

## Limits

- No MySQL library/version was selected and no live MySQL, logical-replication, or change-stream
  topology was exercised.
- Integrated snapshot-to-stream bootstrap, first destination set, delete-policy authoring syntax,
  and the exact large-transaction ceiling require user ratification before executable adapter
  tickets can be cold-start complete.
- Minimal tombstone metadata and checkpoint history still grow over time; this tranche can bound
  heavy package bytes without claiming a distributed ledger-compaction policy.
