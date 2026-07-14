Status: active
Created: 2026-07-13
Updated: 2026-07-13

# State uses one current schema and package-receipt recovery

## Context

The original state operations combined two independent behaviors: upgrading historical local SQLite layouts and recovering checkpoint state from current package/receipt evidence. CDF remains pre-production with no installed artifact population. `.10x/decisions/pre-production-current-format-only.md` requires old CDF artifact readers, migrations, fixtures, and CLI shims to be deleted, while the receipt-gated recovery behavior remains part of the current crash contract.

This decision supersedes `.10x/decisions/superseded/state-migrate-recover-package-receipt.md` and resolves its migration behavior without weakening its package-receipt recovery rules.

## Decision

Each SQLite state component MUST initialize its current schema automatically when its component tables and schema-version record are both absent. The sole current schema generation is version 1 for `checkpoint_store`, `run_ledger`, and `scope_lease_store`; pre-production schema evolution is collapsed rather than advertised as supported predecessor generations.

An existing component MUST open only when its recorded version is exactly current and all required current tables exist. A noncurrent version, an unversioned existing component table, or an incomplete current schema MUST fail closed with a diagnostic naming the component and mismatch. CDF MUST retain the version gate and component-version registry as the migration-ready seam, but MUST NOT ship state upgrade functions, historical SQLite fixtures, or a `cdf state migrate` command before a real supported predecessor exists.

`cdf state recover` remains package-receipt recovery, not open-ended destination-mirror scraping:

```text
cdf state recover --package <package-dir> --to <destination-uri> [--receipt <receipt-id>] [--target <schema.table> --merge-dedup fail]
```

Recovery MUST consume verified current package replay inputs and a uniquely selected durable receipt, verify that receipt through the destination protocol, and advance state only through `CheckpointStore::commit` or exact reuse of an already-committed head. It MUST NOT write destination rows or reconstruct evidence it does not possess. Its output MUST continue to state those evidence limits.

## Alternatives considered

- Retain `cdf state migrate` until 1.0. Rejected because there is no installed base and every historical layout adds production branches, fixtures, and false compatibility obligations.
- Silently stamp an unversioned existing database as current. Rejected because an old or corrupt layout could then be interpreted under the wrong schema and skip data.
- Delete recovery together with migration. Rejected because recovery consumes current durable evidence and is required by the commit/crash contract; it is not a compatibility path.
- Make `state recover` a synonym for `resume` or choose the newest receipt automatically. Rejected because run-ledger recovery and explicit package-fact recovery have different evidence, and ambiguous receipt selection changes the fact authorizing state advancement.

## Consequences

State initialization has one v1 path, a strict version registry, and no migration product yet. Development databases from older snapshots must be deleted and rebuilt. Current package-receipt recovery remains deterministic and receipt-gated. When a real v2 is introduced after a compatibility promise, explicit migrations and `cdf state migrate` can be added at the preserved version gate under a new decision defining supported source versions and upgrade tooling.
