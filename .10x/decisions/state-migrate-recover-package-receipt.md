Status: active
Created: 2026-07-08
Updated: 2026-07-08

# State migrate/recover operational contract

## Context

`.10x/tickets/done/2026-07-07-cli-state-migrate-recover.md` requires `cdf state migrate` and `cdf state recover` without bypassing `CheckpointStore::commit`. `.10x/specs/checkpoint-state-commit-gate.md` requires explicit, fixture-backed migrations and permits destination-mirror recovery only when heads are reconstructed from verified facts with evidence limits. The current runtime already exposes package-artifact recovery through `cdf_project::recover_package_from_artifacts`, which verifies package replay inputs, verifies a supplied durable receipt through the destination protocol, and commits state through the checkpoint store.

Broad destination mirror scraping would require additional destination-specific inventory semantics and is not necessary for the first safe operational recovery command.

## Decision

`cdf state migrate` MUST be a local SQLite state database operation. It MUST report each component's before version, after version, target version, and whether a migration was applied. The current components are:

- `checkpoint_store`, current target version `1`.
- `run_ledger`, current target version `2`, including the existing v1-to-v2 migration.

Opening a missing state database for migration MAY create the current schema and MUST report that as an applied initialization. Re-running against an already-current database MUST be idempotent and report no applied migrations.

`cdf state recover` is initially package-receipt recovery, not open-ended destination mirror scraping. Its CLI contract is:

```text
cdf state recover --package <package-dir> --to <destination-uri> [--receipt <receipt-id>] [--target <schema.table> --merge-dedup fail]
```

The command MUST read package replay inputs and package receipts from `<package-dir>`. If `--receipt` is omitted, exactly one durable package receipt must be present. If zero or multiple receipts are present, recovery MUST fail closed. If `--receipt` is supplied, the matching receipt id MUST exist in the package.

Recovery MUST resolve the destination using the same package-replay destination rules, including explicit Postgres `--target` and `--merge-dedup fail`. Recovery MUST verify the supplied receipt through the destination protocol and then call the lower recovery path that commits through `CheckpointStore::commit` or reuses an exact already-committed head. It MUST NOT write destination rows or advance checkpoint state directly.

The JSON output MUST include the package path, package hash, selected receipt id, destination summary, checkpoint id/status/head, package status, receipt source, and an `evidence_limits` list stating that package-receipt recovery does not reconstruct quarantine lineage or arbitrary missing run-ledger history.

## Alternatives considered

- Destination mirror scraping as the first `state recover` behavior. Rejected for this slice because destination-specific mirror inventory and precedence rules are not yet specified deeply enough, and package-receipt recovery already satisfies the commit-gate-safe crash recovery path.
- Make `state recover` a synonym for `resume`. Rejected because `resume` is run-ledger oriented, while state recovery must also work from explicit package and receipt facts when the interrupted run ledger is unavailable.
- Pick the newest receipt automatically when multiple receipts are present. Rejected because receipt choice changes which destination fact authorizes state recovery; ambiguity must be explicit.

## Consequences

This decision closes the missing API-shape ambiguity for `.10x/tickets/done/2026-07-07-cli-state-migrate-recover.md` while preserving a future destination-mirror recovery lane. Future mirror recovery must supersede or extend this decision with destination-specific mirror inventory, evidence limits, and precedence rules.
