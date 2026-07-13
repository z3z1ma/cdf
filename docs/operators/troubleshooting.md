# Troubleshooting

This page lists current failure modes that operators can resolve without the
future generated error catalog.

## Unexpected Plan Target

Current `cdf plan` can derive a destination target from the resource id. Pass an
explicit target when scripts or documentation need a stable name:

```bash
cdf --project /path/to/project plan <resource-id> --target <target>
```

## Run Identifiers

`cdf run` owns pipeline, target, package, and checkpoint identities. Automation
should consume the JSON report instead of choosing artifact identities:

```bash
cdf --json --project /path/to/project run <resource-id>
```

## `sql query failed: no such table: <target>`

`cdf sql` currently queries CDF local system history, not the destination table.
Use tables such as `packages`, `package_receipts`, and `checkpoints`.

```bash
cdf --project /path/to/project sql \
  'select package_id, status from packages order by package_id'
```

## Replay Collides With an Existing Checkpoint

If replaying a package into the same project ledger reports a duplicate
checkpoint id, use a clean replay project/ledger for the replay target:

```bash
cdf init /tmp/cdf-replay --name cdf_replay
cdf --project /tmp/cdf-replay replay package /path/to/package --to duckdb://.cdf/replay.duckdb
```

## Missing Generated References

The generated command reference and error catalog are pending under
[WS6B](../../.10x/tickets/done/2026-07-08-p1-product-ws6b-generated-reference-freshness.md).
Use `cdf help <command>` for current parser syntax.
