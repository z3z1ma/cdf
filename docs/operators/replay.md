# Replay

Package replay drives an existing package into a destination without contacting
the source.

## Verify the Package First

```bash
cdf package verify /path/to/package
```

Expected shape:

```text
verified package sha256:...: ... file(s), ... archive segment(s)
```

## Replay to DuckDB

```bash
cdf --project /path/to/replay-project replay package \
  /path/to/package \
  --to duckdb://.cdf/replay.duckdb
```

For the current public CLI quickstart, use a clean replay project/ledger when
replaying a package produced by another project. Replaying that exact package
back into the same SQLite checkpoint ledger can collide on the checkpoint id.

## Replay to Postgres

Postgres replay is intentionally explicit:

```bash
cdf --project /path/to/project replay package \
  /path/to/package \
  --to postgres://secret://provider/key \
  --target schema.table \
  --merge-dedup fail
```

CDF does not infer the Postgres target, disposition, merge keys, or merge-dedup
policy from destination introspection.

## Inspect Replay Result

```bash
cdf --project /path/to/replay-project state history <resource-id> \
  --pipeline <pipeline-id>
```

`cdf package ls` lists packages under the selected project's configured package
root. If you replayed a package from an external path, it may not appear in the
replay project's package root.

## Related Contracts

- [Destination receipts and guarantees](../../.10x/specs/destination-receipts-guarantees.md)
- [Destination introspection package and CLI policy](../../.10x/decisions/destination-introspection-package-and-cli-policy.md)
- [Quickstart replay path](../quickstart.md#replay-from-a-clean-ledger)
