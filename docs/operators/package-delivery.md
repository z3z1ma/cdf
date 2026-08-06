# Package delivery

`cdf run --package` drives an existing package into a destination without contacting the source,
parsing authored resource SQL, or rediscovering schema.

## Verify the package first

```bash
cdf package verify /path/to/package
```

## Deliver to DuckDB

```bash
cdf --project /path/to/delivery-project run \
  --package /path/to/package \
  --to duckdb://.cdf/delivery.duckdb
```

Use a clean project ledger when delivering a package produced by another project if the original
checkpoint identity is already present in the source project's state database.

## Deliver to Postgres

Postgres package execution requires an explicit target:

```bash
cdf --project /path/to/project run \
  --package /path/to/package \
  --to postgres://secret://provider/key \
  --target schema.table
```

CDF does not infer the Postgres target, disposition, or merge keys from destination introspection.
The finalized package owns its destination commit contract and keyed-effect winner selection.

## Inspect the result

The command reports the new run id. Inspect its package, receipt, checkpoint, and recovery facts:

```bash
cdf --project /path/to/delivery-project inspect run <run-id>
```

`cdf package ls` lists packages under the selected project's configured package root. A package
delivered from an external path may not appear in that project-local listing.

## Related contracts

- [Destination receipts and guarantees](../../.10x/specs/destination-receipts-guarantees.md)
- [Destination introspection package and CLI policy](../../.10x/decisions/destination-introspection-package-and-cli-policy.md)
- [Quickstart package path](../quickstart.md#5-run-from-a-package-without-source-contact)
