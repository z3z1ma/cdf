# Recovery

CDF recovery is artifact-first. After package finalization, recovery should use
package artifacts, destination receipts, run-ledger events, and checkpoint rows
rather than contacting the source again.

## Resume an Interrupted Run

Use `cdf resume` when the selected environment has a run-ledger entry for the
interrupted run:

```bash
cdf --project /path/to/project resume <run-id>
```

Equivalent parser form:

```bash
cdf --project /path/to/project resume --run <run-id>
```

`resume` fails closed if the state database is missing or the run id is absent
from the selected environment run ledger.

The deterministic conformance proof for the crash window is:

```bash
cargo test -p cdf-conformance mvp_acceptance_demo --locked
```

That fixture simulates a crash after destination receipt verification and before
checkpoint commit, then proves `cdf resume` commits the checkpoint without new
source contact.

## Recover State From a Package Receipt

Use `cdf state recover` when you have a package with a durable receipt and need
to reconstruct checkpoint state from verified facts:

```bash
cdf --project /path/to/project state recover \
  --package /path/to/package \
  --to duckdb://.cdf/dev.duckdb
```

Postgres recovery requires the explicit target and merge-dedup policy:

```bash
cdf --project /path/to/project state recover \
  --package /path/to/package \
  --to postgres://secret://provider/key \
  --target schema.table \
  --merge-dedup fail
```

`state recover` verifies the selected package receipt and commits checkpoint
coverage. It does not rewrite destination rows, reconstruct arbitrary missing
run-ledger history, or reconstruct quarantine lineage.

## Inspect Recovery State

```bash
cdf --project /path/to/project state show \
  --pipeline <pipeline-id> \
  --resource <resource-id>

cdf --project /path/to/project state history \
  --pipeline <pipeline-id> \
  --resource <resource-id>
```

Use `--scope-json` only when the resource uses a non-default scope key.

## Related Contracts

- [Project CLI, observability, and security](../../.10x/specs/project-cli-observability-security.md)
- [Run orchestration ledger](../../.10x/specs/run-orchestration-ledger.md)
- [State recover package receipt decision](../../.10x/decisions/state-migrate-recover-package-receipt.md)
