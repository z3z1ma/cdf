# Recovery

CDF recovery is artifact-first. After package finalization, recovery should use
package artifacts, destination receipts, run-ledger events, and checkpoint rows
rather than contacting the source again.

## Resume an Interrupted Run

Use `cdf run --resume` when the selected environment has a run-ledger entry for the
interrupted run:

```bash
cdf --project /path/to/project run --resume <run-id>
```

When exactly one interrupted run exists, the id may be omitted:

```bash
cdf --project /path/to/project run --resume
```

Bare resume is a clean no-op when no run ledger or interrupted run exists, and
reports every candidate without mutation when several interrupted runs exist.
An explicit id fails closed when the state database or run id is absent.

The deterministic conformance proof for the crash window is:

```bash
cargo test -p cdf-conformance mvp_acceptance_demo --locked
```

That fixture simulates a crash after destination receipt verification and before
checkpoint commit, then proves `cdf run --resume` commits the checkpoint without new
source contact.

## Recover State From a Package Receipt

Use `cdf state recover` when you have a package with a durable receipt and need
to reconstruct checkpoint state from verified facts:

```bash
cdf --project /path/to/project state recover \
  --package /path/to/package \
  --to duckdb://.cdf/dev.duckdb
```

Postgres recovery requires the explicit target:

```bash
cdf --project /path/to/project state recover \
  --package /path/to/package \
  --to postgres://secret://provider/key \
  --target schema.table
```

`state recover` verifies the selected package receipt and commits checkpoint
coverage. It does not rewrite destination rows, reconstruct arbitrary missing
run-ledger history, or reconstruct quarantine lineage.

## Inspect Recovery State

```bash
cdf --project /path/to/project state show <resource-id> \
  --pipeline <pipeline-id>

cdf --project /path/to/project state history <resource-id> \
  --pipeline <pipeline-id>
```

Use `--scope-json` only when the resource uses a non-default scope key.

## Related Contracts

- [Project CLI, observability, and security](../../.10x/specs/project-cli-observability-security.md)
- [Run orchestration ledger](../../.10x/specs/run-orchestration-ledger.md)
- [Current-schema state and package-receipt recovery decision](../../.10x/decisions/state-current-schema-package-receipt-recovery.md)
