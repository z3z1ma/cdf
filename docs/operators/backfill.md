# Backfill

`cdf backfill` plans a bounded cursor window and, with `--execute`, runs each
planned slice through the normal run spine. If `--target` is omitted, the CLI
derives a target from the resource id; pass `--target` when the destination name
must be stable.

## Dry Plan

Dry planning is the default and does not write package artifacts, destination
data, checkpoint rows, or run-ledger events.

```bash
cdf --project /path/to/project backfill <resource-id> \
  --from <cursor-start> \
  --to <cursor-end> \
  --target <target>
```

Optional slice sizing:

```bash
cdf --project /path/to/project backfill <resource-id> \
  --from <cursor-start> \
  --to <cursor-end> \
  --target <target> \
  --slice-size <n>
```

Expected shape:

```text
planned backfill for <resource-id> to <target>: <n> slice(s); wrote no package, destination data, checkpoint rows, or run-ledger events
```

## Execute

```bash
cdf --project /path/to/project backfill <resource-id> \
  --from <cursor-start> \
  --to <cursor-end> \
  --target <target> \
  --execute
```

Each slice receives ordinary run-ledger events, package artifacts, destination
receipt verification, and checkpoint gating. Backfill must not mutate checkpoint
state outside `CheckpointStore::commit`.

## Related Contracts

- [Backfill window planner command contract](../../.10x/decisions/backfill-window-planner-command-contract.md)
- [Run orchestration ledger](../../.10x/specs/run-orchestration-ledger.md)
