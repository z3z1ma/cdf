Status: active
Created: 2026-08-07
Updated: 2026-08-07

# CDC resource authoring and continuous run lifecycle

## Purpose

Define the current SQL/resource surface for CDC, explicit delete application and bootstrap, and
the process-local continuous `cdf run` lifecycle built from finite receipt-gated drain epochs.

## Resource authoring

CDC remains a mode of the existing configured source type. PostgreSQL and MongoDB extend their
existing source kinds; one MySQL source kind owns both finite table reads and binlog CDC. No
`postgres_cdc`, `mongodb_cdc`, or `mysql_cdc` source kind exists.

Source mode and bootstrap are closed driver-owned `upstream(...)` resource arguments. A first-use
CDC resource MUST explicitly select:

```sql
mode => 'cdc', bootstrap => 'snapshot'
```

or:

```sql
mode => 'cdc', bootstrap => 'latest'
```

There is no default. `snapshot` performs the adapter's exact integrated consistent
snapshot-to-stream handoff and then consumes changes. `latest` records the source-proven current
frontier and begins after it; the plan and confirmation output identify that earlier data is
intentionally excluded. After the first committed checkpoint, durable source position authority
wins and bootstrap is not re-executed.

## CDC disposition and delete policy

The SQL envelope extends disposition with:

```text
DISPOSITION CDC_APPLY '(' output_column (',' output_column)* ')'
DELETE HARD
DELETE IGNORE
DELETE SOFT '(' output_column ')'
```

Exactly one `DELETE` clause is mandatory with `CDC_APPLY` and forbidden with dispositions that do
not consume package-native delete effects. There is no delete default.

- `HARD` removes an existing exact-key row and treats a missing row as an idempotent no-op.
- `IGNORE` retains captured package delete effects and deliberately applies no target deletion.
- `SOFT(marker)` requires a non-null Boolean output field. A delete marks an existing row true,
  inserts no missing tombstone, preserves other existing values, and a later complete upsert clears
  the marker to false.

CDC keys follow the same final-output resolution and protection laws as merge keys. The delete
policy, key order, and soft marker are compiled plan, package intent, destination receipt, and
replay identity. PostgreSQL and DuckDB are the first required `cdc_apply` destinations.

## Settlement-unit byte ceiling

`.10x/specs/cdc-log-source-foundation.md` requires `transaction_limit_bytes` to be a mandatory
compiled CDC capability that a project or resource MAY lower but MUST NOT raise above host
authority. This section defines the authoring surface, ratified 2026-08-07.

The declaration is an **optional trailing member of the `EXECUTION DRAIN` clause**, not a driver
argument. It bounds the execution envelope's memory and spill behavior rather than protocol
semantics, so placing it beside the other drain policy members keeps one declaration site instead of
one per adapter:

```sql
EXECUTION DRAIN (
  CHECKPOINT ROWS 100000,
  PACKAGE BYTES 67108864,
  UNTIL DURATION MILLISECONDS 60000,
  WATERMARK DISABLED,
  LATE DATA QUARANTINE,
  SAFE FRONTIER CANONICAL ADMITTED SOURCE POSITION,
  TRANSACTION LIMIT BYTES 268435456
)
```

The member follows the existing purpose-built keyword vocabulary (`PACKAGE BYTES n`), not a
`key => value` map, because the drain members are typed policy rather than an open option bag.

Rules:

- the member is OPTIONAL and MUST be last when present;
- omitting it is distinct from declaring a value: absence means no resource ceiling exists and the
  resolved host spill budget is the sole authority;
- a declared value MUST be a positive integer; zero MUST be rejected at its own token;
- a declared value MUST NOT exceed the resolved host spill budget. Exceeding it is a configuration
  error, not a silent clamp, because raising the bound would let one settlement unit exceed the
  memory envelope the host proved;
- the kernel MUST NOT invent a numeric default at any layer;
- the resolved value is compiled plan authority and travels in `StreamEpochPolicy`.

Only CDC resources bound a settlement unit, which is why the member is optional rather than required
of every finite drain.

## Continuous execution

For a CDC resource, `cdf run` repeatedly executes the existing finite `Drain` epoch:

```text
open/resume -> safe-frontier package -> destination receipt -> checkpoint -> retention collection
-> next epoch
```

The process remains running until interrupted or a terminal error occurs. It does not compile a
resident operator graph and does not introduce a daemon, `resume` command, second checkpoint path,
or ambient in-memory continuation authority. Every epoch can be recovered and audited
independently through ordinary package/receipt/checkpoint artifacts.

One selected run may include several CDC resources and shared-upstream groups under the focused
fan-out contract once activated. Normal progress continuously names source wait/read, transaction
overshoot, package, destination, checkpoint, collection, retry/backoff, and next-epoch states.

## Interruption and failure

- The first interrupt requests graceful stop. Source admission closes at the next protocol-safe
  frontier, the current epoch settles completely, post-checkpoint collection runs, and the command
  exits successfully with the last committed position.
- A second interrupt aborts current work promptly. It does not advance the unfinished epoch's
  checkpoint; durable package/destination state remains available to ordinary recovery.
- A terminal source, contract, package, destination, receipt, checkpoint, or collection error exits
  with the typed diagnostic and last committed frontier. Retryable source/destination failures use
  bounded policy and visible backoff rather than a silent infinite loop.
- `-q`, JSON, and progress suppression keep their existing output contracts.

## Planning and validation

`cdf validate` remains contact-free static project validation. `cdf plan` compiles the explicit
bootstrap/delete/continuous contract without opening a source. Contact-capable prerequisite and
bootstrap checks belong to scoped `cdf doctor`, discovery, and run preflight.

The first run preflight MUST verify protocol prerequisites before destination mutation: MySQL
ROW/FULL/GTID, PostgreSQL publication/slot/materialization requirements, or MongoDB post-images and
change-stream topology as applicable.

## Acceptance scenarios

1. Omitting bootstrap on first-use CDC fails before contact; snapshot/latest each produce explicit
   distinct plan identity and behavior.
2. A committed checkpoint prevents bootstrap from rerunning and resumes from its exact typed
   source position.
3. `CDC_APPLY` without keys or delete policy fails; hard, ignore, and Boolean soft behavior match
   package-native keyed-effect authority.
4. PostgreSQL and DuckDB replay the same CDC package idempotently with the same receipt.
5. Continuous run settles multiple finite epochs and post-epoch retention bounds heavy package
   data.
6. First interrupt settles a safe frontier; second interrupt advances no unfinished checkpoint.
7. Static validate/plan perform no external I/O; doctor/run preflight preserve adapter provenance.

## References

- `.10x/specs/cdc-log-source-foundation.md`
- `.10x/specs/package-keyed-delete-effects.md`
- `.10x/specs/retention-aware-package-collection.md`
- `.10x/specs/sql-project-authoring.md`
- `.10x/specs/runtime-event-spine.md`
