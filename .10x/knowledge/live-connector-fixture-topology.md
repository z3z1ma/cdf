Status: active
Created: 2026-08-07
Updated: 2026-08-07

# Live connector fixture topology

CDF's live database tests and rooflines run against local Docker containers. Until 2026-08-07 that
topology existed only as running containers on a developer machine and in scattered evidence
records — never as institutional memory. This record is that memory.

## Two fixture families, deliberately separate

**Finite-connector fixtures** serve the ordinary source/destination connector wave. They are
long-lived and other tickets depend on their ports and state.

| Container | Image | Server | Host port | Notes |
|---|---|---|---|---|
| `cdf-postgres-source-closure` | `postgres:17-alpine` | 17.10 | 55440 | `POSTGRES_HOST_AUTH_METHOD=trust`, user `cdf` |
| `cdf-mongodb-source` | `mongo` @ `sha256:cf340b1e…` | 8.0.13 | 27018 | `--bind_ip_all`, no auth |
| `cdf-mongodb-auth` | `mongo` @ `sha256:cf340b1e…` | 8.0.13 | 27019 | root `cdf_lifecycle` |
| `cdf-clickhouse-mongodb` | `clickhouse/clickhouse-server` @ `sha256:2173163…` | — | 18124 | `CLICKHOUSE_SKIP_USER_SETUP=1` |

**CDC fixtures** are additive and separate, provisioned by
`.10x/skills/provision-cdc-live-fixtures/SKILL.md` on ports 55441 / 33061 / 27020.

## The finite fixtures cannot be reused for CDC

Verified by direct inspection on 2026-08-07, not inferred:

- `cdf-postgres-source-closure` runs `wal_level = replica`. Logical replication is impossible
  without a restart, and `wal_level` is not runtime-settable.
- `cdf-mongodb-source` reports `NoReplicationEnabled`. Change streams require a replica set.
- No MySQL fixture exists at all, despite MySQL ROW/FULL/GTID being the ratified first CDC proof.

Its `max_replication_slots` and `max_wal_senders` were already 10, so those are not the obstacle —
`wal_level` alone is.

**Never reconfigure or restart a finite fixture to obtain CDC settings.** Provision a separate CDC
container instead. The finite fixtures carry accumulated state that closure evidence for the active
connector children depends on.

## Version pinning is load-bearing, not hygiene

The MongoDB closure roofline (`crates/cdf-benchmarks/src/mongodb_source_roofline.rs`) refuses to
produce a measurement unless the server matches `EXPECTED_SERVER_VERSION` and the endpoint is a
local Docker container whose image it attests via `docker ps` and `docker inspect`. Performance
evidence is only comparable across runs because the server version is fixed.

Consequence: the CDC MongoDB fixture pins the **same digest** as the finite ones
(`sha256:cf340b1e…`, MongoDB 8.0.13). If CDC and finite fixtures drift apart in version, their
evidence stops being comparable and the roofline's attestation stops meaning what it claims.

`postgres:17-alpine` and `mysql:8.4` are moving tags. Digests observed 2026-08-07 are recorded in the
skill; pin by digest whenever a measurement will be compared across time.

## Two test-gating patterns coexist

- **Self-spawned binaries.** `crates/cdf-dest-postgres/src/live_tests.rs` honors
  `TEST_DATABASE_URL`, otherwise starts a throwaway server with `initdb`/`pg_ctl`, otherwise skips
  with an explanatory message. It isolates concurrent tests with a per-process
  `cdf_live_<pid>_<counter>` schema.
- **Attested Docker.** The rooflines require a local Docker endpoint and verify the image before
  measuring.

CDC fixtures follow the Docker pattern because the prerequisites (`wal_level=logical`, binlog
GTID, replica-set initiation) are configuration a self-spawned default server does not have, and
replica-set initiation in particular is materially harder outside a container.

## Operational traps

- `psql -c` wraps multiple statements in one implicit transaction. An error in the last statement
  rolls back every earlier one — a `CREATE PUBLICATION` can report success and then vanish. Issue
  replication DDL as separate `-c` invocations.
- `pg_create_logical_replication_slot` returns `(slot_name, lsn)`. Read `plugin` from
  `pg_replication_slots`.
- MongoDB change streams are tailable cursors. `hasNext()` blocks indefinitely on a drained batch,
  so a `while (cs.hasNext() && n < limit)` loop hangs after the last expected event. Put the counter
  first and always `cs.close()`.
- `changeStreamPreAndPostImages` is a per-collection option, not a server parameter.
- MySQL takes noticeably longer than Postgres or MongoDB to accept connections on first start. Poll.

## Related

- `.10x/skills/provision-cdc-live-fixtures/SKILL.md`
- `.10x/knowledge/performance-evidence-and-regression-triage.md`
- `.10x/skills/run-cdf-ec2-benchmarks/SKILL.md`
- `.10x/decisions/mongodb-srv-topology-egress-residual-risk.md`
