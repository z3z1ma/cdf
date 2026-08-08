---
name: provision-cdc-live-fixtures
description: "Use when running, writing, or debugging CDF tests that need live PostgreSQL logical replication, MySQL binlog, or MongoDB change streams — the CDC prerequisites the ordinary connector fixtures do not provide."
metadata:
  created: 2026-08-07
  updated: 2026-08-08
---

# Provision CDC live fixtures

## Objective

Bring up local PostgreSQL, MySQL, and MongoDB containers that satisfy the change-data-capture
prerequisites, and prove each prerequisite is actually on before any CDC test trusts it.

**Read this first:** the pre-existing finite-connector fixtures do **not** satisfy these
prerequisites and must not be reused for CDC. Verified 2026-08-07: `cdf-postgres-source-closure`
runs `wal_level = replica`, and `cdf-mongodb-source` reports `NoReplicationEnabled`. There is no
MySQL fixture at all. See `.10x/knowledge/live-connector-fixture-topology.md`.

These containers are **additive**. Never reconfigure or restart the finite-connector fixtures to
obtain CDC settings — other tickets depend on their current state and ports.

## Prerequisites

- Docker running (`docker version` succeeds).
- Host ports 55441, 33061, and 27020 free. The finite-connector fixtures already hold 55440
  (Postgres), 27018/27019 (MongoDB), and 18124 (ClickHouse).

Image digests observed 2026-08-07 — pin by digest when reproducibility matters, because
`postgres:17-alpine` and `mysql:8.4` are moving tags:

| Fixture | Tag | Server | Digest |
|---|---|---|---|
| `cdf-cdc-postgres` | `postgres:17-alpine` | 17.10 | `sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193` |
| `cdf-cdc-mysql` | `mysql:8.4` | 8.4.11 | `sha256:b3b90af2a6552ae30c266fdb7d5dd55f3afb72404bb78d37fe8a23eb857fd3fb` |
| `cdf-cdc-mongo` | `mongo` (8.0) | 8.0.13 | `sha256:cf340b1e5283843c63eb12999922f20c463ae31285f746d30f05dcc21cd1d47c` |

The MongoDB digest is deliberately the same one the finite MongoDB fixtures and the closure roofline
already pin (`EXPECTED_SERVER_VERSION` in `crates/cdf-benchmarks/src/mongodb_source_roofline.rs`).
Keep them identical so CDC and finite evidence describe the same server.

## Procedure

### PostgreSQL — logical replication

```bash
docker run -d --name cdf-cdc-postgres \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_USER=cdf -p 55441:5432 \
  postgres:17-alpine \
  -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10
```

Create the replication objects as **separate** `psql -c` invocations. `psql -c` wraps multiple
statements in one implicit transaction, so a syntax error in the last statement silently rolls back
the table and publication created by the earlier ones:

```bash
docker exec cdf-cdc-postgres psql -U cdf -q -c "create table if not exists t(id int primary key, v text);"
docker exec cdf-cdc-postgres psql -U cdf -q -c "alter table t replica identity full;"
docker exec cdf-cdc-postgres psql -U cdf -q -c "create publication cdf_pub for table t;"
docker exec cdf-cdc-postgres psql -U cdf -q -c "select pg_create_logical_replication_slot('cdf_slot','pgoutput');"
```

`pg_create_logical_replication_slot` returns `(slot_name, lsn)` — there is no `plugin` column on the
function result. Read `plugin` from `pg_replication_slots` instead.

### MySQL — binlog ROW/FULL/GTID

```bash
docker run -d --name cdf-cdc-mysql \
  -e MYSQL_ROOT_PASSWORD=cdf-cdc-password -e MYSQL_DATABASE=cdf -p 33061:3306 \
  mysql:8.4 \
  --server-id=1 --log-bin=binlog --binlog-format=ROW --binlog-row-image=FULL \
  --gtid-mode=ON --enforce-gtid-consistency=ON
```

MySQL takes noticeably longer than the others to accept connections on first start. Poll rather than
sleep a fixed interval.

### MongoDB — replica set and pre/post images

Change streams require a replica set; a standalone `mongod` returns `NoReplicationEnabled`.

```bash
docker run -d --name cdf-cdc-mongo -p 27020:27020 \
  mongo@sha256:cf340b1e5283843c63eb12999922f20c463ae31285f746d30f05dcc21cd1d47c \
  --replSet rs0 --bind_ip_all --port 27020

docker exec cdf-cdc-mongo mongosh --port 27020 --quiet --eval \
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27020"}]})'
```

Pre- and post-images are a **per-collection** option, not a server setting:

```bash
docker exec cdf-cdc-mongo mongosh --port 27020 --quiet --eval \
  'db = db.getSiblingDB("cdf");
   if (!db.getCollectionNames().includes("t")) {
     db.createCollection("t", { changeStreamPreAndPostImages: { enabled: true } });
   } else {
     db.runCommand({collMod:"t", changeStreamPreAndPostImages:{enabled:true}});
   }'
```

## Validation

Each prerequisite must be *proven*, not inferred from the flag that was passed. Run all three.

**PostgreSQL** — expect `logical`, a `cdf_pub` row with `tables = 1`, `replident = f`, and a
`logical` slot on `pgoutput`:

```bash
docker exec cdf-cdc-postgres psql -U cdf -c "select setting from pg_settings where name='wal_level';"
docker exec cdf-cdc-postgres psql -U cdf -c "select pubname, (select count(*) from pg_publication_tables x where x.pubname=p.pubname) as tables from pg_publication p;"
docker exec cdf-cdc-postgres psql -U cdf -c "select relname, relreplident::text as replident from pg_class where relname='t';"
docker exec cdf-cdc-postgres psql -U cdf -c "select slot_name, plugin, slot_type, active from pg_replication_slots;"
```

**MySQL** — expect `log_bin = ON`, `binlog_format = ROW`, `binlog_row_image = FULL`,
`gtid_mode = ON`, `enforce_gtid_consistency = ON`:

```bash
docker exec cdf-cdc-mysql mysql -uroot -pcdf-cdc-password -N -B -e \
  "select concat(variable_name,' = ',variable_value) from performance_schema.global_variables
   where variable_name in ('log_bin','binlog_format','binlog_row_image','gtid_mode','enforce_gtid_consistency','server_id');"
```

**MongoDB** — expect two events with resume tokens present and `before="a"` on the update, proving
both change streams and pre-images:

First prove the replica-set address matches the host-published port. Expect both values to name
`27020`; advertising the container-only `27017` makes a host client reconnect to the wrong port.

```bash
docker inspect cdf-cdc-mongo --format '{{(index (index .NetworkSettings.Ports "27020/tcp") 0).HostPort}}'
docker exec cdf-cdc-mongo mongosh --port 27020 --quiet --eval 'print(rs.conf().members[0].host)'
```

```bash
docker exec cdf-cdc-mongo mongosh --port 27020 --quiet --eval '
db = db.getSiblingDB("cdf");
db.t.deleteOne({_id: 99});
const cs = db.t.watch([], { fullDocument: "required", fullDocumentBeforeChange: "required" });
db.t.insertOne({_id: 99, v: "a"});
db.t.updateOne({_id: 99}, {$set: {v: "b"}});
let n = 0;
let after = null;
let before = null;
while (n < 2 && cs.hasNext()) { const e = cs.next(); n++;
  if (e.operationType === "update") {
    after = e.fullDocument ? e.fullDocument.v : null;
    before = e.fullDocumentBeforeChange ? e.fullDocumentBeforeChange.v : null;
  }
  print("event " + n + ": op=" + e.operationType
      + " resumeToken=" + (e._id._data ? "present" : "MISSING")
      + " before=" + (e.fullDocumentBeforeChange ? JSON.stringify(e.fullDocumentBeforeChange.v) : "n/a")
      + " after=" + (e.fullDocument ? JSON.stringify(e.fullDocument.v) : "n/a")); }
cs.close();
if (n !== 2 || before !== "a" || after !== "b") throw new Error("change stream did not return the required update images");
print("events observed = " + n + ", update before=" + before + ", update after=" + after);'
```

Write the loop as `while (n < counter_limit && cs.hasNext())`, never `cs.hasNext() && n < limit`. A
change stream is a tailable cursor: `hasNext()` blocks indefinitely once the batch is drained, so
evaluating it before the counter hangs the shell after the final expected event. Always `cs.close()`.

## Teardown

```bash
docker rm -f cdf-cdc-postgres cdf-cdc-mysql cdf-cdc-mongo
```

Removing these does not affect the finite-connector fixtures. If a `mongosh` change-stream cursor is
left hanging, `docker exec cdf-cdc-mongo pkill mongosh` clears it.
