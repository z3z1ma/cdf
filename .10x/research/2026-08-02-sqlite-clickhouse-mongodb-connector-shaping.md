Status: done
Created: 2026-08-02
Updated: 2026-08-02

# SQLite, ClickHouse, and MongoDB connector shaping

## Question

What source and destination contracts, protocol choices, and performance boundaries should govern first-party SQLite, ClickHouse, and MongoDB connectors without leaking connector-specific lifecycle into CDF's shared runtime?

## Sources and Methods

- Inspected CDF's source and destination extension invariant, driver/session concurrency canon, non-file checkpoint semantics, destination receipt contract, connector-authoring path, built-in catalog, Postgres source, and Postgres/DuckDB destination implementations at revision `5a0169e5`.
- Confirmed that `cdf-state-sqlite` owns CDF state and ledger persistence only. A user-data SQLite source or destination therefore requires separate `cdf-source-sqlite` and `cdf-dest-sqlite` leaf crates.
- Read current official SQLite documentation for [WAL](https://www.sqlite.org/wal.html), [isolation](https://www.sqlite.org/isolation.html), [transactions](https://www.sqlite.org/lang_transaction.html), and the [backup API](https://www.sqlite.org/backup.html).
- Read the current official [`clickhouse-rs`](https://github.com/ClickHouse/clickhouse-rs) client documentation and its [`clickhouse-ext-arrow`](https://github.com/ClickHouse/clickhouse-rs/tree/main/ext-arrow) Arrow 58 compatibility matrix.
- Read current MongoDB Rust driver documentation for [bulk writes](https://www.mongodb.com/docs/drivers/rust/current/crud/bulk/), [inserts](https://www.mongodb.com/docs/drivers/rust/current/crud/insert/), [queries](https://www.mongodb.com/docs/drivers/rust/current/crud/query/retrieve/), [raw BSON](https://www.mongodb.com/docs/drivers/rust/current/data-formats/bson/), [connection pools](https://www.mongodb.com/docs/drivers/rust/current/connect/connection-options/connection-pools/), and [change streams](https://www.mongodb.com/docs/drivers/rust/current/monitoring-logging/change-streams/).

## Findings

### Existing CDF boundary

- Each connector direction is a dependency-isolated leaf crate. First-party construction belongs only in `cdf-builtin-drivers`; conformance is data-driven; shared orchestration must not branch on a concrete connector identity.
- Sources already have a finite snapshot/cursor model with durable numeric, timestamp, and date window-close positions. Resident log CDC is a different lifecycle and disposition family; `cdc_apply` is explicitly outside the current destination MVP.
- Destinations must plan without writing, advertise dispositions truthfully, use the finalized package hash as the idempotency token, produce independently verifiable receipts, and leave checkpoint mutation to generic orchestration.
- SQLite, ClickHouse, and MongoDB currently have no active connector spec or executable ticket. MongoDB is named in the product objective as a planned source, but no mapping, CDC, or destination semantics are established.

### SQLite

- SQLite permits concurrent readers but serializes writes. WAL improves reader/writer concurrency and makes writes sequential, while still allowing only one writer. The correct destination architecture is therefore one prepared-statement writer inside one explicit package transaction, not an artificial parallel writer pool.
- A read transaction provides a stable snapshot. One source connection can stream that snapshot into accounted Arrow batches without a copy or backup step. Parallel connections would not automatically share one snapshot and are not justified without a separately proven snapshot-transfer protocol.
- WAL is persistent database state, does not work on network filesystems, and changes operational behavior for other database users. A connector must not silently force an existing database into WAL mode. It may use the existing mode, expose an explicit opt-in, or recommend WAL operationally.
- The workspace's bundled `libsqlite3-sys 0.38.1` contains SQLite 3.53.2, which is newer than the documented WAL-reset corruption fix in 3.51.3. No dependency upgrade is required for that issue.
- `append`, atomic `replace`, and keyed `merge` are implementable inside one SQLite transaction. Package-token replay and receipt verification require connector-owned destination mirror tables distinct from CDF's state database schema.

### ClickHouse

- The official Rust client uses HTTP transport, connection reuse, compression, and streaming inserts. Native TCP remains a future client feature, so a private protocol implementation would add risk without evidence.
- `clickhouse-ext-arrow 0.1` is explicitly compatible with `clickhouse 0.15` and Arrow 58, matching CDF's Arrow 58.3.0 dependency. Its `fetch_arrow()` and `insert_arrow()` paths avoid a row-by-row Serde bridge and are the retained source/destination data plane.
- ClickHouse recommends large insert batches; useful starting targets are at least 1,000 rows and generally 10,000-100,000 rows, bounded additionally by bytes and CDF memory authority. Compression, pooled clients, and bounded concurrent partitions can saturate network/CPU without an unbounded private executor.
- Async insert acknowledgement is safe only with `wait_for_async_insert=1`; capability/version detection must fail closed or use synchronous inserts. It must not be enabled as a blind performance flag.
- Ordinary ClickHouse tables do not provide Postgres-like transactional merge semantics. `ReplacingMergeTree` deduplication is eventual and engine-specific, and cannot truthfully implement the generic `merge` contract without a CDF-managed table/version/key contract and independently executable verification.
- Atomic `replace` depends on exact table-engine, database-engine, DDL, and cluster topology behavior. It must be proven for a stated target model rather than inferred from a local table rename example.

### MongoDB

- The official asynchronous Rust driver owns topology monitoring and connection pools. CDF should inject bounded task concurrency while reusing one client/pool; it should not create a client per batch or an independent executor.
- Finite extraction can stream a projected/filter-pushed cursor and decode raw BSON directly into accounted Arrow builders. Batch sizing must respect both useful decoded bytes and MongoDB's wire batch behavior.
- `insert_many` provides the broad append bulk path. Client-level mixed-model `bulk_write` requires MongoDB Server 8.0 or later and Rust driver 3.0 or later; selecting it establishes a real product compatibility floor.
- Change streams require a replica set or sharded deployment, resume-token persistence, ordered update/delete semantics, and optional pre/post-image policy. They are a resident CDC protocol, not an incremental flavor of a finite `find` cursor.
- Every MongoDB document requires a unique `_id`. Letting the driver generate random ObjectIds makes package replay non-idempotent. CDF must either require an input `_id`, derive a deterministic `_id` from stable package/row identity, or define another explicit key contract.
- Arrow-to-BSON handling is semantic: nested structs/lists can remain structured BSON, while decimals, unsigned integers, timestamps/timezones, binary values, and unsupported Arrow types require an explicit fidelity sheet and fail-closed policy.

### Performance acceptance

- “Fastest” is not established by using bulk APIs alone. Each connector needs release-mode throughput cells against the same official client/library used directly on the same host and dataset, with useful rows/bytes, wall time, CPU, and destination commit latency separated.
- SQLite is disk/serialization bound and has one writer; saturation means keeping that writer fed with prepared batches while parallelizing only upstream work. ClickHouse should use ArrowStream, compression, connection reuse, byte-aware large blocks, and bounded partition concurrency. MongoDB should use raw BSON reads, batched writes, connection-pool reuse, byte-aware batches, and bounded in-flight operations.
- Local container/file cells are deterministic conformance and regression gates. Remote ClickHouse Cloud and MongoDB Atlas cells are required to support claims about network saturation, but credentials, cost, and external writes require separate explicit authorization.
- No current evidence establishes one universal throughput percentage against the raw-client roofline for these six adapters. A hard ratio must be ratified or established by measured baselines before it becomes acceptance authority.

## Conclusions

1. Implement in the requested order: SQLite source/destination, ClickHouse source/destination, then MongoDB source/destination. Keep each direction in its own leaf crate and enroll only through `cdf-builtin-drivers`.
2. Reuse finite snapshot plus bounded numeric/timestamp/date cursor semantics for the first source tranche. MongoDB change streams should be a separate CDC tranche unless the user explicitly includes resident CDC now; SQLite and ClickHouse must not pretend to offer equivalent CDC.
3. Use the native high-throughput path for each system: prepared transaction batches for SQLite, official ArrowStream for ClickHouse, and raw BSON plus pooled batched operations for MongoDB.
4. Do not claim unsupported dispositions. SQLite can implement append/replace/merge. ClickHouse should begin with append unless atomic replace is limited to a proven CDF-managed target contract; merge requires a separately ratified ReplacingMergeTree contract. MongoDB append/replace/merge requires an explicit deterministic `_id`/merge-key mapping.
5. Benchmark against direct-library baselines and record roofline evidence. External cloud benchmarks remain a separately authorized write/cost action.

The user ratified all five recommendations on 2026-08-02 by approving the recommended source,
destination, and performance options. The governing connector specifications and executable
ticket graph own implementation from this point forward.

## Limits

- No server was started and no external data was read or written. The protocol findings come from current official documentation and repository authority, not connector measurements.
- Exact source query/filter configuration, destination target ownership, type mappings, disposition sets, compatibility floors, and throughput thresholds are not yet ratified.
- ClickHouse atomic replace and MongoDB deterministic document identity are design blockers, not implementation details. Tests cannot safely choose those semantics on the user's behalf.
