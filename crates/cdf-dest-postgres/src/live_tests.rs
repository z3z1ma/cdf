use std::{
    env,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_conformance::resource::{
    PredicateExpectation, ResourceConformanceCase, ResourceExecutionConformanceCase,
    assert_queryable_resource_conformance, assert_resource_stream_execution_conformance,
};
use cdf_kernel::{
    CheckpointId, ContractRef, CursorOrderingClaim, CursorPosition, CursorSpec, CursorValue,
    PartitionId, PipelineId, PredicateId, QueryableResource, ResourceDescriptor, ResourceId,
    ResourceStream, ScanPredicate, ScanRequest, SchemaSource, ScopeKey, SegmentId, SortDirection,
    SourcePosition, TrustLevel,
};
use cdf_package::{
    PackageBuilder, PackageManifest, PackageReader, QuarantineObservedValue, QuarantineRecord,
};
use futures_util::StreamExt;
use postgres::{Client, NoTls};
use tempfile::TempDir;

use super::*;
use crate::{ddl::target_migrations, identifiers::quote_identifier_unchecked};

static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);
static LOCAL_POSTGRES_START: Mutex<()> = Mutex::new(());

struct LivePostgres {
    url: String,
    schema: String,
    _server: Option<LocalPostgres>,
}

struct LocalPostgres {
    data_dir: TempDir,
    _socket_dir: TempDir,
    pg_ctl: PathBuf,
}

impl LivePostgres {
    fn start() -> Option<Self> {
        let (url, server) = match env::var("TEST_DATABASE_URL") {
            Ok(url) if !url.trim().is_empty() => (url, None),
            _ => {
                let Some(server) = LocalPostgres::start() else {
                    eprintln!(
                        "skipping live Postgres test: set TEST_DATABASE_URL or install postgres/initdb/pg_ctl"
                    );
                    return None;
                };
                (server.url(), Some(server))
            }
        };
        let schema = format!(
            "cdf_live_{}_{}",
            std::process::id(),
            SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let mut client = Client::connect(&url, NoTls).unwrap();
        client
            .batch_execute(&format!(
                "CREATE SCHEMA {}",
                quote_identifier(&schema).unwrap()
            ))
            .unwrap();
        Some(Self {
            url,
            schema,
            _server: server,
        })
    }

    fn destination(&self) -> PostgresDestination {
        PostgresDestination::connect(self.url.clone()).unwrap()
    }

    fn client(&self) -> Client {
        let mut client = Client::connect(&self.url, NoTls).unwrap();
        client
            .batch_execute(&format!(
                "SET search_path = {}, public",
                quote_identifier(&self.schema).unwrap()
            ))
            .unwrap();
        client
    }

    fn target(&self, table: &str) -> PostgresTarget {
        PostgresTarget::new(Some(&self.schema), table).unwrap()
    }
}

impl Drop for LivePostgres {
    fn drop(&mut self) {
        if let Ok(mut client) = Client::connect(&self.url, NoTls) {
            let _ = client.batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_identifier(&self.schema).unwrap()
            ));
        }
    }
}

impl LocalPostgres {
    fn start() -> Option<Self> {
        let _guard = LOCAL_POSTGRES_START.lock().unwrap();
        let initdb = find_binary("initdb")?;
        let pg_ctl = find_binary("pg_ctl")?;
        let data_dir = tempfile::tempdir().unwrap();
        let socket_dir = tempfile::tempdir().unwrap();
        let port = free_port();

        let init_status = Command::new(&initdb)
            .args(["-D", data_dir.path().to_str().unwrap()])
            .args(["-A", "trust"])
            .args(["-U", "cdf"])
            .arg("--no-sync")
            .status()
            .unwrap();
        assert!(init_status.success(), "initdb failed");

        let options = format!("-h 127.0.0.1 -p {port} -k {}", socket_dir.path().display());
        let log_path = data_dir.path().join("postgres.log");
        let start_status = Command::new(&pg_ctl)
            .args(["-D", data_dir.path().to_str().unwrap()])
            .args(["-l", log_path.to_str().unwrap()])
            .args(["-o", &options])
            .args(["-w", "start"])
            .status()
            .unwrap();
        assert!(start_status.success(), "pg_ctl start failed");

        Some(Self {
            data_dir,
            _socket_dir: socket_dir,
            pg_ctl,
        })
    }

    fn url(&self) -> String {
        let port = std::fs::read_to_string(self.data_dir.path().join("postmaster.pid"))
            .unwrap()
            .lines()
            .nth(3)
            .unwrap()
            .to_owned();
        format!("postgresql://cdf@127.0.0.1:{port}/postgres")
    }
}

impl Drop for LocalPostgres {
    fn drop(&mut self) {
        let _ = Command::new(&self.pg_ctl)
            .args(["-D", self.data_dir.path().to_str().unwrap()])
            .args(["-m", "fast"])
            .args(["-w", "stop"])
            .status();
    }
}

fn find_binary(name: &str) -> Option<PathBuf> {
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .map(|path| path.join(name))
            .find(|candidate| candidate.is_file())
    })
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn batch(rows: &[(i64, Option<&str>)]) -> RecordBatch {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef =
        std::sync::Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id)));
    let name: ArrayRef =
        std::sync::Arc::new(StringArray::from_iter(rows.iter().map(|(_, name)| *name)));
    RecordBatch::try_new(schema, vec![id, name]).unwrap()
}

fn build_package(
    root: &Path,
    package_id: &str,
    segments: Vec<(&str, RecordBatch)>,
) -> PackageManifest {
    let mut builder = PackageBuilder::create(root, package_id).unwrap();
    for (segment_id, batch) in segments {
        builder
            .write_segment(SegmentId::new(segment_id).unwrap(), &[batch])
            .unwrap();
    }
    builder.finish().unwrap()
}

fn columns() -> Vec<PostgresColumn> {
    vec![
        PostgresColumn::new("id", "BIGINT", false).unwrap(),
        PostgresColumn::new("name", "TEXT", true).unwrap(),
    ]
}

fn decimal_columns() -> Vec<PostgresColumn> {
    vec![
        PostgresColumn::new("id", "BIGINT", false).unwrap(),
        PostgresColumn::new("amount", "NUMERIC(12,2)", true).unwrap(),
    ]
}

fn decimal_batch(rows: &[(i64, Option<i128>)]) -> RecordBatch {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Decimal128(12, 2), true),
    ]));
    let id: ArrayRef =
        std::sync::Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id)));
    let amount: ArrayRef = std::sync::Arc::new(
        Decimal128Array::from(rows.iter().map(|(_, amount)| *amount).collect::<Vec<_>>())
            .with_precision_and_scale(12, 2)
            .unwrap(),
    );
    RecordBatch::try_new(schema, vec![id, amount]).unwrap()
}

fn state_segments(manifest: &PackageManifest) -> Vec<StateSegment> {
    manifest
        .identity
        .segments
        .iter()
        .map(|segment| StateSegment {
            segment_id: segment.segment_id.clone(),
            scope: scope(),
            output_position: position(10),
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        })
        .collect()
}

fn state_delta(manifest: &PackageManifest, checkpoint: &str) -> StateDelta {
    StateDelta {
        checkpoint_id: CheckpointId::new(checkpoint).unwrap(),
        pipeline_id: PipelineId::new("pipe-live").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope: scope(),
        state_version: 1,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: position(10),
        package_hash: PackageHash::new(manifest.package_hash.clone()).unwrap(),
        schema_hash: schema_hash(),
        segments: state_segments(manifest),
    }
}

fn scope() -> ScopeKey {
    ScopeKey::Partition {
        partition_id: PartitionId::new("p0").unwrap(),
    }
}

fn position(value: i64) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: "updated_at".to_owned(),
        value: CursorValue::I64(value),
    })
}

fn schema_hash() -> SchemaHash {
    SchemaHash::new("sha256:live-schema").unwrap()
}

fn plan(
    env: &LivePostgres,
    table: &str,
    manifest: &PackageManifest,
    disposition: WriteDisposition,
    dedup: MergeDedupPolicy,
    state_delta: Option<StateDelta>,
) -> PostgresLoadPlan {
    plan_with_columns(
        env,
        table,
        manifest,
        disposition,
        dedup,
        state_delta,
        columns(),
    )
}

fn plan_with_columns(
    env: &LivePostgres,
    table: &str,
    manifest: &PackageManifest,
    disposition: WriteDisposition,
    dedup: MergeDedupPolicy,
    state_delta: Option<StateDelta>,
    columns: Vec<PostgresColumn>,
) -> PostgresLoadPlan {
    env.destination()
        .plan_load(load_input(
            env,
            table,
            manifest,
            disposition,
            dedup,
            state_delta,
            columns,
        ))
        .unwrap()
}

fn load_input(
    env: &LivePostgres,
    table: &str,
    manifest: &PackageManifest,
    disposition: WriteDisposition,
    dedup: MergeDedupPolicy,
    state_delta: Option<StateDelta>,
    columns: Vec<PostgresColumn>,
) -> PostgresLoadPlanInput {
    PostgresLoadPlanInput {
        package_hash: PackageHash::new(manifest.package_hash.clone()).unwrap(),
        idempotency_token: IdempotencyToken::new(manifest.package_hash.clone()).unwrap(),
        target: env.target(table),
        disposition,
        schema_hash: schema_hash(),
        segments: state_segments(manifest),
        columns,
        merge_keys: vec![PostgresIdentifier::user("id").unwrap()],
        dedup,
        existing_table: None,
        resource_id: Some(ResourceId::new("orders").unwrap()),
        state_delta,
    }
}

fn commit(env: &LivePostgres, package_dir: &Path, plan: PostgresLoadPlan) -> PostgresCommitOutcome {
    env.destination()
        .commit_package(PostgresCommitRequest {
            package_dir: package_dir.to_path_buf(),
            plan,
        })
        .unwrap()
}

fn commit_request(manifest: &PackageManifest, plan: &PostgresLoadPlan) -> DestinationCommitRequest {
    DestinationCommitRequest {
        package_hash: PackageHash::new(manifest.package_hash.clone()).unwrap(),
        target: plan.kernel.target.clone(),
        disposition: plan.kernel.disposition.clone(),
        segments: state_segments(manifest),
        idempotency_token: IdempotencyToken::new(manifest.package_hash.clone()).unwrap(),
    }
}

fn session_commit(
    env: &LivePostgres,
    package_dir: &Path,
    manifest: &PackageManifest,
    plan: PostgresLoadPlan,
) -> Receipt {
    let request = commit_request(manifest, &plan);
    let kernel_plan = plan.kernel.clone();
    let destination = env
        .destination()
        .with_commit_request(PostgresCommitRequest {
            package_dir: package_dir.to_path_buf(),
            plan,
        });
    let mut session = destination.begin(request, kernel_plan).unwrap();
    session.apply_migrations().unwrap();
    let segments = PackageReader::open(package_dir)
        .unwrap()
        .read_commit_segments(&state_segments(manifest))
        .unwrap();
    for segment in segments {
        let ack = session.write_segment(segment).unwrap();
        assert!(manifest.identity.segments.iter().any(|entry| {
            ack.segment_id == entry.segment_id && ack.row_count == entry.row_count
        }));
    }
    session.finalize().unwrap()
}

#[test]
fn live_postgres_catalog_discovery_reads_empty_table_metadata_only() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let target = env.target("catalog_discovery_types");
    let mut client = env.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"VendorID\" INTEGER NOT NULL,
                \"is_active\" BOOLEAN,
                \"ratio\" DOUBLE PRECISION NOT NULL,
                \"name\" TEXT,
                \"service_date\" DATE NOT NULL,
                \"created_at\" TIMESTAMP WITHOUT TIME ZONE,
                \"updated_at\" TIMESTAMP WITH TIME ZONE,
                \"request_uuid\" UUID
            )",
            target.sql()
        ))
        .unwrap();

    let discovery = discover_postgres_table_catalog_schema(
        &env.url,
        &ResourceId::new("warehouse.orders").unwrap(),
        &target,
    )
    .unwrap();

    assert_eq!(discovery.source_identity["source_kind"], "sql");
    assert_eq!(discovery.source_identity["dialect"], "postgres");
    assert_eq!(discovery.source_identity["table"], target.display_name());
    assert!(!format!("{discovery:?}").contains(&env.url));
    let schema = discovery.schema;
    let fields = schema.fields();
    assert_eq!(fields.len(), 8);
    assert_eq!(fields[0].name(), "VendorID");
    assert_eq!(fields[0].data_type(), &DataType::Int64);
    assert!(!fields[0].is_nullable());
    assert_eq!(fields[0].metadata()["cdf:physical_type"], "integer");
    assert_eq!(fields[1].data_type(), &DataType::Boolean);
    assert!(fields[1].is_nullable());
    assert_eq!(fields[2].data_type(), &DataType::Float64);
    assert_eq!(fields[3].data_type(), &DataType::Utf8);
    assert_eq!(fields[4].data_type(), &DataType::Date32);
    assert_eq!(
        fields[5].data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        fields[6].data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(fields[7].data_type(), &DataType::Utf8);
}

#[test]
fn live_postgres_table_resource_executes_scan_and_cursor_conformance() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let target = env.target("source_orders");
    let mut client = env.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"id\" BIGINT NOT NULL,
                \"name\" TEXT,
                \"updated_at\" BIGINT NOT NULL,
                \"active\" BOOLEAN NOT NULL,
                \"score\" DOUBLE PRECISION,
                \"created_on\" DATE,
                \"touched_at\" TIMESTAMPTZ
            );
            INSERT INTO {} (\"id\", \"name\", \"updated_at\", \"active\", \"score\", \"created_on\", \"touched_at\") VALUES
                (1, 'ada', 10, true, 1.5, DATE '2026-07-01', TIMESTAMPTZ '2026-07-01T00:00:00Z'),
                (2, 'grace', 20, false, NULL, DATE '2026-07-02', TIMESTAMPTZ '2026-07-02T01:00:00Z'),
                (3, 'katherine', 30, true, 3.25, DATE '2026-07-03', TIMESTAMPTZ '2026-07-03T02:30:00Z')",
            target.sql(),
            target.sql()
        ))
        .unwrap();

    let descriptor = postgres_source_descriptor();
    let schema = postgres_source_schema();
    let resource =
        PostgresTableResource::new(env.url.clone(), descriptor.clone(), schema.clone(), target)
            .unwrap();
    let predicate_id = PredicateId::new("updated-at").unwrap();
    let request = ScanRequest {
        resource_id: descriptor.resource_id.clone(),
        projection: Some(vec![
            "id".to_owned(),
            "name".to_owned(),
            "updated_at".to_owned(),
            "active".to_owned(),
            "score".to_owned(),
            "created_on".to_owned(),
            "touched_at".to_owned(),
        ]),
        filters: vec![ScanPredicate {
            predicate_id: predicate_id.clone(),
            expression: "updated_at >= 20".to_owned(),
        }],
        limit: Some(10),
        order_by: vec![cdf_kernel::OrderBy {
            field: "updated_at".to_owned(),
            direction: SortDirection::Asc,
        }],
        scope: ScopeKey::Resource,
    };

    assert_queryable_resource_conformance(
        &resource,
        [ResourceConformanceCase::new(request.clone())
            .with_expected_predicates([PredicateExpectation::exact(predicate_id)])],
    );
    let partition = PartitionId::new("sql").unwrap();
    let execution_case = ResourceExecutionConformanceCase::new(
        request.clone(),
        postgres_source_schema_hash(),
        [partition.clone()],
        2,
    )
    .with_expected_partition_rows([(partition, 2)]);
    futures_executor::block_on(assert_resource_stream_execution_conformance(
        &resource,
        [execution_case],
    ));

    let plan = resource.negotiate(&request).unwrap();
    let batches = drain_source_batches(
        futures_executor::block_on(resource.open(plan.partitions[0].clone())).unwrap(),
    );
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].header.observed_schema_hash,
        postgres_source_schema_hash()
    );
    let batch = batches[0].record_batch().unwrap();
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec![
            "id",
            "name",
            "updated_at",
            "active",
            "score",
            "created_on",
            "touched_at",
        ]
    );
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[2, 3]);
    let active = batch
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(!active.value(0));
    assert!(active.value(1));
    let score = batch
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(score.is_null(0));
    assert_eq!(score.value(1), 3.25);
    let created_on = batch
        .column(5)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert!(created_on.value(1) > created_on.value(0));
    let touched_at = batch
        .column(6)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert!(touched_at.value(1) > touched_at.value(0));
    let Some(SourcePosition::Cursor(cursor)) = &batches[0].header.source_position else {
        panic!("expected cursor source position");
    };
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(30));
}

#[test]
fn live_begin_session_returns_verifiable_receipt_and_preserves_duplicate_noop() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let manifest = build_package(
        package_dir.path(),
        "pkg-live-session-append",
        vec![("seg-000001", batch(&[(1, Some("ada")), (2, Some("grace"))]))],
    );
    let plan = plan(
        &env,
        "orders_session_append",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        Some(state_delta(&manifest, "chk-live-session-append")),
    );

    let receipt = session_commit(&env, package_dir.path(), &manifest, plan.clone());
    assert_eq!(receipt.counts.rows_written, 2);
    assert!(env.destination().verify_receipt(&receipt).unwrap().verified);
    assert_eq!(
        cdf_package::PackageReader::open(package_dir.path())
            .unwrap()
            .receipts()
            .unwrap()
            .len(),
        1
    );

    let duplicate = session_commit(&env, package_dir.path(), &manifest, plan);
    assert_eq!(duplicate.receipt_id, receipt.receipt_id);
    assert_eq!(
        cdf_package::PackageReader::open(package_dir.path())
            .unwrap()
            .receipts()
            .unwrap()
            .len(),
        1
    );

    let mut client = env.client();
    let target_count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                env.target("orders_session_append").sql()
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(target_count, 2);
}

#[test]
fn live_begin_session_abort_rolls_back_system_migrations() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let manifest = build_package(
        package_dir.path(),
        "pkg-live-session-abort",
        vec![("seg-000001", batch(&[(1, Some("ada"))]))],
    );
    let plan = plan(
        &env,
        "orders_session_abort",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        None,
    );
    let request = commit_request(&manifest, &plan);
    let kernel_plan = plan.kernel.clone();
    let destination = env
        .destination()
        .with_commit_request(PostgresCommitRequest {
            package_dir: package_dir.path().to_path_buf(),
            plan,
        });
    let mut session = destination.begin(request, kernel_plan).unwrap();
    session.apply_migrations().unwrap();
    session.abort().unwrap();

    let mut client = env.client();
    let loads_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = '_cdf_loads')",
            &[],
        )
        .unwrap()
        .get(0);
    assert!(!loads_exists);
}

fn postgres_source_descriptor() -> ResourceDescriptor {
    ResourceDescriptor {
        resource_id: ResourceId::new("warehouse.source_orders").unwrap(),
        schema_source: SchemaSource::Declared {
            schema_hash: postgres_source_schema_hash(),
            source: "test:postgres-source-live".to_owned(),
        },
        primary_key: vec!["id".to_owned()],
        merge_key: vec!["id".to_owned()],
        cursor: Some(CursorSpec {
            field: "updated_at".to_owned(),
            ordering: CursorOrderingClaim::Exact,
            lag_tolerance_ms: 0,
        }),
        write_disposition: WriteDisposition::Merge,
        contract: Some(ContractRef::new("orders").unwrap()),
        state_scope: ScopeKey::Resource,
        freshness: None,
        trust_level: TrustLevel::Governed,
    }
}

fn postgres_source_schema() -> std::sync::Arc<Schema> {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("updated_at", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("score", DataType::Float64, true),
        Field::new("created_on", DataType::Date32, true),
        Field::new(
            "touched_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

fn postgres_source_schema_hash() -> SchemaHash {
    SchemaHash::new("sha256:postgres-source-live-schema").unwrap()
}

fn drain_source_batches(mut stream: cdf_kernel::BatchStream) -> Vec<cdf_kernel::Batch> {
    futures_executor::block_on(async move {
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        batches
    })
}

#[test]
fn live_append_duplicate_receipt_verification_and_state_mirror() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let manifest = build_package(
        package_dir.path(),
        "pkg-live-append",
        vec![
            ("seg-000001", batch(&[(1, Some("ada")), (2, Some("grace"))])),
            ("seg-000002", batch(&[(3, None)])),
        ],
    );
    let plan = plan(
        &env,
        "orders_append",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        Some(state_delta(&manifest, "chk-live-append")),
    );

    let outcome = commit(&env, package_dir.path(), plan.clone());
    assert!(!outcome.duplicate);
    assert!(outcome.package_receipt_recorded);
    assert_eq!(outcome.receipt.counts.rows_written, 3);
    assert_eq!(outcome.receipt.segment_acks.len(), 2);
    assert_eq!(outcome.receipt.schema_hash, schema_hash());
    assert_eq!(outcome.receipt.verify.kind, "postgres_sql");
    assert!(
        outcome
            .receipt
            .transaction
            .as_ref()
            .unwrap()
            .values
            .get("xid")
            .unwrap()
            .parse::<u64>()
            .is_ok()
    );
    assert!(
        env.destination()
            .verify_receipt(&outcome.receipt)
            .unwrap()
            .verified
    );

    let mut client = env.client();
    let target = env.target("orders_append");
    let rows = client
        .query(
            &format!(
                "SELECT \"id\", \"name\", \"_cdf_load\", \"_cdf_segment\", \"_cdf_row\", \"_cdf_loaded_at_ms\" FROM {} ORDER BY \"id\"",
                target.sql()
            ),
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "ada");
    assert_eq!(rows[0].get::<_, String>(2), manifest.package_hash);
    assert_eq!(rows[0].get::<_, String>(3), "seg-000001");
    assert_eq!(rows[0].get::<_, i64>(4), 0);
    assert!(rows[0].get::<_, i64>(5) > 0);

    let load_count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {} WHERE \"target\" = $1 AND \"package_hash\" = $2",
                quote_identifier_unchecked(CDF_LOADS_TABLE)
            ),
            &[&target.display_name(), &manifest.package_hash],
        )
        .unwrap()
        .get(0);
    assert_eq!(load_count, 1);

    let schema_hash_value = schema_hash();
    let state_count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {} WHERE \"package_hash\" = $1 AND \"schema_hash\" = $2",
                quote_identifier_unchecked(CDF_STATE_TABLE)
            ),
            &[&manifest.package_hash, &schema_hash_value.as_str()],
        )
        .unwrap()
        .get(0);
    assert_eq!(state_count, 1);

    let duplicate = commit(&env, package_dir.path(), plan);
    assert!(duplicate.duplicate);
    assert!(!duplicate.package_receipt_recorded);
    assert!(duplicate.package_receipt_error.is_none());
    assert_eq!(duplicate.receipt.receipt_id, outcome.receipt.receipt_id);
    let target_count: i64 = client
        .query_one(
            &format!("SELECT COUNT(*)::bigint FROM {}", target.sql()),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(target_count, 3);

    let mut different_token_input = load_input(
        &env,
        "orders_append",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        Some(state_delta(&manifest, "chk-live-append")),
        columns(),
    );
    different_token_input.idempotency_token =
        IdempotencyToken::new("token-different-but-same-package").unwrap();
    let duplicate_with_different_token = commit(
        &env,
        package_dir.path(),
        env.destination().plan_load(different_token_input).unwrap(),
    );
    assert!(duplicate_with_different_token.duplicate);
    assert_eq!(
        duplicate_with_different_token.receipt.receipt_id,
        outcome.receipt.receipt_id
    );
}

#[test]
fn live_append_populates_quarantine_mirror_when_sheet_supports_it() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let mut builder =
        PackageBuilder::create(package_dir.path(), "pkg-live-quarantine-mirror").unwrap();
    builder
        .write_quarantine_records(
            "part-000001.parquet",
            &[QuarantineRecord {
                source_row_ordinal: 1,
                rule_id: "row-rule-0000-regex".to_owned(),
                error_code: "regex_violation".to_owned(),
                source_position: Some(position(10)),
                observed_value_redacted: QuarantineObservedValue::Hashed {
                    algorithm: "sha256".to_owned(),
                    value: "sha256:abc123".to_owned(),
                },
            }],
        )
        .unwrap();
    builder
        .write_segment(
            SegmentId::new("seg-000001").unwrap(),
            &[batch(&[(1, Some("ada"))])],
        )
        .unwrap();
    let manifest = builder.finish().unwrap();
    let plan = plan(
        &env,
        "orders_quarantine_mirror",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        Some(state_delta(&manifest, "chk-live-quarantine-mirror")),
    );

    let outcome = commit(&env, package_dir.path(), plan);
    assert!(!outcome.duplicate);
    let mut client = env.client();
    let target = env.target("orders_quarantine_mirror");
    let row = client
        .query_one(
            &format!(
                "SELECT \"source_row_ordinal\", \"rule_id\", \"error_code\", \"observed_value_json\"::text FROM {} WHERE \"target\" = $1 AND \"package_hash\" = $2",
                quote_identifier_unchecked(CDF_QUARANTINE_TABLE)
            ),
            &[&target.display_name(), &manifest.package_hash],
        )
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, String>(1), "row-rule-0000-regex");
    assert_eq!(row.get::<_, String>(2), "regex_violation");
    let observed_json = row.get::<_, String>(3);
    assert!(observed_json.contains("sha256:abc123"));
    assert!(!observed_json.contains("pii-fixture-sensitive"));
}

#[test]
fn live_replace_is_atomic_and_reports_deleted_rows() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let first_dir = tempfile::tempdir().unwrap();
    let first_manifest = build_package(
        first_dir.path(),
        "pkg-live-replace-first",
        vec![("seg-000001", batch(&[(1, Some("ada")), (2, Some("grace"))]))],
    );
    let first_plan = plan(
        &env,
        "orders_replace",
        &first_manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        None,
    );
    commit(&env, first_dir.path(), first_plan);

    let second_dir = tempfile::tempdir().unwrap();
    let second_manifest = build_package(
        second_dir.path(),
        "pkg-live-replace-second",
        vec![("seg-000001", batch(&[(3, Some("katherine"))]))],
    );
    let replace_plan = plan(
        &env,
        "orders_replace",
        &second_manifest,
        WriteDisposition::Replace,
        MergeDedupPolicy::Last,
        None,
    );
    let outcome = commit(&env, second_dir.path(), replace_plan);
    assert_eq!(outcome.receipt.counts.rows_written, 1);
    assert_eq!(outcome.receipt.counts.rows_inserted, Some(1));
    assert_eq!(outcome.receipt.counts.rows_deleted, Some(2));

    let mut client = env.client();
    let rows = client
        .query(
            &format!(
                "SELECT \"id\", \"name\", \"_cdf_load\" FROM {} ORDER BY \"id\"",
                env.target("orders_replace").sql()
            ),
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 3);
    assert_eq!(rows[0].get::<_, String>(1), "katherine");
    assert_eq!(rows[0].get::<_, String>(2), second_manifest.package_hash);
}

#[test]
fn live_merge_deduplicates_last_row_and_updates_existing_keys() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let first_dir = tempfile::tempdir().unwrap();
    let first_manifest = build_package(
        first_dir.path(),
        "pkg-live-merge-first",
        vec![
            ("seg-000001", batch(&[(1, Some("old")), (2, Some("two"))])),
            ("seg-000002", batch(&[(1, Some("new"))])),
        ],
    );
    let first_plan = plan(
        &env,
        "orders_merge",
        &first_manifest,
        WriteDisposition::Merge,
        MergeDedupPolicy::Last,
        None,
    );
    let first = commit(&env, first_dir.path(), first_plan);
    assert_eq!(first.receipt.counts.rows_written, 2);
    assert_eq!(first.receipt.counts.rows_inserted, Some(2));
    assert_eq!(first.receipt.counts.rows_updated, Some(0));

    let second_dir = tempfile::tempdir().unwrap();
    let second_manifest = build_package(
        second_dir.path(),
        "pkg-live-merge-second",
        vec![(
            "seg-000001",
            batch(&[(1, Some("updated")), (3, Some("three"))]),
        )],
    );
    let second_plan = plan(
        &env,
        "orders_merge",
        &second_manifest,
        WriteDisposition::Merge,
        MergeDedupPolicy::Last,
        None,
    );
    let second = commit(&env, second_dir.path(), second_plan);
    assert_eq!(second.receipt.counts.rows_written, 2);
    assert_eq!(second.receipt.counts.rows_inserted, Some(1));
    assert_eq!(second.receipt.counts.rows_updated, Some(1));

    let mut client = env.client();
    let rows = client
        .query(
            &format!(
                "SELECT \"id\", \"name\", \"_cdf_segment\" FROM {} ORDER BY \"id\"",
                env.target("orders_merge").sql()
            ),
            &[],
        )
        .unwrap();
    let actual = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, i64>(0),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            (1, "updated".to_owned(), "seg-000001".to_owned()),
            (2, "two".to_owned(), "seg-000001".to_owned()),
            (3, "three".to_owned(), "seg-000001".to_owned()),
        ]
    );
}

#[test]
fn live_decimal128_values_preserve_exact_numeric_text() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let manifest = build_package(
        package_dir.path(),
        "pkg-live-decimal",
        vec![(
            "seg-000001",
            decimal_batch(&[(1, Some(1234_i128)), (2, Some(-5_i128)), (3, None)]),
        )],
    );
    let plan = plan_with_columns(
        &env,
        "orders_decimal",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        None,
        decimal_columns(),
    );

    let outcome = commit(&env, package_dir.path(), plan);
    assert_eq!(outcome.receipt.counts.rows_written, 3);

    let mut client = env.client();
    let rows = client
        .query(
            &format!(
                "SELECT \"id\", \"amount\"::text FROM {} ORDER BY \"id\"",
                env.target("orders_decimal").sql()
            ),
            &[],
        )
        .unwrap();
    let actual = rows
        .iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<String>>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            (1, Some("12.34".to_owned())),
            (2, Some("-0.05".to_owned())),
            (3, None),
        ]
    );
}

#[cfg(unix)]
#[test]
fn live_package_receipt_append_error_does_not_mask_committed_database_receipt() {
    use std::os::unix::fs::PermissionsExt;

    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let manifest = build_package(
        package_dir.path(),
        "pkg-live-receipt-append-failure",
        vec![("seg-000001", batch(&[(1, Some("ada"))]))],
    );
    let plan = plan(
        &env,
        "orders_receipt_append_failure",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        None,
    );
    let destination_dir = package_dir.path().join("destination");
    let original_mode = std::fs::metadata(&destination_dir)
        .unwrap()
        .permissions()
        .mode();
    let mut readonly = std::fs::metadata(&destination_dir).unwrap().permissions();
    readonly.set_mode(0o500);
    std::fs::set_permissions(&destination_dir, readonly).unwrap();

    let outcome = env
        .destination()
        .commit_package(PostgresCommitRequest {
            package_dir: package_dir.path().to_path_buf(),
            plan,
        })
        .unwrap();

    let mut restored = std::fs::metadata(&destination_dir).unwrap().permissions();
    restored.set_mode(original_mode);
    std::fs::set_permissions(&destination_dir, restored).unwrap();

    assert!(!outcome.duplicate);
    assert!(!outcome.package_receipt_recorded);
    assert!(outcome.package_receipt_error.is_some());
    assert!(
        env.destination()
            .verify_receipt(&outcome.receipt)
            .unwrap()
            .verified
    );

    let mut client = env.client();
    let target_count: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                env.target("orders_receipt_append_failure").sql()
            ),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(target_count, 1);
}

#[test]
fn live_rollback_after_stage_copy_leaves_no_target_or_mirror_partial_commit() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let mut client = env.client();
    let target = env.target("orders_rollback");
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (\"id\" BIGINT PRIMARY KEY, \"name\" TEXT); INSERT INTO {} (\"id\", \"name\") VALUES (1, 'seed')",
            target.sql(),
            target.sql()
        ))
        .unwrap();

    let package_dir = tempfile::tempdir().unwrap();
    let manifest = build_package(
        package_dir.path(),
        "pkg-live-rollback",
        vec![("seg-000001", batch(&[(1, Some("duplicate"))]))],
    );
    let mut rollback_plan = plan(
        &env,
        "orders_rollback",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        Some(state_delta(&manifest, "chk-live-rollback")),
    );
    rollback_plan.target_ddl = target_migrations(&PostgresLoadPlanInput {
        package_hash: PackageHash::new(manifest.package_hash.clone()).unwrap(),
        idempotency_token: IdempotencyToken::new(manifest.package_hash.clone()).unwrap(),
        target: target.clone(),
        disposition: WriteDisposition::Append,
        schema_hash: schema_hash(),
        segments: state_segments(&manifest),
        columns: columns(),
        merge_keys: vec![PostgresIdentifier::user("id").unwrap()],
        dedup: MergeDedupPolicy::Last,
        existing_table: Some(
            PostgresExistingTable::new(
                vec![
                    PostgresExistingColumn::new("id", "BIGINT", false).unwrap(),
                    PostgresExistingColumn::new("name", "TEXT", true).unwrap(),
                ],
                vec!["id"],
            )
            .unwrap(),
        ),
        resource_id: Some(ResourceId::new("orders").unwrap()),
        state_delta: Some(state_delta(&manifest, "chk-live-rollback")),
    })
    .unwrap();

    let error = env
        .destination()
        .commit_package(PostgresCommitRequest {
            package_dir: package_dir.path().to_path_buf(),
            plan: rollback_plan,
        })
        .unwrap_err();
    assert!(!error.message.is_empty());

    let target_rows: Vec<(i64, String)> = client
        .query(
            &format!(
                "SELECT \"id\", \"name\" FROM {} ORDER BY \"id\"",
                target.sql()
            ),
            &[],
        )
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(target_rows, vec![(1, "seed".to_owned())]);
    let load_count = load_mirror_count(&mut client, &target.display_name(), &manifest.package_hash);
    assert_eq!(load_count, 0);
    assert!(
        cdf_package::PackageReader::open(package_dir.path())
            .unwrap()
            .receipts()
            .unwrap()
            .is_empty()
    );
}

fn load_mirror_count(client: &mut Client, target: &str, package_hash: &str) -> i64 {
    let table_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = '_cdf_loads')",
            &[],
        )
        .unwrap()
        .get(0);
    if !table_exists {
        return 0;
    }
    client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {} WHERE \"target\" = $1 AND \"package_hash\" = $2",
                quote_identifier_unchecked(CDF_LOADS_TABLE)
            ),
            &[&target, &package_hash],
        )
        .unwrap()
        .get(0)
}
