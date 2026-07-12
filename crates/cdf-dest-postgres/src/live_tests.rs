use std::{
    collections::BTreeMap,
    env,
    io::Write,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
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
    CanonicalArrowField, CheckpointId, ContractRef, CursorOrderingClaim, CursorPosition,
    CursorSpec, CursorValue, DestinationCorrectionOperation, DestinationCorrectionPlan,
    DestinationCorrectionRequest, PartitionId, PipelineId, PredicateId, PromotionId,
    QueryableResource, ResidualCorrectionOperation, ResourceDescriptor, ResourceId, ResourceStream,
    RowProvenanceAddress, ScanPredicate, ScanRequest, SchemaSnapshotReference, SchemaSource,
    ScopeKey, SegmentId, SortDirection, SourcePosition, TrustLevel, with_source_name,
};
use cdf_package::{
    PackageBuilder, PackageManifest, PackageReader, QuarantineObservedValue, QuarantineRecord,
};
use cdf_source_postgres::{PostgresTableResource, discover_postgres_table_catalog_schema};
use futures_util::StreamExt;
use postgres::{Client, NoTls};
use tempfile::TempDir;

use super::*;
use crate::{ddl::target_migrations, identifiers::quote_identifier_unchecked};

#[test]
#[ignore = "release-mode local PostgreSQL binary-vs-CSV COPY benchmark"]
fn live_binary_copy_is_at_least_twice_csv() {
    fn csv_field(value: Option<&str>) -> String {
        let Some(value) = value else {
            return "\\N".to_owned();
        };
        if value == "\\N" || value.contains([',', '"', '\n', '\r']) {
            format!("\"{}\"", value.replace('"', "\"\""))
        } else {
            value.to_owned()
        }
    }

    const ROWS: usize = 524_288;
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let mut client = postgres.client();
    let user_ddl = std::iter::once("name TEXT".to_owned())
        .chain((0..8).map(|index| format!("integer_{index} BIGINT NOT NULL")))
        .chain((0..8).map(|index| format!("float_{index} DOUBLE PRECISION NOT NULL")))
        .collect::<Vec<_>>()
        .join(", ");
    let table_ddl = |name: &str| {
        format!(
            "CREATE UNLOGGED TABLE {name} (
               {user_ddl},
               _cdf_row_key BIGINT NOT NULL, _cdf_loaded_at_ms BIGINT NOT NULL
             )"
        )
    };
    client
        .batch_execute(&format!(
            "SET synchronous_commit = off; {}; {};",
            table_ddl("binary_copy_bench"),
            table_ddl("csv_copy_bench")
        ))
        .unwrap();
    let mut fields = vec![Field::new("name", DataType::Utf8, true)];
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from_iter(
        (0..ROWS).map(|row| (row % 11 != 0).then_some("yellow-taxi")),
    ))];
    for index in 0..8 {
        fields.push(Field::new(
            format!("integer_{index}"),
            DataType::Int64,
            false,
        ));
        arrays.push(Arc::new(Int64Array::from_iter_values(
            (0..ROWS).map(|row| row as i64 + i64::from(index)),
        )));
    }
    for index in 0..8 {
        fields.push(Field::new(
            format!("float_{index}"),
            DataType::Float64,
            false,
        ));
        arrays.push(Arc::new(Float64Array::from_iter_values(
            (0..ROWS).map(|row| row as f64 / 100.0 + f64::from(index)),
        )));
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();

    let started = Instant::now();
    let writer = client
        .copy_in("COPY binary_copy_bench FROM STDIN WITH (FORMAT binary)")
        .unwrap();
    let mut encoder =
        crate::binary_copy::BinaryCopyEncoder::new(writer, batch.num_columns()).unwrap();
    encoder.write_batch(&batch, 1, 1_700_000_000_000).unwrap();
    let (writer, encoded) = encoder.finish().unwrap();
    let copied = writer.finish().unwrap();
    let binary_elapsed = started.elapsed();
    assert_eq!(encoded, ROWS as u64);
    assert_eq!(copied, encoded);

    let started = Instant::now();
    let mut writer = client
        .copy_in("COPY csv_copy_bench FROM STDIN WITH (FORMAT csv, NULL '\\N')")
        .unwrap();
    for row in 0..ROWS {
        let values = batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .map(|(array, field)| {
                if array.is_null(row) {
                    return None;
                }
                Some(match field.data_type() {
                    DataType::Int64 => array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row)
                        .to_string(),
                    DataType::Utf8 => array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row)
                        .to_owned(),
                    DataType::Float64 => array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(row)
                        .to_string(),
                    other => panic!("unexpected scalar CSV control type {other:?}"),
                })
            })
            .collect::<Vec<_>>();
        let mut fields = values
            .iter()
            .map(|value| csv_field(value.as_deref()))
            .collect::<Vec<_>>();
        fields.push(csv_field(Some(&(row + 1).to_string())));
        fields.push(csv_field(Some(&1_700_000_000_000_i64.to_string())));
        let mut line = fields.join(",");
        line.push('\n');
        writer.write_all(line.as_bytes()).unwrap();
    }
    assert_eq!(writer.finish().unwrap(), ROWS as u64);
    let csv_elapsed = started.elapsed();
    let speedup = csv_elapsed.as_secs_f64() / binary_elapsed.as_secs_f64();
    eprintln!(
        "postgres_local_copy binary_rows_per_second={:.0} csv_rows_per_second={:.0} speedup={speedup:.2}x",
        ROWS as f64 / binary_elapsed.as_secs_f64(),
        ROWS as f64 / csv_elapsed.as_secs_f64(),
    );
    assert!(speedup >= 2.0, "local COPY speedup was {speedup:.2}x");
}

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

fn residual_batch(rows: &[(i64, i64, Option<&str>)]) -> RecordBatch {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        cdf_kernel::SEMANTIC_METADATA_KEY.to_owned(),
        cdf_contract::VARIANT_SEMANTIC_TAG.to_owned(),
    );
    metadata.insert(
        cdf_contract::RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
        cdf_contract::RESIDUAL_ENCODING_NAME.to_owned(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true).with_metadata(metadata),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|(id, _, _)| *id),
    ));
    let ages = Int64Array::from_iter_values(rows.iter().map(|(_, age, _)| *age));
    let untouched = StringArray::from_iter(rows.iter().map(|(_, _, value)| *value));
    let variants = rows
        .iter()
        .enumerate()
        .map(|(row, (_, _, value))| {
            let age = cdf_contract::ResidualFieldRef::new(["age"], &ages, row).unwrap();
            if value.is_some() {
                cdf_contract::encode_residual_json_v1([
                    age,
                    cdf_contract::ResidualFieldRef::new(["untouched"], &untouched, row).unwrap(),
                ])
                .unwrap()
            } else {
                cdf_contract::encode_residual_json_v1([age]).unwrap()
            }
        })
        .map(|bytes| String::from_utf8(bytes).unwrap())
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, vec![ids, Arc::new(StringArray::from(variants))]).unwrap()
}

fn residual_columns() -> Vec<PostgresColumn> {
    postgres_columns_for_schema(residual_batch(&[(1, 1, None)]).schema().as_ref()).unwrap()
}

fn correction_existing_table_live() -> PostgresExistingTable {
    let mut columns = BTreeMap::new();
    for (name, data_type, nullable) in [
        ("id", "BIGINT", false),
        ("_cdf_variant", "TEXT", true),
        (CDF_ROW_KEY_COLUMN, "BIGINT", false),
        (CDF_LOADED_AT_COLUMN, "BIGINT", false),
    ] {
        columns.insert(
            name.to_owned(),
            PostgresExistingColumn {
                name: PostgresIdentifier::system(name).unwrap(),
                data_type: data_type.to_owned(),
                nullable,
            },
        );
    }
    PostgresExistingTable {
        columns,
        primary_key: Vec::new(),
    }
}

fn correction_operation_live(
    original_package_hash: &PackageHash,
    row: u64,
    value: i64,
) -> DestinationCorrectionOperation {
    let values = Int64Array::from(vec![value]);
    let exact = cdf_contract::encode_residual_json_v1([cdf_contract::ResidualFieldRef::new(
        ["age"],
        &values,
        0,
    )
    .unwrap()])
    .unwrap();
    DestinationCorrectionOperation {
        correction: DestinationCorrectionPlan {
            request: DestinationCorrectionRequest {
                promotion_id: PromotionId::new("promotion-live-age").unwrap(),
                original_row: RowProvenanceAddress::new(
                    original_package_hash.clone(),
                    SegmentId::new("seg-000001").unwrap(),
                    row,
                ),
                old_schema_hash: schema_hash(),
                new_schema_hash: SchemaHash::new("sha256:live-schema-with-age").unwrap(),
                promoted_path: "/age".to_owned(),
                promoted_value_json: "inspection-only-not-execution-authority".to_owned(),
                residual_operation: ResidualCorrectionOperation::RemovePromotedPath,
                selected_strategy: CorrectionStrategy::InPlaceUpdate,
            },
            transaction_guarantee: TransactionSupport::AtomicPackage,
            idempotency_guarantee: IdempotencySupport::PackageToken,
        },
        output_field: CanonicalArrowField::from_arrow(&Field::new("age", DataType::Int64, true))
            .unwrap(),
        promoted_value_residual_json_v1: exact,
    }
}

fn correction_request_live(
    target: &PostgresTarget,
    correction_manifest: &PackageManifest,
    operations: Vec<DestinationCorrectionOperation>,
) -> DestinationCorrectionCommitRequest {
    DestinationCorrectionCommitRequest::new(
        PackageHash::new(correction_manifest.package_hash.clone()).unwrap(),
        IdempotencyToken::new(correction_manifest.package_hash.clone()).unwrap(),
        target.target_name().unwrap(),
        WriteDisposition::Append,
        state_segments(correction_manifest),
        operations,
    )
    .unwrap()
}

fn build_package(
    root: &Path,
    package_id: &str,
    segments: Vec<(&str, RecordBatch)>,
) -> PackageManifest {
    let builder = PackageBuilder::create(root, package_id).unwrap();
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
fn live_postgres_table_resource_reads_source_name_physical_columns() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let target = env.target("source_name_orders");
    let mut client = env.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"VendorID\" BIGINT NOT NULL,
                \"updated_at\" BIGINT NOT NULL
            );
            INSERT INTO {} (\"VendorID\", \"updated_at\") VALUES (1, 10), (2, 20), (3, 30)",
            target.sql(),
            target.sql()
        ))
        .unwrap();

    let mut descriptor = postgres_source_descriptor();
    descriptor.schema_source = SchemaSource::Discovered {
        snapshot: SchemaSnapshotReference {
            schema_hash: SchemaHash::new("sha256:postgres-source-name-discovered").unwrap(),
            path:
                ".cdf/schemas/warehouse.source_orders@sha256:postgres-source-name-discovered.json"
                    .to_owned(),
            metadata: BTreeMap::new(),
        },
    };
    descriptor.primary_key = vec!["vendor_id".to_owned()];
    descriptor.merge_key = vec!["vendor_id".to_owned()];
    descriptor.cursor = Some(CursorSpec {
        field: "vendor_id".to_owned(),
        ordering: CursorOrderingClaim::Exact,
        lag_tolerance_ms: 0,
    });
    let schema = Arc::new(Schema::new(vec![
        with_source_name(Field::new("vendor_id", DataType::Int64, false), "VendorID"),
        Field::new("updated_at", DataType::Int64, false),
    ]));
    let resource =
        PostgresTableResource::new(env.url.clone(), descriptor.clone(), schema, target).unwrap();
    let request = ScanRequest {
        resource_id: descriptor.resource_id.clone(),
        projection: Some(vec!["vendor_id".to_owned(), "updated_at".to_owned()]),
        filters: vec![ScanPredicate {
            predicate_id: PredicateId::new("vendor").unwrap(),
            expression: "vendor_id >= 2".to_owned(),
        }],
        limit: None,
        order_by: vec![cdf_kernel::OrderBy {
            field: "vendor_id".to_owned(),
            direction: SortDirection::Desc,
        }],
        scope: ScopeKey::Resource,
    };

    let plan = resource.negotiate(&request).unwrap();
    let batches = drain_source_batches(
        futures_executor::block_on(resource.open(plan.partitions[0].clone())).unwrap(),
    );

    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].header.observed_schema_hash,
        SchemaHash::new("sha256:postgres-source-name-discovered").unwrap()
    );
    let batch = batches[0].record_batch().unwrap();
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["vendor_id", "updated_at"]
    );
    let vendor_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(vendor_ids.values(), &[3, 2]);
    let Some(SourcePosition::Cursor(cursor)) = &batches[0].header.source_position else {
        panic!("expected source-name cursor source position");
    };
    assert_eq!(cursor.field, "vendor_id");
    assert_eq!(cursor.value, CursorValue::I64(3));
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
        deduplication: None,
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
    let mut first_input = load_input(
        &env,
        "orders_append",
        &manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        Some(state_delta(&manifest, "chk-live-append")),
        columns(),
    );
    first_input.idempotency_token = IdempotencyToken::new("operator-append-token").unwrap();
    let plan = env.destination().plan_load(first_input).unwrap();

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
                "SELECT \"target\".\"id\", \"target\".\"name\", \"segment\".\"package_hash\", \"segment\".\"segment_id\", \"target\".\"_cdf_row_key\" - \"segment\".\"row_key_start\", \"target\".\"_cdf_loaded_at_ms\" FROM {} AS \"target\" JOIN {} AS \"segment\" ON \"target\".\"_cdf_row_key\" >= \"segment\".\"row_key_start\" AND \"target\".\"_cdf_row_key\" < \"segment\".\"row_key_end\" WHERE \"segment\".\"target\" = $1 ORDER BY \"target\".\"id\"",
                target.sql(),
                quote_identifier_unchecked(CDF_SEGMENTS_TABLE)
            ),
            &[&target.display_name()],
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
    let builder = PackageBuilder::create(package_dir.path(), "pkg-live-quarantine-mirror").unwrap();
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
                "SELECT \"target\".\"id\", \"target\".\"name\", \"segment\".\"package_hash\" FROM {} AS \"target\" JOIN {} AS \"segment\" ON \"target\".\"_cdf_row_key\" >= \"segment\".\"row_key_start\" AND \"target\".\"_cdf_row_key\" < \"segment\".\"row_key_end\" WHERE \"segment\".\"target\" = $1 ORDER BY \"target\".\"id\"",
                env.target("orders_replace").sql(),
                quote_identifier_unchecked(CDF_SEGMENTS_TABLE)
            ),
            &[&env.target("orders_replace").display_name()],
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
                "SELECT \"target\".\"id\", \"target\".\"name\", \"segment\".\"segment_id\" FROM {} AS \"target\" JOIN {} AS \"segment\" ON \"target\".\"_cdf_row_key\" >= \"segment\".\"row_key_start\" AND \"target\".\"_cdf_row_key\" < \"segment\".\"row_key_end\" WHERE \"segment\".\"target\" = $1 ORDER BY \"target\".\"id\"",
                env.target("orders_merge").sql(),
                quote_identifier_unchecked(CDF_SEGMENTS_TABLE)
            ),
            &[&env.target("orders_merge").display_name()],
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

#[test]
fn live_addressed_correction_updates_exact_rows_preserves_residuals_and_replays() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let original_dir = tempfile::tempdir().unwrap();
    let original_manifest = build_package(
        original_dir.path(),
        "pkg-live-correction-original",
        vec![(
            "seg-000001",
            residual_batch(&[(1, 42, Some("keep")), (2, 84, None)]),
        )],
    );
    let original_plan = plan_with_columns(
        &env,
        "orders_correction",
        &original_manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        None,
        residual_columns(),
    );
    commit(&env, original_dir.path(), original_plan);

    let target = env.target("orders_correction");
    let original_hash = PackageHash::new(original_manifest.package_hash.clone()).unwrap();
    let first_address = RowProvenanceAddress::new(
        original_hash.clone(),
        SegmentId::new("seg-000001").unwrap(),
        0,
    );
    let before = env
        .destination()
        .read_correction_residual(&target.target_name().unwrap(), &first_address)
        .unwrap()
        .unwrap();
    assert_eq!(before.original_row, first_address);
    let before_bytes = before.residual_json_v1.unwrap();
    let before_fields = cdf_contract::decode_residual_json_v1(&before_bytes).unwrap();
    assert_eq!(
        before_fields
            .iter()
            .map(|field| field.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/age", "/untouched"]
    );

    let correction_dir = tempfile::tempdir().unwrap();
    let correction_manifest = build_package(
        correction_dir.path(),
        "pkg-live-correction-update",
        vec![(
            "seg-correction",
            batch(&[(1, Some("first")), (2, Some("second"))]),
        )],
    );
    let request = correction_request_live(
        &target,
        &correction_manifest,
        vec![
            correction_operation_live(&original_hash, 0, 42),
            correction_operation_live(&original_hash, 1, 84),
        ],
    );
    let plan = env
        .destination()
        .plan_addressed_correction(PostgresCorrectionPlanInput {
            request: request.clone(),
            existing_table: correction_existing_table_live(),
        })
        .unwrap();
    let commit_request = PostgresCorrectionCommitRequest {
        package_dir: correction_dir.path().to_path_buf(),
        plan: plan.clone(),
    };
    let destination = env
        .destination()
        .with_correction_request(commit_request.clone());
    let generic_plan = destination.plan_correction(&request).unwrap();
    let mut session = destination
        .begin_correction(request.clone(), generic_plan)
        .unwrap();
    session.apply_migrations().unwrap();
    let counts = session.apply_corrections().unwrap();
    let receipt = session.finalize().unwrap();
    assert_eq!(counts.rows_written, 2);
    assert_eq!(counts.rows_updated, Some(2));
    plan.kernel.validate_receipt(&request, &receipt).unwrap();
    assert!(
        env.destination()
            .verify_correction(&receipt)
            .unwrap()
            .verified
    );

    let mut client = env.client();
    let rows = client
        .query(
            &format!(
                "SELECT \"id\", \"age\", \"_cdf_variant\" FROM {} ORDER BY \"id\"",
                target.sql()
            ),
            &[],
        )
        .unwrap();
    assert_eq!(rows[0].get::<_, i64>(1), 42);
    assert_eq!(rows[1].get::<_, i64>(1), 84);
    let preserved: String = rows[0].get(2);
    let preserved_fields = cdf_contract::decode_residual_json_v1(preserved.as_bytes()).unwrap();
    assert_eq!(preserved_fields.len(), 1);
    assert_eq!(preserved_fields[0].path, "/untouched");
    assert!(rows[1].get::<_, Option<String>>(2).is_none());

    let index_count: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pg_indexes WHERE schemaname = $1 AND tablename = $2 AND indexname ~ '^_cdf_provenance_'",
            &[&env.schema, &target.table.as_str()],
        )
        .unwrap()
        .get(0);
    assert_eq!(index_count, 1);

    let after = env
        .destination()
        .read_correction_residual(&target.target_name().unwrap(), &first_address)
        .unwrap()
        .unwrap();
    assert_eq!(after.residual_json_v1, Some(preserved.into_bytes()));

    let replay = env
        .destination()
        .commit_corrections(request, commit_request)
        .unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.receipt, receipt);
    assert!(!replay.package_receipt_recorded);
    assert_eq!(
        PackageReader::open(correction_dir.path())
            .unwrap()
            .receipts()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn live_correction_missing_duplicate_and_post_update_failures_roll_back() {
    let Some(env) = LivePostgres::start() else {
        return;
    };

    let original_dir = tempfile::tempdir().unwrap();
    let original_manifest = build_package(
        original_dir.path(),
        "pkg-live-correction-negative-original",
        vec![("seg-000001", residual_batch(&[(1, 42, Some("keep"))]))],
    );
    let original_plan = plan_with_columns(
        &env,
        "orders_correction_missing",
        &original_manifest,
        WriteDisposition::Append,
        MergeDedupPolicy::Last,
        None,
        residual_columns(),
    );
    commit(&env, original_dir.path(), original_plan);
    let target = env.target("orders_correction_missing");
    let original_hash = PackageHash::new(original_manifest.package_hash.clone()).unwrap();
    let original_address = RowProvenanceAddress::new(
        original_hash.clone(),
        SegmentId::new("seg-000001").unwrap(),
        0,
    );
    let original_residual = env
        .destination()
        .read_addressed_residual(&target.target_name().unwrap(), &original_address)
        .unwrap()
        .unwrap()
        .residual_json_v1;

    let missing_dir = tempfile::tempdir().unwrap();
    let missing_manifest = build_package(
        missing_dir.path(),
        "pkg-live-correction-missing",
        vec![("seg-correction", batch(&[(1, None)]))],
    );
    let missing_request = correction_request_live(
        &target,
        &missing_manifest,
        vec![correction_operation_live(&original_hash, 99, 42)],
    );
    let missing_plan = env
        .destination()
        .plan_addressed_correction(PostgresCorrectionPlanInput {
            request: missing_request.clone(),
            existing_table: correction_existing_table_live(),
        })
        .unwrap();
    let missing_error = env
        .destination()
        .commit_corrections(
            missing_request,
            PostgresCorrectionCommitRequest {
                package_dir: missing_dir.path().to_path_buf(),
                plan: missing_plan,
            },
        )
        .unwrap_err();
    assert!(missing_error.to_string().contains("matched 0 row(s)"));
    assert!(!target_column_exists(
        &mut env.client(),
        &env.schema,
        "orders_correction_missing",
        "age"
    ));

    let duplicate_target = env.target("orders_correction_duplicate");
    let duplicate_residual = String::from_utf8(
        cdf_contract::encode_residual_json_v1([cdf_contract::ResidualFieldRef::new(
            ["age"],
            &Int64Array::from(vec![7_i64]),
            0,
        )
        .unwrap()])
        .unwrap(),
    )
    .unwrap();
    let duplicate_hash = PackageHash::new("sha256:duplicate-original").unwrap();
    let mut client = env.client();
    for statement in crate::ddl::system_table_ddl() {
        client.batch_execute(&statement.sql).unwrap();
    }
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (\"id\" BIGINT NOT NULL, \"_cdf_variant\" TEXT, \"_cdf_row_key\" BIGINT NOT NULL, \"_cdf_loaded_at_ms\" BIGINT NOT NULL)",
            duplicate_target.sql()
        ))
        .unwrap();
    client
        .execute(
            &format!("INSERT INTO {} (\"row_key_start\", \"row_key_end\", \"target\", \"package_hash\", \"segment_id\") VALUES (1000000000000, 1000000000001, $1, $2, 'seg-000001')", quote_identifier_unchecked(CDF_SEGMENTS_TABLE)),
            &[&duplicate_target.display_name(), &duplicate_hash.as_str()],
        )
        .unwrap();
    for id in [1_i64, 2_i64] {
        client
            .execute(
                &format!(
                    "INSERT INTO {} (\"id\", \"_cdf_variant\", \"_cdf_row_key\", \"_cdf_loaded_at_ms\") VALUES ($1, $2, 1000000000000, 1)",
                    duplicate_target.sql()
                ),
                &[&id, &duplicate_residual],
            )
            .unwrap();
    }
    let duplicate_dir = tempfile::tempdir().unwrap();
    let duplicate_manifest = build_package(
        duplicate_dir.path(),
        "pkg-live-correction-duplicate",
        vec![("seg-correction", batch(&[(1, None)]))],
    );
    let duplicate_request = correction_request_live(
        &duplicate_target,
        &duplicate_manifest,
        vec![correction_operation_live(&duplicate_hash, 0, 7)],
    );
    let duplicate_plan = env
        .destination()
        .plan_addressed_correction(PostgresCorrectionPlanInput {
            request: duplicate_request.clone(),
            existing_table: correction_existing_table_live(),
        })
        .unwrap();
    let duplicate_error = env
        .destination()
        .commit_corrections(
            duplicate_request,
            PostgresCorrectionCommitRequest {
                package_dir: duplicate_dir.path().to_path_buf(),
                plan: duplicate_plan,
            },
        )
        .unwrap_err();
    assert!(duplicate_error.to_string().contains("matched 2 row(s)"));
    assert!(!target_column_exists(
        &mut client,
        &env.schema,
        "orders_correction_duplicate",
        "age"
    ));

    let fail_dir = tempfile::tempdir().unwrap();
    let fail_manifest = build_package(
        fail_dir.path(),
        "pkg-live-correction-failpoint",
        vec![("seg-correction", batch(&[(1, None)]))],
    );
    let fail_request = correction_request_live(
        &target,
        &fail_manifest,
        vec![correction_operation_live(&original_hash, 0, 42)],
    );
    let mut fail_plan = env
        .destination()
        .plan_addressed_correction(PostgresCorrectionPlanInput {
            request: fail_request.clone(),
            existing_table: correction_existing_table_live(),
        })
        .unwrap();
    fail_plan.verify.statement = "SELECT * FROM cdf_missing_correction_receipt".to_owned();
    let fail_hash = fail_request.correction_package_hash.clone();
    let fail_error = env
        .destination()
        .commit_corrections(
            fail_request,
            PostgresCorrectionCommitRequest {
                package_dir: fail_dir.path().to_path_buf(),
                plan: fail_plan,
            },
        )
        .unwrap_err();
    assert!(
        fail_error
            .to_string()
            .contains("verify Postgres correction receipt")
    );
    assert!(!target_column_exists(
        &mut client,
        &env.schema,
        "orders_correction_missing",
        "age"
    ));
    assert_eq!(
        load_mirror_count(&mut client, &target.display_name(), fail_hash.as_str()),
        0
    );
    let after_failure = env
        .destination()
        .read_addressed_residual(&target.target_name().unwrap(), &original_address)
        .unwrap()
        .unwrap()
        .residual_json_v1;
    assert_eq!(after_failure, original_residual);
}

fn target_column_exists(client: &mut Client, schema: &str, table: &str, column: &str) -> bool {
    client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3)",
            &[&schema, &table, &column],
        )
        .unwrap()
        .get(0)
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
