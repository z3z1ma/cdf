use std::{
    collections::BTreeMap, error::Error, fs, hint::black_box, io::Read, path::Path,
    process::Command, sync::Arc, time::Instant,
};

use arrow_array::{
    Array, Decimal128Array, Decimal256Array, Int64Array, RecordBatch, StringArray,
    builder::{ArrayBuilder, Decimal128Builder, Decimal256Builder, Int64Builder, StringBuilder},
};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_bench_core::{
    BenchResult, ComparabilityKey, HostCapabilityProvider, HostFingerprint, HostProbeConfig,
    IoMode, SystemHostProvider, bench_error, host_class,
};
use cdf_http::EgressAllowlist;
use cdf_kernel::{
    CdfError, OrderBy, QueryableResource, ResourceDescriptor, ResourceId, Result, ScanRequest,
    SchemaSource, ScopeKey, SortDirection, TrustLevel, WriteDisposition,
};
use cdf_runtime::{SourceDriverId, SourceEgressScope};
use cdf_source_postgres::{
    PostgresSourceResource, PostgresTarget, discover_postgres_table_catalog_schema,
};
use fallible_iterator::FallibleIterator;
use futures_util::StreamExt;
use nix::sys::{
    resource::{UsageWho, getrusage},
    time::TimeValLike,
};
use postgres::{Client, IsolationLevel, NoTls, binary_copy::BinaryCopyOutIter, types::FromSql};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const PASS_RATIO_PPM: u64 = 900_000;
const MAX_MAD_PERCENT: u64 = 10;
const MANAGED_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const POSTGRES_MAXIMUM_BATCH_BYTES: u64 = 32 * 1024 * 1024;
const IN_FLIGHT_BATCH_BOUND: usize = 3;
const DIRECT_BATCH_ROWS: usize = 64 * 1024;
const LABEL_CARDINALITY: u64 = 1_024;
const AMOUNT_SCALE_FACTOR: i128 = 1_000_000_000;
const AMOUNT_FRACTION: i128 = 123_456_789;
const WIDE_SCALE_FACTOR: i128 = 1_000_000_000_000_000_000;
const WIDE_FRACTION: i128 = 123_456_789_012_345_678;
const NARROW_TABLE: &str = "cdf_postgres_source_roofline_narrow";
const MIXED_TABLE: &str = "cdf_postgres_source_roofline_mixed";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresRooflineSample {
    pub wall_time_ns: u64,
    pub cpu_time_ns: u64,
    pub peak_rss_bytes: u64,
    pub rows: u64,
    pub useful_arrow_bytes: u64,
    pub content_checksum: u64,
    pub batch_count: u64,
    pub maximum_batch_rows: u64,
    pub maximum_batch_retained_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresRooflineCell {
    pub shape: String,
    pub canonical_copy_query: String,
    pub cdf_samples: Vec<PostgresRooflineSample>,
    pub direct_samples: Vec<PostgresRooflineSample>,
    pub cdf_median_ns: u64,
    pub direct_median_ns: u64,
    pub cdf_mad_ns: u64,
    pub direct_mad_ns: u64,
    pub roofline_ratio_ppm: u64,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresSourceRooflineReport {
    pub schema_version: u16,
    pub status: String,
    pub endpoint_authority: String,
    pub server_version: String,
    pub client_version: String,
    pub protocol: String,
    pub snapshot: String,
    pub rows: u64,
    pub samples: u32,
    pub maximum_batch_bytes: u64,
    pub in_flight_batch_bound: usize,
    pub connection_count: u16,
    pub client_concurrency: u16,
    pub physical_wire_bytes: Option<u64>,
    pub physical_wire_bytes_reason: String,
    pub host: HostFingerprint,
    pub comparability: Vec<ComparabilityKey>,
    pub workspace_content_sha256: String,
    pub workspace_content_inputs: Vec<String>,
    pub executable_sha256: String,
    pub semantic_bias: Vec<String>,
    pub cells: Vec<PostgresRooflineCell>,
}

#[derive(Clone, Copy)]
enum Shape {
    Narrow,
    Mixed,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Self::Narrow => "narrow_numeric",
            Self::Mixed => "mixed_text_decimal",
        }
    }

    fn table(self) -> &'static str {
        match self {
            Self::Narrow => NARROW_TABLE,
            Self::Mixed => MIXED_TABLE,
        }
    }

    fn copy_query(self) -> String {
        let projection = match self {
            Self::Narrow => concat!(
                "\"id\"::bigint AS \"id\", ",
                "\"metric\"::bigint AS \"metric\", ",
                "\"updated_at\"::bigint AS \"updated_at\""
            ),
            Self::Mixed => concat!(
                "\"id\"::bigint AS \"id\", ",
                "\"label\"::text AS \"label\", ",
                "\"amount\"::numeric AS \"amount\", ",
                "\"wide\"::numeric AS \"wide\""
            ),
        };
        format!(
            "COPY (SELECT {projection} FROM \"{}\" ORDER BY \"id\"::bigint ASC) TO STDOUT WITH (FORMAT BINARY)",
            self.table()
        )
    }

    fn postgres_types(self) -> &'static [postgres::types::Type] {
        use postgres::types::Type;
        match self {
            Self::Narrow => &[Type::INT8, Type::INT8, Type::INT8],
            Self::Mixed => &[Type::INT8, Type::TEXT, Type::NUMERIC, Type::NUMERIC],
        }
    }
}

pub fn run_postgres_source_roofline(
    database_url: &str,
    output: &Path,
    samples: u32,
    rows: u64,
) -> BenchResult<PostgresSourceRooflineReport> {
    if cfg!(debug_assertions) {
        return Err(bench_error(
            "Postgres source roofline must run from a release build",
        ));
    }
    if samples < 3 || rows < 100_000 {
        return Err(bench_error(
            "Postgres source roofline requires at least three samples and 100,000 rows",
        ));
    }
    let mut setup = Client::connect(database_url, NoTls)?;
    let server_version: String = setup.query_one("SHOW server_version", &[])?.get(0);
    if !server_version.starts_with("17.") {
        return Err(bench_error(format!(
            "Postgres source closure roofline requires PostgreSQL 17, observed {server_version}"
        )));
    }
    seed_fixture(&mut setup, rows)?;
    drop(setup);

    let (execution_host, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(MANAGED_MEMORY_BYTES)?;
    let labels = (0..LABEL_CARDINALITY)
        .map(|value| format!("label-{value}"))
        .collect::<Vec<_>>();
    let mut cells = Vec::new();
    for shape in [Shape::Narrow, Shape::Mixed] {
        let resource = compile_resource(database_url, shape, &execution)?;
        execution_host.block_on_root(read_cdf(resource.as_ref(), shape, rows, &labels))?;
        read_direct(database_url, shape, rows, &labels)?;

        let mut cdf_samples = Vec::with_capacity(samples as usize);
        let mut direct_samples = Vec::with_capacity(samples as usize);
        for index in 0..samples {
            if index.is_multiple_of(2) {
                cdf_samples.push(execution_host.block_on_root(read_cdf(
                    resource.as_ref(),
                    shape,
                    rows,
                    &labels,
                ))?);
                direct_samples.push(read_direct(database_url, shape, rows, &labels)?);
            } else {
                direct_samples.push(read_direct(database_url, shape, rows, &labels)?);
                cdf_samples.push(execution_host.block_on_root(read_cdf(
                    resource.as_ref(),
                    shape,
                    rows,
                    &labels,
                ))?);
            }
        }
        cells.push(roofline_cell(shape, cdf_samples, direct_samples));
    }

    let status = if cells.iter().any(|cell| cell.status == "fail") {
        "fail"
    } else if cells.iter().any(|cell| cell.status == "inconclusive") {
        "inconclusive"
    } else {
        "pass"
    };
    let workspace_root = std::env::current_dir()?;
    let (workspace_content_sha256, workspace_content_inputs) =
        workspace_content_revision(&workspace_root)?;
    let executable_sha256 = file_revision(&std::env::current_exe()?)?;
    let cdf_revision = base_git_revision(&workspace_root)?;
    let host_provider = SystemHostProvider::new(HostProbeConfig {
        cdf_version: env!("CARGO_PKG_VERSION").to_owned(),
        dependency_versions: BTreeMap::from([
            ("postgres".to_owned(), "0.19.14".to_owned()),
            ("postgres-server".to_owned(), server_version.clone()),
        ]),
        benchmark_profile: "release-postgres-source-roofline".to_owned(),
        storage_target: Some(output.parent().unwrap_or(Path::new(".")).to_path_buf()),
    });
    let host = host_provider.fingerprint()?;
    let host_key = host_class(&host)?;
    let comparability = [Shape::Narrow, Shape::Mixed]
        .into_iter()
        .map(|shape| ComparabilityKey {
            dataset_id: format!("postgres-source-roofline-{rows}-rows-{}", shape.name()),
            workload_id: format!("postgres-binary-copy-out-{}-c1", shape.name()),
            timed_region_version: 1,
            cdf_revision: cdf_revision.clone(),
            dependency_tuple: format!(
                "postgres=0.19.14;server={server_version};workspace={workspace_content_sha256};executable={executable_sha256}"
            ),
            host_class: host_key.clone(),
            os_toolchain: format!(
                "{}-{};{}",
                host.os.family, host.architecture, host.rust_version
            ),
            io_mode: IoMode::Warm,
        })
        .collect();
    let report = PostgresSourceRooflineReport {
        schema_version: 1,
        status: status.to_owned(),
        endpoint_authority: endpoint_authority(database_url)?,
        server_version,
        client_version: "postgres=0.19.14;tokio-postgres=0.7.18".to_owned(),
        protocol: "COPY (SELECT ...) TO STDOUT WITH (FORMAT BINARY)".to_owned(),
        snapshot: "read-only repeatable-read".to_owned(),
        rows,
        samples,
        maximum_batch_bytes: POSTGRES_MAXIMUM_BATCH_BYTES,
        in_flight_batch_bound: IN_FLIGHT_BATCH_BOUND,
        connection_count: 1,
        client_concurrency: 1,
        physical_wire_bytes: None,
        physical_wire_bytes_reason: "the synchronous postgres CopyOutReader does not expose protocol-byte counters; no estimate is substituted".to_owned(),
        host,
        comparability,
        workspace_content_sha256,
        workspace_content_inputs,
        executable_sha256,
        semantic_bias: vec![
            "the direct official-client runner omits CDF descriptor checks, NUMERIC domain catalog validation, Arrow construction, retained-memory accounting, leases, batch headers, cancellation, and governed stream completion, so it is a favorable roofline".to_owned(),
            "both timed paths open one connection and one read-only repeatable-read transaction, execute the recorded identical canonical COPY query, consume its trailer/EOF, drop the transaction, and verify every emitted value".to_owned(),
            "the CDF stream has a one-batch queue; queue + producer + consumer gives a truthful maximum overlap of three batches under the 32 MiB per-batch ceiling".to_owned(),
        ],
        cells,
    };
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output, serde_json::to_vec_pretty(&report)?)?;
    if report.status != "pass" {
        return Err(bench_error(format!(
            "Postgres source roofline status is {}; report written to {}",
            report.status,
            output.display()
        )));
    }
    Ok(report)
}

fn seed_fixture(client: &mut Client, rows: u64) -> BenchResult<()> {
    let rows = i64::try_from(rows)?;
    client.batch_execute(&format!(
        "DROP TABLE IF EXISTS {NARROW_TABLE};
         DROP TABLE IF EXISTS {MIXED_TABLE};
         CREATE UNLOGGED TABLE {NARROW_TABLE} (
            id BIGINT PRIMARY KEY,
            metric BIGINT NOT NULL,
            updated_at BIGINT NOT NULL
         );
         CREATE UNLOGGED TABLE {MIXED_TABLE} (
            id BIGINT PRIMARY KEY,
            label TEXT NOT NULL,
            amount NUMERIC(38,9) NOT NULL,
            wide NUMERIC(60,18) NOT NULL
         );"
    ))?;
    client.execute(
        &format!(
            "INSERT INTO {NARROW_TABLE} SELECT g, g * 17, g FROM generate_series(0, $1::bigint - 1) AS g"
        ),
        &[&rows],
    )?;
    client.execute(
        &format!(
            "INSERT INTO {MIXED_TABLE} SELECT g, 'label-' || (g % {LABEL_CARDINALITY})::text, \
             (g::numeric + 0.123456789)::numeric(38,9), \
             (g::numeric + 0.123456789012345678)::numeric(60,18) \
             FROM generate_series(0, $1::bigint - 1) AS g"
        ),
        &[&rows],
    )?;
    client.batch_execute(&format!("ANALYZE {NARROW_TABLE}; ANALYZE {MIXED_TABLE};"))?;
    Ok(())
}

fn compile_resource(
    database_url: &str,
    shape: Shape,
    execution: &cdf_runtime::ExecutionServices,
) -> BenchResult<Arc<dyn QueryableResource>> {
    let target = PostgresTarget::parse(shape.table())?;
    let egress = SourceEgressScope::new(
        SourceDriverId::new("postgres")?,
        Arc::new(EgressAllowlist::allow_any()),
    );
    let resource_id = ResourceId::new(format!("roofline.{}", shape.name()))?;
    let discovery =
        discover_postgres_table_catalog_schema(database_url, &resource_id, &target, &egress)?;
    let schema = Arc::new(discovery.schema);
    let descriptor = ResourceDescriptor {
        resource_id,
        schema_source: SchemaSource::Declared {
            schema_hash: cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?,
            source: "postgres-source-roofline".to_owned(),
        },
        primary_key: vec!["id".to_owned()],
        merge_key: Vec::new(),
        cursor: None,
        write_disposition: WriteDisposition::Append,
        deduplication: None,
        contract: None,
        state_scope: ScopeKey::Resource,
        freshness: None,
        trust_level: TrustLevel::Governed,
    };
    let resource =
        PostgresSourceResource::new_table(database_url, descriptor, schema, target, egress)?
            .with_execution(execution.clone())?;
    Ok(Arc::new(resource))
}

async fn read_cdf(
    resource: &dyn QueryableResource,
    shape: Shape,
    expected_rows: u64,
    labels: &[String],
) -> Result<PostgresRooflineSample> {
    let scan = resource.negotiate(&ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        scope: resource.descriptor().state_scope.clone(),
    })?;
    let partition = scan
        .inline_partitions()
        .and_then(|partitions| partitions.first())
        .cloned()
        .ok_or_else(|| CdfError::internal("Postgres roofline negotiated no partition"))?;
    let (cpu_before, _) = process_counters()?;
    let started = Instant::now();
    let mut stream = resource.open(partition).await?;
    let mut verifier = Verifier::new(shape, labels);
    let mut batch_count = 0_u64;
    let mut maximum_batch_rows = 0_u64;
    let mut maximum_batch_retained_bytes = 0_u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let record_batch = batch.record_batch().ok_or_else(|| {
            CdfError::internal("Postgres roofline source emitted a nonmaterialized batch")
        })?;
        verifier.verify_batch(record_batch)?;
        let retained = cdf_memory::record_batch_retained_bytes(record_batch)?;
        maximum_batch_retained_bytes = maximum_batch_retained_bytes.max(retained);
        maximum_batch_rows = maximum_batch_rows.max(record_batch.num_rows() as u64);
        batch_count = batch_count.saturating_add(1);
    }
    stream.completion().await?;
    let elapsed = elapsed_ns(started);
    let (cpu_after, peak_rss_bytes) = process_counters()?;
    let (rows, useful_arrow_bytes, content_checksum) = verifier.finish(expected_rows)?;
    black_box((useful_arrow_bytes, content_checksum));
    Ok(PostgresRooflineSample {
        wall_time_ns: elapsed,
        cpu_time_ns: cpu_after.saturating_sub(cpu_before),
        peak_rss_bytes,
        rows,
        useful_arrow_bytes,
        content_checksum,
        batch_count,
        maximum_batch_rows,
        maximum_batch_retained_bytes,
    })
}

fn read_direct(
    database_url: &str,
    shape: Shape,
    expected_rows: u64,
    labels: &[String],
) -> BenchResult<PostgresRooflineSample> {
    let (cpu_before, _) = process_counters()?;
    let started = Instant::now();
    let mut verifier = Verifier::new(shape, labels);
    let mut builders = DirectBuilders::new(shape);
    let mut batch_count = 0_u64;
    let mut maximum_batch_rows = 0_u64;
    let mut maximum_batch_retained_bytes = 0_u64;
    {
        let mut client = Client::connect(database_url, NoTls)?;
        let mut transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()?;
        let reader = transaction.copy_out(&shape.copy_query())?;
        let mut rows = BinaryCopyOutIter::new(reader, shape.postgres_types());
        while let Some(row) = rows.next()? {
            builders.append(&row)?;
            if builders.len() == DIRECT_BATCH_ROWS {
                verify_direct_batch(
                    &mut builders,
                    &mut verifier,
                    &mut batch_count,
                    &mut maximum_batch_rows,
                    &mut maximum_batch_retained_bytes,
                )?;
            }
        }
        if builders.len() != 0 {
            verify_direct_batch(
                &mut builders,
                &mut verifier,
                &mut batch_count,
                &mut maximum_batch_rows,
                &mut maximum_batch_retained_bytes,
            )?;
        }
        drop(rows);
        drop(transaction);
        drop(client);
    }
    let elapsed = elapsed_ns(started);
    let (cpu_after, peak_rss_bytes) = process_counters()?;
    let (rows, useful_arrow_bytes, content_checksum) = verifier.finish(expected_rows)?;
    black_box((useful_arrow_bytes, content_checksum));
    Ok(PostgresRooflineSample {
        wall_time_ns: elapsed,
        cpu_time_ns: cpu_after.saturating_sub(cpu_before),
        peak_rss_bytes,
        rows,
        useful_arrow_bytes,
        content_checksum,
        batch_count,
        maximum_batch_rows,
        maximum_batch_retained_bytes,
    })
}

enum DirectBuilders {
    Narrow {
        schema: SchemaRef,
        id: Int64Builder,
        metric: Int64Builder,
        updated_at: Int64Builder,
    },
    Mixed {
        schema: SchemaRef,
        id: Int64Builder,
        label: StringBuilder,
        amount: Decimal128Builder,
        wide: Decimal256Builder,
    },
}

impl DirectBuilders {
    fn new(shape: Shape) -> Self {
        match shape {
            Shape::Narrow => Self::Narrow {
                schema: Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("metric", DataType::Int64, false),
                    Field::new("updated_at", DataType::Int64, false),
                ])),
                id: Int64Builder::with_capacity(DIRECT_BATCH_ROWS),
                metric: Int64Builder::with_capacity(DIRECT_BATCH_ROWS),
                updated_at: Int64Builder::with_capacity(DIRECT_BATCH_ROWS),
            },
            Shape::Mixed => Self::Mixed {
                schema: Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("label", DataType::Utf8, false),
                    Field::new("amount", DataType::Decimal128(38, 9), false),
                    Field::new("wide", DataType::Decimal256(60, 18), false),
                ])),
                id: Int64Builder::with_capacity(DIRECT_BATCH_ROWS),
                label: StringBuilder::with_capacity(DIRECT_BATCH_ROWS, DIRECT_BATCH_ROWS * 10),
                amount: Decimal128Builder::with_capacity(DIRECT_BATCH_ROWS)
                    .with_data_type(DataType::Decimal128(38, 9)),
                wide: Decimal256Builder::with_capacity(DIRECT_BATCH_ROWS)
                    .with_data_type(DataType::Decimal256(60, 18)),
            },
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Narrow { id, .. } | Self::Mixed { id, .. } => id.len(),
        }
    }

    fn append(&mut self, row: &postgres::binary_copy::BinaryCopyOutRow) -> BenchResult<()> {
        match self {
            Self::Narrow {
                id,
                metric,
                updated_at,
                ..
            } => {
                id.append_value(row.try_get::<i64>(0)?);
                metric.append_value(row.try_get::<i64>(1)?);
                updated_at.append_value(row.try_get::<i64>(2)?);
            }
            Self::Mixed {
                id,
                label,
                amount,
                wide,
                ..
            } => {
                id.append_value(row.try_get::<i64>(0)?);
                label.append_value(row.try_get::<&str>(1)?);
                let amount_raw = row.try_get::<RawNumeric<'_>>(2)?;
                amount.append_value(
                    decode_numeric(amount_raw.0, 9)?
                        .to_i128()
                        .ok_or_else(|| bench_error("direct amount exceeds Decimal128"))?,
                );
                let wide_raw = row.try_get::<RawNumeric<'_>>(3)?;
                wide.append_value(decode_numeric(wide_raw.0, 18)?);
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> BenchResult<RecordBatch> {
        Ok(match self {
            Self::Narrow {
                schema,
                id,
                metric,
                updated_at,
            } => RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(id.finish()),
                    Arc::new(metric.finish()),
                    Arc::new(updated_at.finish()),
                ],
            )?,
            Self::Mixed {
                schema,
                id,
                label,
                amount,
                wide,
            } => RecordBatch::try_new(
                Arc::clone(schema),
                vec![
                    Arc::new(id.finish()),
                    Arc::new(label.finish()),
                    Arc::new(amount.finish()),
                    Arc::new(wide.finish()),
                ],
            )?,
        })
    }
}

fn verify_direct_batch(
    builders: &mut DirectBuilders,
    verifier: &mut Verifier<'_>,
    batch_count: &mut u64,
    maximum_batch_rows: &mut u64,
    maximum_batch_retained_bytes: &mut u64,
) -> BenchResult<()> {
    let batch = builders.finish()?;
    verifier.verify_batch(&batch)?;
    let retained = cdf_memory::record_batch_retained_bytes(&batch)?;
    *maximum_batch_retained_bytes = (*maximum_batch_retained_bytes).max(retained);
    *maximum_batch_rows = (*maximum_batch_rows).max(batch.num_rows() as u64);
    *batch_count = batch_count.saturating_add(1);
    Ok(())
}

struct Verifier<'a> {
    shape: Shape,
    labels: &'a [String],
    next_id: u64,
    useful_arrow_bytes: u64,
    checksum: u64,
}

impl<'a> Verifier<'a> {
    fn new(shape: Shape, labels: &'a [String]) -> Self {
        Self {
            shape,
            labels,
            next_id: 0,
            useful_arrow_bytes: 0,
            checksum: 0,
        }
    }

    fn verify_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        match self.shape {
            Shape::Narrow => {
                if batch.num_columns() != 3 {
                    return Err(CdfError::data(
                        "Postgres narrow roofline column count changed",
                    ));
                }
                let id = typed::<Int64Array>(batch, 0, "id")?;
                let metric = typed::<Int64Array>(batch, 1, "metric")?;
                let updated = typed::<Int64Array>(batch, 2, "updated_at")?;
                for row in 0..batch.num_rows() {
                    self.observe_narrow(id.value(row), metric.value(row), updated.value(row))?;
                }
            }
            Shape::Mixed => {
                if batch.num_columns() != 4 {
                    return Err(CdfError::data(
                        "Postgres mixed roofline column count changed",
                    ));
                }
                let id = typed::<Int64Array>(batch, 0, "id")?;
                let label = typed::<StringArray>(batch, 1, "label")?;
                let amount = typed::<Decimal128Array>(batch, 2, "amount")?;
                let wide = typed::<Decimal256Array>(batch, 3, "wide")?;
                for row in 0..batch.num_rows() {
                    self.observe_mixed(
                        id.value(row),
                        label.value(row),
                        i256::from_i128(amount.value(row)),
                        wide.value(row),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn observe_narrow(&mut self, id: i64, metric: i64, updated_at: i64) -> Result<()> {
        let expected = i64::try_from(self.next_id)
            .map_err(|_| CdfError::data("Postgres roofline row identity exceeds i64"))?;
        if id != expected || metric != expected.wrapping_mul(17) || updated_at != expected {
            return Err(CdfError::data(format!(
                "Postgres narrow roofline value differed at row {}",
                self.next_id
            )));
        }
        self.checksum = mix(self.checksum, id as u64);
        self.checksum = mix(self.checksum, metric as u64);
        self.checksum = mix(self.checksum, updated_at as u64);
        self.useful_arrow_bytes = self.useful_arrow_bytes.saturating_add(24);
        self.next_id = self.next_id.saturating_add(1);
        Ok(())
    }

    fn observe_mixed(&mut self, id: i64, label: &str, amount: i256, wide: i256) -> Result<()> {
        let expected = i64::try_from(self.next_id)
            .map_err(|_| CdfError::data("Postgres roofline row identity exceeds i64"))?;
        let expected_label = &self.labels[(self.next_id % LABEL_CARDINALITY) as usize];
        let expected_amount = i256::from_i128(
            i128::from(expected)
                .checked_mul(AMOUNT_SCALE_FACTOR)
                .and_then(|value| value.checked_add(AMOUNT_FRACTION))
                .ok_or_else(|| CdfError::data("Postgres roofline amount overflowed"))?,
        );
        let expected_wide = i256::from_i128(expected.into())
            .checked_mul(i256::from_i128(WIDE_SCALE_FACTOR))
            .and_then(|value| value.checked_add(i256::from_i128(WIDE_FRACTION)))
            .ok_or_else(|| CdfError::data("Postgres roofline wide value overflowed"))?;
        if id != expected
            || label != expected_label
            || amount != expected_amount
            || wide != expected_wide
        {
            return Err(CdfError::data(format!(
                "Postgres mixed roofline value differed at row {}",
                self.next_id
            )));
        }
        self.checksum = mix(self.checksum, id as u64);
        for chunk in label.as_bytes().chunks(8) {
            let mut bytes = [0_u8; 8];
            bytes[..chunk.len()].copy_from_slice(chunk);
            self.checksum = mix(self.checksum, u64::from_le_bytes(bytes));
        }
        for chunk in amount.to_le_bytes().chunks_exact(8) {
            self.checksum = mix(self.checksum, u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        for chunk in wide.to_le_bytes().chunks_exact(8) {
            self.checksum = mix(self.checksum, u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        self.useful_arrow_bytes = self
            .useful_arrow_bytes
            .saturating_add(8 + 4 + label.len() as u64 + 16 + 32);
        self.next_id = self.next_id.saturating_add(1);
        Ok(())
    }

    fn finish(mut self, expected_rows: u64) -> Result<(u64, u64, u64)> {
        if self.next_id != expected_rows {
            return Err(CdfError::data(format!(
                "Postgres roofline read {} rows, expected {expected_rows}",
                self.next_id
            )));
        }
        if matches!(self.shape, Shape::Mixed) {
            self.useful_arrow_bytes = self.useful_arrow_bytes.saturating_add(4);
        }
        Ok((self.next_id, self.useful_arrow_bytes, self.checksum))
    }
}

fn typed<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a T> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| CdfError::data(format!("Postgres roofline field `{name}` has wrong type")))
}

struct RawNumeric<'a>(&'a [u8]);

impl<'a> FromSql<'a> for RawNumeric<'a> {
    fn from_sql(
        _type: &postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(raw))
    }

    fn accepts(type_: &postgres::types::Type) -> bool {
        *type_ == postgres::types::Type::NUMERIC
    }
}

fn decode_numeric(raw: &[u8], target_scale: i8) -> BenchResult<i256> {
    if raw.len() < 8 || !(raw.len() - 8).is_multiple_of(2) {
        return Err(bench_error("direct Postgres NUMERIC framing is invalid"));
    }
    let digits = usize::try_from(read_i16(raw, 0)?)?;
    let weight = read_i16(raw, 2)?;
    let sign = read_u16(raw, 4)?;
    let display_scale = read_u16(raw, 6)?;
    if sign != 0 || display_scale != u16::try_from(target_scale)? || raw.len() != 8 + digits * 2 {
        return Err(bench_error("direct Postgres NUMERIC domain changed"));
    }
    let mut value = i256::ZERO;
    for index in 0..digits {
        let digit = read_u16(raw, 8 + index * 2)?;
        if digit >= 10_000 {
            return Err(bench_error("direct Postgres NUMERIC digit is invalid"));
        }
        value = value
            .checked_mul(i256::from_i128(10_000))
            .and_then(|value| value.checked_add(i256::from_i128(i128::from(digit))))
            .ok_or_else(|| bench_error("direct Postgres NUMERIC exceeds Decimal256"))?;
    }
    let exponent = (i32::from(weight) - i32::try_from(digits)? + 1)
        .checked_mul(4)
        .and_then(|value| value.checked_add(i32::from(target_scale)))
        .ok_or_else(|| bench_error("direct Postgres NUMERIC exponent overflowed"))?;
    if exponent >= 0 {
        value = value
            .checked_mul(
                i256::from_i128(10)
                    .checked_pow(exponent.unsigned_abs())
                    .ok_or_else(|| bench_error("direct Postgres NUMERIC exponent is too large"))?,
            )
            .ok_or_else(|| bench_error("direct Postgres NUMERIC exceeds Decimal256"))?;
    } else {
        let divisor = i256::from_i128(10)
            .checked_pow(exponent.unsigned_abs())
            .ok_or_else(|| bench_error("direct Postgres NUMERIC exponent is too small"))?;
        if value.checked_rem(divisor) != Some(i256::ZERO) {
            return Err(bench_error("direct Postgres NUMERIC would discard digits"));
        }
        value = value
            .checked_div(divisor)
            .ok_or_else(|| bench_error("direct Postgres NUMERIC division failed"))?;
    }
    Ok(value)
}

fn read_i16(raw: &[u8], offset: usize) -> BenchResult<i16> {
    Ok(i16::from_be_bytes(
        raw.get(offset..offset + 2)
            .ok_or_else(|| bench_error("direct Postgres NUMERIC is truncated"))?
            .try_into()?,
    ))
}

fn read_u16(raw: &[u8], offset: usize) -> BenchResult<u16> {
    Ok(u16::from_be_bytes(
        raw.get(offset..offset + 2)
            .ok_or_else(|| bench_error("direct Postgres NUMERIC is truncated"))?
            .try_into()?,
    ))
}

fn roofline_cell(
    shape: Shape,
    cdf_samples: Vec<PostgresRooflineSample>,
    direct_samples: Vec<PostgresRooflineSample>,
) -> PostgresRooflineCell {
    let cdf_times = cdf_samples
        .iter()
        .map(|sample| sample.wall_time_ns)
        .collect::<Vec<_>>();
    let direct_times = direct_samples
        .iter()
        .map(|sample| sample.wall_time_ns)
        .collect::<Vec<_>>();
    let cdf_median_ns = median(&cdf_times);
    let direct_median_ns = median(&direct_times);
    let cdf_mad_ns = median_absolute_deviation(&cdf_times, cdf_median_ns);
    let direct_mad_ns = median_absolute_deviation(&direct_times, direct_median_ns);
    let roofline_ratio_ppm = direct_median_ns
        .saturating_mul(1_000_000)
        .checked_div(cdf_median_ns)
        .unwrap_or(0);
    let equivalent = cdf_samples.iter().chain(&direct_samples).all(|sample| {
        sample.rows == cdf_samples[0].rows
            && sample.useful_arrow_bytes == cdf_samples[0].useful_arrow_bytes
            && sample.content_checksum == cdf_samples[0].content_checksum
    });
    let variance_ok = [
        (cdf_mad_ns, cdf_median_ns),
        (direct_mad_ns, direct_median_ns),
    ]
    .into_iter()
    .all(|(mad, median)| {
        mad.saturating_mul(100)
            .checked_div(median)
            .unwrap_or(u64::MAX)
            <= MAX_MAD_PERCENT
    });
    let status = if !equivalent || roofline_ratio_ppm < PASS_RATIO_PPM {
        "fail"
    } else if !variance_ok {
        "inconclusive"
    } else {
        "pass"
    };
    PostgresRooflineCell {
        shape: shape.name().to_owned(),
        canonical_copy_query: shape.copy_query(),
        cdf_samples,
        direct_samples,
        cdf_median_ns,
        direct_median_ns,
        cdf_mad_ns,
        direct_mad_ns,
        roofline_ratio_ppm,
        status: status.to_owned(),
    }
}

fn mix(checksum: u64, value: u64) -> u64 {
    checksum.rotate_left(11) ^ value.wrapping_mul(0x9E37_79B1_85EB_CA87)
}

fn median(values: &[u64]) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

fn median_absolute_deviation(values: &[u64], center: u64) -> u64 {
    median(
        &values
            .iter()
            .map(|value| value.abs_diff(center))
            .collect::<Vec<_>>(),
    )
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn process_counters() -> Result<(u64, u64)> {
    let usage = getrusage(UsageWho::RUSAGE_SELF)
        .map_err(|_| CdfError::environment("read Postgres roofline process counters"))?;
    let cpu_micros = usage
        .user_time()
        .num_microseconds()
        .saturating_add(usage.system_time().num_microseconds());
    let cpu_time_ns = u64::try_from(cpu_micros)
        .unwrap_or_default()
        .saturating_mul(1_000);
    let max_rss = u64::try_from(usage.max_rss())
        .map_err(|_| CdfError::environment("convert Postgres roofline peak RSS"))?;
    let peak_rss_bytes = if cfg!(any(target_os = "macos", target_os = "ios")) {
        max_rss
    } else {
        max_rss.saturating_mul(1024)
    };
    Ok((cpu_time_ns, peak_rss_bytes))
}

fn endpoint_authority(database_url: &str) -> BenchResult<String> {
    let parsed = url::Url::parse(database_url)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| bench_error("Postgres endpoint has no host"))?;
    Ok(match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    })
}

fn base_git_revision(workspace_root: &Path) -> BenchResult<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args(["rev-parse", "HEAD"])
        .output()?;
    if !output.status.success() {
        return Err(bench_error(
            "Postgres roofline could not resolve the base Git revision",
        ));
    }
    let revision = String::from_utf8(output.stdout)?;
    let revision = revision.trim();
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(bench_error(
            "Postgres roofline resolved an invalid base Git revision",
        ));
    }
    Ok(revision.to_owned())
}

fn workspace_content_revision(workspace_root: &Path) -> BenchResult<(String, Vec<String>)> {
    const INPUTS: &[&str] = &[
        "Cargo.toml",
        "Cargo.lock",
        "crates/cdf-benchmarks/Cargo.toml",
        "crates/cdf-benchmarks/src/bin/postgres-source-roofline.rs",
        "crates/cdf-benchmarks/src/postgres_source_roofline.rs",
        "crates/cdf-postgres/src/lib.rs",
        "crates/cdf-source-postgres/Cargo.toml",
        "crates/cdf-source-postgres/src/binary_copy.rs",
        "crates/cdf-source-postgres/src/catalog.rs",
        "crates/cdf-source-postgres/src/driver.rs",
        "crates/cdf-source-postgres/src/lib.rs",
        "crates/cdf-source-postgres/src/source.rs",
    ];
    let mut hasher = Sha256::new();
    let mut inputs = Vec::with_capacity(INPUTS.len());
    for relative in INPUTS {
        let bytes = fs::read(workspace_root.join(relative))?;
        hasher.update(relative.as_bytes());
        hasher.update([0]);
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
        inputs.push((*relative).to_owned());
    }
    Ok((format!("sha256:{:x}", hasher.finalize()), inputs))
}

fn file_revision(path: &Path) -> BenchResult<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_queries_match_the_production_canonical_cast_boundary() {
        assert_eq!(
            Shape::Narrow.copy_query(),
            "COPY (SELECT \"id\"::bigint AS \"id\", \"metric\"::bigint AS \"metric\", \"updated_at\"::bigint AS \"updated_at\" FROM \"cdf_postgres_source_roofline_narrow\" ORDER BY \"id\"::bigint ASC) TO STDOUT WITH (FORMAT BINARY)"
        );
        assert_eq!(
            Shape::Mixed.copy_query(),
            "COPY (SELECT \"id\"::bigint AS \"id\", \"label\"::text AS \"label\", \"amount\"::numeric AS \"amount\", \"wide\"::numeric AS \"wide\" FROM \"cdf_postgres_source_roofline_mixed\" ORDER BY \"id\"::bigint ASC) TO STDOUT WITH (FORMAT BINARY)"
        );
    }
}
