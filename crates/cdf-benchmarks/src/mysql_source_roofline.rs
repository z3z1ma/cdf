use std::{
    collections::BTreeMap, fs, hint::black_box, path::Path, process::Command, sync::Arc,
    time::Instant,
};

use arrow_array::{
    Array, Int64Array, RecordBatch, StringArray, UInt64Array,
    builder::{ArrayBuilder, Int64Builder, StringBuilder, UInt64Builder},
};
use arrow_schema::{DataType, Field, Schema};
use cdf_bench_core::{
    BenchResult, ComparabilityKey, HostCapabilityProvider, HostFingerprint, HostProbeConfig,
    IoMode, SystemHostProvider, bench_error, host_class,
};
use cdf_http::{EgressAllowlist, SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{
    CdfError, DestinationProtocol, OrderBy, QueryableResource, Result, ScanRequest, SortDirection,
};
use cdf_runtime::{SourceRegistry, SourceResolutionContext};
use futures_util::StreamExt;
use mysql_async::{Conn, IsolationLevel, Opts, Params, Row, TxOpts, Value, prelude::Queryable};
use nix::sys::{
    resource::{UsageWho, getrusage},
    time::TimeValLike,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const PASS_RATIO_PPM: u64 = 900_000;
const MAX_MAD_PERCENT: u64 = 10;
const MANAGED_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const TABLE: &str = "cdf_mysql_source_roofline";
const LABEL_CARDINALITY: u64 = 1_024;
const IN_FLIGHT_BATCH_BOUND: usize = 3;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MySqlRooflineSample {
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
pub struct MySqlRooflineCell {
    pub shape: String,
    pub output_batch_rows: usize,
    pub cdf_samples: Vec<MySqlRooflineSample>,
    pub direct_samples: Vec<MySqlRooflineSample>,
    pub cdf_median_ns: u64,
    pub direct_median_ns: u64,
    pub cdf_mad_ns: u64,
    pub direct_mad_ns: u64,
    pub roofline_ratio_ppm: u64,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MySqlSourceRooflineReport {
    pub schema_version: u16,
    pub status: String,
    pub endpoint_authority: String,
    pub server_version: String,
    pub client_version: String,
    pub protocol: String,
    pub snapshot: String,
    pub rows: u64,
    pub samples: u32,
    pub selected_output_batch_rows: usize,
    pub selected_roofline_ratio_ppm: u64,
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
    pub cells: Vec<MySqlRooflineCell>,
}

#[derive(Clone, Copy, Debug)]
enum Shape {
    Table,
    NativeQuery,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Self::Table => "mixed_table",
            Self::NativeQuery => "native_query_window",
        }
    }

    fn source_option(self) -> (&'static str, String) {
        match self {
            Self::Table => ("table", TABLE.to_owned()),
            Self::NativeQuery => ("query", native_query()),
        }
    }

    fn fields(self) -> &'static [&'static str] {
        match self {
            Self::Table => &["id", "metric", "label", "amount", "payload"],
            Self::NativeQuery => &["id", "metric", "running_metric", "label"],
        }
    }

    fn direct_query(self) -> String {
        let projection = self
            .fields()
            .iter()
            .map(|field| format!("`{field}` AS `{field}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let relation = match self {
            Self::Table => format!("`{TABLE}`"),
            Self::NativeQuery => format!("({}) AS `_cdf_native_query`", native_query()),
        };
        format!("SELECT {projection} FROM {relation} ORDER BY `id` ASC")
    }
}

fn native_query() -> String {
    format!(
        "SELECT id, metric, SUM(metric) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_metric, label FROM `{TABLE}`"
    )
}

struct FixedSecret(String);

impl SecretProvider for FixedSecret {
    fn resolve(&self, _uri: &SecretUri) -> Result<SecretValue> {
        Ok(SecretValue::new(self.0.clone()))
    }
}

pub fn run_mysql_source_roofline(
    database_url: &str,
    output: &Path,
    samples: u32,
    rows: u64,
) -> BenchResult<MySqlSourceRooflineReport> {
    if cfg!(debug_assertions) {
        return Err(bench_error(
            "MySQL source roofline must run from a release build",
        ));
    }
    if samples < 3 || rows < 100_000 {
        return Err(bench_error(
            "MySQL source roofline requires at least three samples and 100,000 rows",
        ));
    }
    let (execution_host, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(MANAGED_MEMORY_BYTES)?;
    let server_version = execution.run_io(seed_fixture(database_url.to_owned(), rows))?;
    if !server_version.starts_with("8.4.") {
        return Err(bench_error(format!(
            "MySQL source roofline requires MySQL 8.4, observed {server_version}"
        )));
    }

    let mut registry = SourceRegistry::new();
    registry.register(cdf_source_mysql::MySqlSourceDriver::new()?)?;
    let mut cells = Vec::new();
    for shape in [Shape::Table, Shape::NativeQuery] {
        for output_batch_rows in [8_192_usize, 32_768, 65_536] {
            let fixture = tempfile::tempdir()?;
            let resource = compile_resource(
                database_url,
                shape,
                output_batch_rows,
                fixture.path(),
                &registry,
                &execution,
            )?;

            execution_host.block_on_root(read_cdf(resource.as_ref(), shape, rows))?;
            execution.run_io(read_direct(
                database_url.to_owned(),
                shape,
                output_batch_rows,
                rows,
            ))?;
            let mut cdf_samples = Vec::with_capacity(samples as usize);
            let mut direct_samples = Vec::with_capacity(samples as usize);
            for index in 0..samples {
                if index.is_multiple_of(2) {
                    cdf_samples.push(execution_host.block_on_root(read_cdf(
                        resource.as_ref(),
                        shape,
                        rows,
                    ))?);
                    direct_samples.push(execution.run_io(read_direct(
                        database_url.to_owned(),
                        shape,
                        output_batch_rows,
                        rows,
                    ))?);
                } else {
                    direct_samples.push(execution.run_io(read_direct(
                        database_url.to_owned(),
                        shape,
                        output_batch_rows,
                        rows,
                    ))?);
                    cdf_samples.push(execution_host.block_on_root(read_cdf(
                        resource.as_ref(),
                        shape,
                        rows,
                    ))?);
                }
            }
            cells.push(roofline_cell(
                shape,
                output_batch_rows,
                cdf_samples,
                direct_samples,
            ));
        }
    }

    let selected_output_batch_rows = [8_192_usize, 32_768, 65_536]
        .into_iter()
        .filter_map(|batch_rows| {
            let selected = cells
                .iter()
                .filter(|cell| cell.output_batch_rows == batch_rows)
                .collect::<Vec<_>>();
            (selected.len() == 2 && selected.iter().all(|cell| cell.status == "pass")).then(|| {
                (
                    batch_rows,
                    selected.iter().map(|cell| cell.cdf_median_ns).sum::<u64>(),
                )
            })
        })
        .min_by_key(|(_, combined_cdf_ns)| *combined_cdf_ns)
        .map(|(batch_rows, _)| batch_rows);
    let selected_output_batch_rows = selected_output_batch_rows.unwrap_or_else(|| {
        cells
            .iter()
            .max_by_key(|cell| cell.roofline_ratio_ppm)
            .map(|cell| cell.output_batch_rows)
            .unwrap_or(65_536)
    });
    let selected = cells
        .iter()
        .filter(|cell| cell.output_batch_rows == selected_output_batch_rows)
        .collect::<Vec<_>>();
    let selected_roofline_ratio_ppm = selected
        .iter()
        .map(|cell| cell.roofline_ratio_ppm)
        .min()
        .unwrap_or(0);
    let status = if selected.len() == 2 && selected.iter().all(|cell| cell.status == "pass") {
        "pass"
    } else if selected.iter().any(|cell| cell.status == "fail") {
        "fail"
    } else {
        "inconclusive"
    };
    let workspace_root = std::env::current_dir()?;
    let (workspace_content_sha256, workspace_content_inputs) =
        workspace_content_revision(&workspace_root)?;
    let executable_sha256 = file_revision(&std::env::current_exe()?)?;
    let cdf_revision = base_git_revision(&workspace_root)?;
    let host_provider = SystemHostProvider::new(HostProbeConfig {
        cdf_version: env!("CARGO_PKG_VERSION").to_owned(),
        dependency_versions: BTreeMap::from([
            ("mysql_async".to_owned(), "0.37.0".to_owned()),
            ("mysql-server".to_owned(), server_version.clone()),
        ]),
        benchmark_profile: "bench-max-mysql-source-roofline".to_owned(),
        storage_target: Some(output.parent().unwrap_or(Path::new(".")).to_path_buf()),
    });
    let host = host_provider.fingerprint()?;
    let host_key = host_class(&host)?;
    let comparability = cells
        .iter()
        .map(|cell| ComparabilityKey {
            dataset_id: format!("mysql-source-roofline-{rows}-rows-{}", cell.shape),
            workload_id: format!(
                "mysql-prepared-binary-{}-batch{}-c1",
                cell.shape, cell.output_batch_rows
            ),
            timed_region_version: 1,
            cdf_revision: cdf_revision.clone(),
            dependency_tuple: format!(
                "mysql_async=0.37.0;server={server_version};workspace={workspace_content_sha256};executable={executable_sha256}"
            ),
            host_class: host_key.clone(),
            os_toolchain: format!(
                "{}-{};{}",
                host.os.family, host.architecture, host.rust_version
            ),
            io_mode: IoMode::Warm,
        })
        .collect();
    let report = MySqlSourceRooflineReport {
        schema_version: 1,
        status: status.to_owned(),
        endpoint_authority: endpoint_authority(database_url)?,
        server_version,
        client_version: "mysql_async=0.37.0;mysql_common=0.36.2".to_owned(),
        protocol: "official asynchronous prepared binary result stream".to_owned(),
        snapshot: "read-only repeatable-read consistent snapshot".to_owned(),
        rows,
        samples,
        selected_output_batch_rows,
        selected_roofline_ratio_ppm,
        maximum_batch_bytes: 64 * 1024 * 1024,
        in_flight_batch_bound: IN_FLIGHT_BATCH_BOUND,
        connection_count: 1,
        client_concurrency: 1,
        physical_wire_bytes: None,
        physical_wire_bytes_reason: "mysql_async does not expose complete compressed protocol-byte counters; no estimate is substituted".to_owned(),
        host,
        comparability,
        workspace_content_sha256,
        workspace_content_inputs,
        executable_sha256,
        semantic_bias: vec![
            "both paths open one mysql_async connection and one read-only repeatable-read consistent-snapshot transaction, execute the same prepared binary SELECT, build equivalent Arrow batches, consume EOF, roll back, and verify ordered row identity plus a full content checksum".to_owned(),
            "the direct path omits CDF descriptor, prepared-generation, retained-memory lease, batch-header, cancellation, egress, and governed completion checks, so it is a favorable roofline".to_owned(),
            "output batch rows are swept independently of the server's streaming binary result protocol; one CDF queue plus producer and consumer bounds retained overlap to three batches".to_owned(),
        ],
        cells,
    };
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output, serde_json::to_vec_pretty(&report)?)?;
    if report.status != "pass" {
        return Err(bench_error(format!(
            "MySQL source roofline status is {}; selected ratio {:.3}; report written to {}",
            report.status,
            report.selected_roofline_ratio_ppm as f64 / 1_000_000.0,
            output.display()
        )));
    }
    Ok(report)
}

async fn seed_fixture(database_url: String, rows: u64) -> Result<String> {
    let opts = Opts::from_url(&database_url)
        .map_err(|_| CdfError::auth("MySQL roofline URL is invalid"))?;
    let mut connection = Conn::new(opts)
        .await
        .map_err(|error| CdfError::environment(format!("connect to MySQL roofline: {error}")))?;
    let version = connection
        .query_first::<String, _>("SELECT VERSION()")
        .await
        .map_err(|error| CdfError::environment(format!("read MySQL version: {error}")))?
        .ok_or_else(|| CdfError::data("MySQL VERSION() returned no row"))?;
    connection
        .query_drop(format!(
            "DROP TABLE IF EXISTS `{TABLE}`; CREATE TABLE `{TABLE}` (id BIGINT UNSIGNED PRIMARY KEY, metric BIGINT NOT NULL, label VARCHAR(64) NOT NULL, amount DECIMAL(38,9) NOT NULL, payload JSON NOT NULL)"
        ))
        .await
        .map_err(|error| CdfError::environment(format!("create MySQL roofline fixture: {error}")))?;
    let digits = "(SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)";
    let generator = format!(
        "SELECT a.d + 10*b.d + 100*c.d + 1000*d.d + 10000*e.d + 100000*f.d AS n FROM {digits} a CROSS JOIN {digits} b CROSS JOIN {digits} c CROSS JOIN {digits} d CROSS JOIN {digits} e CROSS JOIN {digits} f"
    );
    connection
        .exec_drop(
            format!(
                "INSERT INTO `{TABLE}` (id, metric, label, amount, payload) SELECT n, n * 17, CONCAT('label-', MOD(n, {LABEL_CARDINALITY})), CAST(n + 0.123456789 AS DECIMAL(38,9)), JSON_OBJECT('id', n, 'metric', n * 17) FROM ({generator}) AS generated_rows WHERE n < ? ORDER BY n"
            ),
            (rows,),
        )
        .await
        .map_err(|error| CdfError::environment(format!("seed MySQL roofline fixture: {error}")))?;
    Ok(version)
}

fn compile_resource(
    database_url: &str,
    shape: Shape,
    output_batch_rows: usize,
    root: &Path,
    registry: &SourceRegistry,
    execution: &cdf_runtime::ExecutionServices,
) -> BenchResult<Arc<dyn QueryableResource>> {
    let secret_ref = "secret://bench/mysql";
    let project_toml = format!(
        r#"[project]
id = "test-project"
name = "mysql_source_roofline"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.sqlite"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[sources.roofline]
type = "mysql"
connection = "{secret_ref}"
fetch_rows = 8192
output_batch_rows = {output_batch_rows}
"#
    );
    let resource_dir = root.join("cdf/roofline");
    fs::create_dir_all(&resource_dir)?;
    fs::write(root.join("cdf.toml"), &project_toml)?;
    let (option, value) = shape.source_option();
    let fields = shape
        .fields()
        .iter()
        .map(|field| format!("  \"{field}\""))
        .collect::<Vec<_>>()
        .join(",\n");
    fs::write(
        resource_dir.join(format!("{}.cdf.sql", shape.name())),
        format!(
            "RESOURCE\nDISPOSITION REPLACE\nTRUST GOVERNED\nEXECUTION BOUNDED\nAS\nSELECT\n{fields}\nFROM upstream(source => 'roofline', {option} => '{}');\n",
            value.replace('\'', "''")
        ),
    )?;
    let config = cdf_project::parse_cdf_toml(&project_toml)?;
    let destination = cdf_dest_duckdb::DuckDbDestination::new(root.join(".cdf/compile.duckdb"))?;
    let mut entries = cdf_project::compile_query_project_resources(
        registry,
        &config,
        root,
        "dev",
        destination.sheet(),
        &cdf_semantic::SemanticCatalog::builtins()?,
        &BTreeMap::new(),
    )?;
    let mut entry = entries
        .pop()
        .ok_or_else(|| bench_error("MySQL roofline compiled no resource"))?;
    if !entries.is_empty() {
        return Err(bench_error("MySQL roofline compiled multiple resources"));
    }
    let provisional = entry.resource.clone();
    let context = SourceResolutionContext::new(
        root,
        Arc::new(FixedSecret(database_url.to_owned())),
        execution,
        Arc::new(EgressAllowlist::allow_any()),
    );
    let mut discovery = cdf_project::discover_resource_schema_with_source_registry(
        &provisional,
        registry,
        provisional.source_plan(),
        &context,
        cdf_project::SchemaDiscoveryExecutionOptions::new(),
    )?;
    entry.resource =
        cdf_project::compile_discovered_schema_artifacts(&provisional, &mut discovery)?;
    entry = cdf_project::finalize_query_project_resource(
        entry,
        &cdf_semantic::SemanticCatalog::builtins()?,
    )?;
    Ok(registry.resolve(entry.resource.source_plan(), &context)?)
}

async fn read_cdf(
    resource: &dyn QueryableResource,
    shape: Shape,
    expected_rows: u64,
) -> Result<MySqlRooflineSample> {
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
        .ok_or_else(|| CdfError::internal("MySQL roofline negotiated no partition"))?;
    let (cpu_before, _) = process_counters()?;
    let started = Instant::now();
    let mut stream = resource.open(partition).await?;
    let mut observation = Observation::new(shape);
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let record_batch = batch.record_batch().ok_or_else(|| {
            CdfError::internal("MySQL roofline source emitted a nonmaterialized batch")
        })?;
        observation.observe(record_batch)?;
    }
    stream.completion().await?;
    let elapsed = elapsed_ns(started);
    let (cpu_after, peak_rss_bytes) = process_counters()?;
    observation.finish(
        expected_rows,
        elapsed,
        cpu_after.saturating_sub(cpu_before),
        peak_rss_bytes,
    )
}

async fn read_direct(
    database_url: String,
    shape: Shape,
    output_batch_rows: usize,
    expected_rows: u64,
) -> Result<MySqlRooflineSample> {
    let (cpu_before, _) = process_counters()?;
    let started = Instant::now();
    let opts = Opts::from_url(&database_url)
        .map_err(|_| CdfError::auth("MySQL roofline URL is invalid"))?;
    let mut connection = Conn::new(opts).await.map_err(|error| {
        CdfError::environment(format!("connect direct MySQL roofline: {error}"))
    })?;
    let mut tx_options = TxOpts::new();
    tx_options
        .with_isolation_level(IsolationLevel::RepeatableRead)
        .with_consistent_snapshot(true)
        .with_readonly(true);
    let mut transaction = connection
        .start_transaction(tx_options)
        .await
        .map_err(|error| CdfError::environment(format!("begin direct MySQL snapshot: {error}")))?;
    let statement = transaction
        .prep(shape.direct_query())
        .await
        .map_err(|error| CdfError::environment(format!("prepare direct MySQL query: {error}")))?;
    let mut result = transaction
        .exec_iter(&statement, Params::Empty)
        .await
        .map_err(|error| CdfError::environment(format!("execute direct MySQL query: {error}")))?;
    let mut observation = Observation::new(shape);
    loop {
        let mut builder = DirectBuilder::new(shape, output_batch_rows);
        while builder.rows() < output_batch_rows {
            let Some(row) = result.next().await.map_err(|error| {
                CdfError::environment(format!("read direct MySQL result: {error}"))
            })?
            else {
                break;
            };
            builder.append(row)?;
        }
        if builder.rows() == 0 {
            break;
        }
        observation.observe(&builder.finish()?)?;
    }
    drop(result);
    transaction
        .rollback()
        .await
        .map_err(|error| CdfError::environment(format!("close direct MySQL snapshot: {error}")))?;
    let elapsed = elapsed_ns(started);
    let (cpu_after, peak_rss_bytes) = process_counters()?;
    observation.finish(
        expected_rows,
        elapsed,
        cpu_after.saturating_sub(cpu_before),
        peak_rss_bytes,
    )
}

struct DirectBuilder {
    shape: Shape,
    ids: UInt64Builder,
    first: Int64Builder,
    second: Option<StringBuilder>,
    text: StringBuilder,
    amount: Option<StringBuilder>,
    payload: Option<StringBuilder>,
}

impl DirectBuilder {
    fn new(shape: Shape, rows: usize) -> Self {
        Self {
            shape,
            ids: UInt64Builder::with_capacity(rows),
            first: Int64Builder::with_capacity(rows),
            second: matches!(shape, Shape::NativeQuery)
                .then(|| StringBuilder::with_capacity(rows, rows.saturating_mul(20))),
            text: StringBuilder::with_capacity(rows, rows.saturating_mul(12)),
            amount: matches!(shape, Shape::Table)
                .then(|| StringBuilder::with_capacity(rows, rows.saturating_mul(20))),
            payload: matches!(shape, Shape::Table)
                .then(|| StringBuilder::with_capacity(rows, rows.saturating_mul(32))),
        }
    }

    fn rows(&self) -> usize {
        self.ids.len()
    }

    fn append(&mut self, row: Row) -> Result<()> {
        let values = row.unwrap();
        match (self.shape, values.len()) {
            (Shape::Table, 5) => {
                self.ids.append_value(value_u64(&values[0], "id")?);
                self.first.append_value(value_i64(&values[1], "metric")?);
                self.text.append_value(value_utf8(&values[2], "label")?);
                self.amount
                    .as_mut()
                    .expect("table amount builder")
                    .append_value(value_utf8(&values[3], "amount")?);
                self.payload
                    .as_mut()
                    .expect("table payload builder")
                    .append_value(value_utf8(&values[4], "payload")?);
            }
            (Shape::NativeQuery, 4) => {
                self.ids.append_value(value_u64(&values[0], "id")?);
                self.first.append_value(value_i64(&values[1], "metric")?);
                self.second
                    .as_mut()
                    .expect("query running metric builder")
                    .append_value(value_utf8(&values[2], "running_metric")?);
                self.text.append_value(value_utf8(&values[3], "label")?);
            }
            _ => {
                return Err(CdfError::data(
                    "direct MySQL row differs from prepared roofline schema",
                ));
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        let (fields, arrays): (Vec<Field>, Vec<Arc<dyn Array>>) = match self.shape {
            Shape::Table => (
                vec![
                    Field::new("id", DataType::UInt64, false),
                    Field::new("metric", DataType::Int64, false),
                    Field::new("label", DataType::Utf8, false),
                    Field::new("amount", DataType::Utf8, false),
                    Field::new("payload", DataType::Utf8, false),
                ],
                vec![
                    Arc::new(self.ids.finish()),
                    Arc::new(self.first.finish()),
                    Arc::new(self.text.finish()),
                    Arc::new(self.amount.as_mut().expect("table amount").finish()),
                    Arc::new(self.payload.as_mut().expect("table payload").finish()),
                ],
            ),
            Shape::NativeQuery => (
                vec![
                    Field::new("id", DataType::UInt64, false),
                    Field::new("metric", DataType::Int64, false),
                    Field::new("running_metric", DataType::Utf8, false),
                    Field::new("label", DataType::Utf8, false),
                ],
                vec![
                    Arc::new(self.ids.finish()),
                    Arc::new(self.first.finish()),
                    Arc::new(self.second.as_mut().expect("query running").finish()),
                    Arc::new(self.text.finish()),
                ],
            ),
        };
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|error| CdfError::data(format!("build direct MySQL Arrow batch: {error}")))
    }
}

fn value_u64(value: &Value, field: &str) -> Result<u64> {
    match value {
        Value::UInt(value) => Ok(*value),
        Value::Int(value) => u64::try_from(*value)
            .map_err(|_| CdfError::data(format!("direct MySQL `{field}` is negative"))),
        _ => Err(CdfError::data(format!(
            "direct MySQL `{field}` is not an unsigned integer"
        ))),
    }
}

fn value_i64(value: &Value, field: &str) -> Result<i64> {
    match value {
        Value::Int(value) => Ok(*value),
        Value::UInt(value) => i64::try_from(*value)
            .map_err(|_| CdfError::data(format!("direct MySQL `{field}` exceeds i64"))),
        Value::Bytes(value) => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .ok_or_else(|| CdfError::data(format!("direct MySQL `{field}` is not an i64"))),
        _ => Err(CdfError::data(format!(
            "direct MySQL `{field}` is not an i64"
        ))),
    }
}

fn value_utf8<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    let Value::Bytes(value) = value else {
        return Err(CdfError::data(format!(
            "direct MySQL `{field}` is not text"
        )));
    };
    std::str::from_utf8(value)
        .map_err(|_| CdfError::data(format!("direct MySQL `{field}` is not UTF-8")))
}

struct Observation {
    shape: Shape,
    rows: u64,
    useful_arrow_bytes: u64,
    content_checksum: u64,
    batch_count: u64,
    maximum_batch_rows: u64,
    maximum_batch_retained_bytes: u64,
}

impl Observation {
    fn new(shape: Shape) -> Self {
        Self {
            shape,
            rows: 0,
            useful_arrow_bytes: 0,
            content_checksum: 0,
            batch_count: 0,
            maximum_batch_rows: 0,
            maximum_batch_retained_bytes: 0,
        }
    }

    fn observe(&mut self, batch: &RecordBatch) -> Result<()> {
        let ids = column::<UInt64Array>(batch, 0, "id")?;
        let metrics = column::<Int64Array>(batch, 1, "metric")?;
        if ids.len() != metrics.len() {
            return Err(CdfError::data("MySQL roofline column lengths differ"));
        }
        for index in 0..ids.len() {
            let expected = self.rows.saturating_add(index as u64);
            let id = ids.value(index);
            if id != expected {
                return Err(CdfError::data(format!(
                    "MySQL roofline row identity differs: expected {expected}, observed {id}"
                )));
            }
            self.content_checksum = self
                .content_checksum
                .rotate_left(7)
                .wrapping_add(id)
                .wrapping_add(metrics.value(index) as u64);
        }
        match self.shape {
            Shape::Table => {
                for column_index in [2, 3, 4] {
                    let values = column::<StringArray>(batch, column_index, "text")?;
                    self.useful_arrow_bytes = self.useful_arrow_bytes.saturating_add(
                        u64::try_from(values.len().saturating_add(1).saturating_mul(4))
                            .unwrap_or(u64::MAX),
                    );
                    for value in values.iter().flatten() {
                        self.content_checksum ^= text_checksum(value);
                        self.useful_arrow_bytes = self
                            .useful_arrow_bytes
                            .saturating_add(u64::try_from(value.len()).unwrap_or(u64::MAX));
                    }
                }
            }
            Shape::NativeQuery => {
                let running = column::<StringArray>(batch, 2, "running_metric")?;
                let labels = column::<StringArray>(batch, 3, "label")?;
                self.useful_arrow_bytes = self.useful_arrow_bytes.saturating_add(
                    u64::try_from(
                        running
                            .len()
                            .saturating_add(labels.len())
                            .saturating_add(2)
                            .saturating_mul(4),
                    )
                    .unwrap_or(u64::MAX),
                );
                for index in 0..running.len() {
                    self.content_checksum = self
                        .content_checksum
                        .wrapping_add(text_checksum(running.value(index)))
                        ^ text_checksum(labels.value(index));
                    self.useful_arrow_bytes = self
                        .useful_arrow_bytes
                        .saturating_add(
                            u64::try_from(running.value(index).len()).unwrap_or(u64::MAX),
                        )
                        .saturating_add(
                            u64::try_from(labels.value(index).len()).unwrap_or(u64::MAX),
                        );
                }
            }
        }
        let retained = cdf_memory::record_batch_retained_bytes(batch)?;
        self.maximum_batch_retained_bytes = self.maximum_batch_retained_bytes.max(retained);
        self.maximum_batch_rows = self.maximum_batch_rows.max(batch.num_rows() as u64);
        self.useful_arrow_bytes = self.useful_arrow_bytes.saturating_add(
            u64::try_from(batch.num_rows())
                .unwrap_or(u64::MAX)
                .saturating_mul(16),
        );
        self.rows = self.rows.saturating_add(batch.num_rows() as u64);
        self.batch_count = self.batch_count.saturating_add(1);
        Ok(())
    }

    fn finish(
        self,
        expected_rows: u64,
        wall_time_ns: u64,
        cpu_time_ns: u64,
        peak_rss_bytes: u64,
    ) -> Result<MySqlRooflineSample> {
        if self.rows != expected_rows {
            return Err(CdfError::data(format!(
                "MySQL roofline read {} rows, expected {expected_rows}",
                self.rows
            )));
        }
        black_box((self.useful_arrow_bytes, self.content_checksum));
        Ok(MySqlRooflineSample {
            wall_time_ns,
            cpu_time_ns,
            peak_rss_bytes,
            rows: self.rows,
            useful_arrow_bytes: self.useful_arrow_bytes,
            content_checksum: self.content_checksum,
            batch_count: self.batch_count,
            maximum_batch_rows: self.maximum_batch_rows,
            maximum_batch_retained_bytes: self.maximum_batch_retained_bytes,
        })
    }
}

fn column<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a T> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| CdfError::data(format!("MySQL roofline `{name}` has the wrong Arrow type")))
}

fn text_checksum(value: &str) -> u64 {
    value.bytes().fold(0_u64, |checksum, byte| {
        checksum.rotate_left(5) ^ u64::from(byte)
    })
}

fn roofline_cell(
    shape: Shape,
    output_batch_rows: usize,
    cdf_samples: Vec<MySqlRooflineSample>,
    direct_samples: Vec<MySqlRooflineSample>,
) -> MySqlRooflineCell {
    let cdf_wall = cdf_samples
        .iter()
        .map(|sample| sample.wall_time_ns)
        .collect::<Vec<_>>();
    let direct_wall = direct_samples
        .iter()
        .map(|sample| sample.wall_time_ns)
        .collect::<Vec<_>>();
    let cdf_median_ns = median(&cdf_wall);
    let direct_median_ns = median(&direct_wall);
    let cdf_mad_ns = median_absolute_deviation(&cdf_wall, cdf_median_ns);
    let direct_mad_ns = median_absolute_deviation(&direct_wall, direct_median_ns);
    let roofline_ratio_ppm = direct_median_ns
        .saturating_mul(1_000_000)
        .checked_div(cdf_median_ns)
        .unwrap_or(0);
    let variance_ok = [
        (cdf_mad_ns, cdf_median_ns),
        (direct_mad_ns, direct_median_ns),
    ]
    .into_iter()
    .all(|(mad, center)| {
        mad.saturating_mul(100)
            .checked_div(center)
            .unwrap_or(u64::MAX)
            <= MAX_MAD_PERCENT
    });
    let equivalent = cdf_samples.iter().chain(&direct_samples).all(|sample| {
        sample.rows == cdf_samples[0].rows
            && sample.useful_arrow_bytes == cdf_samples[0].useful_arrow_bytes
            && sample.content_checksum == cdf_samples[0].content_checksum
    });
    let status = if !equivalent {
        "fail"
    } else if !variance_ok {
        "inconclusive"
    } else if roofline_ratio_ppm >= PASS_RATIO_PPM {
        "pass"
    } else {
        "fail"
    };
    MySqlRooflineCell {
        shape: shape.name().to_owned(),
        output_batch_rows,
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

fn median(values: &[u64]) -> u64 {
    let mut values = values.to_vec();
    values.sort_unstable();
    values[values.len() / 2]
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
        .map_err(|_| CdfError::environment("read MySQL roofline process counters"))?;
    let cpu_micros = usage
        .user_time()
        .num_microseconds()
        .saturating_add(usage.system_time().num_microseconds());
    let cpu_time_ns = u64::try_from(cpu_micros)
        .unwrap_or_default()
        .saturating_mul(1_000);
    let max_rss = u64::try_from(usage.max_rss())
        .map_err(|_| CdfError::environment("convert MySQL roofline peak RSS"))?;
    let peak_rss_bytes = if cfg!(any(target_os = "macos", target_os = "ios")) {
        max_rss
    } else {
        max_rss.saturating_mul(1_024)
    };
    Ok((cpu_time_ns, peak_rss_bytes))
}

fn endpoint_authority(database_url: &str) -> BenchResult<String> {
    let parsed = url::Url::parse(database_url)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| bench_error("MySQL source URL has no host"))?;
    Ok(match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    })
}

fn workspace_content_revision(root: &Path) -> BenchResult<(String, Vec<String>)> {
    let inputs = vec![
        "Cargo.lock".to_owned(),
        "crates/cdf-source-mysql".to_owned(),
        "crates/cdf-runtime".to_owned(),
        "crates/cdf-kernel".to_owned(),
        "crates/cdf-memory".to_owned(),
        "crates/cdf-benchmarks/src/mysql_source_roofline.rs".to_owned(),
        "crates/cdf-benchmarks/src/bin/mysql-source-roofline.rs".to_owned(),
    ];
    let mut hasher = Sha256::new();
    for input in &inputs {
        let path = root.join(input);
        if path.is_file() {
            hasher.update(input.as_bytes());
            hasher.update(fs::read(path)?);
        } else {
            let output = Command::new("git")
                .args(["ls-files", "-z", "--", input])
                .current_dir(root)
                .output()?;
            for relative in output
                .stdout
                .split(|byte| *byte == 0)
                .filter(|p| !p.is_empty())
            {
                hasher.update(relative);
                hasher.update(fs::read(root.join(std::str::from_utf8(relative)?))?);
            }
        }
    }
    Ok((format!("sha256:{:x}", hasher.finalize()), inputs))
}

fn file_revision(path: &Path) -> BenchResult<String> {
    Ok(format!("sha256:{:x}", Sha256::digest(fs::read(path)?)))
}

fn base_git_revision(root: &Path) -> BenchResult<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()?;
    if !output.status.success() {
        return Err(bench_error(
            "MySQL roofline could not resolve the base Git revision",
        ));
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}
