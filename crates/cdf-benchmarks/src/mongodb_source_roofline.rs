use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    hint::black_box,
    io::Read,
    path::Path,
    process::Command,
    sync::Arc,
    time::Instant,
};

use arrow_array::{
    Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
    builder::{Int64Builder, StringBuilder, TimestampMillisecondBuilder},
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_bench_core::{
    BenchResult, ComparabilityKey, HostCapabilityProvider, HostFingerprint, HostProbeConfig,
    IoMode, SystemHostProvider, bench_error, host_class,
};
use cdf_http::{EgressAllowlist, SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{CdfError, DestinationProtocol, QueryableResource, Result, ScanRequest};
use cdf_runtime::{SourceRegistry, SourceResolutionContext};
use futures_util::StreamExt;
use mongodb::{
    Client,
    bson::{DateTime, Document, RawDocument, doc},
    options::{ClientOptions, ServerApi, ServerApiVersion},
};
use nix::sys::{
    resource::{UsageWho, getrusage},
    time::TimeValLike,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const PASS_RATIO_PPM: u64 = 900_000;
const MAX_MAD_PERCENT: u64 = 10;
const MANAGED_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const STREAM_BUFFER_BATCHES: usize = 1;
const IN_FLIGHT_BATCH_BOUND: usize = STREAM_BUFFER_BATCHES + 2;
const DATABASE: &str = "cdf_source_roofline";
const COLLECTION: &str = "events";
const LABEL_CARDINALITY: u64 = 1_024;
const EXPECTED_SERVER_VERSION: &str = "8.0.13";
const EXPECTED_SERVER_IMAGE: &str =
    "mongo@sha256:cf340b1e5283843c63eb12999922f20c463ae31285f746d30f05dcc21cd1d47c";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MongoDbRooflineSample {
    pub wall_time_ns: u64,
    pub cpu_time_ns: u64,
    pub peak_rss_bytes: u64,
    pub rows: u64,
    pub useful_arrow_bytes: u64,
    pub content_checksum: u64,
    pub batch_count: u64,
    pub maximum_batch_rows: u64,
    pub maximum_batch_retained_bytes: u64,
    pub observed_raw_bson_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MongoDbRooflineCell {
    pub batch_rows: u32,
    pub max_pool_size: u32,
    pub stream_buffer_batches: usize,
    pub client_concurrency: u16,
    pub cdf_samples: Vec<MongoDbRooflineSample>,
    pub direct_samples: Vec<MongoDbRooflineSample>,
    pub cdf_median_ns: u64,
    pub direct_median_ns: u64,
    pub cdf_mad_ns: u64,
    pub direct_mad_ns: u64,
    pub roofline_ratio_ppm: u64,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MongoDbSweepExclusion {
    pub dimension: String,
    pub value: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MongoDbSourceRooflineReport {
    pub schema_version: u16,
    pub status: String,
    pub endpoint_authority: String,
    pub server_image: String,
    pub server_version: String,
    pub client_version: String,
    pub protocol: String,
    pub rows: u64,
    pub samples: u32,
    pub batch_rows: u32,
    pub max_pool_size: u32,
    pub selection_policy: String,
    pub stream_buffer_batches: usize,
    pub in_flight_batch_bound: usize,
    pub connection_count: u16,
    pub client_concurrency: u16,
    pub useful_arrow_bytes: u64,
    pub content_checksum: u64,
    pub cdf_median_ns: u64,
    pub direct_median_ns: u64,
    pub roofline_ratio_ppm: u64,
    pub physical_wire_bytes: Option<u64>,
    pub physical_wire_bytes_reason: String,
    pub host: HostFingerprint,
    pub comparability: ComparabilityKey,
    pub workspace_content_sha256: String,
    pub workspace_content_inputs: Vec<String>,
    pub executable_sha256: String,
    pub semantic_bias: Vec<String>,
    pub sweep: Vec<MongoDbRooflineCell>,
    pub sweep_exclusions: Vec<MongoDbSweepExclusion>,
}

struct NoSecrets;

impl SecretProvider for NoSecrets {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
        Err(CdfError::auth(format!(
            "MongoDB source roofline has no secret for {uri}"
        )))
    }
}

pub fn run_mongodb_source_roofline(
    endpoint: &str,
    output: &Path,
    samples: u32,
    rows: u64,
) -> BenchResult<MongoDbSourceRooflineReport> {
    if cfg!(debug_assertions) {
        return Err(bench_error(
            "MongoDB source roofline must run from a release build",
        ));
    }
    if samples < 3 || rows < 100_000 {
        return Err(bench_error(
            "MongoDB source roofline requires at least three samples and 100,000 rows",
        ));
    }
    let (execution_host, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(MANAGED_MEMORY_BYTES)?;
    let setup_client = execution.run_io(mongodb_client(endpoint.to_owned(), 2))?;
    execution.run_io(seed_fixture(setup_client.clone(), rows))?;
    let server_version = execution.run_io(read_server_version(setup_client))?;
    if server_version != EXPECTED_SERVER_VERSION {
        return Err(bench_error(format!(
            "MongoDB closure roofline requires server {EXPECTED_SERVER_VERSION}, observed {server_version}"
        )));
    }
    let server_image = attest_local_docker_image(endpoint)?;

    let mut registry = SourceRegistry::new();
    registry.register(cdf_source_mongodb::MongoDbSourceDriver::new()?)?;
    let mut sweep = Vec::new();
    for (batch_rows, max_pool_size) in [
        (8_192, 1),
        (32_768, 1),
        (32_768, 2),
        (65_536, 1),
        (65_536, 2),
        (65_536, 4),
    ] {
        let fixture = tempfile::tempdir()?;
        let resource = compile_resource(
            endpoint,
            batch_rows,
            max_pool_size,
            fixture.path(),
            &registry,
            &execution,
        )?;
        let direct_client = execution.run_io(mongodb_client(endpoint.to_owned(), max_pool_size))?;

        execution_host.block_on_root(read_cdf(resource.as_ref(), rows))?;
        execution.run_io(read_direct(direct_client.clone(), batch_rows, rows))?;

        let mut cdf_samples = Vec::with_capacity(samples as usize);
        let mut direct_samples = Vec::with_capacity(samples as usize);
        for index in 0..samples {
            if index.is_multiple_of(2) {
                cdf_samples.push(execution_host.block_on_root(read_cdf(resource.as_ref(), rows))?);
                direct_samples.push(execution.run_io(read_direct(
                    direct_client.clone(),
                    batch_rows,
                    rows,
                ))?);
            } else {
                direct_samples.push(execution.run_io(read_direct(
                    direct_client.clone(),
                    batch_rows,
                    rows,
                ))?);
                cdf_samples.push(execution_host.block_on_root(read_cdf(resource.as_ref(), rows))?);
            }
        }
        sweep.push(roofline_cell(
            batch_rows,
            max_pool_size,
            cdf_samples,
            direct_samples,
        ));
    }

    let selected = select_cell(&sweep)?;
    let status = if selected.status == "pass" {
        "pass"
    } else {
        selected.status.as_str()
    };
    let workspace_root = std::env::current_dir()?;
    let (workspace_content_sha256, workspace_content_inputs) =
        workspace_content_revision(&workspace_root)?;
    let executable_sha256 = file_revision(&std::env::current_exe()?)?;
    let cdf_revision = base_git_revision(&workspace_root)?;
    let host_provider = SystemHostProvider::new(HostProbeConfig {
        cdf_version: env!("CARGO_PKG_VERSION").to_owned(),
        dependency_versions: BTreeMap::from([
            ("mongodb".to_owned(), "3.8.0".to_owned()),
            ("mongodb-server".to_owned(), server_version.clone()),
        ]),
        benchmark_profile: "release-mongodb-source-roofline".to_owned(),
        storage_target: Some(output.parent().unwrap_or(Path::new(".")).to_path_buf()),
    });
    let host = host_provider.fingerprint()?;
    let comparability = ComparabilityKey {
        dataset_id: format!("mongodb-source-roofline-{rows}-rows-mixed-bson"),
        workload_id: format!(
            "mongodb-raw-bson-batch{}-pool{}-reuse-c1",
            selected.batch_rows, selected.max_pool_size
        ),
        timed_region_version: 1,
        cdf_revision,
        dependency_tuple: format!(
            "mongodb=3.8.0;bson=3.1.0;server={server_version};workspace={workspace_content_sha256};executable={executable_sha256}"
        ),
        host_class: host_class(&host)?,
        os_toolchain: format!(
            "{}-{};{}",
            host.os.family, host.architecture, host.rust_version
        ),
        io_mode: IoMode::Warm,
    };
    let report = MongoDbSourceRooflineReport {
        schema_version: 1,
        status: status.to_owned(),
        endpoint_authority: endpoint_authority(endpoint)?,
        server_image,
        server_version,
        client_version: "mongodb=3.8.0;bson=3.1.0".to_owned(),
        protocol: "official asynchronous driver RawBatchCursor; full-document find + stable sort"
            .to_owned(),
        rows,
        samples,
        batch_rows: selected.batch_rows,
        max_pool_size: selected.max_pool_size,
        selection_policy: "minimum pool required for declared client concurrency; then fastest passing CDF median".to_owned(),
        stream_buffer_batches: STREAM_BUFFER_BATCHES,
        in_flight_batch_bound: IN_FLIGHT_BATCH_BOUND,
        connection_count: 1,
        client_concurrency: 1,
        useful_arrow_bytes: selected.cdf_samples[0].useful_arrow_bytes,
        content_checksum: selected.cdf_samples[0].content_checksum,
        cdf_median_ns: selected.cdf_median_ns,
        direct_median_ns: selected.direct_median_ns,
        roofline_ratio_ppm: selected.roofline_ratio_ppm,
        physical_wire_bytes: None,
        physical_wire_bytes_reason: "the official RawBatchCursor exposes decoded BSON envelope bytes but not complete compressed wire/protocol bytes; no estimate is substituted".to_owned(),
        host,
        comparability,
        workspace_content_sha256,
        workspace_content_inputs,
        executable_sha256,
        semantic_bias: vec![
            "the direct runner uses the same official client, full known-fixture documents, stable sort, RawBatchCursor, BSON field decoding, Arrow array construction, record-batch validation, and full content checksum as CDF".to_owned(),
            "CDF retains unknown-field schema evidence before materializing its governed projection; the fixed benchmark fixture contains exactly the four governed fields, so neither timed path receives extra document fields".to_owned(),
            "the direct runner omits CDF schema-evidence binding, descriptor checks, retained-memory accounting, batch headers, cancellation, egress, and governed stream completion, so it remains a favorable roofline".to_owned(),
            format!("stream_buffer_batches={STREAM_BUFFER_BATCHES} is queue capacity; queue + producer + consumer gives a truthful retained-batch overlap of {IN_FLIGHT_BATCH_BOUND}"),
            "both paths reuse one configured client/pool and issue one query at a time; pool-size sweep values above one measure pool bookkeeping rather than manufacturing concurrency".to_owned(),
        ],
        sweep,
        sweep_exclusions: vec![
            MongoDbSweepExclusion {
                dimension: "client_concurrency".to_owned(),
                value: "2+".to_owned(),
                reason: "the finite source advertises one logical partition; parallel reads would change snapshot and ordering semantics".to_owned(),
            },
            MongoDbSweepExclusion {
                dimension: "compression".to_owned(),
                value: "forced".to_owned(),
                reason: "wire compression is server/client negotiation rather than a source option; the local fixture uses the official defaults on both paths".to_owned(),
            },
            MongoDbSweepExclusion {
                dimension: "remote_atlas".to_owned(),
                value: "unavailable".to_owned(),
                reason: "no separately authorized Atlas credentials, external writes, or cost were supplied for local closure".to_owned(),
            },
        ],
    };
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output, serde_json::to_vec_pretty(&report)?)?;
    if report.status != "pass" {
        return Err(bench_error(format!(
            "MongoDB source roofline status is {}; selected ratio {:.3}; report written to {}",
            report.status,
            report.roofline_ratio_ppm as f64 / 1_000_000.0,
            output.display()
        )));
    }
    Ok(report)
}

fn select_cell(sweep: &[MongoDbRooflineCell]) -> BenchResult<&MongoDbRooflineCell> {
    if let Some(selected) = sweep
        .iter()
        .filter(|cell| cell.status == "pass")
        .min_by_key(|cell| (cell.max_pool_size, cell.cdf_median_ns))
    {
        return Ok(selected);
    }
    sweep
        .iter()
        .max_by_key(|cell| cell.roofline_ratio_ppm)
        .ok_or_else(|| bench_error("MongoDB source roofline produced no sweep cells"))
}

async fn mongodb_client(endpoint: String, max_pool_size: u32) -> Result<Client> {
    let mut options = ClientOptions::parse(&endpoint).await.map_err(|error| {
        CdfError::environment(format!("parse MongoDB roofline endpoint: {error}"))
    })?;
    options.app_name = Some("cdf-roofline".to_owned());
    options.max_pool_size = Some(max_pool_size);
    options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
    Client::with_options(options).map_err(|error| {
        CdfError::environment(format!("construct MongoDB roofline client: {error}"))
    })
}

async fn seed_fixture(client: Client, rows: u64) -> Result<()> {
    let collection = client.database(DATABASE).collection::<Document>(COLLECTION);
    collection.drop().await.map_err(|error| {
        CdfError::environment(format!("reset MongoDB roofline collection: {error}"))
    })?;
    for start in (0..rows).step_by(5_000) {
        let end = rows.min(start.saturating_add(5_000));
        let documents = (start..end)
            .map(|id| {
                let id = i64::try_from(id)
                    .map_err(|_| CdfError::data("MongoDB roofline row identity exceeds i64"))?;
                Ok(doc! {
                    "_id": id,
                    "metric": id.saturating_mul(17),
                    "label": format!("label-{}", id.rem_euclid(LABEL_CARDINALITY as i64)),
                    "updated_at": DateTime::from_millis(id),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        collection.insert_many(documents).await.map_err(|error| {
            CdfError::environment(format!("seed MongoDB roofline collection: {error}"))
        })?;
    }
    Ok(())
}

async fn read_server_version(client: Client) -> Result<String> {
    let build_info = client
        .database("admin")
        .run_command(doc! {"buildInfo": 1_i32})
        .await
        .map_err(|error| CdfError::environment(format!("read MongoDB server version: {error}")))?;
    build_info
        .get_str("version")
        .map(str::to_owned)
        .map_err(|_| CdfError::data("MongoDB buildInfo omitted its version string"))
}

fn compile_resource(
    endpoint: &str,
    batch_rows: u32,
    max_pool_size: u32,
    root: &Path,
    registry: &SourceRegistry,
    execution: &cdf_runtime::ExecutionServices,
) -> BenchResult<Arc<dyn QueryableResource>> {
    let project_toml = format!(
        r#"
[project]
name = "mongodb_source_roofline"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.sqlite"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[sources.roofline]
type = "mongodb"
endpoint = "{endpoint}"
database = "{DATABASE}"
batch_rows = {batch_rows}
max_pool_size = {max_pool_size}
stream_buffer_batches = {STREAM_BUFFER_BATCHES}
discovery_records = 2048
discovery_bytes = 16777216
"#
    );
    let resource_dir = root.join("cdf/roofline");
    fs::create_dir_all(&resource_dir)?;
    fs::write(root.join("cdf.toml"), &project_toml)?;
    fs::write(
        resource_dir.join("events.cdf.sql"),
        format!(
            r#"RESOURCE
TARGET events
DISPOSITION APPEND
CURSOR updated_at
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT id, metric, label, updated_at
FROM upstream(source => 'roofline', collection => '{COLLECTION}');
"#
        ),
    )?;
    let config = cdf_project::parse_cdf_toml(&project_toml)?;
    let destination =
        cdf_dest_duckdb::DuckDbDestination::new(root.join(".cdf/compile-only.duckdb"))?;
    let mut entries = cdf_project::compile_query_project_resources(
        registry,
        &config,
        root,
        "dev",
        destination.sheet(),
        &cdf_semantic::SemanticCatalog::builtins()?,
        &BTreeMap::new(),
    )?;
    if entries.len() != 1 {
        return Err(bench_error(format!(
            "MongoDB roofline expected one current query resource, found {}",
            entries.len()
        )));
    }
    let mut entry = entries.remove(0);
    let provisional = entry.resource.clone();
    let context = SourceResolutionContext::new(
        root,
        Arc::new(NoSecrets),
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
    expected_rows: u64,
) -> Result<MongoDbRooflineSample> {
    let scan = resource.negotiate(&ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: resource.descriptor().state_scope.clone(),
    })?;
    let partition = scan
        .inline_partitions()
        .and_then(|partitions| partitions.first())
        .cloned()
        .ok_or_else(|| CdfError::internal("MongoDB roofline negotiated no partition"))?;
    let (cpu_before, _) = process_counters()?;
    let started = Instant::now();
    let mut stream = resource.open(partition).await?;
    let mut observation = PayloadObservation::default();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let record_batch = batch.record_batch().ok_or_else(|| {
            CdfError::internal("MongoDB roofline source emitted a nonmaterialized batch")
        })?;
        observation.observe(record_batch)?;
    }
    stream.completion().await?;
    let elapsed = elapsed_ns(started);
    let (cpu_after, peak_rss_bytes) = process_counters()?;
    observation.finish(expected_rows, "CDF MongoDB")?;
    black_box((observation.retained_total, observation.content_checksum));
    Ok(observation.sample(
        elapsed,
        cpu_after.saturating_sub(cpu_before),
        peak_rss_bytes,
        None,
    ))
}

async fn read_direct(
    client: Client,
    batch_rows: u32,
    expected_rows: u64,
) -> Result<MongoDbRooflineSample> {
    let collection = client.database(DATABASE).collection::<Document>(COLLECTION);
    let (cpu_before, _) = process_counters()?;
    let started = Instant::now();
    let mut cursor = collection
        .find(Document::new())
        .sort(doc! {"updated_at": 1_i32, "_id": 1_i32})
        .batch_size(batch_rows)
        .batch()
        .await
        .map_err(|error| {
            CdfError::environment(format!("open direct MongoDB raw BSON cursor: {error}"))
        })?;
    let mut observation = PayloadObservation::default();
    let mut raw_bson_bytes = 0_u64;
    while let Some(batch) = cursor.next().await.transpose().map_err(|error| {
        CdfError::environment(format!("read direct MongoDB raw BSON cursor: {error}"))
    })? {
        raw_bson_bytes = raw_bson_bytes.saturating_add(
            u64::try_from(batch.as_raw_document().as_bytes().len())
                .map_err(|_| CdfError::data("MongoDB raw BSON batch size exceeds u64"))?,
        );
        let documents = batch
            .doc_slices()
            .map_err(|error| CdfError::data(format!("decode direct MongoDB raw batch: {error}")))?
            .into_iter()
            .map(|value| {
                value
                    .map_err(|error| {
                        CdfError::data(format!("decode direct MongoDB document: {error}"))
                    })?
                    .as_document()
                    .ok_or_else(|| {
                        CdfError::data("direct MongoDB raw batch item is not a document")
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let record_batch = direct_arrow_batch(&documents)?;
        observation.observe(&record_batch)?;
    }
    let elapsed = elapsed_ns(started);
    let (cpu_after, peak_rss_bytes) = process_counters()?;
    observation.finish(expected_rows, "direct MongoDB")?;
    black_box((observation.retained_total, observation.content_checksum));
    Ok(observation.sample(
        elapsed,
        cpu_after.saturating_sub(cpu_before),
        peak_rss_bytes,
        Some(raw_bson_bytes),
    ))
}

fn direct_arrow_batch(documents: &[&RawDocument]) -> Result<RecordBatch> {
    let mut ids = Int64Builder::with_capacity(documents.len());
    let mut metrics = Int64Builder::with_capacity(documents.len());
    let mut labels = StringBuilder::new();
    let mut updated = TimestampMillisecondBuilder::with_capacity(documents.len());
    for document in documents {
        validate_unique_direct_document(document)?;
        ids.append_value(
            document.get_i64("_id").map_err(|_| {
                CdfError::data("direct MongoDB roofline document has no Int64 `_id`")
            })?,
        );
        metrics.append_value(document.get_i64("metric").map_err(|_| {
            CdfError::data("direct MongoDB roofline document has no Int64 `metric`")
        })?);
        labels.append_value(document.get_str("label").map_err(|_| {
            CdfError::data("direct MongoDB roofline document has no String `label`")
        })?);
        updated.append_value(
            document
                .get_datetime("updated_at")
                .map_err(|_| {
                    CdfError::data("direct MongoDB roofline document has no DateTime `updated_at`")
                })?
                .timestamp_millis(),
        );
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("metric", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids.finish()),
            Arc::new(metrics.finish()),
            Arc::new(labels.finish()),
            Arc::new(updated.finish().with_timezone("UTC")),
        ],
    )
    .map_err(|error| CdfError::data(format!("build direct MongoDB Arrow batch: {error}")))
}

fn validate_unique_direct_document(document: &RawDocument) -> Result<()> {
    let mut names = BTreeSet::new();
    for element in document {
        let (name, _) = element.map_err(|error| {
            CdfError::data(format!(
                "direct MongoDB source returned malformed BSON: {error}"
            ))
        })?;
        if !names.insert(name) {
            return Err(CdfError::data(format!(
                "direct MongoDB source document repeats field `{name}`"
            )));
        }
    }
    Ok(())
}

#[derive(Default)]
struct PayloadObservation {
    rows: u64,
    useful_arrow_bytes: u64,
    content_checksum: u64,
    batch_count: u64,
    maximum_batch_rows: u64,
    maximum_batch_retained_bytes: u64,
    retained_total: u64,
}

impl PayloadObservation {
    fn observe(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_columns() != 4
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(CdfError::data(
                "MongoDB roofline payload contradicted its non-null four-column schema",
            ));
        }
        let ids = downcast::<Int64Array>(batch, 0, "id", "Int64")?;
        let metrics = downcast::<Int64Array>(batch, 1, "metric", "Int64")?;
        let labels = downcast::<StringArray>(batch, 2, "label", "Utf8")?;
        let updated = downcast::<TimestampMillisecondArray>(
            batch,
            3,
            "updated_at",
            "Timestamp(Millisecond)",
        )?;
        for row in 0..batch.num_rows() {
            let offset = u64::try_from(row)
                .map_err(|_| CdfError::data("MongoDB roofline row offset exceeds u64"))?;
            let expected = self
                .rows
                .checked_add(offset)
                .ok_or_else(|| CdfError::data("MongoDB roofline row identity overflowed"))?;
            let expected_i64 = i64::try_from(expected)
                .map_err(|_| CdfError::data("MongoDB roofline row identity exceeds i64"))?;
            let expected_label = format!("label-{}", expected % LABEL_CARDINALITY);
            if ids.value(row) != expected_i64
                || metrics.value(row) != expected_i64.saturating_mul(17)
                || labels.value(row) != expected_label
                || updated.value(row) != expected_i64
            {
                return Err(CdfError::data(format!(
                    "MongoDB roofline payload value differed at logical row {expected}"
                )));
            }
            self.useful_arrow_bytes = self
                .useful_arrow_bytes
                .saturating_add(24)
                .saturating_add(u64::try_from(labels.value(row).len()).unwrap_or(u64::MAX));
            self.content_checksum = self
                .content_checksum
                .rotate_left(7)
                .wrapping_add(expected)
                .rotate_left(11)
                .wrapping_add(metrics.value(row) as u64)
                .rotate_left(13)
                .wrapping_add(updated.value(row) as u64)
                .rotate_left(17)
                .wrapping_add(label_checksum(labels.value(row)));
        }
        self.rows = self.rows.saturating_add(batch.num_rows() as u64);
        let retained = cdf_memory::record_batch_retained_bytes(batch)?;
        self.retained_total = self.retained_total.saturating_add(retained);
        self.maximum_batch_retained_bytes = self.maximum_batch_retained_bytes.max(retained);
        self.maximum_batch_rows = self.maximum_batch_rows.max(batch.num_rows() as u64);
        self.batch_count = self.batch_count.saturating_add(1);
        Ok(())
    }

    fn finish(&self, expected_rows: u64, label: &str) -> Result<()> {
        if self.rows != expected_rows {
            return Err(CdfError::data(format!(
                "{label} roofline read {} rows, expected {expected_rows}",
                self.rows
            )));
        }
        Ok(())
    }

    fn sample(
        &self,
        wall_time_ns: u64,
        cpu_time_ns: u64,
        peak_rss_bytes: u64,
        observed_raw_bson_bytes: Option<u64>,
    ) -> MongoDbRooflineSample {
        MongoDbRooflineSample {
            wall_time_ns,
            cpu_time_ns,
            peak_rss_bytes,
            rows: self.rows,
            useful_arrow_bytes: self.useful_arrow_bytes,
            content_checksum: self.content_checksum,
            batch_count: self.batch_count,
            maximum_batch_rows: self.maximum_batch_rows,
            maximum_batch_retained_bytes: self.maximum_batch_retained_bytes,
            observed_raw_bson_bytes,
        }
    }
}

fn downcast<'a, T: 'static>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
    expected: &str,
) -> Result<&'a T> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            CdfError::data(format!(
                "MongoDB roofline field `{name}` was not {expected}"
            ))
        })
}

fn label_checksum(value: &str) -> u64 {
    value.bytes().fold(0_u64, |checksum, byte| {
        checksum.rotate_left(5) ^ u64::from(byte)
    })
}

fn roofline_cell(
    batch_rows: u32,
    max_pool_size: u32,
    cdf_samples: Vec<MongoDbRooflineSample>,
    direct_samples: Vec<MongoDbRooflineSample>,
) -> MongoDbRooflineCell {
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
    MongoDbRooflineCell {
        batch_rows,
        max_pool_size,
        stream_buffer_batches: STREAM_BUFFER_BATCHES,
        client_concurrency: 1,
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

fn endpoint_authority(endpoint: &str) -> BenchResult<String> {
    let parsed = url::Url::parse(endpoint)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| bench_error("MongoDB endpoint has no host"))?;
    Ok(match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    })
}

fn attest_local_docker_image(endpoint: &str) -> BenchResult<String> {
    let parsed = url::Url::parse(endpoint)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| bench_error("MongoDB endpoint has no host"))?;
    if !matches!(host, "127.0.0.1" | "localhost" | "::1") {
        return Err(bench_error(
            "MongoDB closure roofline requires a local Docker endpoint for image attestation",
        ));
    }
    let port = parsed.port().unwrap_or(27_017);
    let listing = Command::new("docker")
        .args(["ps", "--format", "{{.ID}}\t{{.Ports}}"])
        .output()?;
    if !listing.status.success() {
        return Err(bench_error(
            "MongoDB roofline could not inspect running Docker containers",
        ));
    }
    let listing = String::from_utf8(listing.stdout)?;
    let needle = format!(":{port}->");
    let container = listing
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .find_map(|(id, ports)| ports.contains(&needle).then_some(id))
        .ok_or_else(|| {
            bench_error(format!(
                "MongoDB roofline found no running Docker container publishing port {port}"
            ))
        })?;
    let running = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{.Config.Image}} {{.Image}}",
            container,
        ])
        .output()?;
    if !running.status.success() {
        return Err(bench_error(
            "MongoDB roofline could not inspect the endpoint container image",
        ));
    }
    let running = String::from_utf8(running.stdout)?;
    let (configured, image_id) = running
        .trim()
        .split_once(' ')
        .ok_or_else(|| bench_error("MongoDB container image attestation is malformed"))?;
    let expected_id = EXPECTED_SERVER_IMAGE
        .split_once('@')
        .map(|(_, digest)| digest)
        .ok_or_else(|| bench_error("MongoDB expected image omitted its digest"))?;
    if configured != EXPECTED_SERVER_IMAGE || image_id != expected_id {
        return Err(bench_error(format!(
            "MongoDB endpoint container does not match required image {EXPECTED_SERVER_IMAGE}"
        )));
    }
    Ok(EXPECTED_SERVER_IMAGE.to_owned())
}

fn process_counters() -> Result<(u64, u64)> {
    let usage = getrusage(UsageWho::RUSAGE_SELF)
        .map_err(|_| CdfError::environment("read MongoDB roofline process counters"))?;
    let cpu_micros = usage
        .user_time()
        .num_microseconds()
        .saturating_add(usage.system_time().num_microseconds());
    let cpu_time_ns = u64::try_from(cpu_micros)
        .unwrap_or_default()
        .saturating_mul(1_000);
    let max_rss = u64::try_from(usage.max_rss())
        .map_err(|_| CdfError::environment("convert MongoDB roofline peak RSS"))?;
    let peak_rss_bytes = if cfg!(any(target_os = "macos", target_os = "ios")) {
        max_rss
    } else {
        max_rss.saturating_mul(1_024)
    };
    Ok((cpu_time_ns, peak_rss_bytes))
}

fn base_git_revision(workspace_root: &Path) -> BenchResult<String> {
    let status = Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args([
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            "Cargo.toml",
            "Cargo.lock",
            "crates",
        ])
        .output()?;
    if !status.status.success() || !status.stdout.is_empty() {
        return Err(bench_error(
            "MongoDB roofline requires a clean committed participating source snapshot so cdf_revision reconstructs the measured source",
        ));
    }
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args(["rev-parse", "HEAD"])
        .output()?;
    if !output.status.success() {
        return Err(bench_error(
            "MongoDB roofline could not resolve the base Git revision",
        ));
    }
    let revision = String::from_utf8(output.stdout)?;
    let revision = revision.trim();
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(bench_error(
            "MongoDB roofline resolved an invalid base Git revision",
        ));
    }
    let build_revision = option_env!("CDF_BENCHMARK_BUILD_GIT_REVISION")
        .ok_or_else(|| bench_error("MongoDB roofline executable omitted its build Git revision"))?;
    let build_dirty = option_env!("CDF_BENCHMARK_BUILD_GIT_DIRTY").unwrap_or("unknown");
    if build_dirty != "false" || build_revision != revision {
        return Err(bench_error(format!(
            "MongoDB roofline executable was built from revision {build_revision} (dirty={build_dirty}) but runtime HEAD is {revision}; rebuild the release benchmark from this clean snapshot"
        )));
    }
    Ok(revision.to_owned())
}

fn workspace_content_revision(workspace_root: &Path) -> BenchResult<(String, Vec<String>)> {
    const INPUTS: &[&str] = &[
        "Cargo.toml",
        "Cargo.lock",
        "crates/cdf-benchmarks/Cargo.toml",
        "crates/cdf-benchmarks/build.rs",
        "crates/cdf-benchmarks/src/bin/mongodb-source-roofline.rs",
        "crates/cdf-benchmarks/src/mongodb_source_roofline.rs",
        "crates/cdf-source-mongodb/Cargo.toml",
        "crates/cdf-source-mongodb/src/driver.rs",
        "crates/cdf-source-mongodb/src/error.rs",
        "crates/cdf-source-mongodb/src/execution.rs",
        "crates/cdf-source-mongodb/src/identifier.rs",
        "crates/cdf-source-mongodb/src/lib.rs",
        "crates/cdf-source-mongodb/src/query.rs",
        "crates/cdf-source-mongodb/src/resource.rs",
        "crates/cdf-source-mongodb/src/schema.rs",
    ];
    let mut hasher = Sha256::new();
    for relative in INPUTS {
        let contents = fs::read(workspace_root.join(relative))?;
        hasher.update(relative.as_bytes());
        hasher.update([0]);
        hasher.update(u64::try_from(contents.len())?.to_le_bytes());
        hasher.update(contents);
    }
    Ok((
        format!("sha256:{:x}", hasher.finalize()),
        INPUTS.iter().map(|path| (*path).to_owned()).collect(),
    ))
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};

    use super::PayloadObservation;

    fn batch(ids: Vec<i64>, labels: Vec<&str>) -> RecordBatch {
        let metrics = ids.iter().map(|value| value * 17).collect::<Vec<_>>();
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(ids.clone())) as ArrayRef),
            ("metric", Arc::new(Int64Array::from(metrics)) as ArrayRef),
            ("label", Arc::new(StringArray::from(labels)) as ArrayRef),
            (
                "updated_at",
                Arc::new(TimestampMillisecondArray::from(ids).with_timezone("UTC")) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[test]
    fn payload_observation_is_split_invariant_and_rejects_corruption() {
        let mut whole = PayloadObservation::default();
        whole
            .observe(&batch(vec![0, 1, 2], vec!["label-0", "label-1", "label-2"]))
            .unwrap();
        whole.finish(3, "test").unwrap();

        let mut split = PayloadObservation::default();
        split
            .observe(&batch(vec![0, 1], vec!["label-0", "label-1"]))
            .unwrap();
        split.observe(&batch(vec![2], vec!["label-2"])).unwrap();
        assert_eq!(split.content_checksum, whole.content_checksum);
        assert_eq!(split.useful_arrow_bytes, whole.useful_arrow_bytes);

        let mut corrupted = PayloadObservation::default();
        let error = corrupted
            .observe(&batch(vec![0], vec!["wrong"]))
            .unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("logical row 0"));
    }
}
