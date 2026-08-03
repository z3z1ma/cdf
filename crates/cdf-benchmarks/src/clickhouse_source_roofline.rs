use std::{
    collections::BTreeMap, fs, hint::black_box, io::Read, num::NonZeroUsize, path::Path,
    process::Command, sync::Arc, time::Instant,
};

use arrow_array::{Array, BinaryArray, Int64Array, RecordBatch, StringArray, UInt64Array};
use cdf_bench_core::{
    BenchResult, ComparabilityKey, HostCapabilityProvider, HostFingerprint, HostProbeConfig,
    IoMode, SystemHostProvider, bench_error, host_class,
};
use cdf_http::{EgressAllowlist, SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{CdfError, QueryableResource, Result, ScanRequest};
use cdf_runtime::{SourceRegistry, SourceResolutionContext};
use clickhouse::ResponseLimits;
use clickhouse_ext_arrow::{ArrowQueryExt, ArrowStreamLimits};
use futures_util::StreamExt;
use nix::sys::{
    resource::{UsageWho, getrusage},
    time::TimeValLike,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const STREAM_BUFFER_BATCHES: usize = 1;
const PASS_RATIO_PPM: u64 = 900_000;
const MANAGED_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const MAX_MAD_PERCENT: u64 = 10;
const EXPECTED_CLICKHOUSE_VERSION: &str = "25.8.28.1";
const EXPECTED_CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server@sha256:2173163006e08f6b6670017deff19be554b9a05812d11f1c08b027ecf55d7a60";
const LOGICAL_PAYLOAD_BYTES_PER_ROW: u64 = 3 * 8;
const CLICKHOUSE_ERROR_BODY_BYTES: usize = 1024 * 1024;
const CLICKHOUSE_ARROW_MESSAGE_BYTES: usize = 2 * 1024 * 1024;
const CLICKHOUSE_ARROW_BODY_BYTES: usize = 25 * 1024 * 1024;
const CLICKHOUSE_ARROW_SCHEMA_BYTES: usize = 4 * 1024 * 1024;
const CLICKHOUSE_ARROW_SCHEMA_NODES: usize = 4_096;
const CLICKHOUSE_ARROW_SCHEMA_METADATA_ENTRIES: usize = 4_096;
const CLICKHOUSE_ARROW_SCHEMA_DEPTH: usize = 64;
const CLICKHOUSE_MAXIMUM_RECORD_BATCH_ROWS: usize = 1_000_000;
const CLICKHOUSE_HTTP_INPUT_CHUNK_BYTES: usize = clickhouse::DEFAULT_HTTP1_MAX_BUFFER_BYTES;
const IN_FLIGHT_BATCH_BOUND: usize = STREAM_BUFFER_BATCHES + 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseRooflineSample {
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
pub struct ClickHouseRooflineCell {
    pub max_threads: u64,
    pub max_block_rows: u64,
    pub estimated_target_batch_bytes: u64,
    pub compression: String,
    pub connection_reuse: bool,
    pub client_concurrency: u16,
    pub cdf_samples: Vec<ClickHouseRooflineSample>,
    pub direct_samples: Vec<ClickHouseRooflineSample>,
    pub cdf_median_ns: u64,
    pub direct_median_ns: u64,
    pub cdf_mad_ns: u64,
    pub direct_mad_ns: u64,
    pub roofline_ratio_ppm: u64,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseSweepExclusion {
    pub dimension: String,
    pub value: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseSourceRooflineReport {
    pub schema_version: u16,
    pub status: String,
    pub endpoint_authority: String,
    pub server_image: String,
    pub server_version: String,
    pub client_version: String,
    pub arrow_extension_version: String,
    pub compression: String,
    pub max_threads: u64,
    pub max_block_rows: u64,
    pub stream_buffer_batches: usize,
    pub rows: u64,
    pub samples: u32,
    pub cdf_elapsed_ns: Vec<u64>,
    pub direct_elapsed_ns: Vec<u64>,
    pub cdf_median_ns: u64,
    pub direct_median_ns: u64,
    pub roofline_ratio_ppm: u64,
    pub host: HostFingerprint,
    pub comparability: ComparabilityKey,
    pub workspace_content_sha256: String,
    pub workspace_content_inputs: Vec<String>,
    pub executable_sha256: String,
    pub useful_arrow_bytes: u64,
    pub content_checksum: u64,
    pub physical_wire_bytes: Option<u64>,
    pub physical_wire_bytes_reason: String,
    pub in_flight_batch_bound: usize,
    pub connection_count: u16,
    pub client_concurrency: u16,
    pub semantic_bias: Vec<String>,
    pub sweep: Vec<ClickHouseRooflineCell>,
    pub sweep_exclusions: Vec<ClickHouseSweepExclusion>,
}

struct NoSecrets;

impl SecretProvider for NoSecrets {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
        Err(CdfError::auth(format!(
            "ClickHouse source roofline has no secret for {uri}"
        )))
    }
}

pub fn run_clickhouse_source_roofline(
    endpoint: &str,
    output: &Path,
    samples: u32,
    rows: u64,
) -> BenchResult<ClickHouseSourceRooflineReport> {
    if cfg!(debug_assertions) {
        return Err(bench_error(
            "ClickHouse source roofline must run from a release build",
        ));
    }
    if samples < 3 || rows < 100_000 {
        return Err(bench_error(
            "ClickHouse source roofline requires at least three samples and 100,000 rows",
        ));
    }
    let http_endpoint = operational_endpoint(endpoint)?;
    let client = roofline_client(&http_endpoint);
    let (execution_host, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(MANAGED_MEMORY_BYTES)?;
    execution.run_io(seed_fixture(client.clone(), rows))?;
    let server_version = execution.run_io(read_server_version(client.clone()))?;
    if server_version != EXPECTED_CLICKHOUSE_VERSION {
        return Err(bench_error(format!(
            "ClickHouse closure roofline requires server {EXPECTED_CLICKHOUSE_VERSION}, observed {server_version}"
        )));
    }
    let server_image = attest_local_docker_image(endpoint)?;

    let mut registry = SourceRegistry::new();
    registry.register(cdf_source_clickhouse::ClickHouseSourceDriver::new()?)?;
    let mut sweep = Vec::new();
    for connection_reuse in [false, true] {
        for (max_threads, max_block_rows) in [(1, 8_192), (1, 65_536), (4, 8_192), (4, 65_536)] {
            let fixture = tempfile::tempdir()?;
            let resource = compile_resource(
                endpoint,
                max_threads,
                max_block_rows,
                fixture.path(),
                &registry,
                &execution,
            )?;

            // Untimed passes establish query equivalence and, for the reuse cells, warm the pool.
            execution.run_io(read_direct(
                if connection_reuse {
                    client.clone()
                } else {
                    roofline_client(&http_endpoint)
                },
                rows,
                max_threads,
                max_block_rows,
            ))?;
            execution_host.block_on_root(read_cdf(resource.as_ref(), rows))?;

            let mut cdf_samples = Vec::with_capacity(samples as usize);
            let mut direct_samples = Vec::with_capacity(samples as usize);
            for index in 0..samples {
                let sample_resource = if connection_reuse {
                    Arc::clone(&resource)
                } else {
                    compile_resource(
                        endpoint,
                        max_threads,
                        max_block_rows,
                        fixture.path(),
                        &registry,
                        &execution,
                    )?
                };
                let sample_client = if connection_reuse {
                    client.clone()
                } else {
                    roofline_client(&http_endpoint)
                };
                if index.is_multiple_of(2) {
                    cdf_samples.push(
                        execution_host.block_on_root(read_cdf(sample_resource.as_ref(), rows))?,
                    );
                    direct_samples.push(execution.run_io(read_direct(
                        sample_client,
                        rows,
                        max_threads,
                        max_block_rows,
                    ))?);
                } else {
                    direct_samples.push(execution.run_io(read_direct(
                        sample_client,
                        rows,
                        max_threads,
                        max_block_rows,
                    ))?);
                    cdf_samples.push(
                        execution_host.block_on_root(read_cdf(sample_resource.as_ref(), rows))?,
                    );
                }
            }
            sweep.push(roofline_cell(
                max_threads,
                max_block_rows,
                connection_reuse,
                cdf_samples,
                direct_samples,
            ));
        }
    }
    let selected = sweep
        .iter()
        .min_by_key(|cell| cell.cdf_median_ns)
        .cloned()
        .ok_or_else(|| bench_error("ClickHouse roofline sweep produced no comparable cell"))?;
    let status = if sweep.iter().any(|cell| cell.status == "fail") {
        "fail"
    } else if sweep.iter().any(|cell| cell.status == "inconclusive") {
        "inconclusive"
    } else {
        "pass"
    };
    let cdf_elapsed_ns = selected
        .cdf_samples
        .iter()
        .map(|sample| sample.wall_time_ns)
        .collect::<Vec<_>>();
    let direct_elapsed_ns = selected
        .direct_samples
        .iter()
        .map(|sample| sample.wall_time_ns)
        .collect::<Vec<_>>();
    let workspace_root = std::env::current_dir()?;
    let (workspace_content_sha256, workspace_content_inputs) =
        workspace_content_revision(&workspace_root)?;
    let executable_sha256 = executable_revision(&std::env::current_exe()?)?;
    let cdf_revision = base_git_revision(&workspace_root)?;
    let host_provider = SystemHostProvider::new(HostProbeConfig {
        cdf_version: env!("CARGO_PKG_VERSION").to_owned(),
        dependency_versions: BTreeMap::from([
            ("clickhouse".to_owned(), "0.15.1".to_owned()),
            ("clickhouse-ext-arrow".to_owned(), "0.1.0".to_owned()),
            ("clickhouse-server".to_owned(), server_version.clone()),
        ]),
        benchmark_profile: "release-clickhouse-source-roofline".to_owned(),
        storage_target: Some(output.parent().unwrap_or(Path::new(".")).to_path_buf()),
    });
    let host = host_provider.fingerprint()?;
    let comparability = ComparabilityKey {
        dataset_id: format!("clickhouse-source-roofline-{rows}-rows-3xu64"),
        workload_id: format!(
            "clickhouse-arrowstream-t{}-r{}-none-{}-c1",
            selected.max_threads,
            selected.max_block_rows,
            if selected.connection_reuse {
                "reuse"
            } else {
                "cold"
            }
        ),
        timed_region_version: 1,
        cdf_revision,
        dependency_tuple: format!(
            "clickhouse=0.15.1+cdf-bounded-response-v1;arrow-ipc=58.3.0+cdf-bounded-stream-v2;clickhouse-ext-arrow=0.1.0+cdf-bounded-stream-v1;server={server_version};workspace={workspace_content_sha256};executable={executable_sha256}"
        ),
        host_class: host_class(&host)?,
        os_toolchain: format!(
            "{}-{};{}",
            host.os.family, host.architecture, host.rust_version
        ),
        io_mode: IoMode::Warm,
    };
    let report = ClickHouseSourceRooflineReport {
        schema_version: 3,
        status: status.to_owned(),
        endpoint_authority: endpoint_authority(endpoint)?,
        server_image,
        server_version,
        client_version: "0.15.1".to_owned(),
        arrow_extension_version: "0.1.0".to_owned(),
        compression: "http=none; ArrowStream-inner=none; bounded-decode-safe".to_owned(),
        max_threads: selected.max_threads,
        max_block_rows: selected.max_block_rows,
        stream_buffer_batches: STREAM_BUFFER_BATCHES,
        rows,
        samples,
        cdf_elapsed_ns,
        direct_elapsed_ns,
        cdf_median_ns: selected.cdf_median_ns,
        direct_median_ns: selected.direct_median_ns,
        roofline_ratio_ppm: selected.roofline_ratio_ppm,
        host,
        comparability,
        workspace_content_sha256,
        workspace_content_inputs,
        executable_sha256,
        useful_arrow_bytes: selected.cdf_samples[0].useful_arrow_bytes,
        content_checksum: selected.cdf_samples[0].content_checksum,
        physical_wire_bytes: None,
        physical_wire_bytes_reason: "clickhouse-ext-arrow ArrowCursor does not expose the underlying BytesCursor counters; no physical-wire estimate is substituted".to_owned(),
        in_flight_batch_bound: IN_FLIGHT_BATCH_BOUND,
        connection_count: 1,
        client_concurrency: 1,
        semantic_bias: vec![
            "direct runner omits CDF physical-schema binding, batch headers, memory leases, and stream completion governance and is therefore a favorable roofline".to_owned(),
            format!("stream_buffer_batches={STREAM_BUFFER_BATCHES} is queue capacity; the truthful retained-batch overlap is queue + producer + consumer = {IN_FLIGHT_BATCH_BOUND}"),
            "the fixed-width 3x8-byte fixture makes useful_arrow_bytes exactly rows*24; maximum_batch_retained_bytes remains allocator-capacity evidence and is never substituted for useful payload".to_owned(),
            "both timed paths validate every id, metric=id*17, and updated_at=id value and record the same deterministic content checksum; variable-width admission remains governed by the separate 16 MiB row and 25 MiB Arrow-body laws".to_owned(),
        ],
        sweep,
        sweep_exclusions: vec![
            ClickHouseSweepExclusion {
                dimension: "compression".to_owned(),
                value: "lz4".to_owned(),
                reason: "production fixes HTTP and ArrowStream-inner compression to none, so an LZ4 cell would not measure the shipped connector contract; the locally patched official client still enforces declared compressed and decoded frame ceilings".to_owned(),
            },
            ClickHouseSweepExclusion {
                dimension: "client_concurrency".to_owned(),
                value: "2+".to_owned(),
                reason: "compiled source capability maximum_concurrency=1; manufacturing parallel table snapshots would change semantics".to_owned(),
            },
            ClickHouseSweepExclusion {
                dimension: "remote_cloud".to_owned(),
                value: "unavailable".to_owned(),
                reason: "no separately authorized ClickHouse Cloud credentials, external writes, or cost were supplied for this local closure run".to_owned(),
            },
        ],
    };
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output, serde_json::to_vec_pretty(&report)?)?;
    if report.status != "pass" {
        return Err(bench_error(format!(
            "ClickHouse source roofline sweep status is {}; selected ratio {:.3}; report written to {}",
            report.status,
            report.roofline_ratio_ppm as f64 / 1_000_000.0,
            output.display()
        )));
    }
    Ok(report)
}

fn compile_resource(
    endpoint: &str,
    max_threads: u64,
    max_block_rows: u64,
    root: &Path,
    registry: &SourceRegistry,
    execution: &cdf_runtime::ExecutionServices,
) -> BenchResult<Arc<dyn QueryableResource>> {
    let document = cdf_declarative::parse_toml(&format!(
        r#"
[source.roofline]
kind = "clickhouse"
endpoint = "{endpoint}"
database = "default"
max_threads = {max_threads}
max_block_rows = {max_block_rows}
stream_buffer_batches = {STREAM_BUFFER_BATCHES}

[resource.events]
table = "cdf_source_roofline"
stable_key = "id"
cursor = {{ field = "updated_at", ordering = "exact", lag = "0ms" }}
write_disposition = "append"
trust = "governed"
schema_mode = "discover"
"#
    ))?;
    let provisional = cdf_declarative::compile_document(registry, &document)?
        .into_iter()
        .next()
        .ok_or_else(|| bench_error("ClickHouse roofline compiled no resource"))?;
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
    let compiled = cdf_project::compile_discovered_schema_artifacts(&provisional, &mut discovery)?;
    Ok(registry.resolve(compiled.source_plan(), &context)?)
}

fn roofline_cell(
    max_threads: u64,
    max_block_rows: u64,
    connection_reuse: bool,
    cdf_samples: Vec<ClickHouseRooflineSample>,
    direct_samples: Vec<ClickHouseRooflineSample>,
) -> ClickHouseRooflineCell {
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
    .all(|(mad, median)| {
        mad.saturating_mul(100)
            .checked_div(median)
            .unwrap_or(u64::MAX)
            <= MAX_MAD_PERCENT
    });
    let payload_equivalent = cdf_samples.iter().chain(&direct_samples).all(|sample| {
        sample.rows == cdf_samples[0].rows
            && sample.useful_arrow_bytes == cdf_samples[0].useful_arrow_bytes
            && sample.content_checksum == cdf_samples[0].content_checksum
    });
    ClickHouseRooflineCell {
        max_threads,
        max_block_rows,
        estimated_target_batch_bytes: max_block_rows.saturating_mul(LOGICAL_PAYLOAD_BYTES_PER_ROW),
        compression: "none".to_owned(),
        connection_reuse,
        client_concurrency: 1,
        cdf_samples,
        direct_samples,
        cdf_median_ns,
        direct_median_ns,
        cdf_mad_ns,
        direct_mad_ns,
        roofline_ratio_ppm,
        status: if !payload_equivalent {
            "fail"
        } else if !variance_ok {
            "inconclusive"
        } else if roofline_ratio_ppm >= PASS_RATIO_PPM {
            "pass"
        } else {
            "fail"
        }
        .to_owned(),
    }
}

async fn seed_fixture(client: clickhouse::Client, rows: u64) -> Result<()> {
    client
        .query("DROP TABLE IF EXISTS cdf_source_roofline")
        .execute()
        .await
        .map_err(|_| CdfError::environment("reset ClickHouse roofline table"))?;
    client
        .query(concat!(
            "CREATE TABLE cdf_source_roofline (",
            "id UInt64, metric UInt64, updated_at Int64",
            ") ENGINE = Memory"
        ))
        .execute()
        .await
        .map_err(|_| CdfError::environment("create ClickHouse roofline table"))?;
    client
        .query(concat!(
            "INSERT INTO cdf_source_roofline ",
            "SELECT number, number * 17, toInt64(number) FROM numbers(?)"
        ))
        .bind(rows)
        .execute()
        .await
        .map_err(|_| CdfError::environment("seed ClickHouse roofline table"))
}

async fn read_direct(
    client: clickhouse::Client,
    expected_rows: u64,
    max_threads: u64,
    max_block_rows: u64,
) -> Result<ClickHouseRooflineSample> {
    let query = client
        .query(concat!(
            "SELECT id, metric, updated_at FROM cdf_source_roofline ",
            "ORDER BY updated_at ASC, id ASC"
        ))
        .with_setting("readonly", "1")
        .with_setting("max_threads", max_threads.to_string())
        .with_setting("max_block_size", max_block_rows.to_string())
        .with_setting("output_format_arrow_string_as_string", "0");
    let (cpu_before, _) = process_counters()?;
    let started = Instant::now();
    let mut cursor = query
        .fetch_arrow_with_limits(roofline_arrow_limits()?)
        .map_err(|_| CdfError::environment("open direct ClickHouse ArrowStream"))?;
    let mut rows = 0_u64;
    let mut retained_total = 0_u64;
    let mut content_checksum = 0_u64;
    let mut batch_count = 0_u64;
    let mut maximum_batch_rows = 0_u64;
    let mut maximum_batch_retained_bytes = 0_u64;
    while let Some(batch) = cursor
        .next()
        .await
        .map_err(|_| CdfError::data("read direct ClickHouse ArrowStream"))?
    {
        validate_roofline_payload(&batch, rows, &mut content_checksum)?;
        rows = rows.saturating_add(batch.num_rows() as u64);
        let retained = cdf_memory::record_batch_retained_bytes(&batch)?;
        retained_total = retained_total.saturating_add(retained);
        maximum_batch_retained_bytes = maximum_batch_retained_bytes.max(retained);
        maximum_batch_rows = maximum_batch_rows.max(batch.num_rows() as u64);
        batch_count = batch_count.saturating_add(1);
    }
    let elapsed = elapsed_ns(started);
    let (cpu_after, peak_rss_bytes) = process_counters()?;
    if rows != expected_rows {
        return Err(CdfError::data(format!(
            "direct ClickHouse roofline read {rows} rows, expected {expected_rows}"
        )));
    }
    black_box((retained_total, content_checksum));
    Ok(ClickHouseRooflineSample {
        wall_time_ns: elapsed,
        cpu_time_ns: cpu_after.saturating_sub(cpu_before),
        peak_rss_bytes,
        rows,
        useful_arrow_bytes: logical_useful_arrow_bytes(rows)?,
        content_checksum,
        batch_count,
        maximum_batch_rows,
        maximum_batch_retained_bytes,
    })
}

async fn read_cdf(
    resource: &dyn QueryableResource,
    expected_rows: u64,
) -> Result<ClickHouseRooflineSample> {
    let scan = resource.negotiate(&ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: resource.descriptor().state_scope.clone(),
    })?;
    let mut partition = scan
        .inline_partitions()
        .and_then(|partitions| partitions.first())
        .cloned()
        .ok_or_else(|| CdfError::internal("ClickHouse roofline negotiated no partition"))?;
    bind_planned_physical_schema(resource, &mut partition)?;
    let (cpu_before, _) = process_counters()?;
    let started = Instant::now();
    let mut stream = resource.open(partition).await?;
    let mut rows = 0_u64;
    let mut retained_total = 0_u64;
    let mut content_checksum = 0_u64;
    let mut batch_count = 0_u64;
    let mut maximum_batch_rows = 0_u64;
    let mut maximum_batch_retained_bytes = 0_u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let record_batch = batch.record_batch().ok_or_else(|| {
            CdfError::internal("ClickHouse roofline source emitted a nonmaterialized batch")
        })?;
        validate_roofline_payload(record_batch, rows, &mut content_checksum)?;
        rows = rows.saturating_add(record_batch.num_rows() as u64);
        let retained = cdf_memory::record_batch_retained_bytes(record_batch)?;
        retained_total = retained_total.saturating_add(retained);
        maximum_batch_retained_bytes = maximum_batch_retained_bytes.max(retained);
        maximum_batch_rows = maximum_batch_rows.max(record_batch.num_rows() as u64);
        batch_count = batch_count.saturating_add(1);
    }
    stream.completion().await?;
    let elapsed = elapsed_ns(started);
    let (cpu_after, peak_rss_bytes) = process_counters()?;
    if rows != expected_rows {
        return Err(CdfError::data(format!(
            "CDF ClickHouse roofline read {rows} rows, expected {expected_rows}"
        )));
    }
    black_box((retained_total, content_checksum));
    Ok(ClickHouseRooflineSample {
        wall_time_ns: elapsed,
        cpu_time_ns: cpu_after.saturating_sub(cpu_before),
        peak_rss_bytes,
        rows,
        useful_arrow_bytes: logical_useful_arrow_bytes(rows)?,
        content_checksum,
        batch_count,
        maximum_batch_rows,
        maximum_batch_retained_bytes,
    })
}

fn logical_useful_arrow_bytes(rows: u64) -> Result<u64> {
    rows.checked_mul(LOGICAL_PAYLOAD_BYTES_PER_ROW)
        .ok_or_else(|| CdfError::data("ClickHouse roofline logical payload byte count overflowed"))
}

fn validate_roofline_payload(
    batch: &RecordBatch,
    expected_start: u64,
    content_checksum: &mut u64,
) -> Result<()> {
    if batch.num_columns() != 3
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(CdfError::data(
            "ClickHouse roofline payload contradicted its non-null three-column schema",
        ));
    }
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| CdfError::data("ClickHouse roofline id was not UInt64"))?;
    let metrics = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| CdfError::data("ClickHouse roofline metric was not UInt64"))?;
    let updated = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| CdfError::data("ClickHouse roofline updated_at was not Int64"))?;
    for row in 0..batch.num_rows() {
        let row_offset = u64::try_from(row)
            .map_err(|_| CdfError::data("ClickHouse roofline row offset overflowed"))?;
        let expected_id = expected_start
            .checked_add(row_offset)
            .ok_or_else(|| CdfError::data("ClickHouse roofline row identity overflowed"))?;
        let expected_metric = expected_id
            .checked_mul(17)
            .ok_or_else(|| CdfError::data("ClickHouse roofline metric identity overflowed"))?;
        let expected_updated = i64::try_from(expected_id)
            .map_err(|_| CdfError::data("ClickHouse roofline timestamp identity overflowed"))?;
        if ids.value(row) != expected_id
            || metrics.value(row) != expected_metric
            || updated.value(row) != expected_updated
        {
            return Err(CdfError::data(format!(
                "ClickHouse roofline payload value differed at logical row {expected_id}"
            )));
        }
        *content_checksum = content_checksum
            .rotate_left(7)
            .wrapping_add(ids.value(row))
            .rotate_left(11)
            .wrapping_add(metrics.value(row))
            .rotate_left(13)
            .wrapping_add(updated.value(row) as u64);
    }
    Ok(())
}

fn bind_planned_physical_schema(
    resource: &dyn QueryableResource,
    partition: &mut cdf_kernel::PartitionPlan,
) -> Result<()> {
    let runtime = resource.effective_schema_runtime().ok_or_else(|| {
        CdfError::internal("ClickHouse roofline resource omitted effective-schema runtime")
    })?;
    let observation_id = cdf_kernel::partition_schema_observation_id(partition);
    let physical_hash = runtime
        .evidence
        .observation(observation_id)
        .ok_or_else(|| {
            CdfError::internal(
                "ClickHouse roofline partition omitted its effective-schema observation",
            )
        })?
        .physical_schema_hash
        .to_string();
    partition.metadata.insert(
        cdf_kernel::PLAN_PHYSICAL_SCHEMA_HASH_KEY.to_owned(),
        physical_hash,
    );
    Ok(())
}

async fn read_server_version(client: clickhouse::Client) -> Result<String> {
    let mut cursor = client
        .query("SELECT version() AS version")
        .with_setting("readonly", "1")
        .with_setting("output_format_arrow_string_as_string", "0")
        .fetch_arrow_with_limits(roofline_arrow_limits()?)
        .map_err(|_| CdfError::environment("open ClickHouse server-version probe"))?;
    let batch = cursor
        .next()
        .await
        .map_err(|_| CdfError::environment("read ClickHouse server-version probe"))?
        .ok_or_else(|| CdfError::data("ClickHouse server-version probe returned no row"))?;
    if batch.num_rows() != 1 || batch.num_columns() != 1 || batch.column(0).is_null(0) {
        return Err(CdfError::data(
            "ClickHouse server-version probe returned an invalid shape",
        ));
    }
    if let Some(values) = batch.column(0).as_any().downcast_ref::<BinaryArray>() {
        return std::str::from_utf8(values.value(0))
            .map(str::to_owned)
            .map_err(|_| CdfError::data("ClickHouse server version is not UTF-8"));
    }
    if let Some(values) = batch.column(0).as_any().downcast_ref::<StringArray>() {
        return Ok(values.value(0).to_owned());
    }
    Err(CdfError::data(
        "ClickHouse server-version probe returned an unexpected Arrow type",
    ))
}

fn operational_endpoint(endpoint: &str) -> BenchResult<String> {
    endpoint
        .strip_prefix("clickhouse://")
        .map(|authority| format!("http://{authority}"))
        .or_else(|| {
            endpoint
                .strip_prefix("clickhouses://")
                .map(|authority| format!("https://{authority}"))
        })
        .ok_or_else(|| bench_error("ClickHouse endpoint must use clickhouse(s)://"))
}

fn roofline_client(http_endpoint: &str) -> clickhouse::Client {
    clickhouse::Client::default()
        .with_url(http_endpoint)
        .with_database("default")
        .with_compression(clickhouse::Compression::None)
}

fn endpoint_authority(endpoint: &str) -> BenchResult<String> {
    let parsed = url::Url::parse(endpoint)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| bench_error("ClickHouse endpoint has no host"))?;
    Ok(match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    })
}

fn attest_local_docker_image(endpoint: &str) -> BenchResult<String> {
    let parsed = url::Url::parse(endpoint)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| bench_error("ClickHouse endpoint has no host"))?;
    if !matches!(host, "127.0.0.1" | "localhost" | "::1") {
        return Err(bench_error(
            "ClickHouse closure roofline requires a local Docker endpoint for independent image attestation",
        ));
    }
    let port = parsed.port().unwrap_or(8123);
    let output = Command::new("docker")
        .args(["ps", "--format", "{{.ID}}\t{{.Ports}}"])
        .output()?;
    if !output.status.success() {
        return Err(bench_error(
            "ClickHouse roofline could not inspect running Docker containers",
        ));
    }
    let listing = String::from_utf8(output.stdout)?;
    let needle = format!(":{port}->");
    let container = listing
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .find_map(|(id, ports)| ports.contains(&needle).then_some(id))
        .ok_or_else(|| {
            bench_error(format!(
                "ClickHouse roofline found no running Docker container publishing endpoint port {port}"
            ))
        })?;
    let output = Command::new("docker")
        .args(["inspect", "--format", "{{.Image}}", container])
        .output()?;
    if !output.status.success() {
        return Err(bench_error(
            "ClickHouse roofline could not inspect the endpoint container image",
        ));
    }
    let running_image_id = String::from_utf8(output.stdout)?.trim().to_owned();
    if !running_image_id.starts_with("sha256:") || running_image_id.len() != 71 {
        return Err(bench_error(
            "ClickHouse endpoint container did not report an immutable image ID",
        ));
    }
    let output = Command::new("docker")
        .args([
            "image",
            "inspect",
            "--format",
            "{{.Id}}",
            EXPECTED_CLICKHOUSE_IMAGE,
        ])
        .output()?;
    if !output.status.success() {
        return Err(bench_error(format!(
            "ClickHouse roofline could not inspect required image {EXPECTED_CLICKHOUSE_IMAGE}"
        )));
    }
    let expected_image_id = String::from_utf8(output.stdout)?.trim().to_owned();
    if running_image_id != expected_image_id {
        return Err(bench_error(format!(
            "ClickHouse endpoint container image ID does not match required image {EXPECTED_CLICKHOUSE_IMAGE}"
        )));
    }
    Ok(EXPECTED_CLICKHOUSE_IMAGE.to_owned())
}

fn process_counters() -> Result<(u64, u64)> {
    let usage = getrusage(UsageWho::RUSAGE_SELF)
        .map_err(|_| CdfError::environment("read ClickHouse roofline process counters"))?;
    let cpu_micros = usage
        .user_time()
        .num_microseconds()
        .saturating_add(usage.system_time().num_microseconds());
    let cpu_time_ns = u64::try_from(cpu_micros)
        .unwrap_or_default()
        .saturating_mul(1_000);
    let max_rss = u64::try_from(usage.max_rss())
        .map_err(|_| CdfError::environment("convert ClickHouse roofline peak RSS"))?;
    let peak_rss_bytes = if cfg!(any(target_os = "macos", target_os = "ios")) {
        max_rss
    } else {
        max_rss.saturating_mul(1024)
    };
    Ok((cpu_time_ns, peak_rss_bytes))
}

fn executable_revision(executable: &Path) -> BenchResult<String> {
    let mut file = fs::File::open(executable)?;
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

fn base_git_revision(workspace_root: &Path) -> BenchResult<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(workspace_root)
        .args(["rev-parse", "HEAD"])
        .output()?;
    if !output.status.success() {
        return Err(bench_error(
            "ClickHouse roofline could not resolve the base Git revision",
        ));
    }
    let revision = String::from_utf8(output.stdout)?;
    let revision = revision.trim();
    if revision.len() != 40 || !revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(bench_error(
            "ClickHouse roofline resolved an invalid base Git revision",
        ));
    }
    Ok(revision.to_owned())
}

fn workspace_content_revision(workspace_root: &Path) -> BenchResult<(String, Vec<String>)> {
    const INPUTS: &[&str] = &[
        "Cargo.toml",
        "Cargo.lock",
        "crates/cdf-benchmarks/Cargo.toml",
        "crates/cdf-benchmarks/src/bin/clickhouse-source-roofline.rs",
        "crates/cdf-benchmarks/src/clickhouse_source_roofline.rs",
        "crates/cdf-source-clickhouse/Cargo.toml",
        "crates/cdf-source-clickhouse/src/catalog.rs",
        "crates/cdf-source-clickhouse/src/client.rs",
        "crates/cdf-source-clickhouse/src/driver.rs",
        "crates/cdf-source-clickhouse/src/error.rs",
        "crates/cdf-source-clickhouse/src/execution.rs",
        "crates/cdf-source-clickhouse/src/identifier.rs",
        "crates/cdf-source-clickhouse/src/lib.rs",
        "crates/cdf-source-clickhouse/src/memory.rs",
        "crates/cdf-source-clickhouse/src/query.rs",
        "crates/cdf-source-clickhouse/src/resource.rs",
        "crates/cdf-source-clickhouse/src/tests.rs",
        "crates/cdf-source-clickhouse/src/types.rs",
        "third-party/arrow-ipc-58.3.0-cdf/Cargo.toml",
        "third-party/arrow-ipc-58.3.0-cdf/src/compression.rs",
        "third-party/arrow-ipc-58.3.0-cdf/src/convert.rs",
        "third-party/arrow-ipc-58.3.0-cdf/src/reader.rs",
        "third-party/arrow-ipc-58.3.0-cdf/src/reader/stream.rs",
        "third-party/clickhouse-0.15.1-cdf/Cargo.toml",
        "third-party/clickhouse-0.15.1-cdf/src/compression/lz4.rs",
        "third-party/clickhouse-0.15.1-cdf/src/compression/zstd.rs",
        "third-party/clickhouse-0.15.1-cdf/src/error.rs",
        "third-party/clickhouse-0.15.1-cdf/src/http_client.rs",
        "third-party/clickhouse-0.15.1-cdf/src/insert_formatted.rs",
        "third-party/clickhouse-0.15.1-cdf/src/lib.rs",
        "third-party/clickhouse-0.15.1-cdf/src/query.rs",
        "third-party/clickhouse-0.15.1-cdf/src/response.rs",
        "third-party/clickhouse-ext-arrow-0.1.0-cdf/Cargo.toml",
        "third-party/clickhouse-ext-arrow-0.1.0-cdf/src/lib.rs",
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

fn roofline_arrow_limits() -> Result<ArrowStreamLimits> {
    let nonzero = |value| {
        NonZeroUsize::new(value)
            .ok_or_else(|| CdfError::internal("ClickHouse roofline response limit must be nonzero"))
    };
    Ok(ArrowStreamLimits::new(
        ResponseLimits::new(
            nonzero(CLICKHOUSE_ERROR_BODY_BYTES)?,
            nonzero(CLICKHOUSE_ERROR_BODY_BYTES)?,
            nonzero(CLICKHOUSE_HTTP_INPUT_CHUNK_BYTES)?,
            nonzero(CLICKHOUSE_ARROW_BODY_BYTES)?,
        ),
        nonzero(CLICKHOUSE_ARROW_MESSAGE_BYTES)?,
        nonzero(CLICKHOUSE_ARROW_BODY_BYTES)?,
    )
    .with_max_record_batch_rows(CLICKHOUSE_MAXIMUM_RECORD_BATCH_ROWS)
    .with_schema_limits(
        CLICKHOUSE_ARROW_SCHEMA_NODES,
        CLICKHOUSE_ARROW_SCHEMA_METADATA_ENTRIES,
        CLICKHOUSE_ARROW_SCHEMA_BYTES,
        CLICKHOUSE_ARROW_SCHEMA_DEPTH,
    ))
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

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt64Array};

    use super::{MANAGED_MEMORY_BYTES, logical_useful_arrow_bytes, validate_roofline_payload};

    fn payload_batch(ids: Vec<u64>, metrics: Vec<u64>, updated: Vec<i64>) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(UInt64Array::from(ids)) as ArrayRef),
            ("metric", Arc::new(UInt64Array::from(metrics)) as ArrayRef),
            (
                "updated_at",
                Arc::new(Int64Array::from(updated)) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[test]
    fn direct_benchmark_futures_enter_the_injected_io_reactor() {
        let (_host, execution) =
            cdf_engine::StandaloneExecutionHost::default_services(MANAGED_MEMORY_BYTES).unwrap();
        let reactor_available = execution
            .run_io(async {
                Ok::<_, cdf_kernel::CdfError>(tokio::runtime::Handle::try_current().is_ok())
            })
            .unwrap();
        assert!(reactor_available);
    }

    #[test]
    fn payload_law_is_logical_content_not_allocator_capacity() {
        let whole = payload_batch(vec![0, 1, 2], vec![0, 17, 34], vec![0, 1, 2]);
        let mut whole_checksum = 0;
        validate_roofline_payload(&whole, 0, &mut whole_checksum).unwrap();
        assert_eq!(
            logical_useful_arrow_bytes(whole.num_rows() as u64).unwrap(),
            72
        );

        let mut split_checksum = 0;
        validate_roofline_payload(
            &payload_batch(vec![0, 1], vec![0, 17], vec![0, 1]),
            0,
            &mut split_checksum,
        )
        .unwrap();
        validate_roofline_payload(
            &payload_batch(vec![2], vec![34], vec![2]),
            2,
            &mut split_checksum,
        )
        .unwrap();
        assert_eq!(split_checksum, whole_checksum);

        let mut corrupted_checksum = 0;
        let error = validate_roofline_payload(
            &payload_batch(vec![0], vec![1], vec![0]),
            0,
            &mut corrupted_checksum,
        )
        .unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("logical row 0"));
    }
}
