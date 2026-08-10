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
        mpsc,
    },
    thread,
    time::Duration,
    time::Instant,
};

use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray,
    StructArray,
};
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CanonicalArrowField, Checkpoint, CheckpointId, CheckpointStatus,
    CursorPosition, CursorValue, DestinationCorrectionOperation, DestinationCorrectionPlan,
    DestinationCorrectionRequest, PackageHash, PartitionId, PipelineId, PromotionId,
    ResidualCorrectionOperation, ResourceId, RowProvenanceAddress, ScopeKey, SegmentId,
    SourcePosition, StateDelta,
};
use cdf_package::{PackageBuilder, PackageReader};
use cdf_package_contract::{
    DestinationCommitPlanPreimage, PackageManifest, QuarantineObservedValue, QuarantineRecord,
    SegmentEntry, StateDeltaPreimage,
};
use cdf_runtime::DestinationRuntime;
use postgres::{Client, NoTls};
use tempfile::TempDir;

use super::*;
use crate::{api::PostgresCommitRequest, models::PostgresCorrectionCommitRequest};
use crate::{
    commit::validate_session_begin_inputs, ddl::target_migrations,
    identifiers::quote_identifier_unchecked,
};

fn collect_package_receipts(reader: &PackageReader) -> Vec<cdf_kernel::Receipt> {
    let mut receipts = Vec::new();
    reader
        .for_each_receipt(&mut |receipt| {
            receipts.push(receipt);
            Ok(())
        })
        .unwrap();
    receipts
}

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
    let canonical = cdf_package_contract::append_package_row_ord(vec![batch.clone()], 0)
        .unwrap()
        .pop()
        .unwrap();
    encoder
        .write_batch(&canonical, 1, 1_700_000_000_000)
        .unwrap();
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
        "postgres_local_copy rows={} wall_time_ns={} binary_rows_per_second={:.0} csv_rows_per_second={:.0} speedup={speedup:.2}x",
        ROWS,
        binary_elapsed.as_nanos(),
        ROWS as f64 / binary_elapsed.as_secs_f64(),
        ROWS as f64 / csv_elapsed.as_secs_f64(),
    );
    assert!(speedup >= 2.0, "local COPY speedup was {speedup:.2}x");
}

#[test]
fn live_binary_copy_persists_complex_arrow_values_as_jsonb() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let mut client = postgres.client();
    client
        .batch_execute(
            "CREATE TABLE jsonb_copy_test (
                payload JSONB,
                _cdf_row_key BIGINT NOT NULL,
                _cdf_loaded_at_ms BIGINT NOT NULL
            )",
        )
        .unwrap();
    let structure = StructArray::from(vec![
        (
            Arc::new(Field::new("code", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![7_i64])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("label", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("seven")])) as ArrayRef,
        ),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            structure.data_type().clone(),
            false,
        )])),
        vec![Arc::new(structure)],
    )
    .unwrap();
    let canonical = cdf_package_contract::append_package_row_ord(vec![batch], 0)
        .unwrap()
        .pop()
        .unwrap();
    let writer = client
        .copy_in("COPY jsonb_copy_test FROM STDIN WITH (FORMAT binary)")
        .unwrap();
    let mut encoder = crate::binary_copy::BinaryCopyEncoder::new(writer, 1).unwrap();
    encoder.write_batch(&canonical, 1, 1).unwrap();
    let (writer, rows) = encoder.finish().unwrap();
    assert_eq!(rows, 1);
    assert_eq!(writer.finish().unwrap(), 1);

    let row = client
        .query_one(
            "SELECT payload->>'code', payload->>'label', pg_typeof(payload)::text FROM jsonb_copy_test",
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "7");
    assert_eq!(row.get::<_, String>(1), "seven");
    assert_eq!(row.get::<_, String>(2), "jsonb");
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
        let (_, execution) =
            cdf_engine::StandaloneExecutionHost::default_services(512 * 1024 * 1024).unwrap();
        execution
            .ensure_blocking_lanes(&crate::runtime::postgres_runtime_capabilities().blocking_lanes)
            .unwrap();
        PostgresDestination::connect(self.url.clone())
            .unwrap()
            .with_execution_services(Some(execution))
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

fn live_cdc_package(
    target: TargetName,
    package_id: &str,
    parent_package_id: Option<&str>,
    policy: cdf_kernel::DeleteApplicationPolicy,
    upserts: &[(i64, &str, bool)],
    deletes: &[i64],
) -> (
    cdf_package_contract::PackageReplayInputs,
    Schema,
    Vec<cdf_kernel::CommitSegment>,
) {
    let fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(
            upserts.iter().map(|row| row.0),
        )),
        Arc::new(StringArray::from_iter(
            upserts.iter().map(|row| Some(row.1)),
        )),
    ];
    let schema = Schema::new(fields);
    let schema_hash = cdf_kernel::canonical_arrow_schema_hash(&schema).unwrap();
    let delete_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let delete_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&delete_schema).unwrap();
    let content = cdf_kernel::PackageContentAuthority::KeyedChanges {
        logical_schema_hash: schema_hash.clone(),
        upsert_schema_hash: schema_hash.clone(),
        delete_schema_hash: delete_schema_hash.clone(),
        key: cdf_kernel::KeyAuthority {
            version: cdf_kernel::KEYED_EFFECT_AUTHORITY_VERSION,
            fields: vec!["id".to_owned()],
            encoding: cdf_kernel::DEDUP_KEY_ENCODING_VERSION.to_owned(),
            schema_hash: delete_schema_hash,
        },
        reduction: Box::new(cdf_kernel::KeyedEffectReductionAuthority {
            version: cdf_kernel::KEYED_EFFECT_AUTHORITY_VERSION,
            winner: cdf_kernel::KeyedEffectWinnerPolicy::Last,
            input_order: cdf_kernel::KeyedEffectInputOrder::SourceProtocol {
                protocol: "postgres-live-cdc-fixture".to_owned(),
                version: 1,
                scope_sha256: format!("sha256:{}", "d".repeat(64)),
            },
            input: cdf_kernel::KeyedEffectCounts {
                upserts: upserts.len() as u64,
                deletes: deletes.len() as u64,
            },
            duplicate_key_count: 0,
            surviving: cdf_kernel::KeyedEffectCounts {
                upserts: upserts.len() as u64,
                deletes: deletes.len() as u64,
            },
            provenance_format: "parquet".to_owned(),
            provenance_version: 1,
        }),
        deletion_capture: cdf_kernel::DeletionCaptureAuthority {
            support: cdf_kernel::DeletionCaptureSupport::Inherent,
            enabled: true,
            semantics_sha256: format!("sha256:{}", "e".repeat(64)),
        },
        delete_application: cdf_kernel::DeleteApplicationAuthority::Apply { policy },
    };
    let scope = ScopeKey::Partition {
        partition_id: PartitionId::new("p0").unwrap(),
    };
    let position = SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "event".to_owned(),
        value: CursorValue::I64(package_id.bytes().fold(0_i64, |value, byte| {
            value.wrapping_mul(31) + i64::from(byte)
        })),
    });
    let mut states = Vec::new();
    let mut commit_segments = Vec::new();
    let mut ordinal = 0_u64;
    if !upserts.is_empty() {
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays).unwrap();
        let batch = cdf_package_contract::append_package_row_ord(vec![batch], ordinal)
            .unwrap()
            .pop()
            .unwrap();
        let state = StateSegment {
            kind: cdf_kernel::PackageSegmentKind::Upsert,
            segment_id: SegmentId::new(format!("seg-{package_id}-upsert")).unwrap(),
            scope: scope.clone(),
            output_position: position.clone(),
            row_count: upserts.len() as u64,
            byte_count: 1024,
        };
        ordinal += upserts.len() as u64;
        states.push(state.clone());
        commit_segments.push(cdf_kernel::CommitSegment::new(state, 1024, vec![batch]));
    }
    if !deletes.is_empty() {
        let batch = RecordBatch::try_new(
            Arc::new(delete_schema),
            vec![Arc::new(Int64Array::from_iter_values(
                deletes.iter().copied(),
            ))],
        )
        .unwrap();
        let batch = cdf_package_contract::append_package_row_ord(vec![batch], ordinal)
            .unwrap()
            .pop()
            .unwrap();
        let state = StateSegment {
            kind: cdf_kernel::PackageSegmentKind::Delete,
            segment_id: SegmentId::new(format!("seg-{package_id}-delete")).unwrap(),
            scope: scope.clone(),
            output_position: position.clone(),
            row_count: deletes.len() as u64,
            byte_count: 512,
        };
        states.push(state.clone());
        commit_segments.push(cdf_kernel::CommitSegment::new(state, 512, vec![batch]));
    }
    let package_hash = PackageHash::new(format!("sha256:{package_id}")).unwrap();
    let delta = StateDelta {
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        pipeline_id: PipelineId::new("pipeline-postgres-live-cdc").unwrap(),
        resource_id: ResourceId::new(format!(
            "postgres_live_cdc_{}",
            target.as_str().replace('.', "_")
        ))
        .unwrap(),
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: parent_package_id
            .map(|parent| CheckpointId::new(format!("checkpoint-{parent}")))
            .transpose()
            .unwrap(),
        input_position: None,
        output_position: position,
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        package_hash: package_hash.clone(),
        content: content.clone(),
        schema_hash: schema_hash.clone(),
        segments: states.clone(),
    };
    let request = DestinationCommitRequest {
        package_hash: package_hash.clone(),
        content,
        target,
        disposition: WriteDisposition::CdcApply,
        segments: states,
        idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
    };
    (
        cdf_package_contract::PackageReplayInputs {
            input_checkpoint: None,
            state_delta: delta,
            destination_commit: request,
            schema_hash,
            merge_keys: vec!["id".to_owned()],
            destination_policy: Default::default(),
            run_schema_authority: None,
        },
        schema,
        commit_segments,
    )
}

struct LiveVerifiedPackageAccess {
    package_hash: String,
    quarantines: Vec<QuarantineRecord>,
}

impl LiveVerifiedPackageAccess {
    fn empty(inputs: &cdf_package_contract::PackageReplayInputs) -> Self {
        Self {
            package_hash: inputs.destination_commit.package_hash.as_str().to_owned(),
            quarantines: Vec::new(),
        }
    }
}

impl cdf_package_contract::VerifiedPackageAccess for LiveVerifiedPackageAccess {
    fn package_hash(&self) -> &str {
        &self.package_hash
    }

    fn for_each_identity_segment(
        &self,
        _visitor: &mut dyn FnMut(SegmentEntry) -> Result<()>,
    ) -> Result<()> {
        Err(CdfError::internal(
            "live direct-commit package fixture does not expose identity segments",
        ))
    }

    fn recorded_scan_plan(&self) -> Result<cdf_kernel::ScanPlan> {
        Err(CdfError::internal(
            "live direct-commit package fixture does not expose a scan plan",
        ))
    }

    fn replay_inputs(&self) -> Result<cdf_package_contract::PackageReplayInputs> {
        Err(CdfError::internal(
            "live direct-commit package fixture does not expose replay inputs",
        ))
    }

    fn runtime_arrow_schema(&self) -> Result<arrow_schema::SchemaRef> {
        Err(CdfError::internal(
            "live direct-commit package fixture does not expose a runtime schema",
        ))
    }

    fn for_each_quarantine_record(
        &self,
        visitor: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
    ) -> Result<()> {
        self.quarantines.iter().cloned().try_for_each(visitor)
    }
}

fn commit_live_cdc(
    destination: &PostgresDestination,
    inputs: &cdf_package_contract::PackageReplayInputs,
    schema: &Schema,
    segments: &[cdf_kernel::CommitSegment],
) -> cdf_runtime::DestinationCommitOutcome {
    let owned_segments = segments.to_vec();
    let package = LiveVerifiedPackageAccess::empty(inputs);
    crate::cdc::commit_single_target(
        destination,
        &package,
        inputs,
        schema,
        Box::new(owned_segments.into_iter().map(Ok)),
    )
    .unwrap_or_else(|error| {
        panic!(
            "commit {} failed: {error}",
            inputs.destination_commit.package_hash
        )
    })
}

fn live_routed_cdc_package(
    logical_target: TargetName,
    package_id: &str,
    corrupt_second_schema: bool,
) -> (
    cdf_package_contract::PackageReplayInputs,
    Vec<cdf_kernel::CommitSegment>,
    Vec<TargetName>,
) {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]);
    let schema_hash = cdf_kernel::canonical_arrow_schema_hash(&schema).unwrap();
    let delete_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let delete_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&delete_schema).unwrap();
    let route_values = StringArray::from(vec!["alpha", "beta"]);
    let family = cdf_kernel::RouteTargetFamily::new(
        cdf_kernel::RoutePlan::new("kind", 2).unwrap(),
        logical_target.clone(),
        Some(63),
        (0..2).map(|index| {
            (
                cdf_kernel::RouteScalar::from_array(&route_values, index).unwrap(),
                schema_hash.clone(),
            )
        }),
    )
    .unwrap();
    let scope = ScopeKey::Partition {
        partition_id: PartitionId::new("p0").unwrap(),
    };
    let position = SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "event".to_owned(),
        value: CursorValue::I64(1),
    });
    let mut states = Vec::new();
    let mut segments = Vec::new();
    let mut outputs = Vec::new();
    for (index, binding) in family.bindings.iter().enumerate() {
        let segment_id = SegmentId::new(format!("seg-{package_id}-{index}")).unwrap();
        let state = StateSegment {
            kind: cdf_kernel::PackageSegmentKind::Upsert,
            segment_id: segment_id.clone(),
            scope: scope.clone(),
            output_position: position.clone(),
            row_count: 1,
            byte_count: 1024,
        };
        let batch_schema = if corrupt_second_schema && index == 1 {
            Schema::new(vec![Field::new("id", DataType::Int64, false)])
        } else {
            schema.clone()
        };
        let arrays: Vec<ArrayRef> = if corrupt_second_schema && index == 1 {
            vec![Arc::new(Int64Array::from(vec![2_i64]))]
        } else {
            vec![
                Arc::new(Int64Array::from(vec![index as i64 + 1])),
                Arc::new(StringArray::from(vec![format!("route-{index}")])),
            ]
        };
        let batch = RecordBatch::try_new(Arc::new(batch_schema), arrays).unwrap();
        let batch = cdf_package_contract::append_package_row_ord(vec![batch], index as u64)
            .unwrap()
            .pop()
            .unwrap();
        states.push(state.clone());
        segments.push(cdf_kernel::CommitSegment::new(state, 1024, vec![batch]));
        outputs.push(cdf_kernel::RoutedOutputContentAuthority {
            output_binding: binding.output_binding.clone(),
            schema: cdf_kernel::CanonicalArrowSchema::from_arrow(&schema).unwrap(),
            content: Box::new(cdf_kernel::PackageContentAuthority::KeyedChanges {
                logical_schema_hash: schema_hash.clone(),
                upsert_schema_hash: schema_hash.clone(),
                delete_schema_hash: delete_schema_hash.clone(),
                key: cdf_kernel::KeyAuthority {
                    version: cdf_kernel::KEYED_EFFECT_AUTHORITY_VERSION,
                    fields: vec!["id".to_owned()],
                    encoding: cdf_kernel::DEDUP_KEY_ENCODING_VERSION.to_owned(),
                    schema_hash: delete_schema_hash.clone(),
                },
                reduction: Box::new(cdf_kernel::KeyedEffectReductionAuthority {
                    version: cdf_kernel::KEYED_EFFECT_AUTHORITY_VERSION,
                    winner: cdf_kernel::KeyedEffectWinnerPolicy::Last,
                    input_order: cdf_kernel::KeyedEffectInputOrder::SourceProtocol {
                        protocol: "postgres-live-routed-cdc-fixture".to_owned(),
                        version: 1,
                        scope_sha256: format!("sha256:{}", "f".repeat(64)),
                    },
                    input: cdf_kernel::KeyedEffectCounts {
                        upserts: 1,
                        deletes: 0,
                    },
                    duplicate_key_count: 0,
                    surviving: cdf_kernel::KeyedEffectCounts {
                        upserts: 1,
                        deletes: 0,
                    },
                    provenance_format: "parquet".to_owned(),
                    provenance_version: 1,
                }),
                deletion_capture: cdf_kernel::DeletionCaptureAuthority {
                    support: cdf_kernel::DeletionCaptureSupport::Inherent,
                    enabled: true,
                    semantics_sha256: format!("sha256:{}", "1".repeat(64)),
                },
                delete_application: cdf_kernel::DeleteApplicationAuthority::Apply {
                    policy: cdf_kernel::DeleteApplicationPolicy::Hard,
                },
            }),
            segment_ids: vec![segment_id],
        });
    }
    let physical_targets = family
        .bindings
        .iter()
        .map(|binding| binding.physical_target.clone())
        .collect::<Vec<_>>();
    let family_hash = family.schema_family_hash.clone();
    let content = cdf_kernel::PackageContentAuthority::Routed { family, outputs };
    let package_hash = PackageHash::new(format!("sha256:{package_id}")).unwrap();
    let delta = StateDelta {
        checkpoint_id: CheckpointId::new(format!("checkpoint-{package_id}")).unwrap(),
        pipeline_id: PipelineId::new("pipeline-postgres-live-routed-cdc").unwrap(),
        resource_id: ResourceId::new(format!(
            "postgres_live_routed_{}",
            logical_target.as_str().replace('.', "_")
        ))
        .unwrap(),
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: position,
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        package_hash: package_hash.clone(),
        content: content.clone(),
        schema_hash: family_hash.clone(),
        segments: states.clone(),
    };
    let request = DestinationCommitRequest {
        package_hash: package_hash.clone(),
        content,
        target: logical_target,
        disposition: WriteDisposition::CdcApply,
        segments: states,
        idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
    };
    (
        cdf_package_contract::PackageReplayInputs {
            input_checkpoint: None,
            state_delta: delta,
            destination_commit: request,
            schema_hash: family_hash,
            merge_keys: vec!["id".to_owned()],
            destination_policy: Default::default(),
            run_schema_authority: None,
        },
        segments,
        physical_targets,
    )
}

#[test]
fn live_cdc_applies_hard_ignore_soft_missing_and_duplicate_replay() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let destination = env.destination();

    let hard_target = env.target("cdc_hard").target_name().unwrap();
    let (base, schema, base_segments) = live_cdc_package(
        hard_target.clone(),
        "postgres-cdc-hard-base",
        None,
        cdf_kernel::DeleteApplicationPolicy::Hard,
        &[(1, "one", false), (2, "two", false)],
        &[],
    );
    commit_live_cdc(&destination, &base, &schema, &base_segments);
    let (change, schema, change_segments) = live_cdc_package(
        hard_target,
        "postgres-cdc-hard-change",
        Some("postgres-cdc-hard-base"),
        cdf_kernel::DeleteApplicationPolicy::Hard,
        &[(1, "one-updated", false)],
        &[2, 999],
    );
    let first = commit_live_cdc(&destination, &change, &schema, &change_segments);
    assert_eq!(
        first.receipt.counts,
        CommitCounts::keyed_changes(
            cdf_kernel::KeyedEffectCounts {
                upserts: 1,
                deletes: 2,
            },
            Some(0),
            Some(1),
            Some(1),
            None,
            Some(1),
            None,
        )
    );
    let replay = commit_live_cdc(&destination, &change, &schema, &change_segments);
    assert_eq!(replay.receipt, first.receipt);
    assert_eq!(
        replay.reporting_policy,
        cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate: true }
    );
    let mut client = env.client();
    let rows = client
        .query("SELECT id, name FROM cdc_hard ORDER BY id", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "one-updated");

    let (ignored, schema, ignored_segments) = live_cdc_package(
        env.target("cdc_hard").target_name().unwrap(),
        "postgres-cdc-ignore",
        Some("postgres-cdc-hard-change"),
        cdf_kernel::DeleteApplicationPolicy::Ignore,
        &[],
        &[1],
    );
    let ignored = commit_live_cdc(&destination, &ignored, &schema, &ignored_segments);
    assert_eq!(
        ignored.receipt.counts,
        CommitCounts::keyed_changes(
            cdf_kernel::KeyedEffectCounts {
                upserts: 0,
                deletes: 1,
            },
            Some(0),
            Some(0),
            None,
            None,
            None,
            Some(1),
        )
    );
    assert_eq!(
        client
            .query_one("SELECT COUNT(*)::bigint FROM cdc_hard", &[])
            .unwrap()
            .get::<_, i64>(0),
        1
    );

    let soft_target = env.target("cdc_soft").target_name().unwrap();
    let soft_policy = cdf_kernel::DeleteApplicationPolicy::Soft {
        marker_field: "deleted".to_owned(),
    };
    let (soft_base, schema, soft_base_segments) = live_cdc_package(
        soft_target.clone(),
        "postgres-cdc-soft-base",
        None,
        soft_policy.clone(),
        &[(7, "seven", true)],
        &[],
    );
    commit_live_cdc(&destination, &soft_base, &schema, &soft_base_segments);
    let (soft_delete, schema, soft_delete_segments) = live_cdc_package(
        soft_target.clone(),
        "postgres-cdc-soft-delete",
        Some("postgres-cdc-soft-base"),
        soft_policy.clone(),
        &[],
        &[7, 888],
    );
    let soft_delete = commit_live_cdc(&destination, &soft_delete, &schema, &soft_delete_segments);
    let CommitCounts::KeyedChanges {
        soft_deletes,
        missing_delete_keys,
        ..
    } = soft_delete.receipt.counts
    else {
        panic!("expected keyed soft-delete counts");
    };
    assert_eq!(soft_deletes, Some(1));
    assert_eq!(missing_delete_keys, Some(1));
    let (repeat_soft, schema, repeat_soft_segments) = live_cdc_package(
        soft_target.clone(),
        "postgres-cdc-soft-repeat",
        Some("postgres-cdc-soft-delete"),
        soft_policy.clone(),
        &[],
        &[7],
    );
    let repeat_soft = commit_live_cdc(&destination, &repeat_soft, &schema, &repeat_soft_segments);
    let CommitCounts::KeyedChanges {
        soft_deletes,
        missing_delete_keys,
        ..
    } = repeat_soft.receipt.counts
    else {
        panic!("expected repeated soft-delete counts");
    };
    assert_eq!(soft_deletes, None);
    assert_eq!(missing_delete_keys, None);
    let row = client
        .query_one("SELECT name, deleted FROM cdc_soft WHERE id = 7", &[])
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "seven");
    assert!(row.get::<_, bool>(1));
    let (resurrect, schema, resurrect_segments) = live_cdc_package(
        soft_target,
        "postgres-cdc-soft-resurrect",
        Some("postgres-cdc-soft-repeat"),
        soft_policy,
        &[(7, "seven-live", true)],
        &[],
    );
    commit_live_cdc(&destination, &resurrect, &schema, &resurrect_segments);
    let row = client
        .query_one("SELECT name, deleted FROM cdc_soft WHERE id = 7", &[])
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "seven-live");
    assert!(!row.get::<_, bool>(1));
}

#[test]
fn live_routed_cdc_commits_two_targets_atomically_and_rolls_back_family_failure() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let destination = env.destination();
    let (inputs, segments, targets) = live_routed_cdc_package(
        env.target("routed_events").target_name().unwrap(),
        "postgres-routed-success",
        false,
    );
    let routed_package = LiveVerifiedPackageAccess {
        package_hash: inputs.destination_commit.package_hash.as_str().to_owned(),
        quarantines: vec![QuarantineRecord {
            source_row_ordinal: 11,
            rule_id: "routed-cdc-rule".to_owned(),
            error_code: "invalid_value".to_owned(),
            source_position: None,
            observed_value_redacted: QuarantineObservedValue::Omitted,
        }],
    };
    let first = crate::cdc::commit_routed(
        &destination,
        &routed_package,
        &inputs,
        Box::new(segments.clone().into_iter().map(Ok)),
    )
    .unwrap();
    let CommitCounts::Routed { targets: counts } = &first.receipt.counts else {
        panic!("expected routed Postgres counts");
    };
    assert_eq!(counts.len(), 2);
    let replay = crate::cdc::commit_routed(
        &destination,
        &routed_package,
        &inputs,
        Box::new(segments.into_iter().map(Ok)),
    )
    .unwrap();
    assert_eq!(replay.receipt, first.receipt);
    assert_eq!(
        replay.reporting_policy,
        cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate: true }
    );
    let mut client = env.client();
    assert_eq!(
        client
            .query_one(
                &format!(
                    "SELECT COUNT(*)::bigint FROM {} WHERE target = $1 AND package_hash = $2",
                    quote_identifier_unchecked(CDF_QUARANTINE_TABLE)
                ),
                &[
                    &inputs.destination_commit.target.as_str(),
                    &inputs.destination_commit.package_hash.as_str(),
                ],
            )
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    for target in &targets {
        let target = PostgresTarget::parse(target.as_str()).unwrap();
        assert_eq!(
            client
                .query_one(
                    &format!("SELECT COUNT(*)::bigint FROM {}", target.sql()),
                    &[],
                )
                .unwrap()
                .get::<_, i64>(0),
            1
        );
    }

    let (failed_inputs, failed_segments, failed_targets) = live_routed_cdc_package(
        env.target("routed_failure").target_name().unwrap(),
        "postgres-routed-failure",
        true,
    );
    let error = crate::cdc::commit_routed(
        &destination,
        &LiveVerifiedPackageAccess::empty(&failed_inputs),
        &failed_inputs,
        Box::new(failed_segments.into_iter().map(Ok)),
    )
    .unwrap_err();
    assert!(error.message.contains("package schema"), "{error}");
    for target in failed_targets {
        let target = PostgresTarget::parse(target.as_str()).unwrap();
        let schema = target.schema.as_ref().map(PostgresIdentifier::as_str);
        let exists: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = COALESCE($1, current_schema()) AND table_name = $2)",
                &[&schema, &target.table.as_str()],
            )
            .unwrap()
            .get(0);
        assert!(
            !exists,
            "failed routed transaction left {}",
            target.display_name()
        );
    }
    assert_eq!(
        load_mirror_count(
            &mut client,
            failed_inputs.destination_commit.target.as_str(),
            failed_inputs.destination_commit.package_hash.as_str(),
        ),
        0
    );
}

#[test]
fn live_idempotency_lock_serializes_same_package_before_payload_work() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let mut first = env.client();
    for statement in crate::ddl::system_table_ddl() {
        first.batch_execute(&statement.sql).unwrap();
    }
    first.batch_execute("BEGIN").unwrap();
    let target = env.target("orders_race").display_name();
    let package_hash = "sha256:race-package".to_owned();
    let token = package_hash.clone();
    let lock_sql = crate::ddl::idempotency_lock_statement().sql;
    let check_sql = crate::ddl::idempotency_check_statement().sql;
    first
        .query_one(&lock_sql, &[&target, &package_hash])
        .unwrap();
    let initial = first
        .query_opt(&check_sql, &[&target, &package_hash, &token])
        .unwrap()
        .map(|row| row.get::<_, String>(0));
    assert!(initial.is_none());

    let url = env.url.clone();
    let schema = env.schema.clone();
    let second_target = target.clone();
    let second_hash = package_hash.clone();
    let second_token = token.clone();
    let second_lock = lock_sql.clone();
    let second_sql = check_sql.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let mut second = Client::connect(&url, NoTls).unwrap();
        second
            .batch_execute(&format!(
                "SET search_path = {}, public; BEGIN",
                quote_identifier(&schema).unwrap()
            ))
            .unwrap();
        started_tx.send(()).unwrap();
        second
            .query_one(&second_lock, &[&second_target, &second_hash])
            .unwrap();
        let stored = second
            .query_opt(&second_sql, &[&second_target, &second_hash, &second_token])
            .unwrap()
            .map(|row| row.get::<_, String>(0));
        second.batch_execute("COMMIT").unwrap();
        result_tx.send(stored).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(
        result_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "same-package contender crossed the transaction-scoped idempotency lock"
    );

    first
        .execute(
            &format!(
                "INSERT INTO {} (\"receipt_id\", \"destination\", \"target\", \"resource_id\", \"package_hash\", \"idempotency_token\", \"disposition\", \"schema_hash\", \"rows_written\", \"segment_count\", \"migrations_json\", \"receipt_json\", \"xid\", \"duplicate\", \"committed_at_ms\") VALUES ('race-receipt', 'postgres', $1, NULL, $2, $3, 'append', 'schema', 0, 0, '[]'::jsonb, '{{}}'::jsonb, '1', FALSE, 1)",
                quote_identifier_unchecked(CDF_LOADS_TABLE)
            ),
            &[&target, &package_hash, &token],
        )
        .unwrap();
    first.batch_execute("COMMIT").unwrap();

    assert_eq!(
        result_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        Some("{}".to_owned())
    );
    waiter.join().unwrap();
}

#[test]
fn live_state_mirror_cas_uses_checkpoint_lineage_not_commit_time() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let mut client = env.client();
    for statement in crate::ddl::system_table_ddl() {
        client.batch_execute(&statement.sql).unwrap();
    }
    let sql = crate::mirrors::state_mirror_sql();
    let pipeline = "pipe-lineage";
    let resource = "resource-lineage";
    let scope_json = serde_json::to_string(&scope()).unwrap();
    let position_json = serde_json::to_string(&position(10)).unwrap();
    let state_version = i32::from(CHECKPOINT_STATE_VERSION);
    let no_parent: Option<&str> = None;
    assert_eq!(
        client
            .execute(
                &sql,
                &[
                    &pipeline,
                    &resource,
                    &scope_json,
                    &state_version,
                    &"checkpoint-1",
                    &no_parent,
                    &"package-1",
                    &"schema-1",
                    &position_json,
                    &"receipt-1",
                    &1_000_i64,
                ],
            )
            .unwrap(),
        1
    );
    let parent = Some("checkpoint-1");
    assert_eq!(
        client
            .execute(
                &sql,
                &[
                    &pipeline,
                    &resource,
                    &scope_json,
                    &state_version,
                    &"checkpoint-2",
                    &parent,
                    &"package-2",
                    &"schema-1",
                    &position_json,
                    &"receipt-2",
                    &1_i64,
                ],
            )
            .unwrap(),
        1,
        "a valid successor must survive wall-clock regression"
    );
    let stale_parent = Some("checkpoint-1");
    assert_eq!(
        client
            .execute(
                &sql,
                &[
                    &pipeline,
                    &resource,
                    &scope_json,
                    &state_version,
                    &"checkpoint-3",
                    &stale_parent,
                    &"package-3",
                    &"schema-1",
                    &position_json,
                    &"receipt-3",
                    &2_000_i64,
                ],
            )
            .unwrap(),
        0,
        "a competing branch must lose even with a later wall-clock value"
    );
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
        cdf_contract::CDF_VARIANT_SEMANTIC.to_owned(),
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
    let builder = PackageBuilder::create(
        root,
        package_id,
        cdf_kernel::PackageContentAuthority::rows(schema_hash()),
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    if let Some((_, batch)) = segments.first() {
        builder
            .write_runtime_arrow_schema(batch.schema().as_ref())
            .unwrap();
    }
    let mut package_row_ord_start = 0_u64;
    for (segment_id, batch) in segments {
        let rows = batch.num_rows() as u64;
        let batch =
            cdf_package_contract::append_package_row_ord(vec![batch], package_row_ord_start)
                .unwrap();
        builder
            .write_segment(
                cdf_kernel::PackageSegmentKind::Row,
                SegmentId::new(segment_id).unwrap(),
                package_row_ord_start,
                &batch,
            )
            .unwrap();
        package_row_ord_start += rows;
    }
    builder.finish().unwrap();
    cdf_package::read_manifest(root).unwrap()
}

fn replay_merge_keys(disposition: &WriteDisposition) -> Vec<String> {
    match disposition {
        WriteDisposition::Merge | WriteDisposition::CdcApply => vec!["id".to_owned()],
        WriteDisposition::Append | WriteDisposition::Replace => Vec::new(),
    }
}

fn write_replay_artifacts(
    builder: &PackageBuilder,
    target: TargetName,
    disposition: WriteDisposition,
    checkpoint_id: &str,
    parent_checkpoint_id: Option<&str>,
    entries: &[SegmentEntry],
    content: cdf_kernel::PackageContentAuthority,
) {
    let segments = entries
        .iter()
        .map(|entry| StateSegment {
            kind: entry.kind,
            segment_id: entry.segment_id.clone(),
            scope: scope(),
            output_position: position(10),
            row_count: entry.row_count,
            byte_count: entry.byte_count,
        })
        .collect::<Vec<_>>();
    let input_position = parent_checkpoint_id.map(|_| position(10));
    let input_checkpoint = parent_checkpoint_id.map(|parent_checkpoint_id| Checkpoint {
        delta: StateDelta {
            checkpoint_id: CheckpointId::new(parent_checkpoint_id).unwrap(),
            pipeline_id: PipelineId::new("pipe-live").unwrap(),
            resource_id: ResourceId::new("orders").unwrap(),
            scope: scope(),
            state_version: CHECKPOINT_STATE_VERSION,
            parent_checkpoint_id: None,
            input_position: None,
            output_position: position(10),
            output_watermark: None,
            partition_watermarks: Vec::new(),
            late_data_carryover: Vec::new(),
            source_continuation: None,
            package_hash: PackageHash::new("sha256:fixture-parent").unwrap(),
            content: cdf_kernel::PackageContentAuthority::rows(schema_hash()),
            schema_hash: schema_hash(),
            segments: Vec::new(),
        },
        receipt: None,
        status: CheckpointStatus::Committed,
        is_head: true,
        created_at_ms: 1,
        committed_at_ms: Some(2),
        rewind_target_checkpoint_id: None,
    });
    let state_delta = StateDeltaPreimage {
        checkpoint_id: CheckpointId::new(checkpoint_id).unwrap(),
        pipeline_id: PipelineId::new("pipe-live").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope: scope(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: parent_checkpoint_id
            .map(CheckpointId::new)
            .transpose()
            .unwrap(),
        input_position,
        output_position: position(10),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        run_schema_authority: None,
        schema_hash: schema_hash(),
        segments,
    };
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token_with_content(
        content,
        target,
        disposition.clone(),
        replay_merge_keys(&disposition),
        schema_hash(),
    );
    builder
        .write_input_checkpoint_artifact(&input_checkpoint)
        .unwrap();
    builder
        .write_state_delta_preimage_artifact(&state_delta)
        .unwrap();
    builder
        .write_commit_plan_preimage_artifact(&commit_plan)
        .unwrap();
}

fn build_replay_package(
    root: &Path,
    package_id: &str,
    target: TargetName,
    disposition: WriteDisposition,
    checkpoint_id: &str,
    segments: Vec<(&str, RecordBatch)>,
) -> PackageManifest {
    let upserts = segments
        .iter()
        .map(|(_, batch)| batch.num_rows() as u64)
        .sum();
    let content = replay_content(&disposition, upserts);
    let builder = PackageBuilder::create(
        root,
        package_id,
        replay_content(&disposition, 0),
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    if let Some((_, batch)) = segments.first() {
        builder
            .write_runtime_arrow_schema(batch.schema().as_ref())
            .unwrap();
    }
    let mut entries = Vec::with_capacity(segments.len());
    let mut package_row_ord_start = 0_u64;
    for (segment_id, batch) in segments {
        let rows = batch.num_rows() as u64;
        let batch =
            cdf_package_contract::append_package_row_ord(vec![batch], package_row_ord_start)
                .unwrap();
        entries.push(
            builder
                .write_segment(
                    replay_segment_kind(&disposition),
                    SegmentId::new(segment_id).unwrap(),
                    package_row_ord_start,
                    &batch,
                )
                .unwrap(),
        );
        package_row_ord_start += rows;
    }
    builder.set_content_authority(content.clone()).unwrap();
    write_replay_artifacts(
        &builder,
        target,
        disposition,
        checkpoint_id,
        None,
        &entries,
        content,
    );
    builder.finish().unwrap();
    cdf_package::read_manifest(root).unwrap()
}

fn build_replay_package_with_parent(
    root: &Path,
    package_id: &str,
    target: TargetName,
    disposition: WriteDisposition,
    checkpoint_id: &str,
    parent_checkpoint_id: &str,
    segments: Vec<(&str, RecordBatch)>,
) -> PackageManifest {
    let upserts = segments
        .iter()
        .map(|(_, batch)| batch.num_rows() as u64)
        .sum();
    let content = replay_content(&disposition, upserts);
    let builder = PackageBuilder::create(
        root,
        package_id,
        replay_content(&disposition, 0),
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    if let Some((_, batch)) = segments.first() {
        builder
            .write_runtime_arrow_schema(batch.schema().as_ref())
            .unwrap();
    }
    let mut entries = Vec::with_capacity(segments.len());
    let mut package_row_ord_start = 0_u64;
    for (segment_id, batch) in segments {
        let rows = batch.num_rows() as u64;
        let batch =
            cdf_package_contract::append_package_row_ord(vec![batch], package_row_ord_start)
                .unwrap();
        entries.push(
            builder
                .write_segment(
                    replay_segment_kind(&disposition),
                    SegmentId::new(segment_id).unwrap(),
                    package_row_ord_start,
                    &batch,
                )
                .unwrap(),
        );
        package_row_ord_start += rows;
    }
    builder.set_content_authority(content.clone()).unwrap();
    write_replay_artifacts(
        &builder,
        target,
        disposition,
        checkpoint_id,
        Some(parent_checkpoint_id),
        &entries,
        content,
    );
    builder.finish().unwrap();
    cdf_package::read_manifest(root).unwrap()
}

fn replay_content(
    disposition: &WriteDisposition,
    upserts: u64,
) -> cdf_kernel::PackageContentAuthority {
    if disposition == &WriteDisposition::Merge {
        cdf_conformance::destination::representative_merge_content(
            schema_hash(),
            vec!["id".to_owned()],
            upserts,
        )
    } else {
        cdf_kernel::PackageContentAuthority::rows(schema_hash())
    }
}

fn cdc_replay_content(upserts: u64, deletes: u64) -> cdf_kernel::PackageContentAuthority {
    let mut content = cdf_conformance::destination::representative_merge_content(
        schema_hash(),
        vec!["id".to_owned()],
        upserts,
    );
    let cdf_kernel::PackageContentAuthority::KeyedChanges {
        reduction,
        deletion_capture,
        delete_application,
        ..
    } = &mut content
    else {
        unreachable!("representative merge content is keyed")
    };
    reduction.input.deletes = deletes;
    reduction.surviving.deletes = deletes;
    *deletion_capture = cdf_kernel::DeletionCaptureAuthority {
        support: cdf_kernel::DeletionCaptureSupport::Inherent,
        enabled: true,
        semantics_sha256: format!("sha256:{}", "9".repeat(64)),
    };
    *delete_application = cdf_kernel::DeleteApplicationAuthority::Apply {
        policy: cdf_kernel::DeleteApplicationPolicy::Hard,
    };
    content
}

fn build_cdc_replay_package_with_quarantines(
    root: &Path,
    package_id: &str,
    target: TargetName,
    quarantines: &[QuarantineRecord],
) -> PackageManifest {
    let builder = PackageBuilder::create(
        root,
        package_id,
        cdc_replay_content(0, 0),
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    let row = batch(&[(1, Some("ada"))]);
    builder
        .write_runtime_arrow_schema(row.schema().as_ref())
        .unwrap();
    builder
        .write_quarantine_records("part-000001.parquet", quarantines)
        .unwrap();
    let canonical = cdf_package_contract::append_package_row_ord(vec![row], 0).unwrap();
    let entry = builder
        .write_segment(
            cdf_kernel::PackageSegmentKind::Upsert,
            SegmentId::new("seg-000001").unwrap(),
            0,
            &canonical,
        )
        .unwrap();
    let content = cdc_replay_content(1, 0);
    builder.set_content_authority(content.clone()).unwrap();
    write_replay_artifacts(
        &builder,
        target,
        WriteDisposition::CdcApply,
        &format!("checkpoint-{package_id}"),
        None,
        &[entry],
        content,
    );
    builder.finish().unwrap();
    cdf_package::read_manifest(root).unwrap()
}

fn commit_verified_cdc_package(
    env: &LivePostgres,
    package_dir: &Path,
) -> Result<cdf_runtime::DestinationCommitOutcome> {
    let verified = PackageReader::open(package_dir)?.into_verified()?;
    let inputs = cdf_package_contract::VerifiedPackageAccess::replay_inputs(&verified)?;
    let output_schema =
        cdf_package_contract::VerifiedPackageAccess::runtime_arrow_schema(&verified)?;
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new())?,
    );
    let segments = verified
        .reader()
        .verified_commit_segment_stream_with(
            verified.verification(),
            &inputs.destination_commit.segments,
            memory,
            64 * 1024 * 1024,
        )?
        .map(|segment| segment.and_then(|segment| segment.into_commit_segment()));
    let package: cdf_package_contract::SharedVerifiedPackageAccess = Arc::new(verified);
    let destination = env.destination();
    let mut runtime = PostgresRuntime::for_replay(
        &destination,
        PostgresTarget::parse(inputs.destination_commit.target.as_str())?,
        None,
    );
    runtime.commit_cdc_package(package, &inputs, output_schema.as_ref(), Box::new(segments))
}

fn replay_segment_kind(disposition: &WriteDisposition) -> cdf_kernel::PackageSegmentKind {
    if disposition == &WriteDisposition::Merge {
        cdf_kernel::PackageSegmentKind::Upsert
    } else {
        cdf_kernel::PackageSegmentKind::Row
    }
}

fn columns() -> Vec<PostgresColumn> {
    vec![
        PostgresColumn::new("id", "BIGINT", false).unwrap(),
        PostgresColumn::new("name", "TEXT", true).unwrap(),
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

fn exact_value_batch(numeric_declaration: &str, rows: &[(i64, &str, &str, &str)]) -> RecordBatch {
    let exact = |name, semantic: &str, physical| {
        cdf_kernel::with_semantic(
            cdf_kernel::with_physical_type(Field::new(name, DataType::Utf8, false), physical),
            &semantic.parse().unwrap(),
        )
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        exact(
            "document",
            cdf_semantic::POSTGRES_JSON_TEXT_SEMANTIC,
            "json",
        ),
        exact(
            "payload",
            cdf_semantic::POSTGRES_JSONB_TEXT_SEMANTIC,
            "jsonb",
        ),
        exact(
            "amount",
            cdf_semantic::POSTGRES_NUMERIC_TEXT_SEMANTIC,
            numeric_declaration,
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(id, _, _, _)| *id),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(_, document, _, _)| *document),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(_, _, payload, _)| *payload),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(_, _, _, amount)| *amount),
            )),
        ],
    )
    .unwrap()
}

fn state_segments(manifest: &PackageManifest) -> Vec<StateSegment> {
    manifest
        .identity
        .segments
        .iter()
        .map(|segment| StateSegment {
            kind: segment.kind,
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
        state_version: cdf_kernel::CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: position(10),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        package_hash: PackageHash::new(manifest.package_hash.clone()).unwrap(),
        content: manifest.identity.content.clone(),
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
        version: cdf_kernel::SOURCE_POSITION_VERSION,
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
    state_delta: Option<StateDelta>,
) -> PostgresLoadPlan {
    plan_with_columns(env, table, manifest, disposition, state_delta, columns())
}

fn plan_with_columns(
    env: &LivePostgres,
    table: &str,
    manifest: &PackageManifest,
    disposition: WriteDisposition,
    state_delta: Option<StateDelta>,
    columns: Vec<PostgresColumn>,
) -> PostgresLoadPlan {
    env.destination()
        .plan_load(load_input(
            env,
            table,
            manifest,
            disposition,
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
    state_delta: Option<StateDelta>,
    columns: Vec<PostgresColumn>,
) -> PostgresLoadPlanInput {
    let merge_keys = replay_merge_keys(&disposition)
        .into_iter()
        .map(PostgresIdentifier::user)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    PostgresLoadPlanInput {
        package_hash: PackageHash::new(manifest.package_hash.clone()).unwrap(),
        content: manifest.identity.content.clone(),
        idempotency_token: IdempotencyToken::new(manifest.package_hash.clone()).unwrap(),
        target: env.target(table),
        disposition,
        schema_hash: schema_hash(),
        segments: state_segments(manifest),
        columns,
        merge_keys,
        existing_table: None,
        resource_id: Some(ResourceId::new("orders").unwrap()),
        state_delta,
    }
}

struct LiveCommitObservation {
    receipt: Receipt,
}

fn commit(env: &LivePostgres, package_dir: &Path, table: &str) -> LiveCommitObservation {
    let receipt = try_session_commit(env, package_dir, table).unwrap();
    LiveCommitObservation { receipt }
}

fn commit_request(manifest: &PackageManifest, plan: &PostgresLoadPlan) -> DestinationCommitRequest {
    DestinationCommitRequest {
        package_hash: PackageHash::new(manifest.package_hash.clone()).unwrap(),
        content: manifest.identity.content.clone(),
        target: plan.kernel.target.clone(),
        disposition: plan.kernel.disposition.clone(),
        segments: state_segments(manifest),
        idempotency_token: plan.idempotency_token.clone(),
    }
}

fn try_session_commit(env: &LivePostgres, package_dir: &Path, table: &str) -> Result<Receipt> {
    let destination = env.destination();
    let mut runtime = PostgresRuntime::for_replay(&destination, env.target(table), None);
    let reader = PackageReader::open(package_dir)?;
    let verified = reader.verify_for_consumption()?;
    let package = Arc::new(reader.clone().with_verification(verified.clone())?);
    let inputs = reader.replay_inputs_verified(&verified)?;
    let request = &inputs.destination_commit;
    let mut manifest_segments = Vec::new();
    reader.for_each_identity_segment(&mut |entry| {
        manifest_segments.push(entry);
        Ok(())
    })?;
    let output_schema = reader.runtime_arrow_schema_verified(&verified)?;
    let bulk_path = runtime.prepare_selected_bulk_path(
        &cdf_runtime::BulkPathPreparationInput::new(output_schema.as_ref()).with_commit(request),
    )?;
    let mut prepared = match runtime.ingress() {
        cdf_runtime::DestinationIngress::FinalizedPackage(ingress) => ingress
            .prepare_package_commit(
                &inputs,
                &cdf_runtime::DestinationPlanningContext::new(package, &bulk_path),
            )?,
        cdf_runtime::DestinationIngress::StagedSegments(_) => {
            return Err(CdfError::internal(
                "Postgres live runtime exposed staged ingress",
            ));
        }
    };
    prepared.validate_verified_inputs(&inputs)?;
    let mut session = match runtime.ingress() {
        cdf_runtime::DestinationIngress::FinalizedPackage(ingress) => {
            ingress.begin_prepared_commit(&mut prepared)?
        }
        cdf_runtime::DestinationIngress::StagedSegments(_) => {
            return Err(CdfError::internal(
                "Postgres live runtime changed ingress category",
            ));
        }
    };
    session.apply_migrations()?;
    let memory = Arc::new(cdf_memory::DeterministicMemoryCoordinator::new(
        64 * 1024 * 1024,
        BTreeMap::new(),
    )?);
    let segments = reader.verified_commit_segment_stream_with(
        &verified,
        &request.segments,
        memory,
        64 * 1024 * 1024,
    )?;
    let segments =
        segments.map(|segment| segment.and_then(|segment| segment.into_commit_segment()));
    for ack in session.write_segments(Box::new(segments))? {
        assert!(manifest_segments.iter().any(|entry| {
            ack.segment_id == entry.segment_id && ack.row_count == entry.row_count
        }));
    }
    let receipt = session.finalize()?;
    let verification = runtime.protocol().verify(&receipt)?;
    if !verification.verified {
        return Err(CdfError::destination(verification.reason.unwrap_or_else(
            || "Postgres live receipt verification failed".to_owned(),
        )));
    }
    Ok(receipt)
}

fn try_low_level_session_commit(
    env: &LivePostgres,
    package_dir: &Path,
    manifest: &PackageManifest,
    plan: PostgresLoadPlan,
) -> Result<Receipt> {
    let request = commit_request(manifest, &plan);
    let kernel_plan = plan.kernel.clone();
    validate_session_begin_inputs(&request, &kernel_plan, &plan)?;
    let destination = env.destination();
    let reader = PackageReader::open(package_dir)?;
    let verified = reader.verify_for_consumption()?;
    let package = Arc::new(reader.clone().with_verification(verified.clone())?);
    let segments =
        crate::package::expected_segments_for_session(package.as_ref(), &plan, &request)?;
    let mut session = destination.begin_commit_session(PostgresCommitRequest {
        package,
        plan,
        segments,
    })?;
    session.apply_migrations()?;
    let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
        cdf_memory::DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new())?,
    );
    let segments = reader
        .verified_commit_segment_stream_with(
            &verified,
            &request.segments,
            memory,
            64 * 1024 * 1024,
        )?
        .map(|segment| segment.and_then(|segment| segment.into_commit_segment()));
    session.write_segments(Box::new(segments))?;
    Box::new(session).finalize()
}

fn try_correction_commit(
    env: &LivePostgres,
    package_dir: &Path,
    request: DestinationCorrectionCommitRequest,
    plan: PostgresCorrectionPlan,
) -> Result<Receipt> {
    let destination = env
        .destination()
        .with_correction_request(PostgresCorrectionCommitRequest {
            package: Arc::new(PackageReader::open(package_dir)?.into_verified()?),
            plan,
        });
    let generic_plan = destination.plan_correction(&request)?;
    let mut session = destination.begin_correction(request, generic_plan)?;
    session.apply_migrations()?;
    session.apply_corrections()?;
    session.finalize()
}

#[test]
fn live_begin_session_returns_verifiable_receipt_and_preserves_duplicate_noop() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let _manifest = build_replay_package(
        package_dir.path(),
        "pkg-live-session-append",
        env.target("orders_session_append").target_name().unwrap(),
        WriteDisposition::Append,
        "chk-live-session-append",
        vec![("seg-000001", batch(&[(1, Some("ada")), (2, Some("grace"))]))],
    );
    let receipt = try_session_commit(&env, package_dir.path(), "orders_session_append").unwrap();
    assert_eq!(receipt.counts.settled_effect_count(), Some(2));
    assert!(env.destination().verify_receipt(&receipt).unwrap().verified);
    assert!(
        collect_package_receipts(&cdf_package::PackageReader::open(package_dir.path()).unwrap())
            .is_empty()
    );

    let duplicate = try_session_commit(&env, package_dir.path(), "orders_session_append").unwrap();
    assert_eq!(duplicate.receipt_id, receipt.receipt_id);
    assert!(
        collect_package_receipts(&cdf_package::PackageReader::open(package_dir.path()).unwrap())
            .is_empty()
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

    client
        .execute(
            &format!(
                "UPDATE {} SET \"receipt_json\" = jsonb_set(\"receipt_json\", '{{counts,rows_inserted}}', '1'::jsonb) WHERE \"target\" = $1",
                crate::identifiers::quote_identifier_unchecked(CDF_LOADS_TABLE)
            ),
            &[&env
                .target("orders_session_append")
                .target_name()
                .unwrap()
                .as_str()],
        )
        .unwrap();
    let error = try_session_commit(&env, package_dir.path(), "orders_session_append").unwrap_err();
    assert!(
        error
            .to_string()
            .contains("independently stored load evidence"),
        "{error}"
    );
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
        None,
    );
    let request = commit_request(&manifest, &plan);
    let kernel_plan = plan.kernel.clone();
    validate_session_begin_inputs(&request, &kernel_plan, &plan).unwrap();
    let destination = env.destination();
    let reader = PackageReader::open(package_dir.path()).unwrap();
    let verified = reader.verify_for_consumption().unwrap();
    let package = Arc::new(reader.clone().with_verification(verified).unwrap());
    let segments =
        crate::package::expected_segments_for_session(package.as_ref(), &plan, &request).unwrap();
    let mut session = destination
        .begin_commit_session(PostgresCommitRequest {
            package,
            plan,
            segments,
        })
        .unwrap();
    session.apply_migrations().unwrap();
    Box::new(session).abort().unwrap();

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

#[test]
fn live_append_duplicate_receipt_verification_and_state_mirror() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let manifest = build_replay_package(
        package_dir.path(),
        "pkg-live-append",
        env.target("orders_append").target_name().unwrap(),
        WriteDisposition::Append,
        "chk-live-append",
        vec![
            ("seg-000001", batch(&[(1, Some("ada")), (2, Some("grace"))])),
            ("seg-000002", batch(&[(3, None)])),
        ],
    );
    let outcome = commit(&env, package_dir.path(), "orders_append");
    assert_eq!(outcome.receipt.counts.settled_effect_count(), Some(3));
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

    let duplicate = commit(&env, package_dir.path(), "orders_append");
    assert_eq!(duplicate.receipt, outcome.receipt);
    let target_count: i64 = client
        .query_one(
            &format!("SELECT COUNT(*)::bigint FROM {}", target.sql()),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(target_count, 3);
}

#[test]
fn live_append_populates_quarantine_mirror_when_sheet_supports_it() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let builder = PackageBuilder::create(
        package_dir.path(),
        "pkg-live-quarantine-mirror",
        cdf_kernel::PackageContentAuthority::rows(schema_hash()),
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    builder
        .write_runtime_arrow_schema(batch(&[(1, Some("ada"))]).schema().as_ref())
        .unwrap();
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
    let segment = builder
        .write_segment(
            cdf_kernel::PackageSegmentKind::Row,
            SegmentId::new("seg-000001").unwrap(),
            0,
            &cdf_package_contract::append_package_row_ord(vec![batch(&[(1, Some("ada"))])], 0)
                .unwrap(),
        )
        .unwrap();
    write_replay_artifacts(
        &builder,
        env.target("orders_quarantine_mirror")
            .target_name()
            .unwrap(),
        WriteDisposition::Append,
        "chk-live-quarantine-mirror",
        None,
        &[segment],
        cdf_kernel::PackageContentAuthority::rows(schema_hash()),
    );
    let manifest = builder.finish().unwrap();
    commit(&env, package_dir.path(), "orders_quarantine_mirror");
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
fn live_cdc_mirrors_verified_quarantine_once_and_rolls_back_conflicting_evidence() {
    let record = |value: &str| QuarantineRecord {
        source_row_ordinal: 77,
        rule_id: "cdc-row-rule".to_owned(),
        error_code: "invalid_value".to_owned(),
        source_position: Some(position(10)),
        observed_value_redacted: QuarantineObservedValue::Hashed {
            algorithm: "sha256".to_owned(),
            value: value.to_owned(),
        },
    };

    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let manifest = build_cdc_replay_package_with_quarantines(
        package_dir.path(),
        "postgres-cdc-quarantine",
        env.target("cdc_quarantine").target_name().unwrap(),
        &[record("sha256:first")],
    );
    let first = commit_verified_cdc_package(&env, package_dir.path()).unwrap();
    let replay = commit_verified_cdc_package(&env, package_dir.path()).unwrap();
    assert_eq!(replay.receipt, first.receipt);
    assert_eq!(
        replay.reporting_policy,
        cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommit { duplicate: true }
    );
    let mut client = env.client();
    let mirrored: i64 = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {} WHERE target = $1 AND package_hash = $2",
                quote_identifier_unchecked(CDF_QUARANTINE_TABLE)
            ),
            &[
                &env.target("cdc_quarantine").display_name(),
                &manifest.package_hash,
            ],
        )
        .unwrap()
        .get(0);
    assert_eq!(mirrored, 1);
    drop(client);
    drop(env);

    let Some(rollback_env) = LivePostgres::start() else {
        return;
    };
    let rollback_dir = tempfile::tempdir().unwrap();
    build_cdc_replay_package_with_quarantines(
        rollback_dir.path(),
        "postgres-cdc-quarantine-conflict",
        rollback_env
            .target("cdc_quarantine_conflict")
            .target_name()
            .unwrap(),
        &[record("sha256:first"), record("sha256:conflict")],
    );
    let error = commit_verified_cdc_package(&rollback_env, rollback_dir.path()).unwrap_err();
    assert!(
        error
            .message
            .contains("quarantine identity records conflicting evidence"),
        "{error}"
    );
    let mut rollback_client = rollback_env.client();
    let target_exists: bool = rollback_client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
            &[&rollback_env.schema, &"cdc_quarantine_conflict"],
        )
        .unwrap()
        .get(0);
    let mirror_exists: bool = rollback_client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
            &[&rollback_env.schema, &CDF_QUARANTINE_TABLE],
        )
        .unwrap()
        .get(0);
    assert!(!target_exists);
    assert!(!mirror_exists);
}

#[test]
fn live_replace_is_atomic_and_reports_deleted_rows() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let first_dir = tempfile::tempdir().unwrap();
    let _first_manifest = build_replay_package(
        first_dir.path(),
        "pkg-live-replace-first",
        env.target("orders_replace").target_name().unwrap(),
        WriteDisposition::Append,
        "chk-live-replace-first",
        vec![("seg-000001", batch(&[(1, Some("ada")), (2, Some("grace"))]))],
    );
    commit(&env, first_dir.path(), "orders_replace");

    let second_dir = tempfile::tempdir().unwrap();
    let second_manifest = build_replay_package_with_parent(
        second_dir.path(),
        "pkg-live-replace-second",
        env.target("orders_replace").target_name().unwrap(),
        WriteDisposition::Replace,
        "chk-live-replace-second",
        "chk-live-replace-first",
        vec![("seg-000001", batch(&[(3, Some("katherine"))]))],
    );
    let outcome = commit(&env, second_dir.path(), "orders_replace");
    assert_eq!(outcome.receipt.counts.settled_effect_count(), Some(1));
    assert_eq!(outcome.receipt.counts.inserted_outcome(), Some(1));
    assert_eq!(outcome.receipt.counts.row_delete_outcome(), Some(2));

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
fn live_merge_rejects_duplicate_finalized_keys_before_target_mutation() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let package_dir = tempfile::tempdir().unwrap();
    let table = "orders_merge_duplicate_rejected";
    build_replay_package(
        package_dir.path(),
        "pkg-live-merge-duplicate-rejected",
        env.target(table).target_name().unwrap(),
        WriteDisposition::Merge,
        "chk-live-merge-duplicate-rejected",
        vec![
            ("seg-000001", batch(&[(1, Some("old"))])),
            ("seg-000002", batch(&[(1, Some("new"))])),
        ],
    );

    let error = try_session_commit(&env, package_dir.path(), table).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("duplicate merge keys"));
    let target_exists = env
        .client()
        .query_one(
            "SELECT to_regclass($1)::text",
            &[&env.target(table).display_name()],
        )
        .unwrap()
        .get::<_, Option<String>>(0);
    assert_eq!(
        target_exists, None,
        "failed merge must roll back target DDL"
    );
}

#[test]
fn live_merge_applies_finalized_unique_rows_and_updates_existing_keys() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let first_dir = tempfile::tempdir().unwrap();
    let _first_manifest = build_replay_package(
        first_dir.path(),
        "pkg-live-merge-first",
        env.target("orders_merge").target_name().unwrap(),
        WriteDisposition::Merge,
        "chk-live-merge-first",
        vec![("seg-000001", batch(&[(1, Some("new")), (2, Some("two"))]))],
    );
    let first = commit(&env, first_dir.path(), "orders_merge");
    assert_eq!(first.receipt.counts.settled_effect_count(), Some(2));
    assert_eq!(first.receipt.counts.inserted_outcome(), Some(2));
    assert_eq!(first.receipt.counts.updated_outcome(), Some(0));

    let second_dir = tempfile::tempdir().unwrap();
    let _second_manifest = build_replay_package_with_parent(
        second_dir.path(),
        "pkg-live-merge-second",
        env.target("orders_merge").target_name().unwrap(),
        WriteDisposition::Merge,
        "chk-live-merge-second",
        "chk-live-merge-first",
        vec![(
            "seg-000001",
            batch(&[(1, Some("updated")), (3, Some("three"))]),
        )],
    );
    let second = commit(&env, second_dir.path(), "orders_merge");
    assert_eq!(second.receipt.counts.settled_effect_count(), Some(2));
    assert_eq!(second.receipt.counts.inserted_outcome(), Some(1));
    assert_eq!(second.receipt.counts.updated_outcome(), Some(1));

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
    let _manifest = build_replay_package(
        package_dir.path(),
        "pkg-live-decimal",
        env.target("orders_decimal").target_name().unwrap(),
        WriteDisposition::Append,
        "chk-live-decimal",
        vec![(
            "seg-000001",
            decimal_batch(&[(1, Some(1234_i128)), (2, Some(-5_i128)), (3, None)]),
        )],
    );
    let outcome = commit(&env, package_dir.path(), "orders_decimal");
    assert_eq!(outcome.receipt.counts.settled_effect_count(), Some(3));

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

#[test]
fn live_exact_value_text_round_trips_all_dispositions_and_rolls_back_rejections() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let wide = "12345678901234567890123456789012345678901234567890123456789012345678901234567.8900";

    let append_dir = tempfile::tempdir().unwrap();
    let append_manifest = build_replay_package(
        append_dir.path(),
        "pkg-live-exact-append",
        env.target("exact_values").target_name().unwrap(),
        WriteDisposition::Append,
        "chk-live-exact-append",
        vec![(
            "seg-000001",
            exact_value_batch(
                "numeric",
                &[
                    (
                        1,
                        r#"{"large":1e400,"duplicate":1,"duplicate":2}"#,
                        r#"{"b":2,"a":1}"#,
                        wide,
                    ),
                    (2, "null", r#"{"special":"positive"}"#, "Infinity"),
                    (3, "true", r#"{"special":"nan"}"#, "NaN"),
                ],
            ),
        )],
    );
    let append = try_session_commit(&env, append_dir.path(), "exact_values").unwrap();
    assert_eq!(append.counts.settled_effect_count(), Some(3));
    let replay = try_session_commit(&env, append_dir.path(), "exact_values").unwrap();
    assert_eq!(replay.receipt_id, append.receipt_id);

    let mut client = env.client();
    let rows = client
        .query(
            &format!(
                "SELECT \"id\", \"document\"::jsonb->>'large', \"document\"::jsonb->>'duplicate', \"payload\"::text, \"amount\"::text FROM {} ORDER BY \"id\"",
                env.target("exact_values").sql()
            ),
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), format!("1{}", "0".repeat(400)));
    assert_eq!(rows[0].get::<_, String>(2), "2");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&rows[0].get::<_, String>(3)).unwrap(),
        serde_json::json!({"a": 1, "b": 2})
    );
    assert_eq!(rows[0].get::<_, String>(4), wide);
    assert_eq!(rows[1].get::<_, String>(4), "Infinity");
    assert_eq!(rows[2].get::<_, String>(4), "NaN");

    let declarations = client
        .query(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'exact_values' AND column_name IN ('document', 'payload', 'amount') ORDER BY column_name",
            &[&env.schema],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        declarations,
        vec![
            ("amount".to_owned(), "numeric".to_owned()),
            ("document".to_owned(), "json".to_owned()),
            ("payload".to_owned(), "jsonb".to_owned()),
        ]
    );

    let replace_dir = tempfile::tempdir().unwrap();
    build_replay_package_with_parent(
        replace_dir.path(),
        "pkg-live-exact-replace",
        env.target("exact_values").target_name().unwrap(),
        WriteDisposition::Replace,
        "chk-live-exact-replace",
        "chk-live-exact-append",
        vec![(
            "seg-000001",
            exact_value_batch(
                "numeric",
                &[(4, r#"{"replace":true}"#, r#"{"replace":true}"#, "-Infinity")],
            ),
        )],
    );
    let replaced = try_session_commit(&env, replace_dir.path(), "exact_values").unwrap();
    assert_eq!(replaced.counts.settled_effect_count(), Some(1));
    assert_eq!(replaced.counts.row_delete_outcome(), Some(3));
    let replaced_row = client
        .query_one(
            &format!(
                "SELECT \"id\", \"amount\"::text FROM {}",
                env.target("exact_values").sql()
            ),
            &[],
        )
        .unwrap();
    assert_eq!(replaced_row.get::<_, i64>(0), 4);
    assert_eq!(replaced_row.get::<_, String>(1), "-Infinity");

    let constrained =
        "1234567890123456789012345678901234567890123456789012345678901234567890123456.7";
    let merge_dir = tempfile::tempdir().unwrap();
    build_replay_package_with_parent(
        merge_dir.path(),
        "pkg-live-exact-merge",
        env.target("exact_merge").target_name().unwrap(),
        WriteDisposition::Merge,
        "chk-live-exact-merge",
        "chk-live-exact-replace",
        vec![(
            "seg-000001",
            exact_value_batch(
                "numeric(77,1)",
                &[(2, "2", "2", constrained), (1, "3", "3", "2.0")],
            ),
        )],
    );
    let merged = try_session_commit(&env, merge_dir.path(), "exact_merge").unwrap();
    assert_eq!(merged.counts.settled_effect_count(), Some(2));
    let merged_rows = client
        .query(
            &format!(
                "SELECT \"id\", \"amount\"::text FROM {} ORDER BY \"id\"",
                env.target("exact_merge").sql()
            ),
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        merged_rows,
        vec![(1, "2.0".to_owned()), (2, constrained.to_owned())]
    );

    for (package_id, table, numeric, document, expected_fragment) in [
        (
            "pkg-live-exact-invalid-json",
            "exact_invalid_json",
            "numeric",
            "{",
            "document",
        ),
        (
            "pkg-live-exact-range",
            "exact_range",
            "numeric(3,0)",
            "null",
            "amount",
        ),
    ] {
        let rejected_dir = tempfile::tempdir().unwrap();
        build_replay_package(
            rejected_dir.path(),
            package_id,
            env.target(table).target_name().unwrap(),
            WriteDisposition::Append,
            &format!("chk-{package_id}"),
            vec![(
                "seg-000001",
                exact_value_batch(
                    numeric,
                    &[(
                        1,
                        document,
                        "null",
                        if table == "exact_range" { "1000" } else { "1" },
                    )],
                ),
            )],
        );
        let error = try_session_commit(&env, rejected_dir.path(), table).unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data, "{error}");
        assert!(error.message.contains(expected_fragment), "{error}");
        let target_exists: bool = client
            .query_one(
                "SELECT to_regclass($1) IS NOT NULL",
                &[&format!("{}.{}", env.schema, table)],
            )
            .unwrap()
            .get(0);
        assert!(!target_exists, "rejected target {table} survived rollback");
        let receipt_count: i64 = client
            .query_one(
                &format!(
                    "SELECT COUNT(*)::bigint FROM {} WHERE \"target\" = $1",
                    quote_identifier_unchecked(CDF_LOADS_TABLE)
                ),
                &[&env.target(table).display_name()],
            )
            .unwrap()
            .get(0);
        assert_eq!(receipt_count, 0);
    }

    assert_eq!(append_manifest.identity.segments[0].row_count, 3);
}

#[test]
fn live_rollback_after_direct_copy_leaves_no_target_or_mirror_partial_commit() {
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
        Some(state_delta(&manifest, "chk-live-rollback")),
    );
    rollback_plan.target_ddl = target_migrations(&PostgresLoadPlanInput {
        package_hash: PackageHash::new(manifest.package_hash.clone()).unwrap(),
        content: manifest.identity.content.clone(),
        idempotency_token: IdempotencyToken::new(manifest.package_hash.clone()).unwrap(),
        target: target.clone(),
        disposition: WriteDisposition::Append,
        schema_hash: schema_hash(),
        segments: state_segments(&manifest),
        columns: columns(),
        merge_keys: vec![PostgresIdentifier::user("id").unwrap()],
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

    let error = try_low_level_session_commit(&env, package_dir.path(), &manifest, rollback_plan)
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
        collect_package_receipts(&cdf_package::PackageReader::open(package_dir.path()).unwrap())
            .is_empty()
    );
}

#[test]
fn live_addressed_correction_updates_exact_rows_preserves_residuals_and_replays() {
    let Some(env) = LivePostgres::start() else {
        return;
    };
    let original_dir = tempfile::tempdir().unwrap();
    let original_manifest = build_replay_package(
        original_dir.path(),
        "pkg-live-correction-original",
        env.target("orders_correction").target_name().unwrap(),
        WriteDisposition::Append,
        "chk-live-correction-original",
        vec![(
            "seg-000001",
            residual_batch(&[(1, 42, Some("keep")), (2, 84, None)]),
        )],
    );
    commit(&env, original_dir.path(), "orders_correction");

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
        package: Arc::new(
            PackageReader::open(correction_dir.path())
                .unwrap()
                .into_verified()
                .unwrap(),
        ),
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
    assert_eq!(counts.settled_effect_count(), Some(2));
    assert_eq!(counts.updated_outcome(), Some(2));
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

    let replay =
        try_correction_commit(&env, correction_dir.path(), request, commit_request.plan).unwrap();
    assert_eq!(replay, receipt);
    assert!(
        collect_package_receipts(&PackageReader::open(correction_dir.path()).unwrap()).is_empty()
    );
}

#[test]
fn live_correction_missing_duplicate_and_post_update_failures_roll_back() {
    let Some(env) = LivePostgres::start() else {
        return;
    };

    let original_dir = tempfile::tempdir().unwrap();
    let original_manifest = build_replay_package(
        original_dir.path(),
        "pkg-live-correction-negative-original",
        env.target("orders_correction_missing")
            .target_name()
            .unwrap(),
        WriteDisposition::Append,
        "chk-live-correction-negative-original",
        vec![("seg-000001", residual_batch(&[(1, 42, Some("keep"))]))],
    );
    commit(&env, original_dir.path(), "orders_correction_missing");
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
    let missing_error =
        try_correction_commit(&env, missing_dir.path(), missing_request, missing_plan).unwrap_err();
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
            &format!("INSERT INTO {} (\"row_key_start\", \"row_key_end\", \"target\", \"package_hash\", \"idempotency_token\", \"segment_id\", \"row_count\", \"byte_count\", \"committed_at_ms\") VALUES (1000000000000, 1000000000001, $1, $2, $2, 'seg-000001', 1, 1, 1)", quote_identifier_unchecked(CDF_SEGMENTS_TABLE)),
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
    let duplicate_error = try_correction_commit(
        &env,
        duplicate_dir.path(),
        duplicate_request,
        duplicate_plan,
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
    let fail_error =
        try_correction_commit(&env, fail_dir.path(), fail_request, fail_plan).unwrap_err();
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
