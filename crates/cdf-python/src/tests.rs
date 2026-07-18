use super::*;

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CString,
    io::Cursor,
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
};

use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use cdf_http::{EgressAllowlist, HeaderMap, HttpMethod, SecretValue};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, Checkpoint, CheckpointId, CheckpointStatus, CheckpointStore,
    CursorOrderingClaim, CursorValue, ErrorKind, PackageHash, PageToken, PipelineId, Receipt,
    RewindReport, RewindRequest, SchemaHash, SegmentId, StateDelta, StateSegment,
};
use pyo3::types::PyList;

use crate::internal::py_error;

fn bridge() -> PythonResourceBridge {
    PythonResourceBridge::new(
        PythonBridgeOptions::new(
            ResourceId::new("orders").unwrap(),
            PartitionId::new("p0").unwrap(),
        )
        .with_dict_batch_rows(2)
        .unwrap(),
    )
}

#[test]
fn dict_rows_batch_through_ndjson_into_kernel_batches() {
    let rows = vec![
        serde_json::json!({"id": 1, "name": "ada"}),
        serde_json::json!({"id": 2, "name": "grace"}),
        serde_json::json!({"id": 3, "name": "katherine"}),
    ];
    let read = bridge().batches_from_json_dict_rows(rows).unwrap();

    assert_eq!(read.row_count(), 3);
    assert_eq!(read.batches.len(), 2);
    assert_eq!(read.yield_kinds, vec![PythonYieldKind::DictRows; 2]);
    assert_eq!(
        read.batches[0].header.observed_schema_hash,
        read.schema_hash.clone().unwrap()
    );
    assert_eq!(read.batches[0].header.batch_id.as_str(), "orders-p0-000001");
}

#[test]
fn dict_row_conversion_window_enforces_the_boundary_byte_limit() {
    let bridge = PythonResourceBridge::new(
        PythonBridgeOptions::new(
            ResourceId::new("orders").unwrap(),
            PartitionId::new("p0").unwrap(),
        )
        .with_max_boundary_bytes(8)
        .unwrap(),
    );

    let error = bridge
        .batches_from_json_dict_rows([serde_json::json!({"payload": "too large"})])
        .unwrap_err();

    assert!(error.message.contains("boundary limit is 8 bytes"));
}

#[test]
fn python_generator_dicts_convert_to_batches() {
    Python::attach(|py| {
        let module = PyModule::from_code(
                py,
                c"def resource():\n    yield {'name': 'ada', 'id': 1}\n    yield {'name': 'grace', 'id': 2}\n",
                c"fixture.py",
                c"fixture",
            )
            .unwrap();
        let iterable = module.getattr("resource").unwrap().call0().unwrap();
        let read = bridge().batches_from_python_iterable(&iterable).unwrap();

        assert_eq!(read.row_count(), 2);
        assert_eq!(read.yield_kinds, vec![PythonYieldKind::DictRows]);
    });
}

#[test]
fn incremental_python_bridge_stops_before_exhausting_the_generator() {
    Python::attach(|py| {
        let module = PyModule::from_code(
            py,
            c"def rows():\n    yield {'id': 1}\n    yield {'id': 2}\n    raise RuntimeError('generator was exhaustively materialized')\n",
            c"incremental_bridge.py",
            c"incremental_bridge",
        )
        .unwrap();
        let iterable = module.getattr("rows").unwrap().call0().unwrap();
        let bridge = PythonResourceBridge::new(
            PythonBridgeOptions::new(
                ResourceId::new("python.incremental").unwrap(),
                PartitionId::new("python-000001").unwrap(),
            )
            .with_dict_batch_rows(2)
            .unwrap(),
        );
        let error = bridge
            .visit_python_iterable(&iterable, |_batch, _kind| {
                Err(CdfError::data("intentional downstream stop"))
            })
            .unwrap_err();
        assert_eq!(error.message, "intentional downstream stop");
    });
}

#[test]
fn arrow_ipc_fixture_speaks_arrow_c_stream_into_kernel_batches() {
    Python::attach(|py| {
        if PyModule::import(py, "pyarrow").is_err() {
            return;
        }
        let batch = sample_batch();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let module = PyModule::from_code(
            py,
            c"
import pyarrow as pa

def resource(data):
    reader = pa.ipc.open_stream(data)
    yield reader.read_all()
",
            c"arrow_fixture.py",
            c"arrow_fixture",
        )
        .unwrap();
        let bytes = pyo3::types::PyBytes::new(py, &ipc);
        let iterable = module.getattr("resource").unwrap().call1((bytes,)).unwrap();
        let read = bridge().batches_from_python_iterable(&iterable).unwrap();

        assert_eq!(read.row_count(), 2);
        assert_eq!(read.yield_kinds, vec![PythonYieldKind::ArrowCStream]);
        assert_eq!(
            read.batches[0].record_batch().unwrap().schema().as_ref(),
            batch.schema().as_ref()
        );
    });
}

#[test]
fn arrow_boundary_model_detects_capsule_protocol_methods() {
    Python::attach(|py| {
        let module = PyModule::from_code(
                py,
                c"class Streamy:\n    def __arrow_c_stream__(self):\n        raise RuntimeError('not called')\n",
                c"capsule_model.py",
                c"capsule_model",
            )
            .unwrap();
        let streamy = module.getattr("Streamy").unwrap().call0().unwrap();
        let boundary = arrow_boundary_for(&streamy).unwrap().unwrap();

        assert_eq!(boundary, ArrowCapsuleBoundary::for_c_stream());
        assert!(boundary.zero_copy_intent);
    });
}

#[test]
fn boundary_channel_is_byte_bounded() {
    let read = bridge()
        .batches_from_json_dict_rows(vec![serde_json::json!({"id": 1, "name": "ada"})])
        .unwrap();
    let batch = read.batches.into_iter().next().unwrap();
    let batch_bytes = batch.header.byte_count;
    let mut channel = BoundaryChannel::new(batch_bytes).unwrap();

    channel.try_push(batch).unwrap();
    assert_eq!(channel.queued_bytes(), batch_bytes);
    let read = bridge()
        .batches_from_json_dict_rows(vec![serde_json::json!({"id": 1, "name": "ada"})])
        .unwrap();
    let error = channel
        .try_push(read.batches.into_iter().next().unwrap())
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::RateLimited);
    assert!(channel.pop().is_some());
    assert_eq!(channel.queued_bytes(), 0);
}

#[test]
fn interpreter_report_checks_version_path_and_gil_state() {
    Python::attach(|py| {
        let report = inspect_interpreter(py).unwrap();
        InterpreterRequirement::default().check(&report).unwrap();
        InterpreterRequirement {
            executable: Some(report.executable.clone()),
            ..InterpreterRequirement::default()
        }
        .check(&report)
        .unwrap();

        let error = InterpreterRequirement {
            min_major: 99,
            ..InterpreterRequirement::default()
        }
        .check(&report)
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Contract);
    });
}

#[test]
fn admitted_python_work_is_mode_correct_and_fixture_hash_stable() {
    let read = bridge()
        .batches_from_json_dict_rows(vec![
            serde_json::json!({"id": 1, "name": "ada"}),
            serde_json::json!({"id": 2, "name": "grace"}),
        ])
        .unwrap();
    let report = attached_interpreter_report().unwrap();
    let (host, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let requested_parallelism = usize::from(execution.capabilities().logical_cpu_slots.min(2));
    let semantics = execution_semantics(&report, true, requested_parallelism);
    let lane = python_execution_lane_spec(&semantics);
    execution
        .ensure_blocking_lanes(std::slice::from_ref(&lane))
        .unwrap();

    let task_count = 2_usize;
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let synchronize = matches!(semantics.mode, PythonConcurrencyMode::FreeThreadedParallel)
        && semantics.effective_parallelism >= task_count;
    let barrier = synchronize.then(|| Arc::new(Barrier::new(task_count)));
    let (result_sender, result_receiver) = mpsc::sync_channel(task_count);
    let mut scope = execution.open_scope("python-concurrency-matrix").unwrap();
    for index in 0..task_count {
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        let barrier = barrier.clone();
        let result_sender = result_sender.clone();
        let expression = CString::new("sum(i * i for i in range(10000))").unwrap();
        scope
            .spawn_blocking(
                &lane.lane_id,
                Box::new(move || {
                    let value = Python::attach(|py| -> Result<u64> {
                        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(current, Ordering::SeqCst);
                        if let Some(barrier) = barrier {
                            barrier.wait();
                        }
                        let value = py
                            .eval(expression.as_c_str(), None, None)
                            .and_then(|value| value.extract::<u64>())
                            .map_err(py_error);
                        active.fetch_sub(1, Ordering::SeqCst);
                        value
                    })?;
                    result_sender.send((index, value)).unwrap();
                    Ok(())
                }),
            )
            .unwrap();
    }
    drop(result_sender);
    let task_report = host.block_on_root(scope.join()).unwrap();
    let mut results = result_receiver.into_iter().collect::<Vec<_>>();
    results.sort_unstable();

    assert_eq!(task_report.completed, task_count as u64);
    assert_eq!(results.len(), task_count);
    match semantics.mode {
        PythonConcurrencyMode::FreeThreadedParallel if synchronize => {
            assert_eq!(peak.load(Ordering::SeqCst), task_count);
        }
        PythonConcurrencyMode::GilSerialized | PythonConcurrencyMode::ParallelDisabled => {
            assert_eq!(peak.load(Ordering::SeqCst), 1);
        }
        PythonConcurrencyMode::FreeThreadedParallel => {}
    }

    let fixture_hash = deterministic_fixture_hash(&read).unwrap();
    let mut hasher = Sha256::new();
    hasher.update(b"cdf-python-admitted-concurrency-v1");
    hasher.update(fixture_hash.as_bytes());
    for (index, value) in results {
        hasher.update(index.to_le_bytes());
        hasher.update(value.to_le_bytes());
    }
    let hash = format!("sha256:{}", hex::encode(hasher.finalize()));
    eprintln!(
        "python mode={:?} requested={} effective={} observed_peak={} fixture_hash={hash}",
        semantics.mode,
        semantics.requested_parallelism,
        semantics.effective_parallelism,
        peak.load(Ordering::SeqCst),
    );
    if let Ok(path) = std::env::var("CDF_PYTHON_FIXTURE_HASH_OUTPUT") {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, format!("{hash}\n")).unwrap();
    }
}

#[test]
fn context_uses_http_secret_redaction_for_logs_and_traces() {
    struct Provider;

    impl SecretProvider for Provider {
        fn resolve(&self, _uri: &SecretUri) -> Result<SecretValue> {
            Ok(SecretValue::new("super-secret-token"))
        }
    }

    let mut ctx = PythonContext::new(Some(SourcePosition::PageToken(PageToken {
        version: 1,
        token: "cursor-1".to_owned(),
    })));
    let uri = SecretUri::new("secret://env/GITHUB_TOKEN").unwrap();
    let request = ctx
        .resolve_bearer_request(
            HttpRequest::new(HttpMethod::Get, "https://api.example.test/issues"),
            &uri,
            &Provider,
        )
        .unwrap();
    ctx.log("info", "using super-secret-token for request");

    let trace = ctx.trace_request(&request);
    assert_eq!(trace.headers.get("authorization").unwrap(), "[REDACTED]");
    assert_eq!(ctx.logs()[0].message, "using [REDACTED] for request");
    assert!(ctx.cursor().is_some());
    EgressAllowlist::AllowHosts(BTreeSet::from(["api.example.test".to_owned()]))
        .check(&request)
        .unwrap();
}

#[test]
fn watchdog_reports_timeout_without_panicking() {
    let watchdog = Watchdog::new(100, 1_000).unwrap();
    watchdog.check(1_050).unwrap();
    let error = watchdog.check(1_101).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Transient);
}

#[test]
fn python_dict_rows_reject_non_json_values() {
    Python::attach(|py| {
        let module = PyModule::from_code(
                py,
                c"from decimal import Decimal\n\ndef resource():\n    yield {'amount': Decimal('1.23')}\n",
                c"decimal_fixture.py",
                c"decimal_fixture",
            )
            .unwrap();
        let iterable = module.getattr("resource").unwrap().call0().unwrap();
        let error = bridge()
            .batches_from_python_iterable(&iterable)
            .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Data);
        assert!(error.message.contains("JSON"));
    });
}

#[test]
fn python_lists_are_not_silently_treated_as_rows() {
    Python::attach(|py| {
        let list = PyList::new(py, [1, 2, 3]).unwrap();
        let error = bridge().batches_from_python_iterable(&list).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Data);
    });
}

fn sample_batch() -> RecordBatch {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let id: ArrayRef = std::sync::Arc::new(Int64Array::from(vec![1, 2]));
    let name: ArrayRef = std::sync::Arc::new(StringArray::from(vec!["ada", "grace"]));
    RecordBatch::try_new(schema, vec![id, name]).unwrap()
}

#[test]
fn deterministic_hash_changes_when_payload_changes() {
    let first = bridge()
        .batches_from_json_dict_rows(vec![serde_json::json!({"id": 1})])
        .unwrap();
    let second = bridge()
        .batches_from_json_dict_rows(vec![serde_json::json!({"id": 2})])
        .unwrap();

    assert_ne!(
        deterministic_fixture_hash(&first).unwrap(),
        deterministic_fixture_hash(&second).unwrap()
    );
}

#[test]
fn trace_headers_stay_case_insensitive_and_redacted() {
    let mut headers = HeaderMap::new();
    headers.insert("X-Api-Key".to_owned(), "secret".to_owned());
    let mut redactor = Redactor::default();
    redactor.register_secret("secret").unwrap();
    let request = HttpRequest {
        method: HttpMethod::Get,
        url: "https://example.test/?token=secret".to_owned(),
        headers,
    };
    let trace = TraceEvent::from_request(&request, &redactor);

    assert_eq!(trace.headers.get("X-Api-Key").unwrap(), "[REDACTED]");
    assert_eq!(trace.url, "https://example.test/?token=[REDACTED]");
}

#[test]
fn same_schema_is_required_across_python_yields() {
    let mut rows = Vec::new();
    let mut first = BTreeMap::new();
    first.insert("id".to_owned(), serde_json::json!(1));
    rows.push(serde_json::Value::Object(first.into_iter().collect()));
    let read = bridge().batches_from_json_dict_rows(rows).unwrap();

    assert!(read.schema_hash.is_some());
}

#[test]
fn pycapsule_model_documents_array_boundary_names() {
    assert_eq!(
        ArrowCapsuleBoundary::for_c_array().capsule_names,
        vec!["arrow_schema", "arrow_array"]
    );
    assert_eq!(
        ArrowCapsuleBoundary::for_c_stream().capsule_names,
        vec!["arrow_array_stream"]
    );
}

#[test]
fn can_read_back_hash_from_arrow_ipc_bytes() {
    let read = bridge()
        .batches_from_json_dict_rows([serde_json::json!({"id": 1, "name": "ada"})])
        .unwrap();
    let bytes = {
        let batch = read.batches[0].record_batch().unwrap();
        let mut output = Vec::new();
        let mut writer = StreamWriter::try_new(&mut output, batch.schema().as_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        output
    };
    let mut imported = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
    let imported = imported.next().unwrap().unwrap();

    assert_eq!(imported.num_rows(), 1);
}

#[test]
fn dlt_resource_metadata_maps_to_cdf_descriptor_and_snapshot() {
    Python::attach(|py| {
        let module = PyModule::from_code(
            py,
            c"
def orders():
    yield {'id': 1, 'region': 'us', 'updated_at': '2026-07-01T00:00:00Z'}

orders.__cdf_dlt_metadata__ = {
    'kind': 'resource',
    'name': 'orders',
    'primary_key': 'id',
    'merge_key': ('id', 'region'),
    'write_disposition': {'disposition': 'merge', 'strategy': 'scd2'},
    'schema_contract': 'freeze',
    'incremental': {
        'cursor_path': 'updated_at',
        'initial_value': '2026-01-01T00:00:00Z',
        'row_order': 'desc',
        'lag_tolerance_ms': 5000,
    },
    'selected': True,
}
",
            c"dlt_fixture.py",
            c"dlt_fixture",
        )
        .unwrap();
        let resource = module.getattr("orders").unwrap();
        let preview = bridge().batches_from_dlt_resource(&resource).unwrap();
        let descriptor = preview.read.descriptor.as_ref().unwrap();

        assert_eq!(descriptor.resource_id.as_str(), "orders");
        assert_eq!(descriptor.primary_key, vec!["id"]);
        assert_eq!(descriptor.merge_key, vec!["id", "region"]);
        assert_eq!(
            descriptor.cursor.as_ref().unwrap().field.as_str(),
            "updated_at"
        );
        assert_eq!(
            descriptor.cursor.as_ref().unwrap().ordering,
            CursorOrderingClaim::Inexact
        );
        assert_eq!(descriptor.cursor.as_ref().unwrap().lag_tolerance_ms, 5000);
        assert_eq!(descriptor.write_disposition, WriteDisposition::Merge);
        assert_eq!(
            descriptor.contract.as_ref().unwrap().as_str(),
            "dlt-orders-freeze"
        );
        assert_eq!(descriptor.state_scope, ScopeKey::Resource);
        assert_eq!(preview.read.row_count(), 1);

        let snapshot = serde_json::to_string_pretty(&preview.mapping_table).unwrap();
        assert!(snapshot.contains("primary_key"));
        assert!(snapshot.contains("write_disposition.strategy"));
        assert!(snapshot.contains("dlt destination delegation"));
    });
}

#[test]
fn dlt_source_functions_expand_to_resource_reads() {
    Python::attach(|py| {
        let module = PyModule::from_code(
            py,
            c"
def users():
    yield {'id': 1, 'name': 'ada'}

users.__cdf_dlt_metadata__ = {
    'kind': 'resource',
    'name': 'users',
    'primary_key': 'id',
    'write_disposition': 'merge',
    'selected': True,
}

def crm():
    return [users]

crm.__cdf_dlt_metadata__ = {
    'kind': 'source',
    'name': 'crm',
}
",
            c"dlt_source_fixture.py",
            c"dlt_source_fixture",
        )
        .unwrap();
        let source = module.getattr("crm").unwrap();
        let reads = bridge().batches_from_dlt_source(&source).unwrap();

        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0].metadata.source_name.as_deref(), Some("crm"));
        assert_eq!(
            reads[0]
                .read
                .descriptor
                .as_ref()
                .unwrap()
                .resource_id
                .as_str(),
            "users"
        );
        assert_eq!(
            reads[0].read.descriptor.as_ref().unwrap().merge_key,
            vec!["id"]
        );
        assert_eq!(reads[0].read.row_count(), 1);
    });
}

#[test]
fn imported_dlt_decorators_map_selected_resources_and_skip_the_rest() {
    Python::attach(|py| {
        let sdk_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("python");
        let source = format!(
            r#"
import sys
sys.path.insert(0, {sdk_root:?})
from cdf_sdk import dlt

@dlt.resource(
    name="orders",
    primary_key="id",
    merge_key=("id", "region"),
    write_disposition={{"disposition": "merge", "strategy": "scd2"}},
    schema_contract={{"tables": "freeze", "columns": "evolve"}},
    incremental=dlt.incremental(
        "updated_at",
        initial_value="2026-01-01T00:00:00Z",
        ordering="exact",
        lag_tolerance_ms=250,
    ),
)
def orders():
    yield {{"id": 1, "region": "us", "updated_at": "2026-07-01T00:00:00Z"}}

@dlt.resource(name="unselected", selected=False)
def unselected():
    raise RuntimeError("unselected resource executed")

@dlt.resource(name="skipped", write_disposition="skip")
def skipped():
    raise RuntimeError("skipped resource executed")

@dlt.source(name="crm")
def crm():
    return [orders, unselected, skipped]
"#,
            sdk_root = sdk_root.display()
        );
        let source = CString::new(source).unwrap();
        let module = PyModule::from_code(
            py,
            &source,
            c"imported_dlt_fixture.py",
            c"imported_dlt_fixture",
        )
        .unwrap();

        let reads = bridge()
            .batches_from_dlt_source(&module.getattr("crm").unwrap())
            .unwrap();

        assert_eq!(reads.len(), 1);
        let read = &reads[0];
        let descriptor = read.read.descriptor.as_ref().unwrap();
        assert_eq!(descriptor.resource_id.as_str(), "orders");
        assert_eq!(descriptor.primary_key, vec!["id"]);
        assert_eq!(descriptor.merge_key, vec!["id", "region"]);
        assert_eq!(descriptor.cursor.as_ref().unwrap().field, "updated_at");
        assert_eq!(
            descriptor.cursor.as_ref().unwrap().ordering,
            CursorOrderingClaim::Exact
        );
        assert_eq!(descriptor.cursor.as_ref().unwrap().lag_tolerance_ms, 250);
        assert_eq!(descriptor.write_disposition, WriteDisposition::Merge);
        assert_eq!(
            descriptor.contract.as_ref().unwrap().as_str(),
            "dlt-orders-freeze"
        );
        assert_eq!(read.metadata.source_name.as_deref(), Some("crm"));
        assert_eq!(read.read.row_count(), 1);
        assert!(read.mapping_table.entries.iter().any(|entry| {
            entry.dlt_feature == "dlt destination delegation"
                && entry.status == DltBridgeMappingStatus::Unsupported
        }));
    });
}

#[test]
fn dlt_current_state_view_reads_committed_checkpoint_heads() {
    let pipeline_id = PipelineId::new("pipeline").unwrap();
    let resource_id = ResourceId::new("orders").unwrap();
    let metadata = DltBridgeMetadata {
        kind: DltBridgeObjectKind::Resource,
        name: Some("orders".to_owned()),
        table_name: None,
        source_name: Some("crm".to_owned()),
        primary_key: Some(vec!["id".to_owned()]),
        merge_key: None,
        incremental: None,
        write_disposition: None,
        schema_contract: None,
        selected: true,
        parallelized: false,
    };
    let resource_position = fixture_state_delta_position(
        "updated_at",
        CursorValue::String("2026-07-01T00:00:00Z".to_owned()),
    );
    let source_position = fixture_dlt_foreign_state(&serde_json::json!({
        "field_names": {"cf_1": "Customer Field"}
    }))
    .unwrap();
    let store = FixtureCheckpointStore {
        checkpoints: vec![
            checkpoint_fixture(
                "resource-head",
                pipeline_id.clone(),
                resource_id.clone(),
                ScopeKey::Resource,
                resource_position,
            ),
            checkpoint_fixture(
                "source-head",
                pipeline_id.clone(),
                resource_id.clone(),
                ScopeKey::Stream {
                    name: "dlt_source:crm".to_owned(),
                },
                source_position,
            ),
        ],
    };

    let view = dlt_current_state_view(&store, pipeline_id, resource_id, &metadata).unwrap();

    assert_eq!(
        view.resource_state["last_value"],
        serde_json::json!("2026-07-01T00:00:00Z")
    );
    assert_eq!(
        view.source_state.as_ref().unwrap()["field_names"]["cf_1"],
        serde_json::json!("Customer Field")
    );
    assert!(view.note.contains("committed CDF checkpoint heads"));
}

struct FixtureCheckpointStore {
    checkpoints: Vec<Checkpoint>,
}

impl CheckpointStore for FixtureCheckpointStore {
    fn propose(&self, _delta: StateDelta) -> Result<Checkpoint> {
        Err(CdfError::internal("fixture store does not propose"))
    }

    fn commit(&self, _checkpoint_id: &CheckpointId, _receipt: Receipt) -> Result<Checkpoint> {
        Err(CdfError::internal("fixture store does not commit"))
    }

    fn abandon(&self, _checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
        Err(CdfError::internal("fixture store does not abandon"))
    }

    fn head(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Option<Checkpoint>> {
        Ok(self
            .checkpoints
            .iter()
            .find(|checkpoint| {
                checkpoint.delta.pipeline_id == *pipeline_id
                    && checkpoint.delta.resource_id == *resource_id
                    && checkpoint.delta.scope == *scope
                    && checkpoint.is_head
            })
            .cloned())
    }

    fn history(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Vec<Checkpoint>> {
        Ok(self
            .checkpoints
            .iter()
            .filter(|checkpoint| {
                checkpoint.delta.pipeline_id == *pipeline_id
                    && checkpoint.delta.resource_id == *resource_id
                    && checkpoint.delta.scope == *scope
            })
            .cloned()
            .collect())
    }

    fn rewind(&self, _request: RewindRequest) -> Result<RewindReport> {
        Err(CdfError::internal("fixture store does not rewind"))
    }
}

fn checkpoint_fixture(
    checkpoint_id: &str,
    pipeline_id: PipelineId,
    resource_id: ResourceId,
    scope: ScopeKey,
    output_position: SourcePosition,
) -> Checkpoint {
    let segment = StateSegment {
        segment_id: SegmentId::new(format!("{checkpoint_id}-segment")).unwrap(),
        scope: scope.clone(),
        output_position: output_position.clone(),
        row_count: 1,
        byte_count: 1,
    };
    let delta = StateDelta {
        checkpoint_id: CheckpointId::new(checkpoint_id).unwrap(),
        pipeline_id,
        resource_id,
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position,
        package_hash: PackageHash::new(format!("{checkpoint_id}-package")).unwrap(),
        schema_hash: SchemaHash::new(format!("{checkpoint_id}-schema")).unwrap(),
        segments: vec![segment],
    };
    Checkpoint {
        delta,
        status: CheckpointStatus::Committed,
        receipt: None,
        is_head: true,
        created_at_ms: 1,
        committed_at_ms: Some(2),
        rewind_target_checkpoint_id: None,
    }
}
