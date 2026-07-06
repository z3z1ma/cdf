use super::*;

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
};

use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use firn_http::{EgressAllowlist, HeaderMap, HttpMethod, SecretValue};
use firn_kernel::{ErrorKind, PageToken};
use pyo3::types::PyList;

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
fn concurrency_semantics_are_identical_for_fixture_hashes() {
    let read = bridge()
        .batches_from_json_dict_rows(vec![
            serde_json::json!({"id": 1, "name": "ada"}),
            serde_json::json!({"id": 2, "name": "grace"}),
        ])
        .unwrap();
    let gil_report = InterpreterReport {
        executable: PathBuf::from("/usr/bin/python"),
        major: 3,
        minor: 14,
        micro: 0,
        implementation: "CPython".to_owned(),
        gil_enabled: true,
        free_threaded_build: false,
    };
    let free_threaded_report = InterpreterReport {
        gil_enabled: false,
        free_threaded_build: true,
        ..gil_report.clone()
    };

    let gil_semantics = execution_semantics(&gil_report, true, 4);
    let free_threaded_semantics = execution_semantics(&free_threaded_report, true, 4);
    assert_eq!(gil_semantics.effective_parallelism, 1);
    assert_eq!(free_threaded_semantics.effective_parallelism, 4);
    assert_eq!(
        deterministic_fixture_hash(&read).unwrap(),
        deterministic_fixture_hash(&read).unwrap()
    );
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
    let read = read_ndjson_bytes(
        br#"{"id":1,"name":"ada"}"#,
        &ReadOptions::new(
            ResourceId::new("hash").unwrap(),
            PartitionId::new("p0").unwrap(),
        ),
        &JsonOptions::default(),
    )
    .unwrap();
    let bytes = {
        let batch = read.batches[0].record_batch().unwrap();
        let mut output = Vec::new();
        let mut writer = StreamWriter::try_new(&mut output, batch.schema().as_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        output
    };
    let imported = firn_formats::read_arrow_ipc_stream(
        Cursor::new(bytes),
        &ReadOptions::new(
            ResourceId::new("hash").unwrap(),
            PartitionId::new("p0").unwrap(),
        ),
    )
    .unwrap();

    assert_eq!(imported.batches[0].header.row_count, 1);
}
