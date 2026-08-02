use super::*;

#[test]
fn schema_discover_local_parquet_reports_schema_without_project_writes() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "local.events");
    assert_eq!(report["writes"]["schema_snapshot"], false);
    assert_eq!(report["writes"]["lockfile"], false);
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);
    assert!(
        report["schema_snapshot_path"]
            .as_str()
            .unwrap()
            .starts_with(".cdf/schemas/local.events@sha256:")
    );
    assert_eq!(
        report["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    assert_eq!(report["snapshot_metadata"]["source_driver"], "files");
    assert_eq!(report["snapshot_metadata"]["cdf:normalizer"], "namecase-v1");
    assert_eq!(report["fields"][0]["name"], "vendor_id");
    assert_eq!(report["fields"][0]["source_name"], "VendorID");
    assert_eq!(
        report["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );
    assert_eq!(report["source_identity"]["path"], "vendors.parquet");
    assert!(
        report["source_identity"]["driver.footer_sha256"]
            .as_str()
            .is_some()
    );
    assert_eq!(report["next_command"], "cdf plan local.events");
}

#[test]
fn local_arrow_ipc_discover_pin_show_diff_preview_and_run_share_pinned_schema() {
    let project = TestProject::new();
    write_arrow_ipc_discover_resource(&project, "events.arrow");
    remove_resource_format(&project, "arrow_ipc");
    write_large_vendor_arrow_ipc(&project, "events.arrow");

    let discover = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_eq!(discover.exit_code, 0, "stderr: {}", discover.stderr);
    let discover_json = stderr_or_stdout_json(&discover.stdout);
    let discovered = &discover_json["result"];
    assert_eq!(
        discovered["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    assert_eq!(discovered["snapshot_metadata"]["source_driver"], "files");
    assert!(
        discovered["snapshot_metadata"]["source_discovery_binding"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:"))
    );
    assert_eq!(
        discovered["snapshot_metadata"]["cdf:normalizer"],
        "namecase-v1"
    );
    assert_eq!(discovered["source_identity"]["path"], "events.arrow");
    assert_eq!(discovered["source_identity"]["transport"], "files");
    assert!(
        discovered["source_identity"]["driver.schema_hash"]
            .as_str()
            .is_some()
    );
    let source_size = discovered["source_identity"]["driver.size_bytes"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let probe_bytes = discovered["source_identity"]["probe_bytes_read"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!(
        probe_bytes < source_size / 2,
        "generic CLI discovery read {probe_bytes} of {source_size} source bytes"
    );
    assert_eq!(discovered["fields"][0]["name"], "vendor_id");
    assert_eq!(discovered["fields"][0]["source_name"], "VendorID");
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());

    let no_pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--no-pin",
    ]);
    assert_eq!(no_pin.exit_code, 0, "stderr: {}", no_pin.stderr);
    assert_eq!(
        stderr_or_stdout_json(&no_pin.stdout)["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let auto_pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(auto_pin.exit_code, 0, "stderr: {}", auto_pin.stderr);
    let auto_pin_json = stderr_or_stdout_json(&auto_pin.stdout);
    assert_eq!(
        auto_pin_json["result"]["schema_snapshot"]["outcome"],
        "added"
    );
    let pinned_hash = auto_pin_json["result"]["resource_schema"]["schema_hash"]
        .as_str()
        .unwrap();
    let baseline_hash = auto_pin_json["result"]["schema_snapshot"]["schema_hash"]
        .as_str()
        .unwrap();
    assert_eq!(pinned_hash, baseline_hash);
    let snapshot_path = auto_pin_json["result"]["resource_schema"]["snapshot_path"]
        .as_str()
        .unwrap();

    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "stderr: {}", pin.stderr);
    let pin_json = stderr_or_stdout_json(&pin.stdout);
    assert_eq!(pin_json["result"]["status"], "unchanged");
    assert_eq!(pin_json["result"]["schema_hash"], baseline_hash);
    let snapshot = read_snapshot_json(&project, snapshot_path);
    assert_eq!(snapshot["schema_hash"], baseline_hash);
    assert_eq!(snapshot["schema"]["metadata"]["owner"], "source-system");
    assert_eq!(
        snapshot["schema"]["fields"][0]["metadata"]["source-tag"],
        "vendor"
    );
    assert_eq!(
        snapshot["schema"]["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );

    let show = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "show",
        "local.events",
    ]);
    assert_eq!(show.exit_code, 0, "stderr: {}", show.stderr);
    assert_eq!(
        stderr_or_stdout_json(&show.stdout)["result"]["schema_hash"],
        baseline_hash
    );

    let diff = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "diff",
        "local.events",
    ]);
    assert_eq!(diff.exit_code, 0, "stderr: {}", diff.stderr);
    assert_eq!(
        stderr_or_stdout_json(&diff.stdout)["result"]["summary"]["changed"],
        false
    );

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    assert_eq!(
        plan_json["result"]["resource_schema"]["schema_hash"],
        pinned_hash
    );
    assert_eq!(
        plan_json["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    let preview_json = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_json["result"]["row_count"], 2);
    assert_eq!(
        preview_json["result"]["fields"],
        json!(["vendor_id", "note", "_cdf_variant"])
    );
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(run_json["result"]["schema_hash"], pinned_hash);
    assert_eq!(run_json["result"]["row_count"], 2);
    assert_eq!(run_json["result"]["checkpoint"]["status"], "committed");
    let package_dir = run_package_dir(&project, &run_result);
    let reader = PackageReader::open(&package_dir).unwrap();
    reader.verify().unwrap();
    let receipts = collect_package_receipts(&reader);
    assert_eq!(receipts.len(), 1);
    let receipt = &receipts[0];
    assert_eq!(receipt.schema_hash.as_str(), pinned_hash);
    assert_eq!(receipt.disposition, WriteDisposition::Append);
    assert_eq!(receipt.counts.rows_written, 2);
    let destination = DuckDbDestination::new(project.root.join(".cdf/dev.duckdb")).unwrap();
    assert!(destination.verify_receipt(receipt).unwrap().verified);
    let segments = collect_package_segments_for_test(&reader);
    assert_eq!(segments.len(), 1);
    let packaged_schema = segments[0].1[0].schema();
    assert_eq!(packaged_schema.metadata()["owner"], "source-system");
    assert_eq!(
        packaged_schema.field(0).metadata()["cdf:source_name"],
        "VendorID"
    );
    let stream_admission: serde_json::Value = serde_json::from_slice(
        &fs::read(package_dir.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    let coercion: cdf_contract::SchemaCoercionPlan =
        serde_json::from_value(stream_admission["observations"][0]["coercion_plan"].clone())
            .unwrap();
    let vendor = coercion
        .fields
        .iter()
        .find(|field| field.source_name == "VendorID")
        .unwrap();
    assert_eq!(
        vendor.decision,
        cdf_contract::FieldCoercionDecision::Preserved
    );
    assert_eq!(vendor.observed_type.as_deref(), Some("Int32"));
    assert_eq!(vendor.constraint_type.as_deref(), Some("Int32"));

    let replay = reader.replay_inputs().unwrap();
    assert_eq!(replay.state_delta.schema_hash.as_str(), pinned_hash);
    assert!(receipt.covers_state_delta(&replay.state_delta));
    let SourcePosition::FileManifest(manifest) = &replay.state_delta.output_position else {
        panic!("Arrow IPC run must commit FileManifest source position");
    };
    assert_eq!(manifest.files.len(), 1);
    let source_path = project.root.join("data/events.arrow");
    let source_bytes = fs::read(&source_path).unwrap();
    let expected_sha = format!(
        "sha256:{}",
        Sha256::digest(&source_bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    assert_eq!(manifest.files[0].path, "events.arrow");
    assert_eq!(
        manifest.files[0].size_bytes,
        u64::try_from(source_bytes.len()).unwrap()
    );
    assert_eq!(
        manifest.files[0].sha256.as_deref(),
        Some(expected_sha.as_str())
    );
    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &replay.state_delta.pipeline_id,
            &replay.state_delta.resource_id,
            &replay.state_delta.scope,
        )
        .unwrap()
        .expect("committed Arrow IPC checkpoint head");
    assert_eq!(head.delta.schema_hash.as_str(), pinned_hash);
    assert_eq!(
        head.delta.output_position,
        replay.state_delta.output_position
    );
    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);
}

#[test]
fn arrow_ipc_discovery_supports_compression_multi_file_and_remote_without_writes() {
    let malformed = TestProject::new();
    write_arrow_ipc_discover_resource(&malformed, "events.arrow");
    fs::write(malformed.root.join("data/events.arrow"), b"not-arrow-ipc").unwrap();
    let malformed_result = run([
        "cdf",
        "--json",
        "--project",
        malformed.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_ne!(malformed_result.exit_code, 0);
    assert!(
        malformed_result
            .stderr
            .contains("file format confirmation failed")
    );
    assert_no_schema_discovery_writes(&malformed);

    let truncated = TestProject::new();
    write_arrow_ipc_discover_resource(&truncated, "events.arrow");
    write_vendor_arrow_ipc(&truncated, "events.arrow");
    fs::OpenOptions::new()
        .write(true)
        .open(truncated.root.join("data/events.arrow"))
        .unwrap()
        .set_len(16)
        .unwrap();
    let truncated_result = run([
        "cdf",
        "--json",
        "--project",
        truncated.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_ne!(truncated_result.exit_code, 0);
    assert!(
        truncated_result
            .stderr
            .contains("Arrow file does not contain correct footer"),
        "{}",
        truncated_result.stderr
    );
    assert_no_schema_discovery_writes(&truncated);

    let stream = TestProject::new();
    write_arrow_ipc_discover_resource(&stream, "events.arrow");
    let stream_schema = Arc::new(Schema::new(vec![Field::new(
        "VendorID",
        DataType::Int32,
        false,
    )]));
    let stream_batch = RecordBatch::try_new(
        Arc::clone(&stream_schema),
        vec![Arc::new(Int32Array::from_iter_values([1_i32]))],
    )
    .unwrap();
    let mut stream_bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut stream_bytes, stream_schema.as_ref()).unwrap();
        writer.write(&stream_batch).unwrap();
        writer.finish().unwrap();
    }
    fs::write(stream.root.join("data/events.arrow"), stream_bytes).unwrap();
    let stream_result = run([
        "cdf",
        "--json",
        "--project",
        stream.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_ne!(stream_result.exit_code, 0);
    assert!(
        stream_result
            .stderr
            .contains("alternate format `arrow_ipc_stream`"),
        "{}",
        stream_result.stderr
    );
    assert!(
        stream_result
            .stderr
            .contains("stream framing is unsupported")
    );
    assert_no_schema_discovery_writes(&stream);

    let compression_source = TestProject::new();
    write_vendor_arrow_ipc(&compression_source, "events.arrow");
    let arrow_bytes = fs::read(compression_source.root.join("data/events.arrow")).unwrap();
    let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
    gzip.write_all(&arrow_bytes).unwrap();
    let gzip_bytes = gzip.finish().unwrap();
    for (label, bytes, compression_override, succeeds) in [
        ("gzip-auto", gzip_bytes.clone(), None, true),
        ("gzip-override", gzip_bytes, Some("gzip"), true),
        ("zstd-malformed", vec![0x28, 0xb5, 0x2f, 0xfd], None, false),
    ] {
        let compressed = TestProject::new();
        write_arrow_ipc_discover_resource(&compressed, "events.arrow");
        if let Some(compression) = compression_override {
            let resource_path = compressed.root.join("resources/files.toml");
            let resource = fs::read_to_string(&resource_path).unwrap().replace(
                "format = \"arrow_ipc\"",
                &format!("format = \"arrow_ipc\"\ncompression = \"{compression}\""),
            );
            fs::write(resource_path, resource).unwrap();
        }
        fs::write(compressed.root.join("data/events.arrow"), bytes).unwrap();
        let compressed_result = run([
            "cdf",
            "--json",
            "--project",
            compressed.root_str(),
            "schema",
            "discover",
            "local.events",
        ]);
        if succeeds {
            assert_eq!(
                compressed_result.exit_code, 0,
                "{label}: {}",
                compressed_result.stderr
            );
        } else {
            assert_ne!(compressed_result.exit_code, 0, "{label}");
            assert!(
                compressed_result.stderr.contains("failed:"),
                "{label}: {}",
                compressed_result.stderr
            );
        }
        assert!(
            !compressed_result.stderr.contains("excluded"),
            "{label}: {}",
            compressed_result.stderr
        );
        assert_no_schema_discovery_writes(&compressed);
    }

    let multi = TestProject::new();
    write_arrow_ipc_discover_resource(&multi, "*.arrow");
    write_vendor_arrow_ipc(&multi, "first.arrow");
    fs::copy(
        multi.root.join("data/first.arrow"),
        multi.root.join("data/second.arrow"),
    )
    .unwrap();
    let multi_result = run([
        "cdf",
        "--json",
        "--project",
        multi.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_eq!(multi_result.exit_code, 0, "{}", multi_result.stderr);
    let multi_json = stderr_or_stdout_json(&multi_result.stdout);
    let multi_report = &multi_json["result"];
    assert_eq!(
        multi_report["source_identity"]["file_coverage"],
        "all_files"
    );
    assert_eq!(
        multi_report["source_identity"]["within_file_coverage"],
        "format_metadata"
    );
    assert_eq!(multi_report["source_identity"]["matched_files"], "2");
    assert_eq!(multi_report["source_identity"]["selected_files"], "2");
    assert_no_schema_discovery_writes(&multi);
    let pin = run([
        "cdf",
        "--json",
        "--project",
        multi.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "{}", pin.stderr);
    let schema_entries = fs::read_dir(multi.root.join(".cdf/schemas"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(schema_entries.len(), 2);
    assert!(
        schema_entries
            .iter()
            .any(|path| path.ends_with(".discovery.json"))
    );
    let diff = run([
        "cdf",
        "--json",
        "--project",
        multi.root_str(),
        "schema",
        "diff",
        "local.events",
    ]);
    assert_eq!(diff.exit_code, 0, "{}", diff.stderr);

    let remote_source = TestProject::new();
    write_vendor_arrow_ipc(&remote_source, "events.arrow");
    let remote_bytes = fs::read(remote_source.root.join("data/events.arrow")).unwrap();
    let (base_url, _requests) = serve_parquet_file(remote_bytes, 16);
    let remote = TestProject::new();
    fs::write(
        remote.root.join("resources/files.toml"),
        format!(
            r#"
[source.local]
kind = "files"
root = "{base_url}/"
egress_allowlist = ["127.0.0.1"]

[resource.events]
glob = "events.arrow"
format = "arrow_ipc"
write_disposition = "append"
trust = "governed"
"#
        ),
    )
    .unwrap();
    let remote_result = run([
        "cdf",
        "--json",
        "--project",
        remote.root_str(),
        "schema",
        "discover",
        "local.events",
    ]);
    assert_eq!(remote_result.exit_code, 0, "{}", remote_result.stderr);
    let remote_report = stderr_or_stdout_json(&remote_result.stdout);
    assert_eq!(
        remote_report["result"]["source_identity"]["driver.format"],
        "arrow_ipc"
    );
    assert_eq!(
        remote_report["result"]["source_identity"]["transport"],
        "files"
    );
    assert_no_schema_discovery_writes(&remote);
}

#[test]
fn protobuf_descriptor_discovery_run_and_duckdb_commit_share_one_native_driver() {
    let project = TestProject::new();
    write_protobuf_resource(&project);

    let result = run_valid_run_resource(&project, "local.rows");

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let report = stderr_or_stdout_json(&result.stdout);
    assert_eq!(report["result"]["resource_id"], "local.rows");
    assert_eq!(report["result"]["row_count"], 2);
    assert_eq!(report["result"]["checkpoint"]["status"], "committed");
    let connection = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows = connection
        .prepare("SELECT id, name FROM rows ORDER BY id")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, vec![(7, "bob".to_owned()), (42, "alice".to_owned())]);
}

#[test]
fn pinned_arrow_ipc_type_drift_is_observed_and_quarantined_in_the_preview_run_stream() {
    let project = TestProject::new();
    write_arrow_ipc_discover_resource(&project, "events.arrow");
    write_vendor_arrow_ipc(&project, "events.arrow");
    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(pin.exit_code, 0, "stderr: {}", pin.stderr);

    let drift_schema = Arc::new(Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, false),
        Field::new("Note", DataType::Utf8, true),
    ]));
    let drift_batch = RecordBatch::try_new(
        drift_schema,
        vec![
            Arc::new(StringArray::from(vec!["unexpected"])),
            Arc::new(StringArray::from(vec![Some("drifted")])),
        ],
    )
    .unwrap();
    write_arrow_ipc_source(&project, "events.arrow", drift_batch);

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    let preview_report = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_report["result"]["planned_partition_count"], 1);
    assert_eq!(
        preview_report["result"]["payload_opened_partition_count"],
        1
    );
    assert_eq!(preview_report["result"]["attested_partition_count"], 0);
    assert_eq!(preview_report["result"]["terminal_quarantine_count"], 1);
    assert_eq!(preview_report["result"]["row_count"], 0);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "{}", run_result.stderr);
    let package_dir = run_package_dir(&project, &run_result);
    let reader = PackageReader::open(package_dir).unwrap();
    reader.verify().unwrap();
    let mut has_quarantine = false;
    reader
        .for_each_identity_file(&mut |file| {
            has_quarantine |= file.path == "quarantine/schema-observations.json";
            Ok(())
        })
        .unwrap();
    assert!(has_quarantine);
}

#[test]
fn declared_arrow_ipc_lossless_widening_records_physical_and_coercion_evidence() {
    let project = TestProject::new();
    for entry in fs::read_dir(project.root.join("data")).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    fs::write(
        project.root.join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.arrow"
format = "arrow_ipc"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "VendorID", type = "int64", nullable = false },
  { name = "Note", type = "string", nullable = true },
] }
"#,
    )
    .unwrap();
    write_vendor_arrow_ipc(&project, "events.arrow");

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    assert_eq!(
        stderr_or_stdout_json(&preview.stdout)["result"]["fields"],
        json!(["vendor_id", "note", "_cdf_variant"])
    );

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "{}", run_result.stderr);
    let package_dir = run_package_dir(&project, &run_result);
    let reader = PackageReader::open(&package_dir).unwrap();
    reader.verify().unwrap();
    let batches = collect_package_segments_for_test(&reader);
    let schema = batches[0].1[0].schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(0).metadata()["cdf:source_name"], "VendorID");
    assert!(!schema.field(0).metadata().contains_key("cdf:physical_type"));
    assert!(
        !package_dir
            .join("schema/effective-schema-evidence.json")
            .exists(),
        "declared execution must classify the physical schema in-stream without a pre-scan artifact"
    );
    let stream_admission: serde_json::Value = serde_json::from_slice(
        &fs::read(package_dir.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    let coercion: cdf_contract::SchemaCoercionPlan =
        serde_json::from_value(stream_admission["observations"][0]["coercion_plan"].clone())
            .unwrap();
    let vendor = coercion
        .fields
        .iter()
        .find(|field| field.source_name == "VendorID")
        .unwrap();
    assert_eq!(
        vendor.decision,
        cdf_contract::FieldCoercionDecision::Widened
    );
    assert_eq!(vendor.observed_type.as_deref(), Some("Int32"));
    assert_eq!(vendor.constraint_type.as_deref(), Some("Int64"));
}

#[test]
fn hints_schema_discovers_pins_and_constrains_observed_parquet() {
    let project = TestProject::new();
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));
    fs::write(
        project.root.join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "vendors.parquet"
format = "parquet"
schema_mode = "hints"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "VendorID", type = "int64", nullable = false },
] }
"#,
    )
    .unwrap();

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(plan.exit_code, 0, "{}", plan.stderr);
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    assert_eq!(plan_json["result"]["schema_snapshot"]["outcome"], "added");
    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    let initial_reference = lock.resources["local.events"]
        .schema_snapshot
        .as_ref()
        .unwrap()
        .clone();
    let initial_lock = fs::read(project.root.join("cdf.lock")).unwrap();
    let initial_snapshot = fs::read(project.root.join(&initial_reference.path)).unwrap();

    let unchanged = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(unchanged.exit_code, 0, "{}", unchanged.stderr);
    assert_eq!(
        stderr_or_stdout_json(&unchanged.stdout)["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        initial_lock
    );
    assert_eq!(
        fs::read(project.root.join(&initial_reference.path)).unwrap(),
        initial_snapshot
    );

    write_vendor_score_parquet(&project.root.join("data/vendors.parquet"));
    let drifted = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(drifted.exit_code, 0, "{}", drifted.stderr);
    assert_eq!(
        stderr_or_stdout_json(&drifted.stdout)["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        initial_lock
    );
    assert_eq!(
        fs::read(project.root.join(&initial_reference.path)).unwrap(),
        initial_snapshot
    );

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "{}", run_result.stderr);
    let reader = PackageReader::open(run_package_dir(&project, &run_result)).unwrap();
    let batches = collect_package_segments_for_test(&reader);
    assert_eq!(
        batches[0].1[0].schema().field(0).data_type(),
        &DataType::Int64
    );
    assert_eq!(batches[0].1[0].schema().field(0).name(), "vendor_id");
}

#[test]
fn schema_discover_rest_reports_sample_schema_without_project_writes_or_secret_leak() {
    let project = TestProject::new();
    fs::write(project.root.join("rest-token"), "rest-schema-secret\n").unwrap();
    let (base_url, requests) = serve_json_sequence([r#"{ "items": [
        { "VendorID": 1, "updated_at": 10, "active": true, "score": 4.5 },
        { "VendorID": 2, "updated_at": 20, "active": false, "score": null },
        { "VendorID": 3, "updated_at": 30, "active": true }
    ] }"#]);
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );
    fs::write(
        project.root.join("resources/api.toml"),
        rest_discover_resource_with_base_url(&base_url, "secret://file/rest-token"),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "discover",
        "api.items",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "rest-schema-secret");
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "api.items");
    assert_eq!(report["writes"]["schema_snapshot"], false);
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);
    assert_eq!(
        report["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    assert_eq!(report["snapshot_metadata"]["source_driver"], "rest");
    assert_eq!(report["snapshot_metadata"]["cdf:normalizer"], "namecase-v1");
    assert!(
        report["schema_snapshot_path"]
            .as_str()
            .unwrap()
            .starts_with(".cdf/schemas/api.items@sha256:")
    );
    let fields = report["fields"].as_array().unwrap();
    assert!(fields.iter().any(|field| field["name"] == "active"));
    let score = fields
        .iter()
        .find(|field| field["name"] == "score")
        .unwrap();
    assert_eq!(score["nullable"], true);
    let vendor = fields
        .iter()
        .find(|field| field["name"] == "vendor_id")
        .unwrap();
    assert_eq!(vendor["source_name"], "VendorID");
    assert_eq!(
        report["source_identity"]["driver.record_selector"],
        "$.items"
    );
    assert_eq!(report["source_identity"]["driver.sample_pages"], "1");
    assert_eq!(report["source_identity"]["driver.sample_records"], "3");

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].contains("GET /items HTTP/1.1"));
    assert!(requests[0].contains("authorization: Bearer rest-schema-secret"));
}

#[test]
fn schema_discover_postgres_catalog_uses_project_secret_without_writes_or_secret_leak() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("catalog_discover_orders");
    let mut client = postgres.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"VendorID\" INTEGER NOT NULL,
                \"customer_uuid\" UUID,
                \"updated_at\" TIMESTAMP WITH TIME ZONE
            )",
            table
        ))
        .unwrap();

    let project = TestProject::new();
    let source_dsn = postgres.url.replacen(
        "postgresql://cdf@",
        "postgresql://cdf:schema-discover-secret@",
        1,
    );
    fs::write(project.root.join("sql-dsn"), format!("{source_dsn}\n")).unwrap();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/sql-dsn"),
    );
    fs::write(
        project.root.join("resources/sql.toml"),
        sql_discover_resource("secret://file/sql-dsn", &table),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "discover",
        "warehouse.orders",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &source_dsn);
    assert_secret_absent(&result, "schema-discover-secret");
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "warehouse.orders");
    assert_eq!(report["writes"]["schema_snapshot"], false);
    assert_eq!(report["writes"]["lockfile"], false);
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);
    assert_eq!(
        report["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    assert_eq!(report["snapshot_metadata"]["source_driver"], "postgres");
    assert_eq!(report["source_identity"]["driver.dialect"], "postgres");
    assert_eq!(report["source_identity"]["driver.table"], table);
    assert_eq!(report["snapshot_metadata"]["cdf:normalizer"], "namecase-v1");
    assert!(
        report["schema_snapshot_path"]
            .as_str()
            .unwrap()
            .starts_with(".cdf/schemas/warehouse.orders@sha256:")
    );
    assert_eq!(report["fields"][0]["name"], "vendor_id");
    assert_eq!(report["fields"][0]["nullable"], false);
    assert_eq!(report["fields"][0]["source_name"], "VendorID");
    assert_eq!(
        report["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );
    assert_eq!(
        report["fields"][0]["metadata"]["cdf:physical_type"],
        "integer"
    );
    assert_eq!(report["fields"][1]["name"], "customer_uuid");
    assert_eq!(report["fields"][1]["metadata"]["cdf:physical_type"], "uuid");
    assert_eq!(report["fields"][2]["name"], "updated_at");
    assert_eq!(
        report["fields"][2]["metadata"]["cdf:physical_type"],
        "timestamp with time zone"
    );
    assert_eq!(report["source_identity"]["driver.source_kind"], "sql");
    assert_eq!(report["source_identity"]["driver.dialect"], "postgres");
    assert_eq!(report["source_identity"]["driver.table"], table);
    assert_eq!(report["next_command"], "cdf plan warehouse.orders");

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "schema",
        "discover",
        "warehouse.orders",
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert_secret_absent(&human, &source_dsn);
    assert_secret_absent(&human, "schema-discover-secret");
    assert!(human.stdout.contains("registered-source-discovery"));
    assert!(human.stdout.contains("postgres"));
}

#[test]
fn schema_pin_show_and_diff_local_parquet_snapshot_with_lockfile_reference() {
    let project = TestProject::new();
    write_minimal_lockfile(&project);
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);

    assert_eq!(pin.exit_code, 0, "stderr: {}", pin.stderr);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let pin_json = stderr_or_stdout_json(&pin.stdout);
    let pin_report = &pin_json["result"];
    assert_eq!(pin_report["resource_id"], "local.events");
    assert_eq!(pin_report["status"], "added");
    assert_eq!(pin_report["writes"]["schema_snapshot"], true);
    assert_eq!(pin_report["writes"]["lockfile"], true);
    assert_eq!(pin_report["writes"]["package"], false);
    assert_eq!(pin_report["fields"][0]["name"], "vendor_id");
    let snapshot_path = pin_report["schema_snapshot_path"].as_str().unwrap();
    assert!(project.root.join(snapshot_path).is_file());

    let lock_text = fs::read_to_string(project.root.join("cdf.lock")).unwrap();
    let lock = parse_lock(&lock_text).unwrap();
    let locked = lock.resources.get("local.events").unwrap();
    assert_eq!(locked.schema_snapshot.as_ref().unwrap().path, snapshot_path);
    assert_eq!(
        locked
            .schema_snapshot
            .as_ref()
            .unwrap()
            .schema_hash
            .as_str(),
        pin_report["schema_hash"].as_str().unwrap()
    );

    let show = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "show",
        "local.events",
    ]);

    assert_eq!(show.exit_code, 0, "stderr: {}", show.stderr);
    let show_json = stderr_or_stdout_json(&show.stdout);
    let show_report = &show_json["result"];
    assert_eq!(show_report["schema_hash"], pin_report["schema_hash"]);
    assert_eq!(show_report["fields"][0]["source_name"], "VendorID");
    assert_eq!(show_report["writes"]["schema_snapshot"], false);

    let diff = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "diff",
        "local.events",
    ]);

    assert_eq!(diff.exit_code, 0, "stderr: {}", diff.stderr);
    let diff_json = stderr_or_stdout_json(&diff.stdout);
    let diff_report = &diff_json["result"];
    assert_eq!(diff_report["summary"]["changed"], false);
    assert_eq!(diff_report["writes"]["schema_snapshot"], false);
    assert_eq!(diff_report["writes"]["lockfile"], false);

    let pin_again = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);

    assert_eq!(pin_again.exit_code, 0, "stderr: {}", pin_again.stderr);
    let pin_again_json = stderr_or_stdout_json(&pin_again.stdout);
    assert_eq!(pin_again_json["result"]["status"], "unchanged");
}

#[test]
fn schema_pin_without_lockfile_creates_semantic_lockfile_reference() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["writes"]["schema_snapshot"], true);
    assert_eq!(report["writes"]["lockfile"], true);
    assert_eq!(report["unsupported"], serde_json::json!([]));
    assert!(project.root.join(".cdf/schemas").exists());
    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    assert!(lock.resources["local.events"].schema_snapshot.is_some());
}
