use super::*;

#[test]
fn python_resource_plan_preview_run_and_replay_use_the_product_spine() {
    let project = TestProject::new();
    let marker = project.root.join("python-resource-executed");
    let interpreter = cdf_python::attached_interpreter_report()
        .unwrap()
        .executable;
    write_python_frontdoor_project(&project, &interpreter, &marker);

    let inspected = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "resource",
        "events.raw",
    ]);
    assert_eq!(inspected.exit_code, 0, "stderr: {}", inspected.stderr);
    let inspected = stderr_or_stdout_json(&inspected.stdout);
    assert_eq!(inspected["result"]["source_name"], "events");
    assert_eq!(inspected["result"]["resource_name"], "raw");
    assert_eq!(
        inspected["result"]["descriptor"]["freshness"]["max_age_ms"],
        2_700_000
    );
    assert!(!marker.exists(), "inspect executed the Python row callable");

    let before = project_tree_snapshot(&project.root);
    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "events.raw",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    assert_eq!(project_tree_snapshot(&project.root), before);
    assert!(!marker.exists(), "plan executed the Python row callable");
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    assert_eq!(plan_json["result"]["resource_id"], "events.raw");
    assert_eq!(
        plan_json["result"]["will_fetch"]["partitions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        plan_json["result"]["explain"]["source_boundary"]["transfer_modes"],
        serde_json::json!(["arrow_c_data", "row_compat"])
    );
    assert_eq!(
        plan_json["result"]["explain"]["source_boundary"]["execution_lane"],
        "blocking"
    );
    assert_eq!(
        plan_json["result"]["explain"]["source_boundary"]["maximum_internal_parallelism"],
        1
    );

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "--limit",
        "1",
        "events.raw",
    ]);
    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    let preview_json = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_json["result"]["row_count"], 1);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/python.duckdb").exists());
    assert!(marker.is_file());
    fs::remove_file(&marker).unwrap();

    let run_result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "events.raw",
    ]);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let report = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(report["result"]["row_count"], 2);
    assert_eq!(report["result"]["checkpoint"]["status"], "committed");
    assert_eq!(
        report["result"]["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
    assert_eq!(report["result"]["writes"]["package"], true);
    assert_eq!(report["result"]["writes"]["destination"], true);
    assert_eq!(report["result"]["writes"]["checkpoint"], true);
    assert_eq!(report["result"]["source_transfer"]["control_events"], 0);
    assert_eq!(
        report["result"]["source_transfer"]["modes"][0]["mode"],
        "row_compat"
    );
    assert_eq!(
        report["result"]["source_transfer"]["modes"][0]["batches"],
        1
    );
    assert_eq!(report["result"]["source_transfer"]["modes"][0]["rows"], 2);
    assert_eq!(
        report["result"]["source_transfer"]["modes"][0]["known_copy_batches"],
        1
    );
    assert_eq!(
        report["result"]["source_transfer"]["modes"][0]["unknown_copy_batches"],
        0
    );
    let package = run_package_dir(&project, &run_result);
    assert!(package.join("manifest.json").is_file());
    if let Ok(path) = std::env::var("CDF_PYTHON_PACKAGE_DATA_HASH_OUTPUT") {
        let manifest = cdf_package::read_manifest(&package).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(b"cdf-python-package-data-v1");
        for segment in &manifest.identity.segments {
            hasher.update(segment.segment_id.as_str().as_bytes());
            hasher.update(segment.package_row_ord_start.to_le_bytes());
            hasher.update(segment.row_count.to_le_bytes());
            hasher.update(segment.sha256.as_bytes());
        }
        fs::write(path, format!("sha256:{:x}\n", hasher.finalize())).unwrap();
    }
    assert!(marker.is_file());

    fs::write(
        project.root.join("src/events.py"),
        "raise RuntimeError('Python source must not execute during replay')\n",
    )
    .unwrap();
    let replay_project = TestProject::new();
    let replay = run([
        "cdf",
        "--json",
        "--project",
        replay_project.root_str(),
        "replay",
        "package",
        package.to_str().unwrap(),
        "--to",
        "duckdb://.cdf/replayed-python.duckdb",
        "--target",
        "raw_replay",
    ]);
    assert_eq!(replay.exit_code, 0, "stderr: {}", replay.stderr);
    assert!(
        replay_project
            .root
            .join(".cdf/replayed-python.duckdb")
            .is_file()
    );
}

#[test]
fn python_resource_without_schema_bootstraps_and_executes_one_invocation() {
    let project = TestProject::new();
    let marker = project.root.join("python-bootstrap-invocations");
    let interpreter = cdf_python::attached_interpreter_report()
        .unwrap()
        .executable;
    write_python_bootstrap_project(&project, &interpreter, &marker);

    let run_result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "events.raw",
    ]);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let report = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(report["result"]["row_count"], 2);
    assert_eq!(report["result"]["checkpoint"]["status"], "committed");
    assert_eq!(
        fs::read_to_string(&marker).unwrap(),
        "called\n",
        "cold discovery and extraction must continue one producer invocation"
    );
    let package = run_package_dir(&project, &run_result);
    let manifest = cdf_package::read_manifest(&package).unwrap();
    assert_eq!(
        manifest
            .identity
            .segments
            .iter()
            .map(|segment| segment.row_count)
            .sum::<u64>(),
        2,
        "the bootstrap batch must not be consumed or omitted at the schema-freeze barrier"
    );

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "events.raw",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    assert_eq!(
        fs::read_to_string(&marker).unwrap(),
        "called\n",
        "the pinned schema must make later planning metadata-only"
    );
}

#[test]
fn python_resource_errors_route_to_doctor_without_path_escape() {
    let project = TestProject::new();
    let interpreter = cdf_python::attached_interpreter_report()
        .unwrap()
        .executable;
    write_python_frontdoor_project(
        &project,
        &interpreter,
        &project.root.join("must-not-execute"),
    );
    let text = fs::read_to_string(project.root.join("cdf.toml"))
        .unwrap()
        .replace(
            "python://src/events.py#raw_events",
            "python://../events.py#raw_events",
        );
    fs::write(project.root.join("cdf.toml"), text).unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "events.raw",
    ]);
    assert_eq!(result.exit_code, 3);
    let error = stderr_or_stdout_json(&result.stderr);
    assert_eq!(error["error"]["code"], "CDF-SOURCE-REFERENCE");
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("cdf doctor")
    );
    assert!(!project.root.join("must-not-execute").exists());
}
