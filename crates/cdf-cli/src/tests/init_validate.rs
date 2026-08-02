use super::*;

#[test]
fn init_default_directory_creates_scaffold_and_validate_passes() {
    let temp = TempDir::new("cdf-cli-init");
    let target = temp.path().join("fresh-project");
    let target_string = target.to_str().unwrap().to_owned();

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "init".to_owned(),
        target_string.clone(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["ok"], true);
    assert_eq!(json["command"], "init");
    assert_eq!(json["result"]["project_name"], "fresh-project");
    assert_eq!(
        json["result"]["created"],
        json!([
            "cdf.toml",
            "README.md",
            "resources",
            "resources/files.toml",
            "data"
        ])
    );
    assert_eq!(json["result"]["replaced"], json!([]));
    assert_eq!(json["result"]["skipped"], json!([]));
    assert!(target.join("cdf.toml").is_file());
    assert!(target.join("README.md").is_file());
    assert!(target.join("resources/files.toml").is_file());
    assert!(target.join("data").is_dir());
    assert!(fs::read_dir(target.join("data")).unwrap().next().is_none());
    assert!(!target.join(".cdf").exists());
    assert!(!target.join("cdf.lock").exists());
    assert!(!target.join(".cdf/packages").exists());
    assert!(!target.join(".cdf/state.db").exists());
    assert!(!target.join(".cdf/dev.duckdb").exists());

    let project_text = fs::read_to_string(target.join("cdf.toml")).unwrap();
    let readme_text = fs::read_to_string(target.join("README.md")).unwrap();
    let resource_text = fs::read_to_string(target.join("resources/files.toml")).unwrap();
    assert!(project_text.contains("default_environment = \"dev\""));
    assert!(project_text.contains("[resources.\"local.*\"]"));
    assert!(readme_text.contains("docs/quickstart.md"));
    assert!(readme_text.contains("cdf validate"));
    assert!(readme_text.contains("cdf plan local.events"));
    assert!(readme_text.contains("cdf run local.events"));
    assert!(resource_text.contains("[resource.events]"));
    assert!(!project_text.contains("secret://"));
    assert!(!readme_text.contains("secret://"));
    assert!(!readme_text.contains(&target_string));
    assert!(!readme_text.contains(".cdf/"));
    assert!(!resource_text.contains("secret://"));

    let validate = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "--project".to_owned(),
        target_string,
        "validate".to_owned(),
    ]);
    assert_eq!(validate.exit_code, 0, "stderr: {}", validate.stderr);
    let validate_json = stderr_or_stdout_json(&validate.stdout);
    assert_eq!(validate_json["result"]["declarative_resources"], 1);
}

#[test]
fn init_name_sets_project_name_and_json_fields() {
    let temp = TempDir::new("cdf-cli-init-name");
    let target = temp.path().join("named-project");
    let target_string = target.to_str().unwrap().to_owned();

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "init".to_owned(),
        target_string.clone(),
        "--name".to_owned(),
        "warehouse-core".to_owned(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["root"], target_string);
    assert_eq!(json["result"]["project_name"], "warehouse-core");
    assert_eq!(json["result"]["force"], false);
    assert_eq!(
        fs::read_to_string(target.join("cdf.toml")).unwrap(),
        concat!(
            "[project]\n",
            "name = \"warehouse-core\"\n",
            "default_environment = \"dev\"\n",
            "normalizer = \"namecase-v1\"\n",
            "\n",
            "[environments.dev]\n",
            "state = \"sqlite://.cdf/state.db\"\n",
            "packages = \".cdf/packages\"\n",
            "destination = \"duckdb://.cdf/dev.duckdb\"\n",
            "\n",
            "[resources.\"local.*\"]\n",
            "source = \"resources/files.toml\"\n",
        )
    );
}

#[test]
fn init_refuses_existing_scaffold_paths_without_force_and_preserves_contents() {
    let temp = TempDir::new("cdf-cli-init-refuse");
    let root = temp.path();
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(root.join("cdf.toml"), "keep project").unwrap();
    fs::write(root.join("README.md"), "keep readme").unwrap();
    fs::write(root.join("resources/files.toml"), "keep resource").unwrap();
    fs::write(root.join("data/events.ndjson"), "keep data").unwrap();

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "init".to_owned(),
        root.to_str().unwrap().to_owned(),
    ]);

    assert_ne!(result.exit_code, 0);
    let json = assert_json_error_code(&result, "CDF-PROJECT-CONTRACT");
    assert_eq!(json["error"]["kind"], "contract");
    let message = json["error"]["message"].as_str().unwrap();
    assert!(message.contains("cdf.toml"));
    assert!(message.contains("README.md"));
    assert!(message.contains("resources/files.toml"));
    assert!(message.contains("data"));
    assert_eq!(
        fs::read_to_string(root.join("cdf.toml")).unwrap(),
        "keep project"
    );
    assert_eq!(
        fs::read_to_string(root.join("README.md")).unwrap(),
        "keep readme"
    );
    assert_eq!(
        fs::read_to_string(root.join("resources/files.toml")).unwrap(),
        "keep resource"
    );
    assert_eq!(
        fs::read_to_string(root.join("data/events.ndjson")).unwrap(),
        "keep data"
    );
}

#[test]
fn init_force_replaces_scaffold_files_and_preserves_unrelated_runtime_paths() {
    let temp = TempDir::new("cdf-cli-init-force");
    let root = temp.path();
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::create_dir_all(root.join(".cdf/packages")).unwrap();
    fs::write(root.join("cdf.toml"), "old project").unwrap();
    fs::write(root.join("resources/files.toml"), "old resource").unwrap();
    fs::write(root.join("data/existing.ndjson"), "keep input").unwrap();
    fs::write(root.join("README.md"), "keep unrelated").unwrap();
    fs::write(root.join(".cdf/state.db"), "keep state").unwrap();
    fs::write(root.join("cdf.lock"), "keep lock").unwrap();

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "init".to_owned(),
        root.to_str().unwrap().to_owned(),
        "--name".to_owned(),
        "forced-project".to_owned(),
        "--force".to_owned(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        json["result"]["replaced"],
        json!(["cdf.toml", "README.md", "resources/files.toml"])
    );
    assert_eq!(json["result"]["created"], json!([]));
    assert_eq!(json["result"]["skipped"], json!(["resources", "data"]));
    assert_eq!(json["result"]["force"], true);
    assert!(
        fs::read_to_string(root.join("cdf.toml"))
            .unwrap()
            .contains("name = \"forced-project\"")
    );
    assert!(
        fs::read_to_string(root.join("resources/files.toml"))
            .unwrap()
            .contains("[resource.events]")
    );
    assert_eq!(
        fs::read_to_string(root.join("data/existing.ndjson")).unwrap(),
        "keep input"
    );
    let readme_text = fs::read_to_string(root.join("README.md")).unwrap();
    assert!(readme_text.contains("docs/quickstart.md"));
    assert!(readme_text.contains("cdf validate"));
    assert!(readme_text.contains("cdf plan local.events"));
    assert!(readme_text.contains("cdf run local.events"));
    assert!(!readme_text.contains("secret://"));
    assert!(!readme_text.contains(root.to_str().unwrap()));
    assert!(!readme_text.contains(".cdf/"));
    assert_eq!(
        fs::read_to_string(root.join(".cdf/state.db")).unwrap(),
        "keep state"
    );
    assert_eq!(
        fs::read_to_string(root.join("cdf.lock")).unwrap(),
        "keep lock"
    );
    assert!(!root.join(".cdf/dev.duckdb").exists());
}

#[test]
fn validate_json_reports_project_shape() {
    let project = TestProject::new();
    let result = run(["cdf", "--json", "--project", project.root_str(), "validate"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["ok"], true);
    assert_eq!(json["command"], "validate");
    assert_eq!(json["result"]["environment"]["name"], "dev");
    assert_eq!(json["result"]["declarative_resources"], 1);
}

#[test]
fn validate_deep_reports_source_front_end_checks_without_writes() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    remove_resource_format(&project, "parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "validate");
    assert_eq!(json["result"]["mode"], "deep");
    assert_eq!(json["result"]["summary"]["resources"], 1);
    assert_eq!(json["result"]["summary"]["failed"], 0);
    assert_eq!(json["result"]["summary"]["partitions"], 1);
    assert_eq!(json["result"]["summary"]["discovery_probes"], 1);
    assert_eq!(json["result"]["writes"]["package"], false);
    assert_eq!(json["result"]["writes"]["destination"], false);
    assert_eq!(json["result"]["writes"]["checkpoint"], false);
    assert_eq!(json["result"]["writes"]["schema_snapshot"], false);
    assert_eq!(json["result"]["writes"]["lockfile"], false);

    let resource = &json["result"]["resources"][0];
    assert_eq!(resource["resource_id"], "local.events");
    assert_eq!(resource["source_file"], "resources/files.toml");
    assert_eq!(resource["mapping_pattern"], "local.*");
    assert_eq!(resource["mapping_status"], "matched");
    assert_eq!(resource["schema_source"], "discovered");
    assert_eq!(resource["partitions"]["count"], 1);
    assert_eq!(resource["partitions"]["files"][0], "vendors.parquet");
    assert_eq!(resource["discovery"]["status"], "ok");
    assert!(
        resource["discovery"]["schema_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        resource["discovery"]["snapshot_path"]
            .as_str()
            .unwrap()
            .starts_with(".cdf/schemas/local.events@sha256:")
    );
    assert_eq!(resource["validation_program"]["status"], "ok");
    assert_eq!(resource["identifier_normalization"]["status"], "ok");
    assert_eq!(resource["execution_extent"], "bounded");
    assert_eq!(resource["stream_policy"]["status"], "ok");
    assert!(
        resource["stream_policy"]["detail"]
            .as_str()
            .unwrap()
            .contains("sha256:")
    );
    assert_eq!(resource["destination"]["status"], "ok");
}

#[test]
fn validate_deep_rejects_stale_pinned_source_authority_without_runtime_probe() {
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

    fs::create_dir_all(project.root.join("other-data")).unwrap();
    write_vendor_parquet(&project.root.join("other-data/vendors.parquet"));
    let resource_path = project.root.join("resources/files.toml");
    let resource_text = fs::read_to_string(&resource_path).unwrap();
    fs::write(
        &resource_path,
        resource_text.replace("root = \"data\"", "root = \"other-data\""),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);

    assert_eq!(result.exit_code, 3, "stdout: {}", result.stdout);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let json = stderr_or_stdout_json(&result.stdout);
    let resource = &json["result"]["resources"][0];
    assert_eq!(resource["status"], "failed");
    let diagnostics = resource["diagnostics"].as_array().unwrap();
    let authority = diagnostics
        .iter()
        .find(|diagnostic| diagnostic["check"] == "source_schema_authority")
        .expect("deep validation must report stale pinned source authority");
    assert!(
        authority["message"]
            .as_str()
            .unwrap()
            .contains("does not match compiled source authority")
    );
    assert!(
        authority["remediation"]
            .as_str()
            .unwrap()
            .contains("Repin the schema")
    );
}

#[test]
fn validate_deep_inferred_binary_mismatch_names_all_signals_without_writes() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "events.parquet");
    remove_resource_format(&project, "parquet");
    write_vendor_arrow_ipc(&project, "events.parquet");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);

    assert_ne!(result.exit_code, 0);
    let json = stderr_or_stdout_json(&result.stdout);
    let diagnostics = json["result"]["resources"][0]["diagnostics"]
        .as_array()
        .unwrap();
    let message = diagnostics
        .iter()
        .filter_map(|diagnostic| diagnostic["message"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(message.contains("file format confirmation failed for resource `local.events`"));
    assert!(message.contains("file `events.parquet`"));
    assert!(message.contains("declared format `<omitted>`"));
    assert!(message.contains("inferred format `parquet`"));
    assert!(message.contains("extension signal `parquet`"));
    assert!(message.contains("magic bytes signal `arrow_ipc`"));
    assert!(message.contains("format = \"parquet\""));
    assert_no_schema_discovery_writes(&project);
}

#[test]
fn validate_deep_names_quarantined_physical_and_constraint_types_and_honors_allowance() {
    let project = TestProject::new();
    fs::write(
        project.root.join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "vendors.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
schema = { fields = [{ name = "VendorID", type = "int8", nullable = false }] }
"#,
    )
    .unwrap();
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let denied = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);
    assert_eq!(denied.exit_code, 0, "{}", denied.stderr);
    let denied_json = stderr_or_stdout_json(&denied.stdout);
    let messages = denied_json["result"]["resources"][0]["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|diagnostic| diagnostic["message"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(messages.contains("resource `local.events`"), "{messages}");
    assert!(messages.contains("vendors.parquet"), "{messages}");
    assert!(messages.contains("VendorID"), "{messages}");
    assert!(messages.contains("Int32"), "{messages}");
    assert!(messages.contains("Int8"), "{messages}");
    assert!(messages.contains("allow_lossy_mapping"), "{messages}");
    assert_no_schema_discovery_writes(&project);

    let path = project.root.join("resources/files.toml");
    let allowed = fs::read_to_string(&path).unwrap().replace(
        "trust = \"governed\"",
        "trust = \"governed\"\ntypes = { allow_lossy_mapping = true }",
    );
    fs::write(path, allowed).unwrap();
    let allowed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);
    assert_eq!(allowed.exit_code, 0, "{}{}", allowed.stdout, allowed.stderr);
    assert_no_schema_discovery_writes(&project);
}

#[test]
fn validate_deep_reports_json_row_mismatch_as_governed_warning() {
    let project = TestProject::new();
    fs::write(
        project.root.join("data/events.ndjson"),
        b"{\"id\":1,\"updated_at\":1}\n{\"id\":\"bad\",\"updated_at\":2}\n",
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);

    assert_eq!(result.exit_code, 0, "{}{}", result.stdout, result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let diagnostics = json["result"]["resources"][0]["diagnostics"]
        .as_array()
        .unwrap();
    let mismatch = diagnostics
        .iter()
        .find(|diagnostic| diagnostic["check"] == "schema_quarantine")
        .unwrap_or_else(|| panic!("expected typed row-local warning, got {diagnostics:#?}"));
    assert_eq!(mismatch["severity"], "warning");
    assert_eq!(mismatch["code"], "CDF-DEEP-SCHEMA-QUARANTINE");
    assert!(mismatch["message"].as_str().unwrap().contains("id"));
    assert!(mismatch["message"].as_str().unwrap().contains("Utf8"));
    assert!(mismatch["message"].as_str().unwrap().contains("Int64"));
    assert_no_schema_discovery_writes(&project);
}

#[test]
fn validate_deep_rejects_malformed_json_probe_instead_of_downgrading_it() {
    let project = TestProject::new();
    fs::write(project.root.join("data/events.ndjson"), b"{not-json}\n").unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "--deep",
    ]);

    assert_eq!(result.exit_code, 3, "{}{}", result.stdout, result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let diagnostics = json["result"]["resources"][0]["diagnostics"]
        .as_array()
        .unwrap();
    let probe = diagnostics
        .iter()
        .find(|diagnostic| diagnostic["check"] == "physical_schema_probe")
        .unwrap_or_else(|| panic!("expected physical probe failure, got {diagnostics:#?}"));
    assert_eq!(probe["severity"], "error");
    assert_no_schema_discovery_writes(&project);
}

#[test]
fn tier_zero_coerce_types_applies_to_actual_file_execution() {
    let project = TestProject::new();
    let resource_path = project.root.join("resources/files.toml");
    let resource = fs::read_to_string(&resource_path).unwrap().replace(
        "trust = \"governed\"",
        "trust = \"governed\"\ntypes = { coerce_types = true }",
    );
    fs::write(resource_path, resource).unwrap();
    fs::write(
        project.root.join("data/events.ndjson"),
        b"{\"id\":\"1\",\"updated_at\":\"1783296000000000\"}\n{\"id\":\"2\",\"updated_at\":\"1783296060000000\"}\n",
    )
    .unwrap();

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 0, "{}{}", result.stdout, result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 2);
    let connection = duckdb::Connection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let ids = connection
        .prepare("SELECT id FROM events ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn resource_not_compiled_error_names_compiled_ids_origins_and_fix() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.eventz",
    ]);

    assert_eq!(result.exit_code, 3);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["code"], "CDF-RESOURCE-NOT-COMPILED");
    assert_eq!(
        json["error"]["remediation"]["summary"],
        "Use one of the compiled resource ids or repair the project resource mapping."
    );
    let message = json["error"]["message"].as_str().unwrap();
    assert!(message.contains("resource `local.eventz` is not compiled"));
    assert!(message.contains("compiled resource ids: `local.events`"));
    assert!(message.contains("resources/files.toml"));
    assert!(message.contains("mapping `local.*` matched"));
    assert!(message.contains("likely causes"));
    assert!(message.contains("<source>.<resource>"));
    assert!(!message.contains("cdf run requires"));
    assert_eq!(json["error"]["suggestions"][0], "local.events");
}
