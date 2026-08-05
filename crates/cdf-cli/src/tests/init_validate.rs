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
            ".gitignore",
            "cdf",
            "cdf/local",
            "cdf/local/events.cdf.sql",
            "data"
        ])
    );
    assert_eq!(json["result"]["replaced"], json!([]));
    assert_eq!(json["result"]["skipped"], json!([]));
    assert!(target.join("cdf.toml").is_file());
    assert!(target.join("README.md").is_file());
    assert!(target.join("cdf/local/events.cdf.sql").is_file());
    assert!(target.join("data").is_dir());
    assert!(fs::read_dir(target.join("data")).unwrap().next().is_none());
    assert!(!target.join(".cdf").exists());
    assert!(!target.join("cdf.lock").exists());
    assert!(!target.join(".cdf/packages").exists());
    assert!(!target.join(".cdf/state.db").exists());
    assert!(!target.join(".cdf/dev.duckdb").exists());

    let project_text = fs::read_to_string(target.join("cdf.toml")).unwrap();
    let readme_text = fs::read_to_string(target.join("README.md")).unwrap();
    let resource_text = fs::read_to_string(target.join("cdf/local/events.cdf.sql")).unwrap();
    assert!(project_text.contains("default_environment = \"dev\""));
    assert!(project_text.contains("[sources.local]"));
    assert!(project_text.contains("type = \"files\""));
    assert!(readme_text.contains("docs/quickstart.md"));
    assert!(readme_text.contains("cdf validate"));
    assert!(readme_text.contains("cdf plan local.events"));
    assert!(readme_text.contains("cdf run local.events"));
    assert!(resource_text.contains("FROM upstream("));
    assert!(resource_text.contains("source => 'local'"));
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
    assert_eq!(validate_json["result"]["counts"]["selected_resources"], 1);
    assert_eq!(validate_json["result"]["counts"]["authority_missing"], 1);
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
            "[sources.local]\n",
            "type = \"files\"\n",
            "root = \"data\"\n",
        )
    );
}

#[test]
fn init_refuses_existing_scaffold_paths_without_force_and_preserves_contents() {
    let temp = TempDir::new("cdf-cli-init-refuse");
    let root = temp.path();
    fs::create_dir_all(root.join("cdf/local")).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(root.join("cdf.toml"), "keep project").unwrap();
    fs::write(root.join("README.md"), "keep readme").unwrap();
    fs::write(root.join("cdf/local/events.cdf.sql"), "keep resource").unwrap();
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
    assert!(message.contains("cdf/local/events.cdf.sql"));
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
        fs::read_to_string(root.join("cdf/local/events.cdf.sql")).unwrap(),
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
    fs::create_dir_all(root.join("cdf/local")).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::create_dir_all(root.join(".cdf/packages")).unwrap();
    fs::write(root.join("cdf.toml"), "old project").unwrap();
    fs::write(root.join("cdf/local/events.cdf.sql"), "old resource").unwrap();
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
        json!(["cdf.toml", "README.md", "cdf/local/events.cdf.sql"])
    );
    assert_eq!(json["result"]["created"], json!([".gitignore"]));
    assert_eq!(
        json["result"]["skipped"],
        json!(["cdf", "cdf/local", "data"])
    );
    assert_eq!(json["result"]["force"], true);
    assert!(
        fs::read_to_string(root.join("cdf.toml"))
            .unwrap()
            .contains("name = \"forced-project\"")
    );
    assert!(
        fs::read_to_string(root.join("cdf/local/events.cdf.sql"))
            .unwrap()
            .contains("source => 'local'")
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
    assert_eq!(json["result"]["environment"], "dev");
    assert_eq!(json["result"]["counts"]["environments"], 1);
    assert_eq!(json["result"]["counts"]["configured_sources"], 1);
    assert_eq!(json["result"]["counts"]["selected_resources"], 1);
    assert_eq!(json["result"]["counts"]["valid_resources"], 1);
    assert_eq!(json["result"]["counts"]["errors"], 0);
    assert_eq!(json["result"]["counts"]["authority_missing"], 1);
    assert_eq!(json["result"]["effects"]["writes"], "none");
    assert!(
        json["result"]["effects"]["skipped"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check.as_str().unwrap().contains("secret resolution"))
    );
}

#[test]
fn validate_selectors_exclude_unselected_invalid_sql_and_resolve_canonically() {
    let project = TestProject::new();
    fs::create_dir_all(project.root.join("cdf/warehouse")).unwrap();
    fs::write(
        project.root.join("cdf/warehouse/orders.cdf.sql"),
        RESOURCE.replace("TARGET events", "TARGET orders"),
    )
    .unwrap();
    fs::create_dir_all(project.root.join("cdf/broken")).unwrap();
    fs::write(
        project.root.join("cdf/broken/query.cdf.sql"),
        "definitely not SQL",
    )
    .unwrap();
    fs::write(
        project.root.join("cdf/broken/second.cdf.sql"),
        "also not SQL",
    )
    .unwrap();

    let selected = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "warehouse.orders",
        "local.*",
        "--exclude",
        "local.missing",
    ]);
    assert_eq!(selected.exit_code, 0, "stderr: {}", selected.stderr);
    let json = stderr_or_stdout_json(&selected.stdout);
    assert_eq!(
        json["result"]["selection"]["resolved"],
        json!(["local.events", "warehouse.orders"])
    );
    assert_eq!(json["result"]["counts"]["selected_resources"], 2);
    assert_eq!(json["result"]["counts"]["errors"], 0);

    let all = run(["cdf", "--json", "--project", project.root_str(), "validate"]);
    assert_eq!(all.exit_code, 1, "stderr: {}", all.stderr);
    let json = stderr_or_stdout_json(&all.stdout);
    assert_eq!(json["result"]["counts"]["selected_resources"], 4);
    assert_eq!(json["result"]["counts"]["valid_resources"], 2);
    assert_eq!(json["result"]["counts"]["errors"], 2);
    assert_eq!(
        json["result"]["resources"][0]["resource_id"],
        "broken.query"
    );
    assert_eq!(
        json["result"]["resources"][0]["diagnostics"][0]["code"],
        "CDF-VALIDATE-RESOURCE"
    );
}

#[test]
fn validate_is_static_when_data_and_secret_values_are_unavailable() {
    let project = TestProject::new();
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    let project_path = project.root.join("cdf.toml");
    let project_text = fs::read_to_string(&project_path).unwrap();
    fs::write(
        &project_path,
        format!(
            "{}\n[sources.unselected]\ntype = \"files\"\nroot = \"elsewhere\"\ncredentials = \"plaintext-must-not-block-selected-validation\"\n",
            project_text.replace(
                "root = \"data\"",
                "root = \"missing-data\"\ncredentials = \"secret://env/CDF_VALIDATE_MUST_NOT_READ\"",
            )
        ),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["counts"]["valid_resources"], 1);
    assert_eq!(json["result"]["effects"]["writes"], "none");
    assert!(!result.stdout.contains("CDF_VALIDATE_MUST_NOT_READ"));
    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "validate",
        "local.events",
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(human.stdout.contains("validated 1 resource(s): 1 valid"));
    assert!(human.stdout.contains("writes"));
    assert!(human.stdout.contains("none"));
    assert!(human.stdout.contains("secret resolution"));
    assert!(!human.stdout.contains("CDF_VALIDATE_MUST_NOT_READ"));
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
}

#[test]
fn validate_aggregates_malformed_environment_uris_without_writes() {
    for (valid, invalid) in [
        (
            "state = \"sqlite://.cdf/state.db\"",
            "state = \"not-a-uri\"",
        ),
        (
            "destination = \"duckdb://.cdf/dev.duckdb\"",
            "destination = \"not-a-uri\"",
        ),
    ] {
        let project = TestProject::new();
        let project_path = project.root.join("cdf.toml");
        let project_text = fs::read_to_string(&project_path).unwrap();
        fs::write(&project_path, project_text.replace(valid, invalid)).unwrap();

        let result = run(["cdf", "--json", "--project", project.root_str(), "validate"]);

        assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
        let json = stderr_or_stdout_json(&result.stdout);
        assert_eq!(json["result"]["counts"]["errors"], 1);
        assert_eq!(
            json["result"]["diagnostics"][0]["code"],
            "CDF-VALIDATE-ENVIRONMENT"
        );
        assert_eq!(json["result"]["counts"]["valid_resources"], 1);
        assert_eq!(json["result"]["effects"]["writes"], "none");
        assert!(!project.root.join("cdf.lock").exists());
        assert!(!project.root.join(".cdf/schemas").exists());
        assert!(!project.root.join(".cdf/packages").exists());
        assert!(!project.root.join(".cdf/state.db").exists());
        assert!(!project.root.join(".cdf/dev.duckdb").exists());
    }
}

#[test]
fn validate_reports_current_then_stale_local_authority_without_repairing_it() {
    let project = TestProject::new();
    let compile = run([
        "cdf",
        "--project",
        project.root_str(),
        "compile",
        "--refresh",
    ]);
    assert_eq!(compile.exit_code, 0, "stderr: {}", compile.stderr);
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();

    let current = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "local.events",
    ]);
    assert_eq!(current.exit_code, 0, "stderr: {}", current.stderr);
    let current_json = stderr_or_stdout_json(&current.stdout);
    assert_eq!(current_json["result"]["counts"]["authority_current"], 1);
    assert_eq!(current_json["result"]["counts"]["warnings"], 0);

    let resource_path = project.root.join("cdf/local/events.cdf.sql");
    let resource = fs::read_to_string(&resource_path).unwrap();
    fs::write(&resource_path, format!("{resource}\n")).unwrap();
    let stale = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "local.events",
    ]);
    assert_eq!(stale.exit_code, 0, "stderr: {}", stale.stderr);
    let stale_json = stderr_or_stdout_json(&stale.stdout);
    assert_eq!(stale_json["result"]["counts"]["authority_stale"], 1);
    assert_eq!(stale_json["result"]["counts"]["warnings"], 1);
    assert_eq!(
        stale_json["result"]["resources"][0]["diagnostics"][0]["code"],
        "CDF-VALIDATE-AUTHORITY-STALE"
    );
    assert!(!project.root.join("data/events.ndjson").exists());
}

#[test]
fn validate_selector_misses_and_empty_exclusions_are_usage_errors() {
    let project = TestProject::new();
    let exact = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "local.eventz",
    ]);
    let exact_json = assert_json_error_code(&exact, "CDF-CLI-USAGE");
    assert_eq!(
        exact_json["error"]["suggestions"][0],
        "cdf validate local.events"
    );

    let glob = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "missing.*",
    ]);
    let glob_json = assert_json_error_code(&glob, "CDF-CLI-USAGE");
    assert!(
        glob_json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("missing.*")
    );

    let empty = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "validate",
        "local.*",
        "--exclude",
        "local.*",
    ]);
    let empty_json = assert_json_error_code(&empty, "CDF-CLI-USAGE");
    assert!(
        empty_json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("empty set")
    );
}

#[test]
fn validate_reports_corrupt_local_authority_without_contact_or_repair() {
    let project = TestProject::new();
    fs::write(project.root.join("cdf.lock"), "not = [valid").unwrap();

    let result = run(["cdf", "--json", "--project", project.root_str(), "validate"]);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["counts"]["errors"], 1);
    assert_eq!(
        json["result"]["diagnostics"][0]["code"],
        "CDF-VALIDATE-LOCK"
    );
    assert_eq!(json["result"]["effects"]["writes"], "none");
    assert_eq!(
        fs::read_to_string(project.root.join("cdf.lock")).unwrap(),
        "not = [valid"
    );
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
        "Use a compiled resource id or author the expected project SQL resource."
    );
    let message = json["error"]["message"].as_str().unwrap();
    assert!(message.contains("resource `local.eventz` is not compiled"));
    assert!(message.contains("compiled query-first resources"));
    assert!(message.contains("`local.events`"));
    assert!(message.contains("cdf/local/events.cdf.sql"));
    assert!(message.contains("using configured source `local`"));
    assert!(message.contains("cdf/<namespace>/<resource>.cdf.sql"));
    assert!(!message.contains("cdf run requires"));
    assert_eq!(json["error"]["suggestions"][0], "local.events");
}
