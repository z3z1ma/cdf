use super::*;

#[test]
fn contract_show_remains_project_free() {
    let result = run(["cdf", "--json", "contract", "show", "governed"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "contract show");
    assert_eq!(json["result"]["policy"], "governed");
    assert_eq!(
        json["result"]["contract"]["schema"]["review_artifact_required"],
        true
    );
}

#[test]
fn contract_freeze_writes_lock_and_contract_test_passes() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "freeze",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(project.root.join("cdf.lock").is_file());
    assert!(
        !project.root.join(".cdf/dev.duckdb").exists(),
        "contract freeze must not create destination data"
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource_ids"], json!(["local.events"]));
    assert_eq!(json["result"]["counts"]["frozen"], 1);
    let snapshot = &json["result"]["snapshots"]["local.events"];
    assert!(
        snapshot["schema_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        snapshot["policy_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        snapshot["validation_program_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );

    let test = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "test",
    ]);

    assert_eq!(test.exit_code, 0, "stderr: {}", test.stderr);
    let json = stderr_or_stdout_json(&test.stdout);
    assert_eq!(json["result"]["counts"]["passed"], 1);
    assert_eq!(json["result"]["counts"]["drifted"], 0);
    assert_eq!(json["result"]["drift_details"], json!([]));

    let diff = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "diff",
        "schema",
    ]);

    assert_eq!(diff.exit_code, 0, "stderr: {}", diff.stderr);
    let json = stderr_or_stdout_json(&diff.stdout);
    assert_eq!(json["result"]["diffs"], json!([]));
}

#[test]
fn read_only_load_fails_closed_and_real_add_completes_pending_publication() {
    let project = TestProject::new();
    let initial_freeze = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "freeze",
    ]);
    assert_eq!(
        initial_freeze.exit_code, 0,
        "stderr: {}",
        initial_freeze.stderr
    );
    let old_project = fs::read(project.root.join("cdf.toml")).unwrap();
    let old_lock = fs::read(project.root.join("cdf.lock")).unwrap();
    let extra_resource = RESOURCE.replace("[source.local]", "[source.extra]");
    let new_project = format!(
        "{}\n[resources.\"extra.*\"]\nsource = \"resources/extra.toml\"\n",
        String::from_utf8(old_project.clone()).unwrap()
    )
    .into_bytes();
    fs::write(project.root.join("resources/extra.toml"), &extra_resource).unwrap();
    fs::write(project.root.join("cdf.toml"), &new_project).unwrap();
    let new_freeze = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "freeze",
    ]);
    assert_eq!(new_freeze.exit_code, 0, "stderr: {}", new_freeze.stderr);
    let new_lock = fs::read(project.root.join("cdf.lock")).unwrap();
    assert_ne!(new_lock, old_lock);

    fs::write(project.root.join("cdf.toml"), &old_project).unwrap();
    fs::write(project.root.join("cdf.lock"), &old_lock).unwrap();
    fs::remove_file(project.root.join("resources/extra.toml")).unwrap();

    let lock_temporary = ".cdf.lock.999.3.project-txn.tmp";
    fs::write(project.root.join(lock_temporary), &new_lock).unwrap();
    fs::write(project.root.join("resources/extra.toml"), &extra_resource).unwrap();
    fs::write(project.root.join("cdf.toml"), &new_project).unwrap();
    let marker = json!({
        "version": 1,
        "generation": 1,
        "state": "pending",
        "commit_relative_path": "cdf.lock",
        "entries": [
            {
                "relative_path": "resources/extra.toml",
                "temporary_relative_path": "resources/.extra.toml.999.1.project-txn.tmp",
                "prior": { "kind": "absent" },
                "new_len": extra_resource.len(),
                "new_sha256": format!("sha256:{:x}", Sha256::digest(extra_resource.as_bytes())),
            },
            {
                "relative_path": "cdf.toml",
                "temporary_relative_path": ".cdf.toml.999.2.project-txn.tmp",
                "prior": {
                    "kind": "existing",
                    "len": old_project.len(),
                    "sha256": format!("sha256:{:x}", Sha256::digest(&old_project)),
                },
                "new_len": new_project.len(),
                "new_sha256": format!("sha256:{:x}", Sha256::digest(&new_project)),
            },
            {
                "relative_path": "cdf.lock",
                "temporary_relative_path": lock_temporary,
                "prior": {
                    "kind": "existing",
                    "len": old_lock.len(),
                    "sha256": format!("sha256:{:x}", Sha256::digest(&old_lock)),
                },
                "new_len": new_lock.len(),
                "new_sha256": format!("sha256:{:x}", Sha256::digest(&new_lock)),
            },
        ],
    });
    fs::write(
        project.root.join(".cdf/project-files.transaction.json"),
        serde_json::to_vec_pretty(&marker).unwrap(),
    )
    .unwrap();
    let pending_tree = project_tree_snapshot(&project.root);

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "extra.events",
    ]);

    assert_ne!(plan.exit_code, 0);
    assert!(
        format!("{}{}", plan.stdout, plan.stderr).contains("project publication is incomplete")
    );
    assert_project_tree_unchanged(&project.root, &pending_tree);

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "extra.events",
    ]);

    assert_ne!(preview.exit_code, 0);
    assert!(
        format!("{}{}", preview.stdout, preview.stderr)
            .contains("project publication is incomplete")
    );
    assert_project_tree_unchanged(&project.root, &pending_tree);

    write_vendor_parquet(&project.root.join("data/yellow.parquet"));
    let add = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "tlc.yellow",
        project.root.join("data/yellow.parquet").to_str().unwrap(),
    ]);

    assert_eq!(add.exit_code, 0, "stderr: {}", add.stderr);
    let recovered_plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "extra.events",
    ]);
    assert_eq!(
        recovered_plan.exit_code, 0,
        "stderr: {}",
        recovered_plan.stderr
    );
    let committed: Value = serde_json::from_slice(
        &fs::read(project.root.join(".cdf/project-files.transaction.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(committed["state"], "committed");
    assert!(!project.root.join(lock_temporary).exists());
}

#[test]
fn contract_test_fails_closed_when_lock_is_missing() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "test",
    ]);

    assert_eq!(result.exit_code, 3);
    let json = assert_json_error_code(&result, "CDF-CONTRACT-LOCKFILE");
    let message = json["error"]["message"].as_str().unwrap();
    assert!(message.contains("cdf.lock"));
    assert!(message.contains("cdf contract freeze"));
}

#[test]
fn contract_test_reports_schema_and_program_drift() {
    let project = TestProject::new();
    let freeze = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "freeze",
        "local.events",
    ]);
    assert_eq!(freeze.exit_code, 0, "stderr: {}", freeze.stderr);
    write_resource_with_extra_contract_field(&project);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "test",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource_ids"], json!(["local.events"]));
    assert_eq!(json["result"]["counts"]["passed"], 0);
    assert_eq!(json["result"]["counts"]["drifted"], 1);
    let fields = json["result"]["drift_details"]
        .as_array()
        .unwrap()
        .iter()
        .map(|detail| detail["field"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(fields.contains(&"schema_hash"));
    assert!(fields.contains(&"validation_program_hash"));
}

#[test]
fn contract_test_fails_closed_when_selected_snapshot_is_missing() {
    let project = TestProject::new();
    write_minimal_lockfile(&project);
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "contract",
        "test",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 3);
    let json = assert_json_error_code(&result, "CDF-PROJECT-CONTRACT");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("no frozen contract snapshot")
    );
}
