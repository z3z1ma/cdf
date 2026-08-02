use super::*;

#[test]
fn package_verify_uses_lower_package_reader() {
    let temp = TempDir::new("cdf-cli-package");
    let package_dir = temp.path().join("pkg");
    let builder = package_builder!(&package_dir, "pkg-1").unwrap();
    builder.finish_with_status(PackageStatus::Packaged).unwrap();

    let result = run([
        "cdf",
        "--json",
        "package",
        "verify",
        package_dir.to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "package verify");
    assert!(
        json["result"]["package_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(json["result"]["checked_file_count"], 1);
    assert_eq!(json["result"]["checked_archive_count"], 0);
}

#[test]
fn inspect_package_typed_report_preserves_manifest_json_shape() {
    let temp = TempDir::new("cdf-cli-inspect-package");
    let package_dir = temp.path().join("pkg");
    let manifest = package_builder!(&package_dir, "pkg-inspect")
        .unwrap()
        .finish_with_status(PackageStatus::Packaged)
        .unwrap();

    let result = run([
        "cdf",
        "--json",
        "inspect",
        "package",
        package_dir.to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "inspect package");
    assert_eq!(json["result"]["package_hash"], manifest.package_hash);
    assert_eq!(
        json["result"]["identity"]["package_id"],
        manifest.identity.package_id
    );
    assert!(json["result"].get("manifest").is_none());
    assert!(json["result"].get("path").is_none());
}

#[test]
fn package_ls_json_remains_array_while_human_uses_renderer() {
    let temp = TempDir::new("cdf-cli-package-ls");
    let package_dir = build_archive_cli_package(temp.path(), "pkg-ls-json-array");

    let json_result = run([
        "cdf",
        "--json",
        "package",
        "ls",
        temp.path().to_str().unwrap(),
    ]);

    assert_eq!(json_result.exit_code, 0, "stderr: {}", json_result.stderr);
    let json = stderr_or_stdout_json(&json_result.stdout);
    assert_eq!(json["command"], "package ls");
    assert!(json["result"].as_array().is_some());
    assert_eq!(json["result"][0]["path"], package_dir.display().to_string());

    let human = run(["cdf", "package", "ls", temp.path().to_str().unwrap()]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(human.stdout.contains("OK 1 package(s)"));
    assert!(human.stdout.contains("Packages"));
    assert!(human.stdout.contains("path"), "{}", human.stdout);
    assert!(human.stdout.contains("Next: cdf package verify <package>"));
}

#[cfg(unix)]
#[test]
fn package_ls_rejects_symlink_entries_instead_of_following_or_skipping_them() {
    use std::os::unix::fs::symlink;

    let package_root = TempDir::new("cdf-cli-package-ls-symlink");
    let outside = TempDir::new("cdf-cli-package-ls-outside");
    build_archive_cli_package(outside.path(), "outside-package");
    symlink(
        outside.path().join("outside-package"),
        package_root.path().join("linked-package"),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "package",
        "ls",
        package_root.path().to_str().unwrap(),
    ]);

    assert_ne!(result.exit_code, 0);
    assert!(result.stderr.contains("symlink"), "{}", result.stderr);
    assert!(!result.stdout.contains("outside-package"));
}

#[test]
fn package_gc_plans_retention_from_packages_and_checkpoint_history() {
    let project = TestProject::new();
    let package_root = project.root.join(".cdf/packages");
    fs::create_dir_all(&package_root).unwrap();

    let protected_dir = build_archive_cli_package(&package_root, "pkg-gc-protected");
    let protected_manifest = cdf_package::read_manifest(&protected_dir).unwrap();
    commit_status_head(
        &project,
        "pipeline-gc",
        "checkpoint-gc-protected",
        &protected_manifest.package_hash,
        "receipt-gc-protected",
        1_783_296_000_000,
    );
    commit_status_head(
        &project,
        "pipeline-gc-missing",
        "checkpoint-gc-missing",
        "sha256:missing-gc-package",
        "receipt-gc-missing",
        1_783_296_000_001,
    );

    let collectible_dir = package_root.join("pkg-gc-collectible");
    let collectible_builder = package_builder!(&collectible_dir, "pkg-gc-collectible").unwrap();
    let collectible_manifest = collectible_builder
        .finish_with_status(PackageStatus::Validated)
        .unwrap();

    let retained_dir = build_archive_cli_package(&package_root, "pkg-gc-retained");
    let retained_manifest = cdf_package::read_manifest(&retained_dir).unwrap();

    let corrupt_dir = build_archive_cli_package(&package_root, "pkg-gc-corrupt");
    let corrupt_manifest = cdf_package::read_manifest(&corrupt_dir).unwrap();
    fs::write(corrupt_dir.join("data/seg-000001.arrow"), "tampered").unwrap();

    let tombstone_dir = build_archive_cli_package(&package_root, "pkg-gc-tombstone");
    let tombstone_manifest = cdf_package::read_manifest(&tombstone_dir).unwrap();
    cdf_package::tombstone_package(&tombstone_dir).unwrap();

    fs::create_dir_all(package_root.join("pkg-gc-partial")).unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "package",
        "gc",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "package gc");
    assert_eq!(json["result"]["command"], "package gc");
    assert_eq!(json["result"]["mode"], "dry_run");
    assert_eq!(json["result"]["counts"]["protected"], 2);
    assert_eq!(json["result"]["counts"]["collectible"], 1);
    assert_eq!(json["result"]["counts"]["retained"], 1);
    assert_eq!(json["result"]["counts"]["corrupt"], 2);
    assert_eq!(json["result"]["counts"]["missing"], 1);

    assert_gc_artifact(
        &json,
        Some(&protected_manifest.package_hash),
        "protected",
        "committed_checkpoint",
        "retain",
    );
    assert_gc_artifact(
        &json,
        Some(&collectible_manifest.package_hash),
        "collectible",
        "pre_packaged_artifact",
        "would_collect",
    );
    assert_gc_artifact(
        &json,
        Some(&retained_manifest.package_hash),
        "retained",
        "replay_or_recovery_artifact",
        "retain",
    );
    assert_gc_artifact(
        &json,
        Some(&corrupt_manifest.package_hash),
        "corrupt",
        "verification_failed",
        "retain",
    );
    assert_gc_artifact(
        &json,
        Some(&tombstone_manifest.package_hash),
        "protected",
        "retention_tombstone",
        "retain",
    );
    assert_gc_artifact(
        &json,
        Some("sha256:missing-gc-package"),
        "missing",
        "committed_checkpoint_missing_artifact",
        "restore_required",
    );
    assert_gc_artifact(&json, None, "corrupt", "manifest_missing", "retain");
}

#[test]
fn package_gc_reports_last_locally_promotable_residual_bytes() {
    let project = TestProject::new();
    let package_root = project.root.join(".cdf/packages");
    fs::create_dir_all(&package_root).unwrap();
    let (package_dir, residual_bytes) =
        build_gc_residual_package(&package_root, "pkg-gc-residual", "local.events");
    let package_hash = cdf_package::read_manifest(&package_dir)
        .unwrap()
        .package_hash;

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "package",
        "gc",
    ]);
    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let availability = json["result"]["promotion_availability"].as_array().unwrap();
    assert_eq!(availability.len(), 1);
    assert_eq!(availability[0]["resource_id"], "local.events");
    assert_eq!(availability[0]["package_hash"], package_hash);
    assert_eq!(availability[0]["contains_local_residual_bytes"], true);
    assert_eq!(availability[0]["locally_promotable"], true);
    assert_eq!(availability[0]["local_residual_bytes"], residual_bytes);
    assert_eq!(availability[0]["promotable_residual_bytes"], residual_bytes);
    assert_eq!(
        availability[0]["last_locally_promotable_for_resource"],
        true
    );
    assert_eq!(
        availability[0]["collection_removes_last_local_promotable_copy"],
        false
    );
    assert_eq!(availability[0]["planned_action"], "retain");
    assert_eq!(availability[0]["authority"], "retained_package");

    let human = run(["cdf", "--project", project.root_str(), "package", "gc"]);
    assert_eq!(human.exit_code, 0, "{}", human.stderr);
    assert!(human.stdout.contains("local bytes"), "{}", human.stdout);
    assert!(human.stdout.contains("retain"), "{}", human.stdout);
    assert!(human.stdout.contains("Promotion availability"));
    assert!(human.stdout.contains("destination readback inferred"));
    assert!(
        human
            .stdout
            .contains("retain or restore one verified receipted package")
    );
}

#[test]
fn package_gc_explicit_directory_is_dry_run_without_deleting_collectible_artifacts() {
    let temp = TempDir::new("cdf-cli-package-gc-dry-run");
    let package_dir = temp.path().join("pkg-validated");
    let builder = package_builder!(&package_dir, "pkg-validated").unwrap();
    let manifest = builder
        .finish_with_status(PackageStatus::Validated)
        .unwrap();

    let result = run([
        "cdf",
        "--json",
        "package",
        "gc",
        temp.path().to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(package_dir.join("manifest.json").is_file());
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["counts"]["collectible"], 1);
    assert_gc_artifact(
        &json,
        Some(&manifest.package_hash),
        "collectible",
        "pre_packaged_artifact",
        "would_collect",
    );
}

#[test]
fn package_archive_writes_parquet_archive_and_reports_json() {
    let temp = TempDir::new("cdf-cli-package-archive-json");
    let package_dir = build_archive_cli_package(temp.path(), "pkg-archive-cli-json");

    let result = run([
        "cdf",
        "--json",
        "package",
        "archive",
        package_dir.to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "package archive");
    assert_eq!(json["result"]["command"], "package archive");
    assert_eq!(json["result"]["format"], "parquet");
    assert_eq!(json["result"]["status"], "written");
    assert_eq!(
        json["result"]["fidelity_report_path"],
        "archive/parquet/fidelity.json"
    );
    assert_eq!(
        json["result"]["segment_index_path"],
        "archive/parquet/segments.ndjson"
    );
    assert_eq!(json["result"]["segment_count"], 1);
    assert_eq!(json["result"]["row_count"], 2);
    assert!(json["result"].get("segments").is_none());
    assert!(
        package_dir
            .join("archive/parquet/data/seg-000001.parquet")
            .is_file()
    );
    assert!(package_dir.join("archive/parquet/fidelity.json").is_file());
    assert!(
        package_dir
            .join("archive/parquet/segments.ndjson")
            .is_file()
    );
}

#[test]
fn package_archive_supports_local_json_flag_and_human_output() {
    let json_temp = TempDir::new("cdf-cli-package-archive-local-json");
    let json_package = build_archive_cli_package(json_temp.path(), "pkg-archive-cli-local-json");
    let json_result = run([
        "cdf",
        "package",
        "archive",
        json_package.to_str().unwrap(),
        "--json",
    ]);

    assert_eq!(json_result.exit_code, 0, "stderr: {}", json_result.stderr);
    let json = stderr_or_stdout_json(&json_result.stdout);
    assert_eq!(json["command"], "package archive");
    assert_eq!(json["result"]["status"], "written");

    let human_temp = TempDir::new("cdf-cli-package-archive-human");
    let human_package = build_archive_cli_package(human_temp.path(), "pkg-archive-cli-human");
    let human_result = run(["cdf", "package", "archive", human_package.to_str().unwrap()]);

    assert_eq!(human_result.exit_code, 0, "stderr: {}", human_result.stderr);
    assert!(human_result.stdout.contains("OK archived package sha256:"));
    assert!(human_result.stdout.contains("Archive"));
    assert!(human_result.stdout.contains("status     written"));
    assert!(human_result.stdout.contains("segments   1"));
    assert!(
        human_result
            .stdout
            .contains("archive/parquet/fidelity.json")
    );
    assert!(
        human_result
            .stdout
            .contains("Next: cdf package verify <package>")
    );
}

#[test]
fn package_archive_rejects_unsupported_format_before_writes() {
    let temp = TempDir::new("cdf-cli-package-archive-format");
    let package_dir = build_archive_cli_package(temp.path(), "pkg-archive-cli-format");

    let result = run([
        "cdf",
        "--json",
        "package",
        "archive",
        package_dir.to_str().unwrap(),
        "--format",
        "orc",
    ]);

    assert_eq!(result.exit_code, 2);
    assert!(!package_dir.join("archive").exists());
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unsupported package archive format `orc`")
    );
}
