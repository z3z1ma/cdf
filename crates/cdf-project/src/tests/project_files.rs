use super::{
    BTreeMap, DefaultSecretProvider, DurationSpec, EnvSecretProvider, FileSecretProvider,
    ProjectScaffoldOptions, ResolvedProjectDestination, RetentionRule, SecretProvider, SecretRef,
    SecretUri, SemanticCatalog, SourceDeclaration, TargetName, TypeMappingFidelity,
    compile_query_project_resources, env, fs, parse_cdf_toml,
    support::{
        BOOK_PROJECT, GITHUB_RESOURCE, compile_declarative_fixture,
        compile_declarative_fixture_with_root, destination_sheet, test_execution_services,
        test_source_registry,
    },
    validate_environment_uri_fields, validate_project, write_local_project_scaffold,
};

#[test]
fn resolved_destination_binding_configures_direct_runtime_services() {
    let temp = tempfile::tempdir().unwrap();
    let execution = test_execution_services();
    let spill = execution.spill();
    assert_eq!(spill.snapshot().current_bytes, 0);

    {
        let mut destination = ResolvedProjectDestination::new(
            Box::new(
                cdf_dest_duckdb::DuckDbDestination::new(temp.path().join("direct.duckdb")).unwrap(),
            ),
            TargetName::new("events").unwrap(),
        );
        destination
            .bind_execution_services(execution.clone())
            .unwrap();
        assert!(
            spill.snapshot().current_bytes > 0,
            "binding execution services must let direct runtimes reserve native scratch through the shared spill authority"
        );
    }

    assert_eq!(spill.snapshot().current_bytes, 0);
}

#[test]
fn book_project_shape_parses_into_typed_models() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();

    assert_eq!(config.project.name, "acme_data");
    assert_eq!(
        config.driver_options["python"]["interpreter"],
        ".venv/bin/python"
    );
    assert_eq!(config.defaults.contract.as_deref(), Some("governed"));
    assert_eq!(config.sources["github"].source_type, "rest");
    assert_eq!(
        config.sources["github"].options["base_url"],
        "https://api.github.com"
    );
    assert_eq!(
        config.environments["dev"]
            .retention
            .as_ref()
            .unwrap()
            .default,
        Some(RetentionRule::Runs(5))
    );
}

#[test]
fn environment_overlays_inherit_unspecified_settings() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let prod = config.effective_environment("prod").unwrap();

    assert_eq!(prod.state, "sqlite://.cdf/state.db");
    assert_eq!(prod.packages, ".cdf/packages");
    assert_eq!(prod.destination, "postgres://secret://env/PROD_DWH");
    assert_eq!(
        prod.retention.as_ref().unwrap().default,
        Some(RetentionRule::Duration(DurationSpec::from_millis(
            90 * 86_400_000
        )))
    );
    assert_eq!(
        prod.retention.as_ref().unwrap().financial,
        Some(RetentionRule::Duration(DurationSpec::from_millis(
            400 * 86_400_000
        )))
    );
}

#[test]
fn destination_policy_overlays_from_default_environment() {
    let project = BOOK_PROJECT
        .replace(
            "retention = { default = \"5 runs\" }\n\n",
            "retention = { default = \"5 runs\" }\n\n[environments.dev.destination_policy.clickhouse]\nmerge_mode = \"replacing_merge_tree\"\n\n",
        );
    let config = parse_cdf_toml(&project).unwrap();
    let prod = config.effective_environment("prod").unwrap();

    assert_eq!(
        cdf_runtime::DestinationPolicyProvider::value(
            &prod.destination_policy,
            "clickhouse",
            "merge_mode"
        ),
        Some("replacing_merge_tree")
    );
}

#[test]
fn removed_postgres_merge_dedup_policy_is_rejected() {
    let project = BOOK_PROJECT.replace(
        "retention = { default = \"5 runs\" }\n",
        "retention = { default = \"5 runs\" }\n\n[environments.dev.destination_policy.postgres]\nmerge_dedup = \"fail\"\n",
    );

    let error = parse_cdf_toml(&project).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert!(
        error
            .message
            .contains("unsupported destination_policy.postgres")
    );
}

#[test]
fn clickhouse_merge_mode_policy_uses_the_ratified_environment_shape() {
    let project = BOOK_PROJECT.to_owned()
        + "\n[environments.prod.destination_policy.clickhouse]\nmerge_mode = \"atomic_copy_on_write\"\n";
    let config = parse_cdf_toml(&project).unwrap();
    let prod = config.effective_environment("prod").unwrap();

    assert_eq!(
        cdf_runtime::DestinationPolicyProvider::value(
            &prod.destination_policy,
            "clickhouse",
            "merge_mode"
        ),
        Some("atomic_copy_on_write")
    );
}

#[test]
fn validation_resolves_compiled_resources_and_redacts_secret_values() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resources = compile_declarative_fixture(&test_source_registry(), GITHUB_RESOURCE).unwrap();
    let provider = DefaultSecretProvider::new(
        EnvSecretProvider::from_map([
            ("GITHUB_TOKEN", "github-token-value"),
            ("PROD_DWH", "postgres-dsn-value"),
        ]),
        FileSecretProvider::without_root(),
    );

    let report = validate_project(
        &test_source_registry(),
        &config,
        Some("prod"),
        &resources,
        &provider,
    )
    .unwrap();

    assert_eq!(report.resources, 1);
    assert_eq!(report.checked_secrets.len(), 2);
    let debug = format!("{report:?}");
    assert!(!debug.contains("github-token-value"));
    assert!(!debug.contains("postgres-dsn-value"));
    assert!(debug.contains("secret://env/GITHUB_TOKEN"));
}

#[test]
fn validation_checks_missing_secret_without_printing_values() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resources = compile_declarative_fixture(&test_source_registry(), GITHUB_RESOURCE).unwrap();
    let provider = EnvSecretProvider::from_map([("GITHUB_TOKEN", "github-token-value")]);

    let error = validate_project(
        &test_source_registry(),
        &config,
        Some("prod"),
        &resources,
        &provider,
    )
    .unwrap_err();

    assert!(error.to_string().contains("secret://env/PROD_DWH"));
    assert!(!error.to_string().contains("github-token-value"));
}

#[test]
fn plaintext_secret_values_are_rejected_where_references_are_required() {
    let bad_resource = GITHUB_RESOURCE.replace("secret://env/GITHUB_TOKEN", "plain-token-value");
    let error = compile_declarative_fixture(&test_source_registry(), &bad_resource).unwrap_err();

    assert!(error.to_string().contains("secret://"), "{error}");
    assert!(!error.to_string().contains("plain-token-value"));
}

#[test]
fn file_secret_provider_resolves_without_exposing_contents() {
    let root = env::temp_dir().join(format!("cdf-project-secret-test-{}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("api-token"), "file-secret-value\n").unwrap();
    let provider = FileSecretProvider::new(&root);
    let uri = SecretUri::new("secret://file/api-token").unwrap();

    let value = provider.resolve(&uri).unwrap();

    assert_eq!(value.as_str().unwrap(), "file-secret-value");
    assert_eq!(format!("{value:?}"), "[REDACTED]");
    assert_eq!(format!("{value}"), "[REDACTED]");
    let _ = fs::remove_file(root.join("api-token"));
    let _ = fs::remove_dir(root);
}

#[test]
fn inline_uri_credentials_are_rejected() {
    let input = BOOK_PROJECT.replace(
        "destination = \"duckdb://.cdf/dev.duckdb\"",
        "destination = \"postgres://user:password@example.com/db\"",
    );
    let config = parse_cdf_toml(&input).unwrap();

    let error = config.effective_environment("dev").and_then(|env| {
        validate_environment_uri_fields(&env)?;
        Ok(())
    });

    assert!(
        error
            .unwrap_err()
            .to_string()
            .contains("inline credentials")
    );
}

#[test]
fn secret_ref_requires_provider_and_key() {
    assert!(SecretRef::new("secret://env/TOKEN").is_ok());
    assert!(SecretRef::new("env:TOKEN").is_err());
    assert!(SecretRef::new("secret://env").is_err());
}

#[test]
fn declarative_resource_compilation_hook_uses_cdf_declarative() {
    let resources = compile_declarative_fixture(&test_source_registry(), GITHUB_RESOURCE).unwrap();

    assert_eq!(resources.len(), 1);
    assert_eq!(
        resources[0].descriptor().resource_id.as_str(),
        "github.issues"
    );
}

#[test]
fn declarative_fixture_file_roots_resolve_under_project_root() {
    let temp = tempfile::tempdir().unwrap();
    let resource = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "*.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
"#;
    let resources =
        compile_declarative_fixture_with_root(&test_source_registry(), resource, temp.path())
            .unwrap();

    assert_eq!(
        resources[0].source_plan().physical_plan["source"]["root"],
        "data"
    );
    assert_eq!(resources[0].project_root(), Some(temp.path()));
}

#[test]
fn local_project_scaffold_writes_valid_project_without_runtime_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("fresh-project");

    let report = write_local_project_scaffold(ProjectScaffoldOptions {
        root: root.clone(),
        project_name: None,
        force: false,
    })
    .unwrap();

    assert_eq!(report.project_name, "fresh-project");
    assert_eq!(
        report.created,
        vec![
            "cdf.toml",
            "README.md",
            ".gitignore",
            "cdf",
            "cdf/local",
            "cdf/local/events.cdf.sql",
            "data"
        ]
    );
    assert!(root.join("cdf.toml").is_file());
    assert!(root.join("README.md").is_file());
    assert_eq!(
        fs::read_to_string(root.join(".gitignore")).unwrap(),
        ".cdf/\n"
    );
    assert!(root.join("cdf/local/events.cdf.sql").is_file());
    assert!(root.join("data").is_dir());
    assert!(fs::read_dir(root.join("data")).unwrap().next().is_none());
    assert!(!root.join(".cdf").exists());

    let config = parse_cdf_toml(&fs::read_to_string(root.join("cdf.toml")).unwrap()).unwrap();
    let readme = fs::read_to_string(root.join("README.md")).unwrap();
    let resource = fs::read_to_string(root.join("cdf/local/events.cdf.sql")).unwrap();
    assert!(readme.contains("docs/quickstart.md"));
    assert!(readme.contains("cdf validate"));
    assert!(readme.contains("cdf compile local.events"));
    assert!(readme.contains("compilation_resources"));
    assert!(readme.contains("cdf plan local.events"));
    assert!(readme.contains("cdf run local.events"));
    assert!(!readme.contains("secret://"));
    assert!(!readme.contains(root.to_str().unwrap()));
    assert!(!resource.contains("primary_key"));
    assert!(!resource.contains("merge_key"));
    let compiled = compile_query_project_resources(
        &test_source_registry(),
        &config,
        &root,
        "dev",
        &destination_sheet("duckdb", TypeMappingFidelity::Lossless),
        &SemanticCatalog::builtins().unwrap(),
        &BTreeMap::new(),
    )
    .unwrap();
    let compiled_resources = compiled
        .iter()
        .map(|entry| entry.resource.clone())
        .collect::<Vec<_>>();
    let provider = EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>());
    let validation = validate_project(
        &test_source_registry(),
        &config,
        Some("dev"),
        &compiled_resources,
        &provider,
    )
    .unwrap();

    assert_eq!(validation.resources, 1);
    assert!(validation.checked_secrets.is_empty());
}

#[test]
fn declarative_postgres_secret_is_collected_for_validation() {
    let project = BOOK_PROJECT.replace(
        "[sources.github]\ntype = \"rest\"\nbase_url = \"https://api.github.com\"\nauth = { kind = \"bearer\", token = \"secret://env/GITHUB_TOKEN\" }",
        "[sources.warehouse]\ntype = \"postgres\"\nconnection = \"secret://env/POSTGRES_URL\"",
    );
    let postgres_resource = r#"
[source.warehouse]
kind = "postgres"
connection = "secret://env/POSTGRES_URL"

[resource.orders]
table = "public.orders"
primary_key = ["id"]
merge_key = ["id"]
write_disposition = "merge"
trust = "governed"
    "#;
    let config = parse_cdf_toml(&project).unwrap();
    let resources =
        compile_declarative_fixture(&test_source_registry(), postgres_resource).unwrap();
    let provider = EnvSecretProvider::from_map([
        ("POSTGRES_URL", "postgres-url-value"),
        ("PROD_DWH", "postgres-dsn-value"),
    ]);

    let report = validate_project(
        &test_source_registry(),
        &config,
        Some("prod"),
        &resources,
        &provider,
    )
    .unwrap();

    assert!(
        report
            .checked_secrets
            .iter()
            .any(|check| check.uri.as_str() == "secret://env/POSTGRES_URL")
    );
    assert!(!format!("{report:?}").contains("postgres-url-value"));
}

#[test]
fn unsupported_keychain_provider_is_explicit_not_guessy() {
    let provider = DefaultSecretProvider::default();
    let uri = SecretUri::new("secret://keychain/prod-token").unwrap();
    let error = provider.resolve(&uri).unwrap_err();

    assert!(error.to_string().contains("not available"));
    assert!(!error.to_string().contains("prod-token-value"));
}

#[test]
fn source_declaration_is_registry_open_and_preserves_secret_references() {
    let source = SourceDeclaration {
        kind: "external_api".to_owned(),
        options: BTreeMap::from([(
            "token".to_owned(),
            serde_json::Value::String("secret://env/TOKEN".to_owned()),
        )]),
    };

    assert_eq!(source.kind, "external_api");
    assert_eq!(source.options["token"], "secret://env/TOKEN");
}
