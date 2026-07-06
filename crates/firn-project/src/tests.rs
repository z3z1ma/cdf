use super::*;
use crate::internal::*;
use firn_declarative::{AuthDeclaration, SourceDeclaration};
use firn_kernel::{
    CapabilitySupport, ConcurrencyLimit, DestinationId, DestinationSheet, IdempotencySupport,
    IdentifierRules, TransactionSupport, TypeMapping, TypeMappingFidelity, WriteDisposition,
};

const BOOK_PROJECT: &str = r#"
[project]
name = "acme_data"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.firn/state.db"
packages = ".firn/packages"
destination = "duckdb://.firn/dev.duckdb"
retention = { default = "5 runs" }

[environments.prod]
destination = "postgres://secret://env/PROD_DWH"
retention = { default = "90d", financial = "400d" }

[python]
interpreter = ".venv/bin/python"

[defaults]
contract = "governed"

[resources."github.*"]
source = "resources/github.toml"

[resources."events.raw"]
source = "python://src/events.py#raw_events"
trust = "serving"
freshness = { expect_every = "15m", alert_after = "45m" }
"#;

const GITHUB_RESOURCE: &str = r#"
[source.github]
kind = "rest"
base_url = "https://api.github.com"
auth = { kind = "bearer", token = "secret://env/GITHUB_TOKEN" }

[resource.issues]
path = "/repos/{owner}/{repo}/issues"
records = "$"
primary_key = ["id"]
cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "5m" }
write_disposition = "merge"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "timestamp_micros", nullable = false, timezone = "UTC" },
] }
"#;

#[test]
fn book_project_shape_parses_into_typed_models() {
    let config = parse_firn_toml(BOOK_PROJECT).unwrap();

    assert_eq!(config.project.name, "acme_data");
    assert_eq!(
        config.python.interpreter.as_deref(),
        Some(".venv/bin/python")
    );
    assert_eq!(config.defaults.contract.as_deref(), Some("governed"));
    assert_eq!(
        config.resources["events.raw"]
            .freshness
            .as_ref()
            .unwrap()
            .alert_after
            .unwrap()
            .millis(),
        2_700_000
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
    let config = parse_firn_toml(BOOK_PROJECT).unwrap();
    let prod = config.effective_environment("prod").unwrap();

    assert_eq!(prod.state, "sqlite://.firn/state.db");
    assert_eq!(prod.packages, ".firn/packages");
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
fn validation_resolves_declarative_sources_and_redacts_secret_values() {
    let config = parse_firn_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let provider = DefaultSecretProvider::new(
        EnvSecretProvider::from_map([
            ("GITHUB_TOKEN", "github-token-value"),
            ("PROD_DWH", "postgres-dsn-value"),
        ]),
        FileSecretProvider::without_root(),
    );

    let report = validate_project(&config, Some("prod"), &resolver, &provider).unwrap();

    assert_eq!(report.declarative_resources, 1);
    assert_eq!(report.external_resources, 1);
    assert_eq!(report.checked_secrets.len(), 2);
    let debug = format!("{report:?}");
    assert!(!debug.contains("github-token-value"));
    assert!(!debug.contains("postgres-dsn-value"));
    assert!(debug.contains("secret://env/GITHUB_TOKEN"));
}

#[test]
fn validation_checks_missing_secret_without_printing_values() {
    let config = parse_firn_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let provider = EnvSecretProvider::from_map([("GITHUB_TOKEN", "github-token-value")]);

    let error = validate_project(&config, Some("prod"), &resolver, &provider).unwrap_err();

    assert!(error.to_string().contains("secret://env/PROD_DWH"));
    assert!(!error.to_string().contains("github-token-value"));
}

#[test]
fn plaintext_secret_values_are_rejected_where_references_are_required() {
    let bad_resource = GITHUB_RESOURCE.replace("secret://env/GITHUB_TOKEN", "plain-token-value");
    let config = parse_firn_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", bad_resource);
    let provider = EnvSecretProvider::from_map([("PROD_DWH", "postgres-dsn-value")]);

    let error = validate_project(&config, Some("prod"), &resolver, &provider).unwrap_err();

    assert!(error.to_string().contains("secret://"));
    assert!(!error.to_string().contains("plain-token-value"));
}

#[test]
fn file_secret_provider_resolves_without_exposing_contents() {
    let root = env::temp_dir().join(format!("firn-project-secret-test-{}", std::process::id()));
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
fn lockfile_generation_round_trips_and_diffs_semantic_changes() {
    let config = parse_firn_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let resources = compile_project_declarative_resources(&config, &resolver).unwrap();
    let sheet = destination_sheet("duckdb", TypeMappingFidelity::Lossless);
    let dependency_tuple = DependencyTuple {
        firn: "0.1.0".to_owned(),
        arrow_rs: "59.0.0".to_owned(),
        datafusion: Some("54.0.0".to_owned()),
        object_store: None,
        duckdb_rs: None,
        rust: None,
    };

    let lock = generate_lockfile(
        &config,
        &resources,
        dependency_tuple.clone(),
        std::slice::from_ref(&sheet),
        BTreeMap::new(),
    )
    .unwrap();
    let encoded = lock_to_toml(&lock).unwrap();
    let decoded = parse_lock(&encoded).unwrap();
    assert_eq!(decoded, lock);
    assert_eq!(lock.normalizer, NORMALIZER_NAMECASE_V1);
    let resource = lock.resources.get("github.issues").unwrap();
    assert!(resource.capability_sheet_hash.starts_with("sha256:"));
    assert!(
        resource
            .schema_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(
        lock.destinations["duckdb"].sheet.type_mappings[0].fidelity,
        TypeMappingFidelity::Lossless
    );

    let changed = generate_lockfile(
        &config,
        &resources,
        dependency_tuple,
        &[destination_sheet(
            "duckdb",
            TypeMappingFidelity::LossyRequiresContractAllowance,
        )],
        BTreeMap::new(),
    )
    .unwrap();
    let diffs = diff_lockfiles(&lock, &changed).unwrap();

    assert!(diffs.iter().any(|diff| diff.path.contains("sheet_hash")));
    assert!(diffs.iter().any(|diff| {
        diff.path
            .contains("destinations.duckdb.sheet.type_mappings")
    }));
}

fn destination_sheet(name: &str, fidelity: TypeMappingFidelity) -> DestinationSheet {
    DestinationSheet {
        destination: DestinationId::new(name).unwrap(),
        supported_dispositions: vec![WriteDisposition::Append, WriteDisposition::Merge],
        transactions: TransactionSupport::AtomicPackage,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: vec![TypeMapping {
            arrow_type: "utf8".to_owned(),
            destination_type: "text".to_owned(),
            fidelity,
        }],
        identifier_rules: IdentifierRules {
            normalizer: NORMALIZER_NAMECASE_V1.to_owned(),
            max_length: Some(63),
            allowed_pattern: Some("[a-z_][a-z0-9_]*".to_owned()),
        },
        migration_support: CapabilitySupport::Supported,
        quarantine_tables: CapabilitySupport::Supported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    }
}

#[test]
fn inline_uri_credentials_are_rejected() {
    let input = BOOK_PROJECT.replace(
        "destination = \"duckdb://.firn/dev.duckdb\"",
        "destination = \"postgres://user:password@example.com/db\"",
    );
    let config = parse_firn_toml(&input).unwrap();

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
fn declarative_resource_compilation_hook_uses_firn_declarative() {
    let config = parse_firn_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);

    let resources = compile_project_declarative_resources(&config, &resolver).unwrap();

    assert_eq!(resources.len(), 1);
    assert_eq!(
        resources[0].descriptor().resource_id.as_str(),
        "github.issues"
    );
}

#[test]
fn declarative_sql_secret_is_collected_for_validation() {
    let project = BOOK_PROJECT.replace(
        "source = \"resources/github.toml\"",
        "source = \"resources/sql.toml\"",
    );
    let sql_resource = r#"
[source.warehouse]
kind = "sql"
connection = "secret://env/POSTGRES_URL"

[resource.orders]
table = "public.orders"
primary_key = ["id"]
write_disposition = "merge"
trust = "governed"
"#;
    let config = parse_firn_toml(&project).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/sql.toml", sql_resource);
    let provider = EnvSecretProvider::from_map([
        ("POSTGRES_URL", "postgres-url-value"),
        ("PROD_DWH", "postgres-dsn-value"),
    ]);

    let report = validate_project(&config, Some("prod"), &resolver, &provider).unwrap();

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
fn auth_declaration_secret_uri_model_still_rejects_values() {
    let auth = AuthDeclaration::Bearer {
        token: "secret://env/TOKEN".to_owned(),
    };
    let source = SourceDeclaration::Rest(firn_declarative::RestSourceDeclaration {
        base_url: "https://api.example.com".to_owned(),
        auth: Some(auth),
        rate_limit: None,
        egress_allowlist: Vec::new(),
    });

    assert!(matches!(source, SourceDeclaration::Rest(_)));
}
