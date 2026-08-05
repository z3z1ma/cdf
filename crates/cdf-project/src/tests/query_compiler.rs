use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{
    CapabilitySupport, ConcurrencyLimit, DestinationId, DestinationSheet, IdempotencySupport,
    IdentifierRules, SchemaSource, TransactionSupport, WriteDisposition,
};
use tempfile::TempDir;

use super::*;

fn project(root: &TempDir, sql: &str, extra_source: &str) -> ProjectConfig {
    fs::create_dir_all(root.path().join("cdf/analytics")).unwrap();
    fs::write(root.path().join("cdf/analytics/orders.cdf.sql"), sql).unwrap();
    toml::from_str(&format!(
        r#"
[project]
name = "query_project"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "postgres://secret://env/WAREHOUSE"

[sources.warehouse]
type = "postgres"
connection = "secret://env/SOURCE_DATABASE"
{extra_source}
"#
    ))
    .unwrap()
}

fn registry() -> cdf_runtime::SourceRegistry {
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry
        .register(cdf_source_postgres::PostgresSourceDriver::new().unwrap())
        .unwrap();
    registry
}

fn destination() -> DestinationSheet {
    DestinationSheet {
        destination: DestinationId::new("postgres").unwrap(),
        supported_dispositions: vec![
            WriteDisposition::Append,
            WriteDisposition::Replace,
            WriteDisposition::Merge,
        ],
        transactions: TransactionSupport::AtomicPackage,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: Vec::new(),
        identifier_rules: IdentifierRules {
            normalizer: NORMALIZER_NAMECASE_V1.to_owned(),
            max_length: Some(63),
            allowed_pattern: None,
        },
        migration_support: CapabilitySupport::Supported,
        quarantine_tables: CapabilitySupport::Supported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(4),
        },
    }
}

fn input_schema() -> ProjectInputSchemaAuthority {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
    ]);
    let hash = cdf_kernel::canonical_arrow_schema_hash(&schema).unwrap();
    ProjectInputSchemaAuthority::new(
        SchemaSource::Declared {
            schema_hash: hash,
            source: "project-test".to_owned(),
        },
        schema,
    )
    .unwrap()
}

#[test]
fn query_compiler_resolves_independent_source_defaults_and_native_plan() {
    let root = TempDir::new().unwrap();
    let config = project(
        &root,
        "RESOURCE\nTARGET warehouse.customer_names\nDISPOSITION MERGE(id)\nTRUST GOVERNED\nSEMANTICS (display_name => 'cdf.pii@1(class=\"name\")')\nEXECUTION BOUNDED\nAS\nSELECT id, upper(name) AS display_name FROM upstream(source => 'warehouse', table => 'public.orders') WHERE active",
        "",
    );
    let schemas = BTreeMap::from([("analytics.orders".to_owned(), input_schema())]);
    let compiled = compile_query_project_resources(
        &registry(),
        &config,
        root.path(),
        "dev",
        &destination(),
        &SemanticCatalog::builtins().unwrap(),
        &schemas,
    )
    .unwrap();

    assert_eq!(compiled.len(), 1);
    let compiled = &compiled[0];
    assert_eq!(compiled.query.resource_id, "analytics.orders");
    assert_eq!(
        compiled.query.configured_source.configured_source,
        "warehouse"
    );
    assert_eq!(
        compiled.query.effective.target.value.as_str(),
        "warehouse.customer_names"
    );
    assert_eq!(
        compiled.query.effective.target.origin,
        ResolutionOrigin::Authored
    );
    assert_eq!(
        compiled.query.effective.disposition.value,
        WriteDisposition::Merge
    );
    assert_eq!(compiled.resource.schema().fields().len(), 2);
    assert_eq!(
        cdf_kernel::semantic(compiled.resource.schema().field(1)),
        Some("cdf.pii@1(class=\"name\")")
    );
    assert!(compiled.resource.relational_expression_plan().is_some());
    assert_eq!(compiled.query.parsed_query.upstream.span.start_line, 8);
}

#[test]
fn query_compiler_requires_all_configured_sources_to_be_referenced() {
    let root = TempDir::new().unwrap();
    let mut config = project(
        &root,
        "RESOURCE DISPOSITION APPEND AS SELECT * FROM upstream(source => 'warehouse', table => 'public.orders')",
        "",
    );
    config.sources.insert(
        "unused".to_owned(),
        ProjectSourceConfig {
            source_type: "postgres".to_owned(),
            options: BTreeMap::from([(
                "connection".to_owned(),
                serde_json::json!("secret://env/UNUSED_DATABASE"),
            )]),
        },
    );
    let schemas = BTreeMap::from([("analytics.orders".to_owned(), input_schema())]);
    let error = compile_query_project_resources(
        &registry(),
        &config,
        root.path(),
        "dev",
        &destination(),
        &SemanticCatalog::builtins().unwrap(),
        &schemas,
    )
    .unwrap_err();

    assert!(error.message.contains("CDF-SOURCE-UNREFERENCED"));
}

#[test]
fn query_compiler_rejects_unknown_resource_options_before_analysis() {
    let root = TempDir::new().unwrap();
    let config = project(
        &root,
        "RESOURCE DISPOSITION APPEND AS SELECT * FROM upstream(source => 'warehouse', table => 'public.orders', password => 'do-not-accept')",
        "",
    );
    let schemas = BTreeMap::from([("analytics.orders".to_owned(), input_schema())]);
    let error = compile_query_project_resources(
        &registry(),
        &config,
        root.path(),
        "dev",
        &destination(),
        &SemanticCatalog::builtins().unwrap(),
        &schemas,
    )
    .unwrap_err();

    assert!(error.message.contains("CDF-SOURCE-RESOURCE-OPTIONS"));
    assert!(!error.message.contains("do-not-accept"));
}

#[test]
fn query_compiler_rejects_unsafe_built_in_replace_default() {
    let root = TempDir::new().unwrap();
    let config = project(
        &root,
        "SELECT * FROM upstream(source => 'warehouse', table => 'public.orders')",
        "",
    );
    let schemas = BTreeMap::from([("analytics.orders".to_owned(), input_schema())]);
    let error = compile_query_project_resources(
        &registry(),
        &config,
        root.path(),
        "dev",
        &destination(),
        &SemanticCatalog::builtins().unwrap(),
        &schemas,
    )
    .unwrap_err();

    assert!(error.message.contains("CDF-DISPOSITION-DEFAULT"));
}

#[test]
fn query_compiler_accepts_namespace_source_mismatch_and_path_target_default() {
    let root = TempDir::new().unwrap();
    let config = project(
        &root,
        "RESOURCE DISPOSITION APPEND AS SELECT id FROM upstream(source => 'warehouse', table => 'public.orders')",
        "",
    );
    let schemas = BTreeMap::from([("analytics.orders".to_owned(), input_schema())]);
    let compiled = compile_query_project_resources(
        &registry(),
        &config,
        root.path(),
        "dev",
        &destination(),
        &SemanticCatalog::builtins().unwrap(),
        &schemas,
    )
    .unwrap();

    assert_eq!(
        compiled[0].query.effective.target.value.as_str(),
        "analytics.orders"
    );
    assert_eq!(
        compiled[0].query.effective.target.origin,
        ResolutionOrigin::ResourcePathDefault
    );
}

#[test]
fn authored_envelope_does_not_pollute_equivalent_execution_identity() {
    let bare_root = TempDir::new().unwrap();
    let explicit_root = TempDir::new().unwrap();
    let bare_config = project(
        &bare_root,
        "SELECT id FROM upstream(source => 'warehouse', table => 'public.orders')",
        "",
    );
    let explicit_config = project(
        &explicit_root,
        "RESOURCE TARGET analytics.orders DISPOSITION REPLACE TRUST EXPERIMENTAL EXECUTION BOUNDED AS SELECT id FROM upstream(source => 'warehouse', table => 'public.orders')",
        "",
    );
    let schemas = BTreeMap::from([("analytics.orders".to_owned(), input_schema())]);
    let compile = |root: &TempDir, config: &ProjectConfig| {
        compile_query_project_resources(
            &registry(),
            config,
            root.path(),
            "dev",
            &destination(),
            &SemanticCatalog::builtins().unwrap(),
            &schemas,
        )
        .unwrap()
        .remove(0)
    };

    let bare = compile(&bare_root, &bare_config);
    let explicit = compile(&explicit_root, &explicit_config);

    assert_ne!(
        bare.query.authored_content_hash,
        explicit.query.authored_content_hash
    );
    assert_eq!(bare.query.source_node_id, explicit.query.source_node_id);
    assert_eq!(bare.query.effective, explicit.query.effective);
    assert_eq!(bare.query.relational_plan, explicit.query.relational_plan);
}
