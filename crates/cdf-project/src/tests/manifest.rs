use std::collections::BTreeMap;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_kernel::{ErrorKind, SchemaSource, SemanticParameterValue, TypeMappingFidelity};
use cdf_semantic::{
    ArrowPattern, ArrowTypeFamily, DEFINITION_SCHEMA_VERSION, ParameterDefinition, ParameterFormat,
    ParameterKind, PrivacyClassification, SemanticCatalog, SemanticDefinition, SemanticNullability,
    ValidationPredicate,
};
use sha2::{Digest, Sha256};

use super::{
    DependencyTuple, DestinationProtocolCapabilities, DestinationSheetArtifact, ManifestInputKind,
    ManifestSemanticSource, ProjectCompilationMode, ProjectInputSchemaAuthority,
    ProjectManifestAuthoredInput, ProjectManifestCompileRequest, compile_project_manifest,
    compile_query_project_resources, generate_lockfile_with_destination_artifacts, lock_to_toml,
    parse_cdf_toml, parse_project_manifest, publish_project_manifest,
    publish_project_manifest_and_lock,
    support::{BOOK_PROJECT, destination_sheet, test_source_registry},
};

const RESOURCE_PATH: &str = "cdf/github/issues.cdf.sql";
const RESOURCE_SQL: &str = r#"RESOURCE
DISPOSITION MERGE(id)
TRUST GOVERNED
SEMANTICS (amount => 'finance.currency@1(code="USD")')
EXECUTION BOUNDED
AS
SELECT id, updated_at, amount
FROM upstream(source => 'github', path => '/repos/acme/cdf/issues', records => '$');
"#;
const SAFE_WHITESPACE_RESOURCE_SQL: &str = "RESOURCE\r\nDISPOSITION MERGE(id)\r\nTRUST GOVERNED\r\nSEMANTICS (amount => 'finance.currency@1(code=\"USD\")')\r\nEXECUTION BOUNDED\r\nAS\r\n\tSELECT id, updated_at, amount\r\nFROM upstream(source => 'github', path => '/repos/acme/cdf/issues', records => '$');\r\n";

fn currency_definition() -> SemanticDefinition {
    SemanticDefinition {
        definition_schema_version: DEFINITION_SCHEMA_VERSION,
        namespace: "finance".to_owned(),
        name: "currency".to_owned(),
        version: 1,
        description: "ISO currency identity carried over a decimal Arrow value".to_owned(),
        owning_namespace: "finance".to_owned(),
        supersedes: None,
        deprecated: false,
        arrow_patterns: vec![ArrowPattern::Family {
            family: ArrowTypeFamily::Decimal,
        }],
        nullability: SemanticNullability::Any,
        parameters: BTreeMap::from([(
            "code".to_owned(),
            ParameterDefinition {
                kind: ParameterKind::String,
                required: true,
                format: ParameterFormat::Any,
                allowed_values: vec![
                    SemanticParameterValue::String("EUR".to_owned()),
                    SemanticParameterValue::String("USD".to_owned()),
                ],
            },
        )]),
        required_metadata: Vec::new(),
        validation: vec![ValidationPredicate::NonEmptyStringParameter {
            parameter: "code".to_owned(),
        }],
        privacy: PrivacyClassification::Ordinary,
        equivalence: Vec::new(),
        casts: Vec::new(),
        destination_mappings: Vec::new(),
        base_arrow_fallback: true,
    }
}

fn manifest_fixture(
    generated_at_unix_ms: Option<i64>,
) -> (
    tempfile::TempDir,
    super::ProjectConfig,
    SemanticCatalog,
    super::CdfLock,
    Vec<u8>,
    super::ProjectManifest,
) {
    manifest_fixture_with_sql(generated_at_unix_ms, RESOURCE_SQL)
}

fn manifest_fixture_with_sql(
    generated_at_unix_ms: Option<i64>,
    resource_sql: &str,
) -> (
    tempfile::TempDir,
    super::ProjectConfig,
    SemanticCatalog,
    super::CdfLock,
    Vec<u8>,
    super::ProjectManifest,
) {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(root.path().join("cdf/github")).unwrap();
    std::fs::write(root.path().join(RESOURCE_PATH), resource_sql).unwrap();
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let catalog = SemanticCatalog::with_builtins(vec![currency_definition()]).unwrap();
    let input_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("amount", DataType::Decimal128(38, 9), false),
    ]);
    let input_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&input_schema).unwrap();
    let schemas = BTreeMap::from([(
        "github.issues".to_owned(),
        ProjectInputSchemaAuthority::new(
            SchemaSource::Declared {
                schema_hash: input_schema_hash,
                source: "manifest-test".to_owned(),
            },
            input_schema,
        )
        .unwrap(),
    )]);
    let destination = DestinationSheetArtifact::new(
        destination_sheet("duckdb", TypeMappingFidelity::Lossless),
        DestinationProtocolCapabilities::default(),
    )
    .unwrap();
    let entries = compile_query_project_resources(
        &test_source_registry(),
        &config,
        root.path(),
        "dev",
        &destination.sheet,
        &catalog,
        &schemas,
    )
    .unwrap();
    let resources = entries
        .iter()
        .map(|entry| entry.resource.clone())
        .collect::<Vec<_>>();
    let lock = generate_lockfile_with_destination_artifacts(
        &config,
        &resources,
        DependencyTuple {
            cdf: "test".to_owned(),
            arrow_rs: "58.3.0".to_owned(),
            datafusion: Some("54.0.0".to_owned()),
            object_store: None,
            duckdb_rs: None,
            rust: None,
        },
        &[destination],
        BTreeMap::new(),
        &catalog,
    )
    .unwrap();
    let lock_bytes = lock_to_toml(&lock).unwrap().into_bytes();
    let environment = config.effective_environment("dev").unwrap();
    let manifest = compile_project_manifest(ProjectManifestCompileRequest {
        config: &config,
        environment: &environment,
        lock: &lock,
        lock_bytes: &lock_bytes,
        resources: &entries,
        authored_inputs: vec![
            ProjectManifestAuthoredInput::explicit_file(
                "cdf.toml",
                ManifestInputKind::Project,
                BOOK_PROJECT.as_bytes(),
                "cdf-project-toml",
                1,
            )
            .unwrap(),
            ProjectManifestAuthoredInput::explicit_file(
                RESOURCE_PATH,
                ManifestInputKind::ResourceSql,
                resource_sql.as_bytes(),
                "cdf-resource-sql",
                1,
            )
            .unwrap(),
        ],
        semantic_catalog: &catalog,
        semantic_sources: BTreeMap::from([(
            "finance.currency@1".to_owned(),
            ManifestSemanticSource::Project,
        )]),
        selected_destination_id: "duckdb",
        compilation_mode: ProjectCompilationMode::LockedOffline,
        generated_at_unix_ms,
        diagnostics: Vec::new(),
    })
    .unwrap();
    (root, config, catalog, lock, lock_bytes, manifest)
}

#[test]
fn custom_currency_semantic_is_pinned_and_fully_snapshotted() {
    let (_, _, _, lock, _, manifest) = manifest_fixture(None);
    let reference = r#"finance.currency@1(code="USD")"#;
    assert!(lock.semantics.contains_key(reference));
    let semantic = manifest
        .semantics
        .iter()
        .find(|semantic| semantic.definition_id == "finance.currency@1")
        .unwrap();
    assert_eq!(semantic.source, ManifestSemanticSource::Project);
    assert_eq!(semantic.definition, currency_definition());
    assert_eq!(semantic.references.len(), 1);
    assert_eq!(semantic.references[0].reference, reference);
    assert_eq!(
        semantic.references[0].normalized_parameters["code"],
        SemanticParameterValue::String("USD".to_owned())
    );
    assert_eq!(semantic.references[0].fields[0].field_path, "amount");
}

#[test]
fn manifest_identity_is_stable_and_excludes_generation_time() {
    let (_, _, _, _, _, first) = manifest_fixture(Some(100));
    let (_, _, _, _, _, second) = manifest_fixture(Some(200));
    assert_eq!(first.manifest_hash, second.manifest_hash);
    assert_eq!(first.hashes, second.hashes);
    assert_ne!(
        first.canonical_json_bytes().unwrap(),
        second.canonical_json_bytes().unwrap()
    );
    let (_, _, _, _, _, repeated) = manifest_fixture(Some(100));
    assert_eq!(
        first.canonical_json_bytes().unwrap(),
        repeated.canonical_json_bytes().unwrap()
    );
}

#[test]
fn multiline_authored_sql_round_trips_with_exact_content_authority() {
    let (_, _, _, _, _, manifest) = manifest_fixture_with_sql(None, SAFE_WHITESPACE_RESOURCE_SQL);
    let resource = &manifest.resources[0];
    assert_eq!(resource.origin.authored_sql, SAFE_WHITESPACE_RESOURCE_SQL);
    let input = manifest
        .inputs
        .iter()
        .find(|input| input.input_id == RESOURCE_PATH)
        .unwrap();
    let expected_hash = format!(
        "sha256:{}",
        hex::encode(Sha256::digest(SAFE_WHITESPACE_RESOURCE_SQL.as_bytes()))
    );
    assert_eq!(resource.origin.authored_content_hash, expected_hash);
    assert_eq!(input.content_hash.as_str(), expected_hash);

    let bytes = manifest.canonical_json_bytes().unwrap();
    let parsed = parse_project_manifest(&bytes).unwrap();
    assert_eq!(
        parsed.resources[0].origin.authored_sql,
        SAFE_WHITESPACE_RESOURCE_SQL
    );
    assert_eq!(parsed.manifest_hash, manifest.manifest_hash);
}

#[test]
fn manifest_parser_rejects_unknown_fields_and_hash_tampering() {
    let (_, _, _, _, _, manifest) = manifest_fixture(None);
    let bytes = manifest.canonical_json_bytes().unwrap();
    assert_eq!(parse_project_manifest(&bytes).unwrap(), manifest);

    let mut unknown: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    unknown
        .as_object_mut()
        .unwrap()
        .insert("unexpected".to_owned(), serde_json::json!(true));
    let error = parse_project_manifest(&serde_json::to_vec(&unknown).unwrap()).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Data);

    let mut tampered: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    tampered["header"]["normalizer"] = serde_json::json!("different");
    let error = parse_project_manifest(&serde_json::to_vec_pretty(&tampered).unwrap()).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Data);
}

#[test]
fn manifest_publication_preserves_expectations_and_commits_lock_last() {
    let (root, config, _, lock, lock_bytes, manifest) = manifest_fixture(None);
    std::fs::write(root.path().join("cdf.toml"), BOOK_PROJECT).unwrap();
    let mut prior_lock = lock.clone();
    prior_lock.dependency_tuple.cdf = "prior".to_owned();
    let prior_lock_bytes = lock_to_toml(&prior_lock).unwrap().into_bytes();
    std::fs::write(root.path().join("cdf.lock"), &prior_lock_bytes).unwrap();

    let report = publish_project_manifest_and_lock(
        root.path(),
        &manifest,
        &lock,
        None,
        Some(prior_lock_bytes),
    )
    .unwrap();
    assert_eq!(
        report.installed_paths,
        vec![
            std::path::PathBuf::from(super::PROJECT_MANIFEST_RELATIVE_PATH),
            std::path::PathBuf::from(super::LOCK_FILE_NAME),
        ]
    );
    assert_eq!(
        std::fs::read(root.path().join("cdf.lock")).unwrap(),
        lock_bytes
    );
    let snapshot = super::load_project_manifest_snapshot(root.path(), Some("dev")).unwrap();
    assert_eq!(snapshot.config, config);
    assert_eq!(snapshot.manifest, manifest);

    let public = std::fs::read(root.path().join(super::PROJECT_MANIFEST_RELATIVE_PATH)).unwrap();
    std::fs::write(
        root.path().join(super::PROJECT_MANIFEST_RELATIVE_PATH),
        b"unrelated",
    )
    .unwrap();
    let error = publish_project_manifest(root.path(), &manifest, &lock, lock_bytes, Some(public))
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert_eq!(
        std::fs::read(root.path().join(super::PROJECT_MANIFEST_RELATIVE_PATH)).unwrap(),
        b"unrelated"
    );
}
