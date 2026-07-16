use std::{collections::HashMap, path::Path, sync::Arc};

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_kernel::{
    CdfError, DeduplicationSpec, QueryableResource, ResourceCapabilities, Result, SchemaSource,
    ScopeKey, TrustLevel, WriteDisposition,
};
use cdf_runtime::{
    CompiledSourcePlan, CompiledSourcePlanInput, SourceAttestationStrength,
    SourceBatchMemoryContract, SourceCompileRequest, SourceDiscoverySession, SourceDriver,
    SourceDriverDescriptor, SourceDriverId, SourceExecutionCapabilities, SourceExecutorClass,
    SourceHealthRequest, SourceHealthResult, SourceHealthStatus, SourceRegistry,
    SourceResolutionContext, SourceRetryGranularity, artifact_hash,
};

use crate::*;

#[derive(Clone)]
struct TestSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
}

impl TestSourceDriver {
    fn new(driver_id: &str, kind: &str, source_option: &str, resource_option: &str) -> Self {
        let option_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "source": {
                "type": "object",
                "additionalProperties": false,
                "required": [source_option],
                "properties": {
                    source_option: {"type": "string", "minLength": 1}
                }
            },
            "resource": {
                "type": "object",
                "additionalProperties": false,
                "required": [resource_option],
                "properties": {
                    resource_option: {"type": "string", "minLength": 1}
                }
            }
        });
        Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new(driver_id).unwrap(),
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema).unwrap(),
                kinds: vec![kind.to_owned()],
                schemes: Vec::new(),
            },
            option_schema,
        }
    }
}

impl SourceDriver for TestSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn health(
        &self,
        _request: SourceHealthRequest,
        _context: &SourceResolutionContext<'_>,
        output: &mut dyn cdf_runtime::SourceHealthSink,
    ) -> Result<()> {
        output.emit(SourceHealthResult {
            probe_id: "compile_only".to_owned(),
            status: SourceHealthStatus::Unsupported,
            message: "compile-only test driver has no runtime health probe".to_owned(),
            details: serde_json::json!({}),
        })
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        let physical_plan = serde_json::json!({
            "source_name": request.context.source_name,
            "project_root": request.context.project_root,
            "cursor_pushdown": request.context.cursor_pushdown,
            "source": request.source_options,
            "resource": request.resource_options,
        });
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            ResourceCapabilities::default(),
            test_execution_capabilities(),
            CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog: request.baseline_observation_schema_catalog,
                redacted_options: physical_plan.clone(),
                physical_plan,
            },
        )
    }

    fn discovery_session(
        &self,
        _plan: &CompiledSourcePlan,
        _context: &SourceResolutionContext<'_>,
    ) -> Result<Box<dyn SourceDiscoverySession>> {
        Err(CdfError::internal("test driver is compile-only"))
    }

    fn resolve(
        &self,
        _plan: &CompiledSourcePlan,
        _context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        Err(CdfError::internal("test driver is compile-only"))
    }
}

fn test_execution_capabilities() -> SourceExecutionCapabilities {
    SourceExecutionCapabilities {
        minimum_poll_bytes: 1,
        maximum_poll_bytes: 1024,
        minimum_decode_bytes: 1,
        maximum_decode_bytes: 4096,
        maximum_concurrency: 1,
        useful_concurrency: 1,
        executor_class: SourceExecutorClass::Io,
        blocking_lane: None,
        pausable: true,
        spillable: false,
        idempotent_reads: true,
        reopenable: true,
        resumable: false,
        speculative_safe: false,
        retry_granularity: SourceRetryGranularity::None,
        retryable_errors: Vec::new(),
        retry_policy: None,
        attestation: SourceAttestationStrength::None,
        rate_limit: None,
        quota_authority: None,
        canonical_order: true,
        bounded: true,
        batch_memory: SourceBatchMemoryContract::FrontierReserved,
        telemetry_version: "test-v1".to_owned(),
    }
}

fn test_registry() -> SourceRegistry {
    let mut registry = SourceRegistry::new();
    registry
        .register(TestSourceDriver::new(
            "httpish", "httpish", "endpoint", "path",
        ))
        .unwrap();
    registry
        .register(TestSourceDriver::new(
            "tableish", "tableish", "dsn", "table",
        ))
        .unwrap();
    registry
}

fn compile(input: &str) -> Result<Vec<CompiledResource>> {
    compile_document(&test_registry(), &parse_toml(input)?)
}

fn base_document(extra_resource: &str) -> String {
    format!(
        r#"
[source.api]
kind = "httpish"
endpoint = "https://example.test"

[resource.events]
path = "/events"
trust = "governed"
{extra_resource}
"#
    )
}

#[test]
fn open_declarations_preserve_driver_options_without_source_enums() {
    let document = parse_toml(&base_document("format_hint = \"json\"")).unwrap();
    let source = &document.source["api"];
    assert_eq!(source.kind, "httpish");
    assert_eq!(source.options["endpoint"], "https://example.test");
    let resource = &document.resource["events"];
    assert_eq!(resource.options["path"], "/events");
    assert_eq!(resource.options["format_hint"], "json");
}

#[test]
fn registry_compilation_produces_one_compiled_source_plan_and_canonical_id() {
    let resource = compile(&base_document("")).unwrap().remove(0);
    assert_eq!(resource.descriptor().resource_id.as_str(), "api.events");
    assert_eq!(resource.source_name(), "api");
    assert_eq!(resource.resource_name(), "events");
    assert_eq!(resource.source_plan().driver.driver_id.as_str(), "httpish");
    assert_eq!(
        resource.source_plan().physical_plan["source"]["endpoint"],
        "https://example.test"
    );
    assert_eq!(
        resource.source_plan().physical_plan["resource"]["path"],
        "/events"
    );
    assert_eq!(resource.source_plan().descriptor, *resource.descriptor());
}

#[test]
fn project_root_is_compiler_context_not_a_driver_specific_declaration() {
    let document = parse_toml(&base_document("")).unwrap();
    let resource = compile_document_with_project_root(&test_registry(), &document, "/tmp/project")
        .unwrap()
        .remove(0);
    assert_eq!(resource.project_root(), Some(Path::new("/tmp/project")));
    assert_eq!(
        resource.source_plan().physical_plan["project_root"],
        "/tmp/project"
    );
}

#[test]
fn multiple_sources_require_an_explicit_resource_source() {
    let input = r#"
[source.api]
kind = "httpish"
endpoint = "https://example.test"

[source.db]
kind = "tableish"
dsn = "secret://db/main"

[resource.events]
path = "/events"
trust = "governed"
"#;
    let error = compile(input).unwrap_err();
    assert!(error.message.contains("source must be declared"));
}

#[test]
fn explicit_source_selects_the_matching_driver() {
    let input = r#"
[source.api]
kind = "httpish"
endpoint = "https://example.test"

[source.db]
kind = "tableish"
dsn = "secret://db/main"

[resource.orders]
source = "db"
table = "public.orders"
trust = "governed"
"#;
    let resource = compile(input).unwrap().remove(0);
    assert_eq!(resource.descriptor().resource_id.as_str(), "db.orders");
    assert_eq!(resource.source_plan().driver.driver_id.as_str(), "tableish");
}

#[test]
fn unknown_source_kind_fails_at_the_registry_boundary() {
    let input = base_document("").replace("kind = \"httpish\"", "kind = \"missing\"");
    let error = compile(&input).unwrap_err();
    assert!(error.message.contains("missing"));
}

#[test]
fn append_is_keyless_by_default_and_merge_names_both_fixes() {
    let append = compile(&base_document("")).unwrap().remove(0);
    assert_eq!(
        append.descriptor().write_disposition,
        WriteDisposition::Append
    );
    assert!(append.descriptor().primary_key.is_empty());
    assert!(append.descriptor().merge_key.is_empty());

    let error = compile(&base_document("write_disposition = \"merge\"")).unwrap_err();
    assert!(error.message.contains("merge_key"));
    assert!(error.message.contains("append"));
}

#[test]
fn merge_and_exact_row_dedup_compile_only_for_their_valid_dispositions() {
    let merged = compile(&base_document(
        "write_disposition = \"merge\"\nmerge_key = [\"id\"]",
    ))
    .unwrap()
    .remove(0);
    assert_eq!(merged.descriptor().merge_key, vec!["id"]);

    let deduped = compile(&base_document("deduplicate = \"exact_row\""))
        .unwrap()
        .remove(0);
    assert_eq!(
        deduped.descriptor().deduplication,
        Some(DeduplicationSpec::ExactRow)
    );

    let error = compile(&base_document(
        "write_disposition = \"replace\"\ndeduplicate = \"exact_row\"",
    ))
    .unwrap_err();
    assert!(error.message.contains("valid only with append"));
}

#[test]
fn schema_modes_have_one_unambiguous_baseline() {
    let discovered = compile(&base_document("schema_mode = \"discover\""))
        .unwrap()
        .remove(0);
    assert_eq!(
        discovered.descriptor().schema_source,
        SchemaSource::Discover
    );

    let hints = compile(&base_document(
        "schema_mode = \"hints\"\nschema = { fields = [{ name = \"id\", type = \"int64\" }] }",
    ))
    .unwrap()
    .remove(0);
    assert!(matches!(
        hints.descriptor().schema_source,
        SchemaSource::Hints { .. }
    ));

    let error = compile(&base_document("schema_mode = \"hints\"")).unwrap_err();
    assert!(error.message.contains("requires a schema block"));
    let error = compile(&base_document(
        "schema_mode = \"discover\"\nschema = { fields = [{ name = \"id\", type = \"int64\" }] }",
    ))
    .unwrap_err();
    assert!(error.message.contains("cannot carry a schema block"));
}

#[test]
fn declared_schema_is_normalized_and_preserves_source_identity() {
    let resource = compile(&base_document(
        "schema = { fields = [{ name = \"VendorID\", type = \"int32\", nullable = false }] }",
    ))
    .unwrap()
    .remove(0);
    let schema = resource.schema();
    assert_eq!(schema.field(0).name(), "vendor_id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(schema.field(0).metadata()["cdf:source_name"], "VendorID");
    assert!(matches!(
        resource.descriptor().schema_source,
        SchemaSource::Declared { .. }
    ));
}

#[test]
fn normalization_collisions_fail_before_driver_compilation() {
    let error = compile(&base_document(
        "schema = { fields = [{ name = \"VendorID\", type = \"int32\" }, { name = \"vendor_id\", type = \"int64\" }] }",
    ))
    .unwrap_err();
    assert!(error.message.contains("collision"));
}

#[test]
fn required_keys_and_cursor_fields_must_exist_in_declared_schema_and_sample() {
    let error = compile(&base_document(
        "primary_key = [\"missing\"]\nschema = { fields = [{ name = \"id\", type = \"int64\" }] }",
    ))
    .unwrap_err();
    assert!(error.message.contains("missing required field"));

    let error = compile(&base_document(
        "cursor = { field = \"updated_at\", ordering = \"exact\", lag = \"0ms\" }\nsample = { fields = [\"id\"] }",
    ))
    .unwrap_err();
    assert!(error.message.contains("sample"));
}

#[test]
fn type_allowances_are_explicit_and_compiled_into_the_source_plan() {
    let defaults = compile(&base_document("")).unwrap().remove(0);
    assert!(!defaults.type_policy_allowances().coerce_types);
    assert!(!defaults.type_policy_allowances().allow_lossy_mapping);

    let configured = compile(&base_document(
        "types = { coerce_types = true, allow_lossy_mapping = true }",
    ))
    .unwrap()
    .remove(0);
    assert!(configured.source_plan().type_policy_allowances.coerce_types);
    assert!(
        configured
            .source_plan()
            .type_policy_allowances
            .allow_lossy_mapping
    );
}

#[test]
fn trust_and_partition_semantics_are_common_compiler_concerns() {
    let resource = compile(&base_document(
        "partition = { by = \"cursor_window\", width = \"2h\" }",
    ))
    .unwrap()
    .remove(0);
    assert_eq!(resource.descriptor().trust_level, TrustLevel::Governed);
    assert_eq!(
        resource.descriptor().state_scope,
        ScopeKey::Window {
            start: "cursor".to_owned(),
            end: "cursor+2h".to_owned(),
        }
    );

    let error = compile(&base_document("partition = { by = \"cursor_window\" }")).unwrap_err();
    assert!(error.message.contains("must declare width"));
}

#[test]
fn yaml_and_toml_share_the_open_declaration_model() {
    let yaml = r#"
source:
  api:
    kind: httpish
    endpoint: https://example.test
resource:
  events:
    path: /events
    trust: governed
"#;
    let resource = compile_document(&test_registry(), &parse_yaml(yaml).unwrap())
        .unwrap()
        .remove(0);
    assert_eq!(resource.descriptor().resource_id.as_str(), "api.events");
}

#[test]
fn arrow_type_vocabulary_covers_widths_decimal_temporal_binary_and_nested_types() {
    let cases = [
        ("int8", DataType::Int8),
        ("uint32", DataType::UInt32),
        ("float16", DataType::Float16),
        ("decimal(38,9)", DataType::Decimal128(38, 9)),
        ("decimal256(76,18)", DataType::Decimal256(76, 18)),
        ("date64", DataType::Date64),
        ("time32(ms)", DataType::Time32(TimeUnit::Millisecond)),
        ("duration(ns)", DataType::Duration(TimeUnit::Nanosecond)),
        ("large_utf8", DataType::LargeUtf8),
        ("large_binary", DataType::LargeBinary),
        ("list<int64>", DataType::new_list(DataType::Int64, true)),
    ];
    for (declaration, expected) in cases {
        assert_eq!(
            parse_arrow_field_type(declaration).unwrap(),
            expected,
            "{declaration}"
        );
    }
    assert_eq!(
        parse_arrow_field_type("timestamp(us, UTC)").unwrap(),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        parse_arrow_field_type("struct<id: int64, labels: list<utf8>>").unwrap(),
        DataType::Struct(
            vec![
                Field::new("id", DataType::Int64, true),
                Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
            ]
            .into()
        )
    );
}

#[test]
fn invalid_arrow_type_names_the_offending_declaration() {
    let error = parse_arrow_field_type("decimal(0,9)").unwrap_err();
    assert!(error.message.contains("decimal(0,9)"));
    let error = parse_arrow_field_type("list<int64").unwrap_err();
    assert!(error.message.contains("list<int64"));
}

#[test]
fn physical_schema_hash_is_stable_and_metadata_sensitive() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    assert_eq!(
        physical_arrow_schema_hash(&schema).unwrap(),
        physical_arrow_schema_hash(&schema).unwrap()
    );
    let changed = Schema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(HashMap::from([(
            "source".to_owned(),
            "physical".to_owned(),
        )])),
    ]);
    assert_ne!(
        physical_arrow_schema_hash(&schema).unwrap(),
        physical_arrow_schema_hash(&changed).unwrap()
    );
}

#[test]
fn schema_rebinding_updates_the_compiled_plan_without_recompiling_the_driver() {
    let resource = compile(&base_document("")).unwrap().remove(0);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let hash = physical_arrow_schema_hash(schema.as_ref()).unwrap();
    let rebound = resource.with_schema_source_and_schema(
        SchemaSource::Declared {
            schema_hash: hash,
            source: "test".to_owned(),
        },
        Arc::clone(&schema),
    );
    assert_eq!(rebound.schema(), schema);
    assert_eq!(rebound.source_plan().schema, *schema);
    assert_eq!(
        rebound.source_plan().descriptor.schema_source,
        rebound.descriptor().schema_source
    );
}

#[test]
fn generated_schema_merges_common_and_driver_fields_into_closed_objects() {
    let artifact = declarative_json_schema_artifact(&test_registry()).unwrap();
    assert_eq!(artifact.version, "cdf-declarative-v3");
    let definitions = artifact.schema["$defs"].as_object().unwrap();
    let sources = definitions["SourceDeclaration"]["oneOf"]
        .as_array()
        .unwrap();
    assert_eq!(sources.len(), 2);
    assert!(sources.iter().all(|variant| variant.get("allOf").is_none()));
    let httpish = sources
        .iter()
        .find(|variant| variant["properties"]["kind"]["const"] == "httpish")
        .unwrap();
    assert_eq!(httpish["additionalProperties"], false);
    assert!(httpish["properties"].get("endpoint").is_some());
    assert!(httpish["properties"].get("kind").is_some());

    let resources = definitions["ResourceDeclaration"]["anyOf"]
        .as_array()
        .unwrap();
    assert_eq!(resources.len(), 2);
    assert!(
        resources
            .iter()
            .all(|variant| variant.get("allOf").is_none())
    );
    let http_resource = resources
        .iter()
        .find(|variant| variant["properties"].get("path").is_some())
        .unwrap();
    assert_eq!(http_resource["additionalProperties"], false);
    assert!(http_resource["properties"].get("trust").is_some());
    assert!(http_resource["properties"].get("path").is_some());
}
