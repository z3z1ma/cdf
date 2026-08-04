use std::{collections::BTreeMap, sync::Arc};

use arrow_schema::{DataType, Schema};
use cdf_kernel::{
    CheckpointId, CursorOrderingClaim, CursorSpec, PipelineId, ResourceDescriptor, ResourceId,
    RunId, SchemaSource, ScopeKey, TrustLevel, TypePolicyAllowances, WriteDisposition,
    canonical_arrow_schema_hash,
};
use cdf_package::PackageReader;
use cdf_project::{ProjectRunRequest, ProjectRunSource, StateStorePathOwnership, run_project};
use cdf_runtime::{
    SourceCompileContext, SourceCompileRequest, SourceDriverId, SourceEgressScope, SourceRegistry,
    SourceResolutionContext,
};
use cdf_source_postgres::{
    PostgresSourceDriver, PostgresTarget, discover_postgres_table_catalog_schema,
};
use postgres::{Client, NoTls};

use super::{
    MatrixDestination, MatrixDisposition, RunMatrixCell, SourceArchetype,
    destinations::{ConformanceEnvironment, destination_for_cell, target_table_for_cell},
    local_postgres::qualified_name,
    plan_json::planned_engine_plan,
    test_support::StaticSecretProvider,
};

const SOURCE_TABLE: &str = "postgres_exact_source";
const SECRET_REF: &str = "secret://env/POSTGRES_EXACT_URL";
const DECIMAL128: &str = "12345678901234567890123456789.123456789";
const DECIMAL256: &str = "123456789012345678901234567890123456789012.123456789012345678";
const WIDE_NUMERIC: &str =
    "1234567890123456789012345678901234567890123456789012345678901234567890123456.7";

#[test]
fn postgres_binary_source_to_native_destination_preserves_exact_values() {
    let environment = ConformanceEnvironment::start().unwrap();
    let postgres = environment.postgres().unwrap();
    let source_name = qualified_name(postgres.schema(), SOURCE_TABLE);
    let mut client = Client::connect(postgres.url(), NoTls).unwrap();
    seed_source(&mut client, &source_name);

    let temp = tempfile::tempdir().unwrap();
    let execution = crate::test_execution_services();
    let egress = SourceEgressScope::new(
        SourceDriverId::new("postgres").unwrap(),
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let resource_id = ResourceId::new("postgres_exact.source").unwrap();
    let source_target = PostgresTarget::new(Some(postgres.schema()), SOURCE_TABLE).unwrap();
    let discovery = discover_postgres_table_catalog_schema(
        postgres.url(),
        &resource_id,
        &source_target,
        &egress,
    )
    .unwrap();
    let schema = Arc::new(discovery.schema);
    assert_discovered_exact_schema(schema.as_ref());
    let descriptor = ResourceDescriptor {
        resource_id,
        schema_source: SchemaSource::Declared {
            schema_hash: canonical_arrow_schema_hash(schema.as_ref()).unwrap(),
            source: "postgres-exact-integration".to_owned(),
        },
        primary_key: vec!["id".to_owned()],
        merge_key: Vec::new(),
        cursor: Some(CursorSpec {
            field: "id".to_owned(),
            ordering: CursorOrderingClaim::Exact,
            lag_tolerance_ms: 0,
        }),
        write_disposition: WriteDisposition::Append,
        deduplication: None,
        contract: None,
        state_scope: ScopeKey::Resource,
        freshness: None,
        trust_level: TrustLevel::Governed,
    };
    let mut registry = SourceRegistry::new();
    registry
        .register(PostgresSourceDriver::new().unwrap())
        .unwrap();
    let compiled_source = registry
        .compile(SourceCompileRequest {
            source_kind: "postgres".to_owned(),
            context: SourceCompileContext {
                source_name: "postgres_exact".to_owned(),
                project_root: Some(temp.path().to_path_buf()),
                cursor_pushdown: None,
            },
            source_options: BTreeMap::from([
                (
                    "connection".to_owned(),
                    serde_json::Value::String(SECRET_REF.to_owned()),
                ),
                (
                    "dialect".to_owned(),
                    serde_json::Value::String("postgres".to_owned()),
                ),
            ]),
            resource_options: BTreeMap::from([(
                "table".to_owned(),
                serde_json::Value::String(source_target.display_name()),
            )]),
            descriptor,
            schema: schema.as_ref().clone(),
            type_policy_allowances: TypePolicyAllowances::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        })
        .unwrap();
    let source_context = SourceResolutionContext::new(
        temp.path(),
        Arc::new(StaticSecretProvider::new([(
            SECRET_REF,
            postgres.url().to_owned(),
        )])),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let resource = registry.resolve(&compiled_source, &source_context).unwrap();

    let cell = RunMatrixCell::new(
        SourceArchetype::new("postgres_exact").unwrap(),
        MatrixDestination::new("postgres").unwrap(),
        MatrixDisposition::Append,
    );
    let destination = destination_for_cell(&cell, temp.path(), &environment).unwrap();
    let resolved_destination = destination.resolved().unwrap();
    let identifier_policy = resolved_destination.column_identifier_policy().unwrap();
    let package_id = "postgres-exact-integration";
    let plan = planned_engine_plan(resource.as_ref(), package_id, identifier_policy.as_ref())
        .unwrap()
        .bind_compiled_source(&compiled_source)
        .unwrap();

    let report = futures_executor::block_on(run_project(
        ProjectRunRequest {
            resource: ProjectRunSource::new(resource.as_ref()),
            plan,
            package_root: temp.path().join(".cdf/packages"),
            state_store_path: temp.path().join(".cdf/state.sqlite"),
            state_store_path_ownership: StateStorePathOwnership::Configured,
            pipeline_id: PipelineId::new("pipeline-postgres-exact-integration").unwrap(),
            package_id: package_id.to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-postgres-exact-integration").unwrap(),
            destination: resolved_destination,
            run_id: Some(RunId::new("run-postgres-exact-integration").unwrap()),
            event_sink: None,
            after_receipt_verified: None,
        },
        &execution,
    ))
    .unwrap()
    .into_committed()
    .unwrap();

    assert_eq!(report.row_count, 2);
    destination
        .assert_receipt_identity(&report.receipt)
        .unwrap();
    destination.verify_trait_receipt(&report.receipt).unwrap();
    let package = PackageReader::open(&report.package_dir).unwrap();
    package.verify().unwrap();
    assert_discovered_exact_schema(package.runtime_arrow_schema().unwrap().as_ref());

    let target_table = target_table_for_cell(&cell);
    let target_name = qualified_name(postgres.schema(), &target_table);
    assert_native_target_types(&mut client, postgres.schema(), &target_table);
    assert_source_and_target_equal(&mut client, &source_name, &target_name);
}

fn seed_source(client: &mut Client, source_name: &str) {
    client
        .batch_execute(&format!(
            "CREATE TABLE {source_name} (
                id BIGINT NOT NULL,
                document JSON NOT NULL,
                payload JSONB NOT NULL,
                decimal128 NUMERIC(38,9) NOT NULL,
                decimal256 NUMERIC(60,18) NOT NULL,
                wide_numeric NUMERIC(77,1) NOT NULL,
                unbounded_numeric NUMERIC NOT NULL,
                ordinary_text TEXT NOT NULL
            );
            INSERT INTO {source_name} VALUES
                (1, '{{\"large\":1e400,\"duplicate\":1,\"duplicate\":2}}',
                 '{{\"b\":2,\"a\":[1,true,null]}}', {DECIMAL128}, {DECIMAL256},
                 {WIDE_NUMERIC}, 'Infinity'::numeric, '001.2300'),
                (2, 'null', '{{\"special\":\"negative\"}}', -0.000000001,
                 -0.000000000000000001, -1.0, '-Infinity'::numeric, 'Infinity')"
        ))
        .unwrap();
}

fn assert_discovered_exact_schema(schema: &Schema) {
    assert_field(
        schema,
        "document",
        &DataType::Utf8,
        Some("postgres.json_text@1"),
        "json",
    );
    assert_field(
        schema,
        "payload",
        &DataType::Utf8,
        Some("postgres.jsonb_text@1"),
        "jsonb",
    );
    assert_field(
        schema,
        "decimal128",
        &DataType::Decimal128(38, 9),
        None,
        "numeric(38,9)",
    );
    assert_field(
        schema,
        "decimal256",
        &DataType::Decimal256(60, 18),
        None,
        "numeric(60,18)",
    );
    assert_field(
        schema,
        "wide_numeric",
        &DataType::Utf8,
        Some("postgres.numeric_text@1"),
        "numeric(77,1)",
    );
    assert_field(
        schema,
        "unbounded_numeric",
        &DataType::Utf8,
        Some("postgres.numeric_text@1"),
        "numeric",
    );
    assert_field(schema, "ordinary_text", &DataType::Utf8, None, "text");
}

fn assert_field(
    schema: &Schema,
    name: &str,
    data_type: &DataType,
    semantic: Option<&str>,
    physical_type: &str,
) {
    let field = schema.field_with_name(name).unwrap();
    assert_eq!(field.data_type(), data_type, "Arrow type for {name}");
    assert_eq!(
        field.metadata().get("cdf:semantic").map(String::as_str),
        semantic,
        "semantic tag for {name}"
    );
    assert_eq!(
        field
            .metadata()
            .get("cdf:physical_type")
            .map(String::as_str),
        Some(physical_type),
        "physical type for {name}"
    );
}

fn assert_native_target_types(client: &mut Client, schema: &str, table: &str) {
    let declarations = client
        .query(
            "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
             FROM pg_catalog.pg_attribute a
             JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2
               AND a.attnum > 0 AND NOT a.attisdropped",
            &[&schema, &table],
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<BTreeMap<_, _>>();

    for (column, expected) in [
        ("document", "json"),
        ("payload", "jsonb"),
        ("decimal128", "numeric(38,9)"),
        ("decimal256", "numeric(60,18)"),
        ("wide_numeric", "numeric(77,1)"),
        ("unbounded_numeric", "numeric"),
        ("ordinary_text", "text"),
    ] {
        assert_eq!(
            declarations.get(column).map(String::as_str),
            Some(expected),
            "native target declaration for {column}"
        );
    }
}

fn assert_source_and_target_equal(client: &mut Client, source: &str, target: &str) {
    let row = client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint,
                        COUNT(*) FILTER (WHERE s.id IS NOT NULL AND d.id IS NOT NULL)::bigint,
                        BOOL_AND(CASE WHEN s.id IS NULL OR d.id IS NULL THEN FALSE ELSE
                            s.document::text = d.document::text
                            AND s.payload::text = d.payload::text
                            AND s.decimal128 = d.decimal128
                            AND s.decimal256 = d.decimal256
                            AND s.wide_numeric = d.wide_numeric
                            AND s.unbounded_numeric = d.unbounded_numeric
                            AND s.ordinary_text = d.ordinary_text
                        END)
                 FROM {source} s FULL JOIN {target} d USING (id)"
            ),
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 2);
    assert_eq!(row.get::<_, i64>(1), 2);
    assert!(row.get::<_, bool>(2));
}
