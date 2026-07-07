use super::*;
use cdf_conformance::resource::{
    PredicateExpectation, ResourceConformanceCase, assert_queryable_resource_conformance,
};
use cdf_kernel::{
    CursorOrderingClaim, DeliveryGuarantee, IncrementalShape, PredicateId, PushdownFidelity,
    QueryableResource, ResourceStream, ScanPredicate, ScanRequest, ScopeKey, SortDirection,
};

const BOOK_REST_EXAMPLE: &str = r#"
[source.github]
kind = "rest"
base_url = "https://api.github.com"
auth = { kind = "bearer", token = "secret://env/GITHUB_TOKEN" }
rate_limit = { requests_per_minute = 300, respect_headers = ["Retry-After", "X-RateLimit-Reset"] }

[resource.issues]
path = "/repos/{owner}/{repo}/issues"
params = { state = "all", per_page = 100 }
paginate = { kind = "link_header" }
records = "$"
primary_key = ["id"]
cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "5m" }
write_disposition = "merge"
contract = "governed"
partition = { by = "cursor_window", width = "7d" }
records_transform = "python://./src/gh.py#flatten_reactions"
"#;

#[test]
fn book_rest_example_parses_and_negotiates_inexact_cursor_pushdown() {
    let document = parse_toml(BOOK_REST_EXAMPLE).unwrap();
    let resources = compile_document(&document).unwrap();
    assert_eq!(resources.len(), 1);

    let resource = &resources[0];
    assert_eq!(resource.descriptor().resource_id.as_str(), "github.issues");
    assert_eq!(resource.descriptor().primary_key, vec!["id"]);
    assert_eq!(
        resource.descriptor().cursor.as_ref().unwrap().ordering,
        CursorOrderingClaim::Inexact
    );
    assert_eq!(
        resource.capabilities().filters.default_fidelity,
        PushdownFidelity::Inexact
    );

    let CompiledResourcePlan::Rest(plan) = resource.plan() else {
        panic!("book example must compile as REST");
    };
    assert_eq!(plan.path, "/repos/{owner}/{repo}/issues");
    assert_eq!(
        plan.pagination.as_ref().unwrap().kind().to_string(),
        "link_header"
    );
    assert_eq!(plan.rate_limit.requests_per_minute, Some(300));
    assert_eq!(plan.cursor_param.as_deref(), Some("since"));
    assert_eq!(
        plan.records_transform.as_deref(),
        Some("python://./src/gh.py#flatten_reactions")
    );

    let cursor_predicate_id = PredicateId::new("p1").unwrap();
    let unsupported_predicate_id = PredicateId::new("p2").unwrap();
    let request = ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: vec![
            ScanPredicate {
                predicate_id: cursor_predicate_id.clone(),
                expression: "updated_at >= checkpoint.cursor".to_owned(),
            },
            ScanPredicate {
                predicate_id: unsupported_predicate_id.clone(),
                expression: "id = 1".to_owned(),
            },
        ],
        limit: None,
        order_by: vec![],
        scope: ScopeKey::Resource,
    };

    assert_queryable_resource_conformance(
        resource,
        [
            ResourceConformanceCase::new(request.clone()).with_expected_predicates([
                PredicateExpectation::inexact(cursor_predicate_id),
                PredicateExpectation::unsupported(unsupported_predicate_id),
            ]),
        ],
    );
    let plan = resource.negotiate(&request).unwrap();
    assert_eq!(plan.pushed_predicates.len(), 1);
    assert_eq!(
        plan.pushed_predicates[0].fidelity,
        PushdownFidelity::Inexact
    );
    assert_eq!(plan.unsupported_predicates.len(), 1);
    assert_eq!(
        plan.delivery_guarantee,
        DeliveryGuarantee::EffectivelyOncePerKey
    );
    assert_eq!(
        plan.partitions[0].metadata.get("pagination").unwrap(),
        "link_header"
    );
}

#[test]
fn rest_cursor_pushdown_can_be_explicit_exact() {
    let input = BOOK_REST_EXAMPLE.replace(
        r#"cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "5m" }"#,
        r#"cursor = { field = "updated_at", param = "since", ordering = "exact", lag = "0ms", filter_fidelity = "exact" }"#,
    );
    let resource = compile_document(&parse_toml(&input).unwrap())
        .unwrap()
        .remove(0);

    assert_eq!(
        resource.capabilities().filters.default_fidelity,
        PushdownFidelity::Exact
    );
}

#[test]
fn yaml_sql_and_file_resources_compile_to_mvp_descriptors() {
    let input = r#"
source:
  warehouse:
    kind: sql
    connection: secret://env/POSTGRES_URL
    dialect: postgres
  local:
    kind: files
    root: ./data
resource:
  orders:
    source: warehouse
    table: public.orders
    primary_key: [id]
    cursor: { field: updated_at, ordering: exact, lag: 0ms }
    write_disposition: merge
    trust: governed
    schema:
      fields:
        - { name: id, type: int64, nullable: false }
        - { name: updated_at, type: timestamp_micros, nullable: false, timezone: UTC }
  events:
    source: local
    glob: events/*.json
    format: ndjson
    primary_key: [event_id]
    write_disposition: append
    trust: experimental
    partition: { by: file }
    sample: { fields: [event_id, payload] }
"#;

    let document = parse_yaml(input).unwrap();
    let resources = compile_document(&document).unwrap();
    let ids = resources
        .iter()
        .map(|resource| resource.descriptor().resource_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["local.events", "warehouse.orders"]);

    let file_resource = resources
        .iter()
        .find(|resource| resource.descriptor().resource_id.as_str() == "local.events")
        .unwrap();
    assert_eq!(
        file_resource.capabilities().incremental,
        IncrementalShape::File
    );
    let file_predicate_id = PredicateId::new("file-p1").unwrap();
    let file_request = ScanRequest {
        resource_id: file_resource.descriptor().resource_id.clone(),
        projection: None,
        filters: vec![ScanPredicate {
            predicate_id: file_predicate_id.clone(),
            expression: "event_id = 1".to_owned(),
        }],
        limit: None,
        order_by: vec![],
        scope: ScopeKey::Resource,
    };
    assert_queryable_resource_conformance(
        file_resource,
        [ResourceConformanceCase::new(file_request)
            .with_expected_predicates([PredicateExpectation::unsupported(file_predicate_id)])],
    );

    let sql_resource = resources
        .iter()
        .find(|resource| resource.descriptor().resource_id.as_str() == "warehouse.orders")
        .unwrap();
    assert_eq!(
        sql_resource.capabilities().filters.default_fidelity,
        PushdownFidelity::Exact
    );
    assert_eq!(sql_resource.schema().fields().len(), 2);
}

#[test]
fn file_runtime_rejects_partition_metadata_that_does_not_match_plan() {
    let input = r#"
[source.local]
kind = "files"
root = "/"

[resource.events]
glob = "*.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
"#;
    let resource = compile_document(&parse_toml(input).unwrap())
        .unwrap()
        .remove(0);
    let request = ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: ScopeKey::Resource,
    };
    let mut partition = resource.plan_partitions(&request).unwrap().remove(0);
    partition
        .metadata
        .insert("glob".to_owned(), "other.ndjson".to_owned());

    let error = match futures_executor::block_on(resource.open(partition)) {
        Ok(_) => panic!("file runtime accepted a mismatched partition"),
        Err(error) => error,
    };

    assert!(
        error
            .to_string()
            .contains("declarative file partition glob does not match")
    );
}

#[test]
fn semantic_validation_rejects_missing_declared_schema_key() {
    let input = r#"
[source.github]
kind = "rest"
base_url = "https://api.github.com"

[resource.issues]
path = "/issues"
records = "$"
primary_key = ["id"]
cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "5m" }
write_disposition = "merge"
trust = "governed"
schema = { fields = [{ name = "updated_at", type = "timestamp_micros" }] }
"#;
    let error = compile_document(&parse_toml(input).unwrap()).unwrap_err();
    assert!(error.to_string().contains("id"));
    assert!(error.to_string().contains("declared schema"));
}

#[test]
fn semantic_validation_rejects_missing_sample_cursor() {
    let input = r#"
[source.github]
kind = "rest"
base_url = "https://api.github.com"

[resource.issues]
path = "/issues"
records = "$"
primary_key = ["id"]
cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "5m" }
write_disposition = "merge"
trust = "governed"
sample = { fields = ["id"] }
"#;
    let error = validate_document(&parse_toml(input).unwrap()).unwrap_err();
    assert!(error.to_string().contains("updated_at"));
    assert!(error.to_string().contains("sample"));
}

#[test]
fn json_schema_artifact_exposes_editor_schema_model() {
    let artifact = declarative_json_schema_artifact();
    assert_eq!(artifact.version, DECLARATIVE_SCHEMA_VERSION);
    assert_eq!(artifact.path, DECLARATIVE_SCHEMA_ARTIFACT_PATH);

    let schema = serde_json::to_string_pretty(&artifact.schema).unwrap();
    assert!(schema.contains("DeclarativeDocument"));
    assert!(schema.contains("link_header"));
    assert!(schema.contains("records_transform"));
}

#[test]
fn sql_negotiate_pushes_filters_exactly() {
    let input = r#"
[source.warehouse]
kind = "sql"
connection = "secret://env/POSTGRES_URL"

[resource.orders]
table = "public.orders"
primary_key = ["id"]
cursor = { field = "updated_at", ordering = "exact", lag = "0ms" }
write_disposition = "merge"
trust = "governed"
"#;
    let resource = compile_document(&parse_toml(input).unwrap())
        .unwrap()
        .remove(0);
    let predicate_id = PredicateId::new("p1").unwrap();
    let request = ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: Some(vec!["id".to_owned()]),
        filters: vec![ScanPredicate {
            predicate_id: predicate_id.clone(),
            expression: "id = 1".to_owned(),
        }],
        limit: Some(10),
        order_by: vec![cdf_kernel::OrderBy {
            field: "updated_at".to_owned(),
            direction: SortDirection::Asc,
        }],
        scope: ScopeKey::Resource,
    };

    assert_queryable_resource_conformance(
        &resource,
        [ResourceConformanceCase::new(request.clone())
            .with_expected_predicates([PredicateExpectation::exact(predicate_id)])],
    );
    let plan = resource.negotiate(&request).unwrap();
    assert_eq!(plan.pushed_predicates[0].fidelity, PushdownFidelity::Exact);
}
