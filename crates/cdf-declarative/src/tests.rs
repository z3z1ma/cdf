use super::*;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
    TimestampMillisecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use cdf_conformance::resource::{
    PredicateExpectation, ResourceConformanceCase, ResourceExecutionConformanceCase,
    assert_queryable_resource_conformance, assert_resource_stream_execution_conformance,
};
use cdf_http::{
    HttpRequest, HttpResponse, HttpTransport, ProviderRefreshHook, RetryPolicy, SecretProvider,
    SecretUri, SecretValue,
};
use cdf_kernel::{
    CdfError, CursorOrderingClaim, CursorValue, DeliveryGuarantee, ErrorKind, IncrementalShape,
    PartitionId, PredicateId, PushdownFidelity, QueryableResource, ResourceStream, ScanPredicate,
    ScanRequest, SchemaHash, SchemaSource, ScopeKey, SortDirection, SourcePosition,
};
use futures_util::StreamExt;

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
                expression: "updated_at >= \"2026-07-01T00:00:00Z\"".to_owned(),
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
fn rest_runtime_executes_json_pages_with_explicit_dependencies() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= \"2026-07-01T00:00:00Z\"");
    let plan = resource.negotiate(&request).unwrap();
    assert_eq!(plan.pushed_predicates.len(), 1);
    assert_eq!(
        plan.partitions[0].metadata.get("cursor_query_param"),
        Some(&"since".to_owned())
    );
    assert_eq!(
        plan.partitions[0].metadata.get("cursor_query_value"),
        Some(&"2026-07-01T00:00:00Z".to_owned())
    );

    let transport = RecordingTransport::new([
        json_response(
            r#"{
                "items": [
                    { "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 },
                    { "id": 2, "name": "grace", "updated_at": "2026-07-03T00:00:00Z", "active": false, "score": null }
                ],
                "next_token": "n2"
            }"#,
        ),
        json_response(
            r#"{
                "items": [
                    { "id": 3, "name": "katherine", "updated_at": "2026-07-04T00:00:00Z", "active": true, "score": 9.25 }
                ]
            }"#,
        ),
    ]);
    let dependencies = RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
        StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
    );
    let rest = resource.to_rest_resource(dependencies).unwrap();

    let batches =
        drain_batches(futures_executor::block_on(rest.open(plan.partitions[0].clone())).unwrap());
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].header.row_count, 2);
    assert_eq!(batches[1].header.row_count, 1);
    assert_eq!(
        batches[0].header.observed_schema_hash,
        declared_schema_hash(&resource)
    );

    let first = batches[0].record_batch().unwrap();
    let ids = first
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);

    let first_position = cursor_micros(&batches[0].header.source_position);
    let second_position = cursor_micros(&batches[1].header.source_position);
    assert!(second_position > first_position);

    let requests = transport.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].url,
        "https://api.example.com/v1/items?existing=1&from_path=yes&state=all&since=2026-07-01T00%3A00%3A00Z"
    );
    assert_eq!(
        requests[1].url,
        "https://api.example.com/v1/items?existing=1&from_path=yes&state=all&since=2026-07-01T00%3A00%3A00Z&page_token=n2"
    );
    assert_eq!(
        requests[0].headers.get("authorization").map(String::as_str),
        Some("Bearer token-1")
    );
}

#[test]
fn rest_runtime_satisfies_execution_conformance_helper() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= \"2026-07-01T00:00:00Z\"");
    let transport = RecordingTransport::new([
        json_response(
            r#"{
                "items": [
                    { "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 },
                    { "id": 2, "name": "grace", "updated_at": "2026-07-03T00:00:00Z", "active": false, "score": 2.0 }
                ],
                "next_token": "n2"
            }"#,
        ),
        json_response(
            r#"{
                "items": [
                    { "id": 3, "name": "katherine", "updated_at": "2026-07-04T00:00:00Z", "active": true, "score": 9.25 }
                ]
            }"#,
        ),
    ]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();
    let partition = PartitionId::new("rest").unwrap();
    let case = ResourceExecutionConformanceCase::new(
        request,
        declared_schema_hash(&resource),
        [partition.clone()],
        3,
    )
    .with_expected_partition_rows([(partition, 3)]);

    futures_executor::block_on(assert_resource_stream_execution_conformance(&rest, [case]));
}

#[test]
fn rest_runtime_forwards_capabilities_and_debugs_dependency_shape() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let dependencies =
        RestRuntimeDependencies::new(RecordingTransport::new([])).with_secret_provider(
            StaticSecretProvider::new([("secret://env/API_TOKEN", "debug-secret")]),
        );
    let debug = format!("{dependencies:?}");
    assert!(debug.contains("RestRuntimeDependencies"));
    assert!(debug.contains("secret_provider"));
    assert!(debug.contains("true"));
    assert!(!debug.contains("debug-secret"));

    let rest = resource.to_rest_resource(dependencies).unwrap();
    assert_eq!(
        rest.capabilities().filters.default_fidelity,
        resource.capabilities().filters.default_fidelity
    );
    assert_eq!(
        rest.capabilities().incremental,
        resource.capabilities().incremental
    );
}

#[test]
fn rest_cursor_pushdown_accepts_only_safe_literal_tokens() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);

    let plan = resource
        .negotiate(&rest_cursor_request(&resource, "updated_at >= -20260701.5"))
        .unwrap();
    assert_eq!(plan.pushed_predicates.len(), 1);
    assert_eq!(
        plan.partitions[0].metadata.get("cursor_query_value"),
        Some(&"-20260701.5".to_owned())
    );

    let plan = resource
        .negotiate(&rest_cursor_request(&resource, "updated_at >= abc123"))
        .unwrap();
    assert_eq!(plan.pushed_predicates.len(), 0);
    assert_eq!(plan.unsupported_predicates.len(), 1);
    assert!(
        !plan.partitions[0]
            .metadata
            .contains_key("cursor_query_value")
    );
}

#[test]
fn rest_default_open_remains_unsupported_without_runtime_dependencies() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= \"2026-07-01T00:00:00Z\"");
    let partition = resource.plan_partitions(&request).unwrap().remove(0);

    let error = expect_open_error(futures_executor::block_on(resource.open(partition)));
    assert!(error.to_string().contains("outside the MVP compiler crate"));
}

#[test]
fn rest_runtime_does_not_smuggle_unsupported_predicates_into_urls() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let predicate_id = PredicateId::new("unsupported").unwrap();
    let request = ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: vec![ScanPredicate {
            predicate_id,
            expression: "id = 2".to_owned(),
        }],
        limit: None,
        order_by: vec![],
        scope: ScopeKey::Resource,
    };
    let plan = resource.negotiate(&request).unwrap();
    assert_eq!(plan.pushed_predicates.len(), 0);
    assert_eq!(plan.unsupported_predicates.len(), 1);
    assert!(
        !plan.partitions[0]
            .metadata
            .contains_key("cursor_query_value")
    );

    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [{ "id": 2, "name": "grace", "updated_at": "2026-07-03T00:00:00Z", "active": true, "score": 1.0 }] }"#,
    )]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();
    drain_batches(futures_executor::block_on(rest.open(plan.partitions[0].clone())).unwrap());

    let requests = transport.requests();
    assert!(!requests[0].url.contains("id=2"));
    assert!(!requests[0].url.contains("since="));

    let unsupported_cursor_input = REST_RUNTIME_EXAMPLE.replace(
        r#"cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "0ms" }"#,
        r#"cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "0ms", filter_fidelity = "unsupported" }"#,
    );
    let unsupported_cursor_resource =
        compile_document(&parse_toml(&unsupported_cursor_input).unwrap())
            .unwrap()
            .remove(0);
    let unsupported_cursor_plan = unsupported_cursor_resource
        .negotiate(&rest_cursor_request(
            &unsupported_cursor_resource,
            "updated_at >= \"2026-07-01T00:00:00Z\"",
        ))
        .unwrap();
    assert_eq!(unsupported_cursor_plan.pushed_predicates.len(), 0);
    assert_eq!(unsupported_cursor_plan.unsupported_predicates.len(), 1);
    assert!(
        !unsupported_cursor_plan.partitions[0]
            .metadata
            .contains_key("cursor_query_value")
    );
}

#[test]
fn rest_runtime_does_not_treat_symbolic_cursor_placeholder_as_executable_pushdown() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= checkpoint.cursor");
    let plan = resource.negotiate(&request).unwrap();

    assert_eq!(plan.pushed_predicates.len(), 0);
    assert_eq!(plan.unsupported_predicates.len(), 1);
    assert!(
        !plan.partitions[0]
            .metadata
            .contains_key("cursor_query_value")
    );
}

#[test]
fn rest_runtime_refreshes_auth_once_when_configured() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= \"2026-07-01T00:00:00Z\"");
    let plan = resource.negotiate(&request).unwrap();
    let transport = RecordingTransport::new([
        HttpResponse::new(401).with_body(r#"{ "message": "expired" }"#),
        json_response(
            r#"{ "items": [{ "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 }] }"#,
        ),
    ]);
    let provider = RotatingSecretProvider::new(["old-token", "new-token"]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone())
                .with_secret_provider(provider)
                .with_auth_refresh_hook(ProviderRefreshHook),
        )
        .unwrap();

    let batches =
        drain_batches(futures_executor::block_on(rest.open(plan.partitions[0].clone())).unwrap());
    assert_eq!(batches[0].header.row_count, 1);
    let requests = transport.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].headers.get("authorization").map(String::as_str),
        Some("Bearer old-token")
    );
    assert_eq!(
        requests[1].headers.get("authorization").map(String::as_str),
        Some("Bearer new-token")
    );
}

#[test]
fn rest_runtime_applies_header_auth_without_leaking_secret_in_debug() {
    let input = REST_RUNTIME_EXAMPLE.replace(
        r#"auth = { kind = "bearer", token = "secret://env/API_TOKEN" }"#,
        r#"auth = { kind = "header", name = "X-Api-Key", value = "secret://env/API_TOKEN" }"#,
    );
    let resource = compile_document(&parse_toml(&input).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= \"2026-07-01T00:00:00Z\"");
    let plan = resource.negotiate(&request).unwrap();
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [{ "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 }] }"#,
    )]);
    let dependencies = RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
        StaticSecretProvider::new([("secret://env/API_TOKEN", "header-secret-value")]),
    );
    assert!(!format!("{dependencies:?}").contains("header-secret-value"));
    let rest = resource.to_rest_resource(dependencies).unwrap();

    drain_batches(futures_executor::block_on(rest.open(plan.partitions[0].clone())).unwrap());
    let requests = transport.requests();
    assert_eq!(
        requests[0].headers.get("x-api-key").map(String::as_str),
        Some("header-secret-value")
    );
    assert!(!format!("{:?}", requests[0]).contains("header-secret-value"));
}

#[test]
fn rest_runtime_denies_allowlist_before_transport_use() {
    let input = REST_RUNTIME_EXAMPLE.replace(
        r#"base_url = "https://api.example.com/v1?existing=1""#,
        r#"base_url = "https://blocked.example.net/v1?existing=1""#,
    );
    let resource = compile_document(&parse_toml(&input).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= \"2026-07-01T00:00:00Z\"");
    let plan = resource.negotiate(&request).unwrap();
    let transport = RecordingTransport::new([json_response(r#"{ "items": [] }"#)]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();

    let error = expect_open_error(futures_executor::block_on(
        rest.open(plan.partitions[0].clone()),
    ));
    assert_eq!(error.kind, ErrorKind::Auth);
    assert_eq!(transport.requests().len(), 0);
}

#[test]
fn rest_runtime_rejects_non_http_url_before_transport_use() {
    let input = REST_RUNTIME_EXAMPLE.replace(
        r#"base_url = "https://api.example.com/v1?existing=1""#,
        r#"base_url = "ftp://api.example.com/v1?existing=1""#,
    );
    let resource = compile_document(&parse_toml(&input).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= \"2026-07-01T00:00:00Z\"");
    let plan = resource.negotiate(&request).unwrap();
    let transport = RecordingTransport::new([json_response(r#"{ "items": [] }"#)]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();

    let error = expect_open_error(futures_executor::block_on(
        rest.open(plan.partitions[0].clone()),
    ));
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.to_string().contains("http or https"));
    assert_eq!(transport.requests().len(), 0);
}

#[test]
fn rest_runtime_rejects_request_urls_with_whitespace_hosts() {
    let input = REST_RUNTIME_EXAMPLE.replace(
        r#"base_url = "https://api.example.com/v1?existing=1""#,
        r#"base_url = "https://api example.com/v1?existing=1""#,
    );
    let resource = compile_document(&parse_toml(&input).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= \"2026-07-01T00:00:00Z\"");
    let plan = resource.negotiate(&request).unwrap();
    let transport = RecordingTransport::new([json_response(r#"{ "items": [] }"#)]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();

    let error = expect_open_error(futures_executor::block_on(
        rest.open(plan.partitions[0].clone()),
    ));
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.to_string().contains("whitespace"));
    assert_eq!(transport.requests().len(), 0);
}

#[test]
fn rest_runtime_fails_closed_for_missing_secret() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= \"2026-07-01T00:00:00Z\"");
    let plan = resource.negotiate(&request).unwrap();
    let transport = RecordingTransport::new([json_response(r#"{ "items": [] }"#)]);
    let rest = resource
        .to_rest_resource(RestRuntimeDependencies::new(transport.clone()))
        .unwrap();

    let error = expect_open_error(futures_executor::block_on(
        rest.open(plan.partitions[0].clone()),
    ));
    assert_eq!(error.kind, ErrorKind::Auth);
    assert!(error.to_string().contains("SecretProvider"));
    assert_eq!(transport.requests().len(), 0);
}

#[test]
fn rest_runtime_dependency_preflight_resolves_auth_secret() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let transport = RecordingTransport::new([json_response(r#"{ "items": [] }"#)]);

    let valid = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();
    valid.validate_runtime_dependencies().unwrap();

    let missing = resource
        .to_rest_resource(RestRuntimeDependencies::new(transport.clone()))
        .unwrap();
    let error = missing.validate_runtime_dependencies().unwrap_err();
    assert_eq!(error.kind, ErrorKind::Auth);
    assert!(error.to_string().contains("SecretProvider"));

    let empty = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone())
                .with_secret_provider(StaticSecretProvider::new([("secret://env/API_TOKEN", "")])),
        )
        .unwrap();
    let error = empty.validate_runtime_dependencies().unwrap_err();
    assert_eq!(error.kind, ErrorKind::Auth);
    assert!(error.to_string().contains("empty value"));
    assert_eq!(transport.requests().len(), 0);
}

#[test]
fn rest_runtime_fails_closed_for_non_json_response() {
    let error = rest_open_error(
        REST_RUNTIME_EXAMPLE,
        [HttpResponse::new(200).with_body("not json")],
    );
    assert_eq!(error.kind, ErrorKind::Data);
    assert!(error.to_string().contains("not valid JSON"));
}

#[test]
fn rest_runtime_fails_closed_for_selector_mismatch() {
    let error = rest_open_error(REST_RUNTIME_EXAMPLE, [json_response(r#"{ "other": [] }"#)]);
    assert_eq!(error.kind, ErrorKind::Data);
    assert!(error.to_string().contains("selector target `items`"));
}

#[test]
fn rest_runtime_rejects_invalid_record_selectors_before_batching() {
    for (selector, response) in [
        (r#"$."#, r#"{ "": [] }"#),
        (r#"$.items.nested"#, r#"{ "items.nested": [] }"#),
    ] {
        let input = REST_RUNTIME_EXAMPLE.replace(
            r#"records = "$.items""#,
            &format!(r#"records = "{selector}""#),
        );
        let error = rest_open_error(&input, [json_response(response)]);
        assert_eq!(error.kind, ErrorKind::Data);
        assert!(error.to_string().contains("supports only one object field"));
    }
}

#[test]
fn rest_runtime_requires_non_nullable_record_fields() {
    let error = rest_open_error(
        REST_RUNTIME_EXAMPLE,
        [json_response(
            r#"{ "items": [
                { "id": 1, "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 }
            ] }"#,
        )],
    );
    assert_eq!(error.kind, ErrorKind::Data);
    assert!(error.to_string().contains("non-nullable field `name`"));
}

#[test]
fn rest_runtime_fails_closed_for_cursor_field_absence() {
    let input = REST_RUNTIME_EXAMPLE.replace(
        r#"{ name = "updated_at", type = "timestamp_micros", nullable = false, timezone = "UTC" }"#,
        r#"{ name = "updated_at", type = "timestamp_micros", nullable = true, timezone = "UTC" }"#,
    );
    let error = rest_open_error(
        &input,
        [json_response(
            r#"{ "items": [{ "id": 1, "name": "ada", "active": true, "score": 4.5 }] }"#,
        )],
    );
    assert_eq!(error.kind, ErrorKind::Data);
    assert!(error.to_string().contains("cursor field `updated_at`"));
}

#[test]
fn rest_runtime_fails_closed_for_schema_coercion_error_without_partial_stream() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= \"2026-07-01T00:00:00Z\"");
    let plan = resource.negotiate(&request).unwrap();
    let transport = RecordingTransport::new([
        json_response(
            r#"{ "items": [{ "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 }], "next_token": "n2" }"#,
        ),
        json_response(
            r#"{ "items": [{ "id": "not-a-number", "name": "bad", "updated_at": "2026-07-03T00:00:00Z", "active": true, "score": 1.0 }] }"#,
        ),
    ]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();

    let error = expect_open_error(futures_executor::block_on(
        rest.open(plan.partitions[0].clone()),
    ));
    assert_eq!(error.kind, ErrorKind::Data);
    assert!(error.to_string().contains("id"));
    assert_eq!(transport.requests().len(), 2);
}

#[test]
fn rest_runtime_terminates_duplicate_token_and_empty_page_pagination() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= \"2026-07-01T00:00:00Z\"");
    let plan = resource.negotiate(&request).unwrap();
    let transport = RecordingTransport::new([
        json_response(
            r#"{ "items": [{ "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 }], "next_token": "same" }"#,
        ),
        json_response(
            r#"{ "items": [{ "id": 2, "name": "grace", "updated_at": "2026-07-03T00:00:00Z", "active": true, "score": 1.0 }], "next_token": "same" }"#,
        ),
        json_response(
            r#"{ "items": [{ "id": 3, "name": "unreached", "updated_at": "2026-07-04T00:00:00Z", "active": true, "score": 1.0 }] }"#,
        ),
    ]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();
    let batches =
        drain_batches(futures_executor::block_on(rest.open(plan.partitions[0].clone())).unwrap());
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        2
    );
    assert_eq!(transport.requests().len(), 2);

    let empty_input = REST_RUNTIME_EXAMPLE.replace(
        r#"paginate = { kind = "next_token", query_param = "page_token", response_field = "next_token" }"#,
        r#"paginate = { kind = "page_number", query_param = "page", start_page = 1 }"#,
    );
    let empty_resource = compile_document(&parse_toml(&empty_input).unwrap())
        .unwrap()
        .remove(0);
    let empty_plan = empty_resource
        .negotiate(&rest_cursor_request(
            &empty_resource,
            "updated_at >= \"2026-07-01T00:00:00Z\"",
        ))
        .unwrap();
    let empty_transport = RecordingTransport::new([json_response(r#"{ "items": [] }"#)]);
    let empty_rest = empty_resource
        .to_rest_resource(
            RestRuntimeDependencies::new(empty_transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();
    let batches = drain_batches(
        futures_executor::block_on(empty_rest.open(empty_plan.partitions[0].clone())).unwrap(),
    );
    assert!(batches.is_empty());
    assert_eq!(empty_transport.requests().len(), 1);

    let blank_token_transport = RecordingTransport::new([
        json_response(
            r#"{ "items": [
                { "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 }
            ], "next_token": "   " }"#,
        ),
        json_response(
            r#"{ "items": [
                { "id": 2, "name": "unreached", "updated_at": "2026-07-03T00:00:00Z", "active": true, "score": 5.5 }
            ] }"#,
        ),
    ]);
    let blank_token_rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(blank_token_transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();
    let batches = drain_batches(
        futures_executor::block_on(blank_token_rest.open(plan.partitions[0].clone())).unwrap(),
    );
    assert_eq!(batches.len(), 1);
    assert_eq!(blank_token_transport.requests().len(), 1);
}

#[test]
fn rest_runtime_uses_cursor_and_offset_paginators() {
    let cursor_input = REST_RUNTIME_EXAMPLE.replace(
        r#"paginate = { kind = "next_token", query_param = "page_token", response_field = "next_token" }"#,
        r#"paginate = { kind = "cursor_param", query_param = "cursor", response_field = "next_cursor" }"#,
    );
    let cursor_resource = compile_document(&parse_toml(&cursor_input).unwrap())
        .unwrap()
        .remove(0);
    let cursor_plan = cursor_resource
        .negotiate(&rest_cursor_request(
            &cursor_resource,
            "updated_at >= \"2026-07-01T00:00:00Z\"",
        ))
        .unwrap();
    let cursor_transport = RecordingTransport::new([
        json_response(
            r#"{ "items": [{ "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 }], "next_cursor": "c2" }"#,
        ),
        json_response(
            r#"{ "items": [{ "id": 2, "name": "grace", "updated_at": "2026-07-03T00:00:00Z", "active": true, "score": 5.5 }] }"#,
        ),
    ]);
    let cursor_rest = cursor_resource
        .to_rest_resource(
            RestRuntimeDependencies::new(cursor_transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();
    drain_batches(
        futures_executor::block_on(cursor_rest.open(cursor_plan.partitions[0].clone())).unwrap(),
    );
    let cursor_requests = cursor_transport.requests();
    assert_eq!(cursor_requests.len(), 2);
    assert!(cursor_requests[1].url.contains("cursor=c2"));

    let offset_input = REST_RUNTIME_EXAMPLE.replace(
        r#"paginate = { kind = "next_token", query_param = "page_token", response_field = "next_token" }"#,
        r#"paginate = { kind = "offset", offset_param = "offset", limit_param = "limit", start_offset = 0, limit = 2 }"#,
    );
    let offset_resource = compile_document(&parse_toml(&offset_input).unwrap())
        .unwrap()
        .remove(0);
    let offset_plan = offset_resource
        .negotiate(&rest_cursor_request(
            &offset_resource,
            "updated_at >= \"2026-07-01T00:00:00Z\"",
        ))
        .unwrap();
    let offset_transport = RecordingTransport::new([
        json_response(
            r#"{ "items": [
                { "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 },
                { "id": 2, "name": "grace", "updated_at": "2026-07-03T00:00:00Z", "active": true, "score": 5.5 }
            ] }"#,
        ),
        json_response(
            r#"{ "items": [{ "id": 3, "name": "katherine", "updated_at": "2026-07-04T00:00:00Z", "active": true, "score": 6.5 }] }"#,
        ),
    ]);
    let offset_rest = offset_resource
        .to_rest_resource(
            RestRuntimeDependencies::new(offset_transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();
    drain_batches(
        futures_executor::block_on(offset_rest.open(offset_plan.partitions[0].clone())).unwrap(),
    );
    let offset_requests = offset_transport.requests();
    assert_eq!(offset_requests.len(), 2);
    assert!(offset_requests[0].url.contains("offset=0"));
    assert!(offset_requests[0].url.contains("limit=2"));
    assert!(offset_requests[1].url.contains("offset=2"));
}

#[test]
fn rest_runtime_rechecks_allowlist_for_link_header_next_url() {
    let input = REST_RUNTIME_EXAMPLE.replace(
        r#"paginate = { kind = "next_token", query_param = "page_token", response_field = "next_token" }"#,
        r#"paginate = { kind = "link_header" }"#,
    );
    let resource = compile_document(&parse_toml(&input).unwrap())
        .unwrap()
        .remove(0);
    let plan = resource
        .negotiate(&rest_cursor_request(
            &resource,
            "updated_at >= \"2026-07-01T00:00:00Z\"",
        ))
        .unwrap();
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [{ "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 }] }"#,
    )
    .with_header(
        "Link",
        r#"<https://blocked.example.net/v1/items?page=2>; rel="next""#,
    )]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();

    let error = expect_open_error(futures_executor::block_on(
        rest.open(plan.partitions[0].clone()),
    ));
    assert_eq!(error.kind, ErrorKind::Auth);
    assert_eq!(transport.requests().len(), 1);
}

#[test]
fn rest_runtime_joins_absolute_paths_against_base_origin() {
    let input = REST_RUNTIME_EXAMPLE
        .replace(
            r#"base_url = "https://api.example.com/v1?existing=1""#,
            r#"base_url = "https://api.example.com/base/v1?existing=1""#,
        )
        .replace(
            r#"path = "items?from_path=yes""#,
            r#"path = "/v2/items?from_path=yes""#,
        );
    let resource = compile_document(&parse_toml(&input).unwrap())
        .unwrap()
        .remove(0);
    let plan = resource
        .negotiate(&rest_cursor_request(
            &resource,
            "updated_at >= \"2026-07-01T00:00:00Z\"",
        ))
        .unwrap();
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 }
        ] }"#,
    )]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();

    drain_batches(futures_executor::block_on(rest.open(plan.partitions[0].clone())).unwrap());
    assert_eq!(
        transport.requests()[0].url,
        "https://api.example.com/v2/items?existing=1&from_path=yes&state=all&since=2026-07-01T00%3A00%3A00Z"
    );
}

#[test]
fn rest_runtime_supports_top_level_array_selector() {
    let input = REST_RUNTIME_EXAMPLE
        .replace(r#"records = "$.items""#, r#"records = "$""#)
        .replace(
            r#"paginate = { kind = "next_token", query_param = "page_token", response_field = "next_token" }"#,
            "",
        );
    let resource = compile_document(&parse_toml(&input).unwrap())
        .unwrap()
        .remove(0);
    let plan = resource
        .negotiate(&rest_cursor_request(
            &resource,
            "updated_at >= \"2026-07-01T00:00:00Z\"",
        ))
        .unwrap();
    let transport = RecordingTransport::new([json_response(
        r#"[
            { "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 }
        ]"#,
    )]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();

    let batches =
        drain_batches(futures_executor::block_on(rest.open(plan.partitions[0].clone())).unwrap());
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].header.row_count, 1);
}

#[test]
fn rest_runtime_materializes_scalar_values_and_timestamp_cursor_max() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let plan = resource
        .negotiate(&rest_cursor_request(
            &resource,
            "updated_at >= \"2026-07-01T00:00:00Z\"",
        ))
        .unwrap();
    let transport = RecordingTransport::new([
        json_response(
            r#"{ "items": [
                { "id": "42", "name": 123, "updated_at": "2026-07-01T00:00:00Z", "active": "false", "score": "6.25" },
                { "id": 7, "name": true, "updated_at": "2026-07-03T00:00:00Z", "active": true, "score": 9.5 },
                { "id": 8, "name": { "nested": true }, "updated_at": "2026-07-02T00:00:00Z", "active": "TRUE", "score": null }
            ], "next_token": "n2" }"#,
        ),
        json_response(
            r#"{ "items": [
                { "id": 9, "name": "page2", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 1.0 }
            ] }"#,
        ),
    ]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();

    let batches =
        drain_batches(futures_executor::block_on(rest.open(plan.partitions[0].clone())).unwrap());
    assert_eq!(batches.len(), 2);
    assert!(
        cursor_micros(&batches[0].header.source_position)
            > cursor_micros(&batches[1].header.source_position)
    );

    let first = batches[0].record_batch().unwrap();
    let ids = first
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 42);
    assert_eq!(ids.value(1), 7);
    assert_eq!(ids.value(2), 8);

    let names = first
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(names.value(0), "123");
    assert_eq!(names.value(1), "true");
    assert_eq!(names.value(2), r#"{"nested":true}"#);

    let active = first
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(!active.value(0));
    assert!(active.value(1));
    assert!(active.value(2));

    let scores = first
        .column(4)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((scores.value(0) - 6.25).abs() < f64::EPSILON);
    assert!((scores.value(1) - 9.5).abs() < f64::EPSILON);
    assert!(scores.is_null(2));
}

#[test]
fn rest_runtime_materializes_int64_date32_and_timestamp_millis_values() {
    let input = REST_RUNTIME_EXAMPLE
        .replace(
            r#"{ name = "id", type = "u_int64", nullable = false }"#,
            r#"{ name = "id", type = "int64", nullable = false }"#,
        )
        .replace(
            r#"{ name = "score", type = "float64", nullable = true }"#,
            r#"{ name = "score", type = "float64", nullable = true },
    { name = "event_day", type = "date32", nullable = false },
    { name = "seen_at", type = "timestamp_millis", nullable = false, timezone = "UTC" }"#,
        );
    let resource = compile_document(&parse_toml(&input).unwrap())
        .unwrap()
        .remove(0);
    let plan = resource
        .negotiate(&rest_cursor_request(
            &resource,
            "updated_at >= \"2026-07-01T00:00:00Z\"",
        ))
        .unwrap();
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "id": "-5", "name": "ada", "updated_at": "2026-07-01T00:00:00Z", "active": true, "score": 1.0, "event_day": "1970-01-02", "seen_at": "1970-01-01T00:00:01.234Z" },
            { "id": 7, "name": "grace", "updated_at": "2026-07-02T00:00:00Z", "active": false, "score": 2.0, "event_day": 2, "seen_at": 5678 }
        ] }"#,
    )]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();

    let batches =
        drain_batches(futures_executor::block_on(rest.open(plan.partitions[0].clone())).unwrap());
    let first = batches[0].record_batch().unwrap();
    let ids = first
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), -5);
    assert_eq!(ids.value(1), 7);

    let event_days = first
        .column(5)
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(event_days.value(0), 1);
    assert_eq!(event_days.value(1), 2);

    let seen_at = first
        .column(6)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(seen_at.value(0), 1234);
    assert_eq!(seen_at.value(1), 5678);
}

#[test]
fn rest_runtime_uses_type_specific_cursor_maxima() {
    let string_cursor_input = REST_RUNTIME_EXAMPLE.replace(
        r#"cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "0ms" }"#,
        r#"cursor = { field = "name", param = "after_name", ordering = "best_effort", lag = "0ms" }"#,
    );
    assert_eq!(
        first_cursor_value(
            &string_cursor_input,
            "name >= \"a\"",
            r#"{ "items": [
                { "id": 1, "name": "ada", "updated_at": "2026-07-01T00:00:00Z", "active": true, "score": 1.0 },
                { "id": 2, "name": "zoe", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 2.0 },
                { "id": 3, "name": "maria", "updated_at": "2026-07-03T00:00:00Z", "active": true, "score": 3.0 }
            ] }"#,
        ),
        CursorValue::String("zoe".to_owned())
    );

    let int_cursor_input = REST_RUNTIME_EXAMPLE
        .replace(
            r#"{ name = "id", type = "u_int64", nullable = false }"#,
            r#"{ name = "id", type = "int64", nullable = false }"#,
        )
        .replace(
            r#"cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "0ms" }"#,
            r#"cursor = { field = "id", param = "after_id", ordering = "best_effort", lag = "0ms" }"#,
        );
    assert_eq!(
        first_cursor_value(
            &int_cursor_input,
            "id >= -10",
            r#"{ "items": [
                { "id": -5, "name": "low", "updated_at": "2026-07-01T00:00:00Z", "active": true, "score": 1.0 },
                { "id": 7, "name": "high", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 2.0 },
                { "id": 2, "name": "mid", "updated_at": "2026-07-03T00:00:00Z", "active": true, "score": 3.0 }
            ] }"#,
        ),
        CursorValue::I64(7)
    );

    let uint_cursor_input = REST_RUNTIME_EXAMPLE.replace(
        r#"cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "0ms" }"#,
        r#"cursor = { field = "id", param = "after_id", ordering = "best_effort", lag = "0ms" }"#,
    );
    assert_eq!(
        first_cursor_value(
            &uint_cursor_input,
            "id >= 0",
            r#"{ "items": [
                { "id": 1, "name": "low", "updated_at": "2026-07-01T00:00:00Z", "active": true, "score": 1.0 },
                { "id": 10, "name": "high", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 2.0 },
                { "id": 5, "name": "mid", "updated_at": "2026-07-03T00:00:00Z", "active": true, "score": 3.0 }
            ] }"#,
        ),
        CursorValue::U64(10)
    );
}

#[test]
fn rest_runtime_rejects_mismatched_partition_metadata() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let plan = resource
        .negotiate(&rest_cursor_request(
            &resource,
            "updated_at >= \"2026-07-01T00:00:00Z\"",
        ))
        .unwrap();
    let mut partition = plan.partitions[0].clone();
    partition
        .metadata
        .insert("path".to_owned(), "other".to_owned());
    let transport = RecordingTransport::new([json_response(r#"{ "items": [] }"#)]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone()).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();

    let error = expect_open_error(futures_executor::block_on(rest.open(partition)));
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.to_string().contains("partition path"));
    assert_eq!(transport.requests().len(), 0);

    let mut partition = plan.partitions[0].clone();
    partition.scope = ScopeKey::File {
        path: "not-rest".to_owned(),
    };
    let error = expect_open_error(futures_executor::block_on(rest.open(partition)));
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.to_string().contains("partition scope"));
    assert_eq!(transport.requests().len(), 0);
}

#[test]
fn rest_runtime_uses_numeric_float_cursor_max() {
    let input = REST_RUNTIME_EXAMPLE
        .replace(
            r#"cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "0ms" }"#,
            r#"cursor = { field = "score", param = "min_score", ordering = "best_effort", lag = "0ms" }"#,
        )
        .replace(
            r#"{ name = "score", type = "float64", nullable = true }"#,
            r#"{ name = "score", type = "float64", nullable = false }"#,
        );
    let resource = compile_document(&parse_toml(&input).unwrap())
        .unwrap()
        .remove(0);
    let plan = resource
        .negotiate(&rest_cursor_request(&resource, "score >= 0"))
        .unwrap();
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "id": 1, "name": "low", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 2.0 },
            { "id": 2, "name": "high", "updated_at": "2026-07-03T00:00:00Z", "active": true, "score": 10.0 },
            { "id": 3, "name": "mid", "updated_at": "2026-07-04T00:00:00Z", "active": true, "score": 5.0 }
        ] }"#,
    )]);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();

    let batches =
        drain_batches(futures_executor::block_on(rest.open(plan.partitions[0].clone())).unwrap());
    let Some(SourcePosition::Cursor(position)) = &batches[0].header.source_position else {
        panic!("expected cursor source position");
    };
    assert_eq!(position.value, CursorValue::DecimalString("10".to_owned()));
}

#[test]
fn rest_runtime_retries_transient_transport_errors() {
    let resource = compile_document(&parse_toml(REST_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let plan = resource
        .negotiate(&rest_cursor_request(
            &resource,
            "updated_at >= \"2026-07-01T00:00:00Z\"",
        ))
        .unwrap();
    let transport = FlakyTransport::new(
        1,
        json_response(
            r#"{ "items": [{ "id": 1, "name": "ada", "updated_at": "2026-07-02T00:00:00Z", "active": true, "score": 4.5 }] }"#,
        ),
    );
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport.clone())
                .with_secret_provider(StaticSecretProvider::new([(
                    "secret://env/API_TOKEN",
                    "token-1",
                )]))
                .with_retry_policy(RetryPolicy {
                    max_attempts: 1,
                    budget_ms: 1_000,
                    base_delay_ms: 1,
                    max_delay_ms: 1,
                }),
        )
        .unwrap();

    let batches =
        drain_batches(futures_executor::block_on(rest.open(plan.partitions[0].clone())).unwrap());
    assert_eq!(batches[0].header.row_count, 1);
    assert_eq!(transport.requests().len(), 2);
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
fn declarative_arrow_type_strings_compile_from_toml() {
    let input = r#"
[source.local]
kind = "files"
root = "."

[resource.events]
glob = "*.ndjson"
format = "ndjson"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "old_string", type = "string" },
  { name = "old_json", type = "json" },
  { name = "old_uint64", type = "u_int64" },
  { name = "uint64", type = "uint64" },
  { name = "int8", type = "int8" },
  { name = "uint32", type = "uint32" },
  { name = "float16", type = "float16" },
  { name = "decimal", type = "decimal(38,9)" },
  { name = "decimal256", type = "decimal256(76,10)" },
  { name = "date64", type = "date(ms)" },
  { name = "time64", type = "time64(ns)" },
  { name = "timestamp", type = "timestamp(us, UTC)" },
  { name = "duration", type = "duration(ms)" },
  { name = "binary", type = "binary" },
  { name = "large_binary", type = "large_binary" },
  { name = "large_utf8", type = "large_utf8" },
  { name = "items", type = "list<int64>" },
  { name = "payload", type = "struct<amount: decimal(38,9), tags: list<utf8>>" },
  { name = "counts", type = "map<utf8,int64>" },
] }
"#;

    let resource = compile_document(&parse_toml(input).unwrap())
        .unwrap()
        .remove(0);
    let schema = resource.schema();
    let field_type = |name: &str| schema.field_with_name(name).unwrap().data_type();

    assert_eq!(field_type("old_string"), &DataType::Utf8);
    assert_eq!(field_type("old_json"), &DataType::Utf8);
    assert_eq!(field_type("old_uint64"), &DataType::UInt64);
    assert_eq!(field_type("uint64"), &DataType::UInt64);
    assert_eq!(field_type("int8"), &DataType::Int8);
    assert_eq!(field_type("uint32"), &DataType::UInt32);
    assert_eq!(field_type("float16"), &DataType::Float16);
    assert_eq!(field_type("decimal"), &DataType::Decimal128(38, 9));
    assert_eq!(field_type("decimal256"), &DataType::Decimal256(76, 10));
    assert_eq!(field_type("date64"), &DataType::Date64);
    assert_eq!(
        field_type("time64"),
        &DataType::Time64(TimeUnit::Nanosecond)
    );
    assert_eq!(
        field_type("timestamp"),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        field_type("duration"),
        &DataType::Duration(TimeUnit::Millisecond)
    );
    assert_eq!(field_type("binary"), &DataType::Binary);
    assert_eq!(field_type("large_binary"), &DataType::LargeBinary);
    assert_eq!(field_type("large_utf8"), &DataType::LargeUtf8);
    assert_eq!(
        field_type("items"),
        &DataType::new_list(DataType::Int64, true)
    );
    assert_eq!(
        field_type("payload"),
        &DataType::Struct(Fields::from(vec![
            Field::new("amount", DataType::Decimal128(38, 9), true),
            Field::new("tags", DataType::new_list(DataType::Utf8, true), true),
        ]))
    );
    assert_eq!(
        field_type("counts"),
        &DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, true),
                ])),
                false,
            )),
            false,
        )
    );
}

#[test]
fn declarative_arrow_type_strings_compile_from_yaml() {
    let input = r#"
source:
  local:
    kind: files
    root: .
resource:
  events:
    glob: "*.ndjson"
    format: ndjson
    write_disposition: append
    trust: governed
    schema:
      fields:
        - { name: id, type: int32, nullable: false }
        - { name: tags, type: "list<large_utf8>" }
        - { name: amount, type: "decimal128(20,4)" }
"#;

    let resource = compile_document(&parse_yaml(input).unwrap())
        .unwrap()
        .remove(0);
    let schema = resource.schema();

    assert_eq!(
        schema.field_with_name("id").unwrap().data_type(),
        &DataType::Int32
    );
    assert_eq!(
        schema.field_with_name("tags").unwrap().data_type(),
        &DataType::new_list(DataType::LargeUtf8, true)
    );
    assert_eq!(
        schema.field_with_name("amount").unwrap().data_type(),
        &DataType::Decimal128(20, 4)
    );
}

#[test]
fn declarative_arrow_type_error_names_offending_string() {
    let input = r#"
[source.local]
kind = "files"
root = "."

[resource.events]
glob = "*.ndjson"
format = "ndjson"
write_disposition = "append"
trust = "governed"
schema = { fields = [{ name = "items", type = "list<not_a_type>" }] }
"#;

    let error = compile_document(&parse_toml(input).unwrap()).unwrap_err();
    assert!(error.to_string().contains("list<not_a_type>"));
    assert!(error.to_string().contains("invalid declarative field type"));
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

    let field_type_schema = artifact
        .schema
        .pointer("/$defs/FieldDeclaration/properties/type")
        .unwrap();
    let field_type_schema = resolve_schema_ref(&artifact.schema, field_type_schema);
    assert_eq!(
        field_type_schema
            .get("type")
            .and_then(serde_json::Value::as_str),
        Some("string")
    );
    assert!(field_type_schema.get("enum").is_none());
}

#[test]
fn sql_negotiate_pushes_filters_exactly() {
    let resource = compile_document(&parse_toml(SQL_RUNTIME_EXAMPLE).unwrap())
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
    assert!(
        plan.partitions[0]
            .metadata
            .contains_key("postgres_sql_scan")
    );
}

#[test]
fn sql_negotiate_does_not_smuggle_unstructured_predicates() {
    let resource = compile_document(&parse_toml(SQL_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let safe = PredicateId::new("safe").unwrap();
    let unsafe_predicate = PredicateId::new("unsafe").unwrap();
    let request = ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: Some(vec!["id".to_owned(), "updated_at".to_owned()]),
        filters: vec![
            ScanPredicate {
                predicate_id: safe.clone(),
                expression: "updated_at >= 10".to_owned(),
            },
            ScanPredicate {
                predicate_id: unsafe_predicate.clone(),
                expression: "id = 1 OR 1 = 1".to_owned(),
            },
        ],
        limit: Some(10),
        order_by: vec![],
        scope: ScopeKey::Resource,
    };

    assert_queryable_resource_conformance(
        &resource,
        [
            ResourceConformanceCase::new(request.clone()).with_expected_predicates([
                PredicateExpectation::exact(safe),
                PredicateExpectation::unsupported(unsafe_predicate),
            ]),
        ],
    );
    let plan = resource.negotiate(&request).unwrap();
    assert_eq!(plan.pushed_predicates.len(), 1);
    assert_eq!(plan.unsupported_predicates.len(), 1);
    let scan = plan.partitions[0]
        .metadata
        .get("postgres_sql_scan")
        .unwrap();
    assert!(scan.contains("updated_at"));
    assert!(!scan.contains("OR 1"));
}

#[test]
fn sql_default_open_remains_unsupported_without_runtime_dependencies() {
    let resource = compile_document(&parse_toml(SQL_RUNTIME_EXAMPLE).unwrap())
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
    let partition = resource.plan_partitions(&request).unwrap().remove(0);

    let error = expect_open_error(futures_executor::block_on(resource.open(partition)));
    assert!(error.to_string().contains("outside the MVP compiler crate"));
}

#[test]
fn sql_runtime_requires_explicit_secret_provider() {
    let resource = compile_document(&parse_toml(SQL_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let sql = resource
        .to_sql_resource(SqlRuntimeDependencies::new())
        .unwrap();
    let request = ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: ScopeKey::Resource,
    };
    let partition = sql.plan_partitions(&request).unwrap().remove(0);
    let error = expect_open_error(futures_executor::block_on(sql.open(partition)));
    assert!(error.to_string().contains("SecretProvider"));
}

#[test]
fn sql_runtime_dependency_preflight_resolves_connection_secret() {
    let resource = compile_document(&parse_toml(SQL_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);

    let missing = resource
        .to_sql_resource(SqlRuntimeDependencies::new())
        .unwrap();
    let error = missing.validate_runtime_dependencies().unwrap_err();
    assert_eq!(error.kind, ErrorKind::Auth);
    assert!(error.to_string().contains("SecretProvider"));

    let empty = resource
        .to_sql_resource(SqlRuntimeDependencies::new().with_secret_provider(
            StaticSecretProvider::new([("secret://env/POSTGRES_URL", "")]),
        ))
        .unwrap();
    let error = empty.validate_runtime_dependencies().unwrap_err();
    assert_eq!(error.kind, ErrorKind::Auth);
    assert!(error.to_string().contains("empty value"));
}

#[test]
fn sql_runtime_rejects_empty_connection_secret_before_connecting() {
    let resource = compile_document(&parse_toml(SQL_RUNTIME_EXAMPLE).unwrap())
        .unwrap()
        .remove(0);
    let sql = resource
        .to_sql_resource(SqlRuntimeDependencies::new().with_secret_provider(
            StaticSecretProvider::new([("secret://env/POSTGRES_URL", "")]),
        ))
        .unwrap();
    let request = ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: ScopeKey::Resource,
    };
    let partition = sql.plan_partitions(&request).unwrap().remove(0);
    let error = expect_open_error(futures_executor::block_on(sql.open(partition)));
    assert!(error.to_string().contains("empty value"));
}

#[test]
fn sql_runtime_fails_closed_for_query_and_non_postgres_dialect() {
    let query = SQL_RUNTIME_EXAMPLE.replace(
        r#"table = "public.orders""#,
        r#"query = "SELECT * FROM public.orders""#,
    );
    let query_resource = compile_document(&parse_toml(&query).unwrap())
        .unwrap()
        .remove(0);
    let error = query_resource
        .to_sql_resource(SqlRuntimeDependencies::new())
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("query resources are not supported")
    );

    let non_postgres =
        SQL_RUNTIME_EXAMPLE.replace(r#"dialect = "postgres""#, r#"dialect = "sqlite""#);
    let non_postgres_resource = compile_document(&parse_toml(&non_postgres).unwrap())
        .unwrap()
        .remove(0);
    let error = non_postgres_resource
        .to_sql_resource(SqlRuntimeDependencies::new())
        .unwrap_err();
    assert!(error.to_string().contains("dialect `postgres`"));
}

#[test]
fn sql_runtime_rejects_malformed_table_and_empty_declared_schema() {
    let malformed = SQL_RUNTIME_EXAMPLE.replace(r#"table = "public.orders""#, r#"table = "a.b.c""#);
    let malformed_resource = compile_document(&parse_toml(&malformed).unwrap())
        .unwrap()
        .remove(0);
    assert!(
        malformed_resource
            .to_sql_resource(SqlRuntimeDependencies::new())
            .is_err()
    );

    let empty_schema = r#"
[source.warehouse]
kind = "sql"
connection = "secret://env/POSTGRES_URL"
dialect = "postgres"

[resource.orders]
table = "public.orders"
write_disposition = "append"
trust = "governed"
schema = { fields = [] }
"#;
    let empty_resource = compile_document(&parse_toml(empty_schema).unwrap())
        .unwrap()
        .remove(0);
    let error = empty_resource
        .to_sql_resource(SqlRuntimeDependencies::new())
        .unwrap_err();
    assert!(error.to_string().contains("declared schema"));
}

const SQL_RUNTIME_EXAMPLE: &str = r#"
[source.warehouse]
kind = "sql"
connection = "secret://env/POSTGRES_URL"
dialect = "postgres"

[resource.orders]
table = "public.orders"
primary_key = ["id"]
cursor = { field = "updated_at", ordering = "exact", lag = "0ms" }
write_disposition = "merge"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "int64", nullable = false },
] }
"#;

const REST_RUNTIME_EXAMPLE: &str = r#"
[source.api]
kind = "rest"
base_url = "https://api.example.com/v1?existing=1"
auth = { kind = "bearer", token = "secret://env/API_TOKEN" }
egress_allowlist = ["api.example.com"]

[resource.items]
path = "items?from_path=yes"
params = { state = "all" }
paginate = { kind = "next_token", query_param = "page_token", response_field = "next_token" }
records = "$.items"
primary_key = ["id"]
cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "0ms" }
write_disposition = "merge"
trust = "governed"
schema = { fields = [
    { name = "id", type = "u_int64", nullable = false },
    { name = "name", type = "string", nullable = false },
    { name = "updated_at", type = "timestamp_micros", nullable = false, timezone = "UTC" },
    { name = "active", type = "boolean", nullable = false },
    { name = "score", type = "float64", nullable = true },
] }
"#;

#[derive(Clone, Default)]
struct RecordingTransport {
    state: Arc<Mutex<RecordingTransportState>>,
}

#[derive(Default)]
struct RecordingTransportState {
    requests: Vec<HttpRequest>,
    responses: VecDeque<HttpResponse>,
}

impl RecordingTransport {
    fn new<I>(responses: I) -> Self
    where
        I: IntoIterator<Item = HttpResponse>,
    {
        Self {
            state: Arc::new(Mutex::new(RecordingTransportState {
                requests: Vec::new(),
                responses: responses.into_iter().collect(),
            })),
        }
    }

    fn requests(&self) -> Vec<HttpRequest> {
        self.state.lock().unwrap().requests.clone()
    }
}

impl HttpTransport for RecordingTransport {
    fn send(&mut self, request: HttpRequest) -> cdf_kernel::Result<HttpResponse> {
        let mut state = self.state.lock().unwrap();
        state.requests.push(request);
        state
            .responses
            .pop_front()
            .ok_or_else(|| CdfError::internal("test transport exhausted responses"))
    }
}

#[derive(Clone)]
struct FlakyTransport {
    state: Arc<Mutex<FlakyTransportState>>,
}

struct FlakyTransportState {
    failures_remaining: usize,
    requests: Vec<HttpRequest>,
    response: Option<HttpResponse>,
}

impl FlakyTransport {
    fn new(failures_remaining: usize, response: HttpResponse) -> Self {
        Self {
            state: Arc::new(Mutex::new(FlakyTransportState {
                failures_remaining,
                requests: Vec::new(),
                response: Some(response),
            })),
        }
    }

    fn requests(&self) -> Vec<HttpRequest> {
        self.state.lock().unwrap().requests.clone()
    }
}

impl HttpTransport for FlakyTransport {
    fn send(&mut self, request: HttpRequest) -> cdf_kernel::Result<HttpResponse> {
        let mut state = self.state.lock().unwrap();
        state.requests.push(request);
        if state.failures_remaining > 0 {
            state.failures_remaining -= 1;
            return Err(CdfError::transient("temporary test transport failure"));
        }
        state
            .response
            .take()
            .ok_or_else(|| CdfError::internal("test transport exhausted response"))
    }
}

struct StaticSecretProvider {
    values: BTreeMap<String, String>,
}

impl StaticSecretProvider {
    fn new<I, K, V>(values: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            values: values
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }
}

impl SecretProvider for StaticSecretProvider {
    fn resolve(&self, uri: &SecretUri) -> cdf_kernel::Result<SecretValue> {
        self.values
            .get(uri.as_str())
            .map(|value| SecretValue::new(value.clone()))
            .ok_or_else(|| CdfError::auth(format!("missing test secret `{uri}`")))
    }
}

struct RotatingSecretProvider {
    values: Mutex<VecDeque<String>>,
}

impl RotatingSecretProvider {
    fn new<I, V>(values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<String>,
    {
        Self {
            values: Mutex::new(values.into_iter().map(Into::into).collect()),
        }
    }
}

impl SecretProvider for RotatingSecretProvider {
    fn resolve(&self, _uri: &SecretUri) -> cdf_kernel::Result<SecretValue> {
        self.values
            .lock()
            .unwrap()
            .pop_front()
            .map(SecretValue::new)
            .ok_or_else(|| CdfError::auth("rotating test secret provider exhausted"))
    }
}

fn resolve_schema_ref<'a>(
    root: &'a serde_json::Value,
    schema: &'a serde_json::Value,
) -> &'a serde_json::Value {
    let Some(reference) = schema.get("$ref").and_then(serde_json::Value::as_str) else {
        return schema;
    };
    let pointer = reference
        .strip_prefix('#')
        .expect("local JSON Schema references must start with #");
    root.pointer(pointer)
        .expect("JSON Schema reference must resolve")
}

fn json_response(body: &str) -> HttpResponse {
    HttpResponse::new(200)
        .with_header("content-type", "application/json")
        .with_body(body)
}

fn rest_cursor_request(resource: &CompiledResource, expression: &str) -> ScanRequest {
    ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: vec![ScanPredicate {
            predicate_id: PredicateId::new("cursor").unwrap(),
            expression: expression.to_owned(),
        }],
        limit: None,
        order_by: vec![],
        scope: ScopeKey::Resource,
    }
}

fn rest_open_error<I>(input: &str, responses: I) -> CdfError
where
    I: IntoIterator<Item = HttpResponse>,
{
    let resource = compile_document(&parse_toml(input).unwrap())
        .unwrap()
        .remove(0);
    let request = rest_cursor_request(&resource, "updated_at >= \"2026-07-01T00:00:00Z\"");
    let plan = resource.negotiate(&request).unwrap();
    let transport = RecordingTransport::new(responses);
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(transport).with_secret_provider(
                StaticSecretProvider::new([("secret://env/API_TOKEN", "token-1")]),
            ),
        )
        .unwrap();
    expect_open_error(futures_executor::block_on(
        rest.open(plan.partitions[0].clone()),
    ))
}

fn drain_batches(mut stream: cdf_kernel::BatchStream) -> Vec<cdf_kernel::Batch> {
    futures_executor::block_on(async move {
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        batches
    })
}

fn expect_open_error(result: cdf_kernel::Result<cdf_kernel::BatchStream>) -> CdfError {
    match result {
        Ok(_) => panic!("REST open unexpectedly succeeded"),
        Err(error) => error,
    }
}

fn declared_schema_hash(resource: &CompiledResource) -> SchemaHash {
    match &resource.descriptor().schema_source {
        SchemaSource::Declared { schema_hash, .. } => schema_hash.clone(),
        other => panic!("expected declared schema hash, got {other:?}"),
    }
}

fn cursor_micros(position: &Option<SourcePosition>) -> i64 {
    match position {
        Some(SourcePosition::Cursor(position)) => match &position.value {
            CursorValue::TimestampMicros { micros, .. } => *micros,
            other => panic!("expected timestamp cursor position, got {other:?}"),
        },
        other => panic!("expected cursor source position, got {other:?}"),
    }
}

fn first_cursor_value(input: &str, expression: &str, body: &str) -> CursorValue {
    let resource = compile_document(&parse_toml(input).unwrap())
        .unwrap()
        .remove(0);
    let plan = resource
        .negotiate(&rest_cursor_request(&resource, expression))
        .unwrap();
    let rest = resource
        .to_rest_resource(
            RestRuntimeDependencies::new(RecordingTransport::new([json_response(body)]))
                .with_secret_provider(StaticSecretProvider::new([(
                    "secret://env/API_TOKEN",
                    "token-1",
                )])),
        )
        .unwrap();
    let batches =
        drain_batches(futures_executor::block_on(rest.open(plan.partitions[0].clone())).unwrap());
    match &batches[0].header.source_position {
        Some(SourcePosition::Cursor(position)) => position.value.clone(),
        other => panic!("expected cursor source position, got {other:?}"),
    }
}
