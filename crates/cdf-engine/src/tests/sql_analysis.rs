use arrow_schema::{DataType, Field, Schema};

use super::*;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
    ])
}

#[test]
fn project_query_parses_canonical_recursive_upstream_arguments() {
    let query = parse_project_query(
        "SELECT id FROM upstream(source => 'warehouse', table => 'orders', batch => 1000, flags => ARRAY [true, NULL], cursor => OBJECT(column => 'id'))",
        "cdf/analytics/orders.cdf.sql",
    )
    .unwrap();

    assert_eq!(query.upstream.configured_source, "warehouse");
    assert_eq!(query.upstream.resource_options["table"], "orders");
    assert_eq!(query.upstream.resource_options["batch"], 1000);
    assert_eq!(
        query.upstream.resource_options["flags"],
        serde_json::json!([true, null])
    );
    assert_eq!(
        query.upstream.resource_options["cursor"],
        serde_json::json!({"column": "id"})
    );
    assert_eq!(query.upstream.span.start_line, 1);
    assert!(query.normalized_query.contains("upstream"));
}

#[test]
fn project_query_argument_identity_is_order_independent() {
    let first = parse_project_query(
        "SELECT id FROM upstream(source => 'warehouse', table => 'orders', batch => 1000)",
        "cdf/analytics/orders.cdf.sql",
    )
    .unwrap();
    let second = parse_project_query(
        "SELECT id FROM upstream(batch => 1000, source => 'warehouse', table => 'orders')",
        "cdf/analytics/orders.cdf.sql",
    )
    .unwrap();

    assert_eq!(
        first.upstream.canonical_arguments_hash,
        second.upstream.canonical_arguments_hash
    );
    assert_ne!(first.authored_ast_hash, second.authored_ast_hash);
}

#[test]
fn project_query_lowers_datafusion_analysis_to_native_relational_ir() {
    let query = analyze_project_query(
        "SELECT id, upper(name) AS display_name FROM upstream(source => 'warehouse', table => 'orders') WHERE active AND id > 7",
        "cdf/analytics/orders.cdf.sql",
        &schema(),
        Vec::new(),
    )
    .unwrap();

    assert!(query.relational_plan.filter.is_some());
    assert_eq!(query.relational_plan.projection.len(), 2);
    assert_eq!(
        query.output_schema.to_arrow().unwrap().field(0).name(),
        "id"
    );
    assert_eq!(
        query.output_schema.to_arrow().unwrap().field(1).name(),
        "display_name"
    );
    query.relational_plan.validate_recorded().unwrap();
}

#[test]
fn project_query_rejects_non_data_arguments_and_relational_expansion() {
    for sql in [
        "SELECT id FROM upstream(source => env('SOURCE'), table => 'orders')",
        "SELECT id FROM upstream(source => 'warehouse', table => concat('ord', 'ers'))",
        "SELECT id FROM upstream(source => 'warehouse') UNION ALL SELECT id FROM upstream(source => 'warehouse')",
        "SELECT count(*) FROM upstream(source => 'warehouse')",
        "WITH q AS (SELECT id FROM upstream(source => 'warehouse')) SELECT * FROM q",
    ] {
        let error =
            analyze_project_query(sql, "cdf/analytics/orders.cdf.sql", &schema(), Vec::new())
                .unwrap_err();
        assert!(error.message.contains("CDF-SQL"), "{error:?}");
    }
}

#[test]
fn project_query_requires_one_literal_source_and_named_arrow_arguments() {
    for sql in [
        "SELECT id FROM upstream(table => 'orders')",
        "SELECT id FROM upstream(source => 'warehouse', source => 'replica')",
        "SELECT id FROM upstream('warehouse')",
        "SELECT id FROM upstream(source = 'warehouse')",
        "SELECT id FROM orders",
        "SELECT id FROM upstream(source => \"warehouse\")",
    ] {
        let error = parse_project_query(sql, "cdf/analytics/orders.cdf.sql").unwrap_err();
        assert!(error.message.contains("CDF-SQL"), "{error:?}");
    }
}

#[test]
fn project_query_rejects_multiple_statements_and_non_query_statements() {
    for sql in [
        "SELECT id FROM upstream(source => 'warehouse'); SELECT 1",
        "CREATE TABLE resource AS SELECT id FROM upstream(source => 'warehouse')",
        "DELETE FROM orders",
    ] {
        let error = parse_project_query(sql, "cdf/analytics/orders.cdf.sql").unwrap_err();
        assert!(error.message.contains("CDF-SQL"), "{error:?}");
    }
}
