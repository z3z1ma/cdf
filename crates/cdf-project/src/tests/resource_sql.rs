use cdf_declarative::{
    DrainTerminationDeclaration, EpochClosureDeclaration, ExecutionDeclaration, LateDataDeclaration,
};

use super::*;

#[test]
fn resource_file_accepts_bare_select_as_normal_form() {
    let parsed = parse_resource_file(
        "-- resource\nSELECT * FROM upstream(source => 'warehouse', table => 'orders')",
        "cdf/analytics/orders.cdf.sql",
    )
    .unwrap();

    assert_eq!(parsed.form, AuthoredResourceForm::BareSelect);
    assert_eq!(parsed.query_span.start_line, 2);
    assert!(parsed.query_sql.starts_with("SELECT"));
    assert_eq!(parsed.envelope, AuthoredResourceEnvelope::default());
}

#[test]
fn resource_file_parses_ordered_metadata_envelope() {
    let parsed = parse_resource_file(
        "RESOURCE\nTARGET warehouse.orders\nDISPOSITION MERGE(id, tenant_id)\nCURSOR id\nTRUST GOVERNED\nSEMANTICS (id => 'example.identifier@1', tenant_id => 'example.tenant@1')\nEXECUTION BOUNDED\nAS\nSELECT id, tenant_id FROM upstream(source => 'warehouse', table => 'orders')",
        "cdf/analytics/orders.cdf.sql",
    )
    .unwrap();

    assert_eq!(parsed.form, AuthoredResourceForm::ResourceEnvelope);
    assert_eq!(
        parsed.envelope.target.unwrap().value.as_str(),
        "warehouse.orders"
    );
    let AuthoredDisposition::Merge { keys } = parsed.envelope.disposition.unwrap().value else {
        panic!("expected merge");
    };
    assert_eq!(
        keys.into_iter().map(|key| key.value).collect::<Vec<_>>(),
        ["id", "tenant_id"]
    );
    assert_eq!(parsed.envelope.trust.unwrap().value, TrustPreset::Governed);
    assert_eq!(parsed.envelope.semantics.len(), 2);
    assert_eq!(
        parsed.envelope.execution.unwrap().value,
        ExecutionDeclaration::Bounded
    );
    assert_eq!(parsed.query_span.start_line, 9);
}

#[test]
fn resource_file_parses_complete_drain_policy() {
    let parsed = parse_resource_file(
        "RESOURCE\nEXECUTION DRAIN (\n CHECKPOINT ROWS 100000,\n PACKAGE BYTES 67108864,\n UNTIL DURATION MILLISECONDS 60000,\n WATERMARK DISABLED,\n LATE DATA QUARANTINE,\n SAFE FRONTIER CANONICAL ADMITTED SOURCE POSITION\n)\nAS SELECT * FROM upstream(source => 'events', table => 'activity')",
        "cdf/analytics/activity.cdf.sql",
    )
    .unwrap();

    let ExecutionDeclaration::Drain {
        checkpoint_cadence,
        package_rotation,
        termination,
        late_data,
        ..
    } = parsed.envelope.execution.unwrap().value
    else {
        panic!("expected drain");
    };
    assert_eq!(
        checkpoint_cadence,
        EpochClosureDeclaration::Rows { count: 100_000 }
    );
    assert_eq!(
        package_rotation,
        EpochClosureDeclaration::Bytes { count: 67_108_864 }
    );
    assert_eq!(
        *termination,
        DrainTerminationDeclaration::Duration {
            milliseconds: 60_000
        }
    );
    assert_eq!(late_data, LateDataDeclaration::Quarantine);
}

#[test]
fn resource_file_rejects_ids_repeated_out_of_order_and_retired_ddl() {
    for sql in [
        "RESOURCE analytics.orders AS SELECT * FROM upstream(source => 'warehouse')",
        "RESOURCE TRUST GOVERNED TARGET warehouse.orders AS SELECT * FROM upstream(source => 'warehouse')",
        "RESOURCE TRUST GOVERNED TRUST EXPERIMENTAL AS SELECT * FROM upstream(source => 'warehouse')",
        "CREATE RESOURCE analytics.orders AS SELECT * FROM upstream(source => 'warehouse')",
    ] {
        let error = parse_resource_file(sql, "cdf/analytics/orders.cdf.sql").unwrap_err();
        assert!(error.message.contains("CDF-D3-RESOURCE"), "{error:?}");
    }
}

#[test]
fn resource_file_rejects_empty_duplicate_merge_and_semantic_bindings() {
    for sql in [
        "RESOURCE DISPOSITION MERGE() AS SELECT * FROM upstream(source => 'warehouse')",
        "RESOURCE DISPOSITION MERGE(id, id) AS SELECT * FROM upstream(source => 'warehouse')",
        "RESOURCE SEMANTICS () AS SELECT * FROM upstream(source => 'warehouse')",
        "RESOURCE SEMANTICS (id => 'a@1', id => 'b@1') AS SELECT * FROM upstream(source => 'warehouse')",
    ] {
        let error = parse_resource_file(sql, "cdf/analytics/orders.cdf.sql").unwrap_err();
        assert!(error.message.contains("CDF-D3-RESOURCE"), "{error:?}");
    }
}

#[test]
fn resource_file_rejects_incomplete_or_zero_drain_policy() {
    for sql in [
        "RESOURCE EXECUTION DRAIN (CHECKPOINT ROWS 0, PACKAGE BYTES 1, UNTIL QUIESCENT, WATERMARK DISABLED, LATE DATA QUARANTINE, SAFE FRONTIER CANONICAL ADMITTED SOURCE POSITION) AS SELECT * FROM upstream(source => 'events')",
        "RESOURCE EXECUTION DRAIN (CHECKPOINT ROWS 1, PACKAGE BYTES 1, UNTIL QUIESCENT, LATE DATA QUARANTINE, SAFE FRONTIER CANONICAL ADMITTED SOURCE POSITION) AS SELECT * FROM upstream(source => 'events')",
    ] {
        let error = parse_resource_file(sql, "cdf/analytics/activity.cdf.sql").unwrap_err();
        assert!(error.message.contains("CDF-D3-RESOURCE"), "{error:?}");
    }
}
