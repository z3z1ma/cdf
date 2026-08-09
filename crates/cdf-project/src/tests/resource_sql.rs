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
        "RESOURCE\nTARGET warehouse.orders\nROUTE BY tenant_id MAX TARGETS 256\nDISPOSITION MERGE(id, tenant_id)\nCURSOR id\nTRUST GOVERNED\nSEMANTICS (id => 'example.identifier@1', tenant_id => 'example.tenant@1')\nEXECUTION BOUNDED\nAS\nSELECT id, tenant_id FROM upstream(source => 'warehouse', table => 'orders')",
        "cdf/analytics/orders.cdf.sql",
    )
    .unwrap();

    assert_eq!(parsed.form, AuthoredResourceForm::ResourceEnvelope);
    assert_eq!(
        parsed.envelope.target.unwrap().value.as_str(),
        "warehouse.orders"
    );
    let route = parsed.envelope.route.unwrap().value;
    assert_eq!(route.field.value, "tenant_id");
    assert_eq!(route.maximum_targets, 256);
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
    assert_eq!(parsed.query_span.start_line, 10);
}

#[test]
fn resource_file_requires_a_positive_bounded_route_ceiling() {
    for sql in [
        "RESOURCE ROUTE BY tenant_id MAX TARGETS 0 AS SELECT tenant_id FROM upstream(source => 'warehouse')",
        "RESOURCE ROUTE BY tenant_id MAX TARGETS 4294967296 AS SELECT tenant_id FROM upstream(source => 'warehouse')",
        "RESOURCE ROUTE tenant_id MAX TARGETS 10 AS SELECT tenant_id FROM upstream(source => 'warehouse')",
        "RESOURCE ROUTE BY tenant_id TARGETS 10 AS SELECT tenant_id FROM upstream(source => 'warehouse')",
    ] {
        let error = parse_resource_file(sql, "cdf/analytics/orders.cdf.sql").unwrap_err();
        assert!(
            error.message.contains("CDF-RESOURCE-ROUTE"),
            "unexpected error: {error:?}"
        );
    }
}

#[test]
fn resource_file_accepts_every_current_trust_preset() {
    for (authored, expected) in [
        ("EXPERIMENTAL", TrustPreset::Experimental),
        ("GOVERNED", TrustPreset::Governed),
        ("FINANCIAL", TrustPreset::Financial),
        ("SERVING", TrustPreset::Serving),
    ] {
        let parsed = parse_resource_file(
            &format!(
                "RESOURCE\nTRUST {authored}\nAS\nSELECT * FROM upstream(source => 'warehouse')"
            ),
            "cdf/analytics/orders.cdf.sql",
        )
        .unwrap();
        assert_eq!(parsed.envelope.trust.unwrap().value, expected);
    }
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
fn resource_file_rejects_ids_and_repeated_or_out_of_order_clauses() {
    for sql in [
        "RESOURCE analytics.orders AS SELECT * FROM upstream(source => 'warehouse')",
        "RESOURCE TRUST GOVERNED TARGET warehouse.orders AS SELECT * FROM upstream(source => 'warehouse')",
        "RESOURCE TRUST GOVERNED TRUST EXPERIMENTAL AS SELECT * FROM upstream(source => 'warehouse')",
    ] {
        let error = parse_resource_file(sql, "cdf/analytics/orders.cdf.sql").unwrap_err();
        assert!(error.message.contains("CDF-RESOURCE"), "{error:?}");
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
        assert!(error.message.contains("CDF-RESOURCE"), "{error:?}");
    }
}

#[test]
fn resource_file_parses_cdc_apply_and_each_delete_policy() {
    for (authored, expected) in [
        ("DELETE HARD", AuthoredDeletePolicy::Hard),
        ("DELETE IGNORE", AuthoredDeletePolicy::Ignore),
    ] {
        let parsed = parse_resource_file(
            &format!(
                "RESOURCE DISPOSITION CDC_APPLY(id, tenant_id) {authored} AS SELECT id, tenant_id FROM upstream(source => 'warehouse')"
            ),
            "cdf/analytics/orders.cdf.sql",
        )
        .unwrap();
        let AuthoredDisposition::CdcApply { keys } = parsed.envelope.disposition.unwrap().value
        else {
            panic!("expected cdc_apply");
        };
        assert_eq!(
            keys.into_iter().map(|key| key.value).collect::<Vec<_>>(),
            ["id", "tenant_id"]
        );
        assert_eq!(parsed.envelope.delete.unwrap().value, expected);
    }

    let parsed = parse_resource_file(
        "RESOURCE DISPOSITION CDC_APPLY(id) DELETE SOFT(is_deleted) AS SELECT id, is_deleted FROM upstream(source => 'warehouse')",
        "cdf/analytics/orders.cdf.sql",
    )
    .unwrap();
    let AuthoredDeletePolicy::Soft { marker_field } = parsed.envelope.delete.unwrap().value else {
        panic!("expected soft delete");
    };
    assert_eq!(marker_field.value, "is_deleted");
}

#[test]
fn resource_file_rejects_malformed_cdc_apply_and_delete_policy() {
    for sql in [
        "RESOURCE DISPOSITION CDC_APPLY() DELETE HARD AS SELECT id FROM upstream(source => 'warehouse')",
        "RESOURCE DISPOSITION CDC_APPLY(id, id) DELETE HARD AS SELECT id FROM upstream(source => 'warehouse')",
        "RESOURCE DISPOSITION CDC_APPLY(id) DELETE SOFT() AS SELECT id FROM upstream(source => 'warehouse')",
        "RESOURCE DISPOSITION CDC_APPLY(id) DELETE SOMETIMES AS SELECT id FROM upstream(source => 'warehouse')",
    ] {
        let error = parse_resource_file(sql, "cdf/analytics/orders.cdf.sql").unwrap_err();
        assert!(error.message.contains("CDF-RESOURCE"), "{error:?}");
    }
}

#[test]
fn resource_file_rejects_incomplete_or_zero_drain_policy() {
    for sql in [
        "RESOURCE EXECUTION DRAIN (CHECKPOINT ROWS 0, PACKAGE BYTES 1, UNTIL QUIESCENT, WATERMARK DISABLED, LATE DATA QUARANTINE, SAFE FRONTIER CANONICAL ADMITTED SOURCE POSITION) AS SELECT * FROM upstream(source => 'events')",
        "RESOURCE EXECUTION DRAIN (CHECKPOINT ROWS 1, PACKAGE BYTES 1, UNTIL QUIESCENT, LATE DATA QUARANTINE, SAFE FRONTIER CANONICAL ADMITTED SOURCE POSITION) AS SELECT * FROM upstream(source => 'events')",
    ] {
        let error = parse_resource_file(sql, "cdf/analytics/activity.cdf.sql").unwrap_err();
        assert!(error.message.contains("CDF-RESOURCE"), "{error:?}");
    }
}

#[test]
fn drain_policy_accepts_an_optional_transaction_limit_bytes() {
    let parsed = parse_resource_file(
        "RESOURCE\nEXECUTION DRAIN (\n CHECKPOINT ROWS 100000,\n PACKAGE BYTES 67108864,\n UNTIL DURATION MILLISECONDS 60000,\n WATERMARK DISABLED,\n LATE DATA QUARANTINE,\n SAFE FRONTIER CANONICAL ADMITTED SOURCE POSITION,\n TRANSACTION LIMIT BYTES 268435456\n)\nAS SELECT * FROM upstream(source => 'events', table => 'activity')",
        "cdf/analytics/activity.cdf.sql",
    )
    .unwrap();

    let ExecutionDeclaration::Drain {
        transaction_limit_bytes,
        ..
    } = parsed.envelope.execution.unwrap().value
    else {
        panic!("expected drain");
    };
    assert_eq!(transaction_limit_bytes, Some(268_435_456));
}

#[test]
fn drain_policy_without_transaction_limit_bytes_declares_none() {
    let parsed = parse_resource_file(
        "RESOURCE\nEXECUTION DRAIN (\n CHECKPOINT ROWS 100000,\n PACKAGE BYTES 67108864,\n UNTIL DURATION MILLISECONDS 60000,\n WATERMARK DISABLED,\n LATE DATA QUARANTINE,\n SAFE FRONTIER CANONICAL ADMITTED SOURCE POSITION\n)\nAS SELECT * FROM upstream(source => 'events', table => 'activity')",
        "cdf/analytics/activity.cdf.sql",
    )
    .unwrap();

    let ExecutionDeclaration::Drain {
        transaction_limit_bytes,
        ..
    } = parsed.envelope.execution.unwrap().value
    else {
        panic!("expected drain");
    };
    assert_eq!(
        transaction_limit_bytes, None,
        "absent must stay distinct from a declared value"
    );
}

#[test]
fn transaction_limit_bytes_rejects_zero_at_its_token() {
    let error = parse_resource_file(
        "RESOURCE\nEXECUTION DRAIN (\n CHECKPOINT ROWS 100000,\n PACKAGE BYTES 67108864,\n UNTIL DURATION MILLISECONDS 60000,\n WATERMARK DISABLED,\n LATE DATA QUARANTINE,\n SAFE FRONTIER CANONICAL ADMITTED SOURCE POSITION,\n TRANSACTION LIMIT BYTES 0\n)\nAS SELECT * FROM upstream(source => 'events', table => 'activity')",
        "cdf/analytics/activity.cdf.sql",
    )
    .unwrap_err();
    assert!(
        error.message.contains("greater than zero"),
        "unexpected message: {}",
        error.message
    );
}
