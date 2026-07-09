use super::*;
use arrow_schema::{DataType, TimeUnit};
use cdf_conformance::destination::{
    DestinationConformanceCase, assert_destination_conformance, representative_commit_request,
};
use cdf_kernel::{
    CheckpointId, CursorPosition, CursorValue, PartitionId, PipelineId, ResourceId, ScopeKey,
    SegmentId, SourcePosition,
};

fn columns() -> Vec<PostgresColumn> {
    vec![
        PostgresColumn::new("id", "BIGINT", false).unwrap(),
        PostgresColumn::new("name", "TEXT", true).unwrap(),
        PostgresColumn::new("amount", "NUMERIC(12,2)", true).unwrap(),
    ]
}

#[test]
fn catalog_schema_maps_supported_postgres_types_to_runtime_arrow_subset() {
    let schema = crate::catalog::schema_from_catalog_columns(
        &ResourceId::new("warehouse.orders").unwrap(),
        vec![
            crate::catalog::PostgresCatalogColumn {
                name: "VendorID".to_owned(),
                observed_type: "integer".to_owned(),
                nullable: false,
            },
            crate::catalog::PostgresCatalogColumn {
                name: "is_active".to_owned(),
                observed_type: "boolean".to_owned(),
                nullable: true,
            },
            crate::catalog::PostgresCatalogColumn {
                name: "ratio".to_owned(),
                observed_type: "double precision".to_owned(),
                nullable: false,
            },
            crate::catalog::PostgresCatalogColumn {
                name: "customer_uuid".to_owned(),
                observed_type: "uuid".to_owned(),
                nullable: true,
            },
            crate::catalog::PostgresCatalogColumn {
                name: "service_date".to_owned(),
                observed_type: "date".to_owned(),
                nullable: false,
            },
            crate::catalog::PostgresCatalogColumn {
                name: "created_at".to_owned(),
                observed_type: "timestamp without time zone".to_owned(),
                nullable: true,
            },
            crate::catalog::PostgresCatalogColumn {
                name: "updated_at".to_owned(),
                observed_type: "timestamp with time zone".to_owned(),
                nullable: false,
            },
        ],
    )
    .unwrap();

    let fields = schema.fields();
    assert_eq!(fields[0].data_type(), &DataType::Int64);
    assert!(!fields[0].is_nullable());
    assert_eq!(fields[0].metadata()["cdf:physical_type"], "integer");
    assert_eq!(fields[1].data_type(), &DataType::Boolean);
    assert!(fields[1].is_nullable());
    assert_eq!(fields[2].data_type(), &DataType::Float64);
    assert_eq!(fields[3].data_type(), &DataType::Utf8);
    assert_eq!(fields[4].data_type(), &DataType::Date32);
    assert_eq!(
        fields[5].data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        fields[6].data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
}

#[test]
fn catalog_schema_rejects_unsupported_type_with_resource_column_and_remediation() {
    let error = crate::catalog::schema_from_catalog_columns(
        &ResourceId::new("warehouse.orders").unwrap(),
        vec![crate::catalog::PostgresCatalogColumn {
            name: "amount".to_owned(),
            observed_type: "numeric".to_owned(),
            nullable: true,
        }],
    )
    .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("warehouse.orders"));
    assert!(message.contains("amount"));
    assert!(message.contains("numeric"));
    assert!(message.contains("not yet supported by the Postgres discovery/execution slice"));
}

fn segment(id: &str, rows: u64) -> StateSegment {
    StateSegment {
        segment_id: SegmentId::new(id).unwrap(),
        scope: ScopeKey::Partition {
            partition_id: PartitionId::new("p0").unwrap(),
        },
        output_position: SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "updated_at".to_owned(),
            value: CursorValue::I64(10),
        }),
        row_count: rows,
        byte_count: rows * 16,
    }
}

fn input(disposition: WriteDisposition, dedup: MergeDedupPolicy) -> PostgresLoadPlanInput {
    let segments = vec![segment("seg-0001", 3), segment("seg-0002", 2)];
    let state_delta = StateDelta {
        checkpoint_id: CheckpointId::new("chk-1").unwrap(),
        pipeline_id: PipelineId::new("pipe-1").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope: ScopeKey::Partition {
            partition_id: PartitionId::new("p0").unwrap(),
        },
        state_version: 1,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "updated_at".to_owned(),
            value: CursorValue::I64(10),
        }),
        package_hash: PackageHash::new("sha256:abcdef0123456789").unwrap(),
        schema_hash: SchemaHash::new("sha256:schema").unwrap(),
        segments: segments.clone(),
    };
    PostgresLoadPlanInput {
        package_hash: PackageHash::new("sha256:abcdef0123456789").unwrap(),
        idempotency_token: IdempotencyToken::new("sha256:abcdef0123456789").unwrap(),
        target: PostgresTarget::parse("raw.orders").unwrap(),
        disposition,
        schema_hash: SchemaHash::new("sha256:schema").unwrap(),
        segments,
        columns: columns(),
        merge_keys: vec![PostgresIdentifier::user("id").unwrap()],
        dedup,
        existing_table: None,
        resource_id: Some(ResourceId::new("orders").unwrap()),
        state_delta: Some(state_delta),
    }
}

fn conformance_case(
    destination: &PostgresDestination,
    disposition: WriteDisposition,
) -> DestinationConformanceCase {
    let request = representative_commit_request(disposition);
    let migrations = destination.plan_commit(&request).unwrap().migrations;
    assert!(
        migrations
            .iter()
            .any(|migration| migration.migration_id == "postgres.create_cdf_loads")
    );
    assert!(
        migrations
            .iter()
            .any(|migration| migration.migration_id == "postgres.create_cdf_state")
    );
    assert!(
        migrations
            .iter()
            .any(|migration| { migration.migration_id == "postgres.create_cdf_quarantine" })
    );
    DestinationConformanceCase::new(request).with_expected_migrations(migrations)
}

#[test]
fn sheet_declares_postgres_capabilities_and_full_mapping_fidelity() {
    let destination = PostgresDestination::new();
    let sheet = destination.postgres_sheet();

    assert_eq!(sheet.kernel.destination.as_str(), POSTGRES_DESTINATION_ID);
    assert_eq!(sheet.kernel.transactions, TransactionSupport::AtomicPackage);
    assert_eq!(sheet.kernel.idempotency, IdempotencySupport::PackageToken);
    assert_eq!(sheet.kernel.identifier_rules.max_length, Some(63));
    assert!(
        sheet
            .kernel
            .supported_dispositions
            .contains(&WriteDisposition::Merge)
    );

    let decimal = sheet
        .type_mappings
        .iter()
        .find(|mapping| mapping.arrow_type == "Decimal128(p,s)")
        .unwrap();
    assert_eq!(decimal.fidelity, PostgresTypeFidelity::Exact);

    let nested = sheet
        .type_mappings
        .iter()
        .find(|mapping| mapping.arrow_type == "Struct")
        .unwrap();
    assert_eq!(
        nested.fidelity,
        PostgresTypeFidelity::LossyRequiresContractAllowance
    );

    let dictionary = sheet
        .type_mappings
        .iter()
        .find(|mapping| mapping.arrow_type == "Dictionary")
        .unwrap();
    assert_eq!(dictionary.fidelity, PostgresTypeFidelity::Unsupported);
}

#[test]
fn reusable_destination_conformance_suite_accepts_postgres_sheet_and_plans() {
    let destination = PostgresDestination::new();

    assert_destination_conformance(
        &destination,
        [
            conformance_case(&destination, WriteDisposition::Append),
            conformance_case(&destination, WriteDisposition::Replace),
            conformance_case(&destination, WriteDisposition::Merge),
        ],
    );
}

#[test]
fn identifiers_quote_safely_and_reject_reserved_user_names() {
    assert_eq!(quote_identifier("User Name").unwrap(), "\"User Name\"");
    assert_eq!(quote_identifier("a\"b").unwrap(), "\"a\"\"b\"");

    let long = "a".repeat(64);
    assert!(PostgresIdentifier::user(long).is_err());
    assert!(PostgresIdentifier::user("_cdf_load").is_err());
    assert!(PostgresColumn::new("id", "TEXT; DROP TABLE x", false).is_err());
}

#[test]
fn append_replace_and_merge_plans_include_transactional_sql() {
    let destination = PostgresDestination::new();
    let append = destination
        .plan_load(input(WriteDisposition::Append, MergeDedupPolicy::Last))
        .unwrap();
    assert!(
        append
            .system_ddl
            .iter()
            .any(|statement| statement.sql.contains(CDF_LOADS_TABLE))
    );
    assert!(
        append.target_ddl[0]
            .sql
            .contains("CREATE TABLE IF NOT EXISTS")
    );
    assert!(
        append
            .write_sql
            .iter()
            .any(|statement| statement.sql.contains("INSERT INTO \"raw\".\"orders\""))
    );
    assert_eq!(
        append.kernel.delivery_guarantee,
        DeliveryGuarantee::EffectivelyOncePerPackage
    );

    let replace = destination
        .plan_load(input(WriteDisposition::Replace, MergeDedupPolicy::Last))
        .unwrap();
    assert!(
        replace
            .write_sql
            .iter()
            .any(|statement| statement.sql == "TRUNCATE TABLE \"raw\".\"orders\"")
    );
    assert_eq!(
        replace.kernel.delivery_guarantee,
        DeliveryGuarantee::EffectivelyOncePerTarget
    );

    let merge = destination
        .plan_load(input(WriteDisposition::Merge, MergeDedupPolicy::Last))
        .unwrap();
    let merge_sql = merge
        .write_sql
        .iter()
        .find(|statement| statement.name == "merge_from_stage")
        .unwrap()
        .sql
        .as_str();
    assert!(merge_sql.contains("ROW_NUMBER() OVER"));
    assert!(merge_sql.contains("ORDER BY \"_cdf_segment\" DESC, \"_cdf_row\" DESC"));
    assert!(merge_sql.contains("ON CONFLICT (\"id\") DO UPDATE SET"));
    assert_eq!(
        merge.kernel.delivery_guarantee,
        DeliveryGuarantee::EffectivelyOncePerKey
    );

    let transaction_names = merge
        .transactional_statements()
        .into_iter()
        .map(|statement| statement.name)
        .collect::<Vec<_>>();
    assert_eq!(transaction_names.first().unwrap(), "begin");
    assert_eq!(transaction_names.last().unwrap(), "commit");
}

#[test]
fn merge_dedup_policy_is_explicit_and_can_fail_on_duplicates() {
    let destination = PostgresDestination::new();
    let first = destination
        .plan_load(input(WriteDisposition::Merge, MergeDedupPolicy::First))
        .unwrap();
    let first_sql = first
        .write_sql
        .iter()
        .find(|statement| statement.name == "merge_from_stage")
        .unwrap()
        .sql
        .as_str();
    assert!(first_sql.contains("ORDER BY \"_cdf_segment\" ASC, \"_cdf_row\" ASC"));

    let fail = destination
        .plan_load(input(WriteDisposition::Merge, MergeDedupPolicy::Fail))
        .unwrap();
    let guard = fail
        .write_sql
        .iter()
        .find(|statement| statement.name == "merge_duplicate_key_guard")
        .unwrap();
    assert_eq!(guard.expectation, StatementExpectation::ReturnsZeroRows);
    assert!(guard.sql.contains("HAVING COUNT(*) > 1"));
}

#[test]
fn receipt_contains_postgres_xid_verify_clause_and_segment_acks() {
    let destination = PostgresDestination::new();
    let plan = destination
        .plan_load(input(WriteDisposition::Merge, MergeDedupPolicy::Last))
        .unwrap();
    let receipt = build_receipt(
        &plan,
        PostgresReceiptInput {
            receipt_id: ReceiptId::new("receipt-1").unwrap(),
            xid: "123456".to_owned(),
            committed_at_ms: 1_788_000_000_000,
            counts: CommitCounts {
                rows_written: 5,
                rows_inserted: Some(3),
                rows_updated: Some(2),
                rows_deleted: Some(0),
            },
            duplicate: false,
        },
    )
    .unwrap();

    let transaction = receipt.transaction.unwrap();
    assert_eq!(transaction.system, POSTGRES_DESTINATION_ID);
    assert_eq!(transaction.values.get("xid"), Some(&"123456".to_owned()));
    assert_eq!(
        transaction.values.get("quarantine_table"),
        Some(&CDF_QUARANTINE_TABLE.to_owned())
    );
    assert_eq!(receipt.verify.kind, "postgres_sql");
    assert!(receipt.verify.statement.contains(CDF_LOADS_TABLE));
    assert_eq!(receipt.segment_acks.len(), 2);
    assert_eq!(receipt.counts.rows_written, 5);
}

#[test]
fn mirror_and_drift_hooks_expose_load_and_state_tables() {
    let destination = PostgresDestination::new();
    let plan = destination
        .plan_load(input(WriteDisposition::Append, MergeDedupPolicy::Last))
        .unwrap();

    assert!(
        plan.mirror_sql
            .iter()
            .any(|statement| statement.sql.contains(CDF_LOADS_TABLE))
    );
    assert!(
        plan.mirror_sql
            .iter()
            .any(|statement| statement.sql.contains(CDF_STATE_TABLE))
    );
    assert!(
        plan.mirror_sql
            .iter()
            .any(|statement| statement.sql.contains(CDF_QUARANTINE_TABLE))
    );
    assert!(plan.drift.load_for_package.sql.contains(CDF_LOADS_TABLE));
    assert!(plan.drift.state_for_scope.sql.contains(CDF_STATE_TABLE));
    assert_eq!(
        plan.drift.state_heads.expectation,
        StatementExpectation::ReturnsMirrorRows
    );
}

#[test]
fn existing_table_migrations_add_only_safe_missing_columns() {
    let destination = PostgresDestination::new();
    let mut nullable_add = input(WriteDisposition::Append, MergeDedupPolicy::Last);
    nullable_add.existing_table = Some(
        PostgresExistingTable::new(
            vec![PostgresExistingColumn::new("id", "BIGINT", false).unwrap()],
            vec![],
        )
        .unwrap(),
    );
    let plan = destination.plan_load(nullable_add).unwrap();
    assert!(plan.target_ddl.iter().any(
        |statement| statement.sql == "ALTER TABLE \"raw\".\"orders\" ADD COLUMN \"name\" TEXT"
    ));
    assert!(
        plan.target_ddl.iter().any(|statement| statement.sql
            == "ALTER TABLE \"raw\".\"orders\" ADD COLUMN \"_cdf_load\" TEXT")
    );

    let mut unsafe_add = input(WriteDisposition::Append, MergeDedupPolicy::Last);
    unsafe_add.existing_table = Some(PostgresExistingTable::new(Vec::new(), vec![]).unwrap());
    assert!(destination.plan_load(unsafe_add).is_err());
}

#[test]
fn source_exercise_hooks_require_deterministic_ordering() {
    let target = PostgresTarget::parse("raw.orders").unwrap();
    let columns = vec![
        PostgresIdentifier::user("id").unwrap(),
        PostgresIdentifier::user("name").unwrap(),
    ];
    assert!(source_exercise_hooks(&target, &columns, &[], None).is_err());

    let hooks = source_exercise_hooks(
        &target,
        &columns,
        &[PostgresIdentifier::user("id").unwrap()],
        Some(&PostgresIdentifier::user("updated_at").unwrap()),
    )
    .unwrap();
    assert_eq!(
        hooks.snapshot_page.sql,
        "SELECT \"id\", \"name\" FROM \"raw\".\"orders\" ORDER BY \"id\" LIMIT $1 OFFSET $2"
    );
    assert!(
        hooks
            .incremental_page
            .unwrap()
            .sql
            .contains("WHERE \"updated_at\" > $1")
    );
}

#[test]
fn merge_requires_keys_and_rejects_existing_key_drift() {
    let destination = PostgresDestination::new();
    let mut no_keys = input(WriteDisposition::Merge, MergeDedupPolicy::Last);
    no_keys.merge_keys.clear();
    assert!(destination.plan_load(no_keys).is_err());

    let mut drift = input(WriteDisposition::Merge, MergeDedupPolicy::Last);
    drift.existing_table = Some(
        PostgresExistingTable::new(
            vec![
                PostgresExistingColumn::new("id", "BIGINT", false).unwrap(),
                PostgresExistingColumn::new("name", "TEXT", true).unwrap(),
                PostgresExistingColumn::new("amount", "NUMERIC(12,2)", true).unwrap(),
            ],
            vec!["name"],
        )
        .unwrap(),
    );
    assert!(destination.plan_load(drift).is_err());
}
