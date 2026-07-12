use super::*;
use crate::ddl::target_migrations;
use arrow_schema::{DataType, Field, Schema};
use cdf_conformance::destination::{
    DestinationConformanceCase, DestinationCorrectionConformanceEvidence,
    assert_destination_conformance, assert_destination_correction_conformance,
    representative_commit_request,
};
use cdf_kernel::{
    CanonicalArrowField, CheckpointId, CursorPosition, CursorValue, DestinationCorrectionOperation,
    DestinationCorrectionPlan, DestinationCorrectionRequest, PartitionId, PipelineId, PromotionId,
    ResidualCorrectionOperation, ResourceId, RowProvenanceAddress, ScopeKey, SegmentId,
    SourcePosition,
};

fn columns() -> Vec<PostgresColumn> {
    vec![
        PostgresColumn::new("id", "BIGINT", false).unwrap(),
        PostgresColumn::new("name", "TEXT", true).unwrap(),
        PostgresColumn::new("amount", "NUMERIC(12,2)", true).unwrap(),
    ]
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

fn zero_data_input(disposition: WriteDisposition) -> PostgresLoadPlanInput {
    let mut input = input(disposition, MergeDedupPolicy::Last);
    input.segments.clear();
    if let Some(state_delta) = &mut input.state_delta {
        state_delta.segments.clear();
    }
    input
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
    let corrections = destination.protocol_capabilities().corrections;
    assert_eq!(
        corrections.row_provenance.persistence,
        CapabilitySupport::Supported
    );
    assert_eq!(
        corrections.row_provenance.targetability,
        CapabilitySupport::Supported
    );
    assert_eq!(corrections.residual_readback, CapabilitySupport::Supported);
    assert_eq!(corrections.strategies.len(), 1);
    assert_eq!(
        corrections.strategies[0].strategy,
        CorrectionStrategy::InPlaceUpdate
    );
    let artifact = destination.sheet_artifact().unwrap();
    assert_eq!(artifact.protocol_capabilities.corrections, corrections);
    assert!(
        serde_json::to_string(&artifact)
            .unwrap()
            .contains("\"corrections\"")
    );

    let append_input = input(WriteDisposition::Append, MergeDedupPolicy::Last);
    let create_target = target_migrations(&append_input)
        .unwrap()
        .into_iter()
        .find(|statement| statement.name == "create_target")
        .unwrap();
    assert!(create_target.sql.contains(CDF_ROW_KEY_COLUMN));
    assert!(create_target.sql.contains("UNIQUE (\"_cdf_row_key\")"));
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
    assert_destination_correction_conformance(
        &destination,
        &DestinationCorrectionConformanceEvidence {
            row_provenance_persistence: CapabilitySupport::Supported,
            row_provenance_targetability: CapabilitySupport::Supported,
            residual_readback: CapabilitySupport::Supported,
            strategies: vec![CorrectionStrategyCapability::new(
                CorrectionStrategy::InPlaceUpdate,
                TransactionSupport::AtomicPackage,
                IdempotencySupport::PackageToken,
            )],
        },
    );
}

fn correction_existing_table(nullable_provenance: bool) -> PostgresExistingTable {
    let mut columns = BTreeMap::new();
    for (name, data_type, nullable) in [
        ("id", "BIGINT", false),
        ("name", "TEXT", true),
        ("_cdf_variant", "TEXT", true),
        (CDF_ROW_KEY_COLUMN, "BIGINT", nullable_provenance),
        (CDF_LOADED_AT_COLUMN, "BIGINT", false),
    ] {
        columns.insert(
            name.to_owned(),
            PostgresExistingColumn {
                name: PostgresIdentifier::system(name).unwrap(),
                data_type: data_type.to_owned(),
                nullable,
            },
        );
    }
    PostgresExistingTable {
        columns,
        primary_key: Vec::new(),
    }
}

fn correction_operation_for_test(path: &str, output: &str) -> DestinationCorrectionOperation {
    let value = arrow_array::Int64Array::from(vec![42_i64]);
    let exact = cdf_contract::encode_residual_json_v1([cdf_contract::ResidualFieldRef::new(
        [path.trim_start_matches('/')],
        &value,
        0,
    )
    .unwrap()])
    .unwrap();
    DestinationCorrectionOperation {
        correction: DestinationCorrectionPlan {
            request: DestinationCorrectionRequest {
                promotion_id: PromotionId::new("promotion-test").unwrap(),
                original_row: RowProvenanceAddress::new(
                    PackageHash::new("sha256:original").unwrap(),
                    SegmentId::new("seg-000001").unwrap(),
                    0,
                ),
                old_schema_hash: SchemaHash::new("sha256:old").unwrap(),
                new_schema_hash: SchemaHash::new("sha256:new").unwrap(),
                promoted_path: path.to_owned(),
                promoted_value_json: "42".to_owned(),
                residual_operation: ResidualCorrectionOperation::RemovePromotedPath,
                selected_strategy: CorrectionStrategy::InPlaceUpdate,
            },
            transaction_guarantee: TransactionSupport::AtomicPackage,
            idempotency_guarantee: IdempotencySupport::PackageToken,
        },
        output_field: CanonicalArrowField::from_arrow(&Field::new(output, DataType::Int64, true))
            .unwrap(),
        promoted_value_residual_json_v1: exact,
    }
}

fn correction_request_for_test() -> DestinationCorrectionCommitRequest {
    DestinationCorrectionCommitRequest::new(
        PackageHash::new("sha256:correction").unwrap(),
        IdempotencyToken::new("sha256:correction").unwrap(),
        TargetName::new("raw.orders").unwrap(),
        WriteDisposition::Append,
        vec![segment("seg-correction", 1)],
        vec![correction_operation_for_test("/age", "age")],
    )
    .unwrap()
}

#[test]
fn correction_plan_is_dry_runnable_nullable_and_keyless() {
    let destination = PostgresDestination::new();
    let request = correction_request_for_test();
    let plan = destination
        .plan_addressed_correction(PostgresCorrectionPlanInput {
            request: request.clone(),
            existing_table: correction_existing_table(false),
        })
        .unwrap();

    plan.kernel
        .validate_for(
            &request,
            &postgres_correction_capabilities(),
            &TransactionSupport::AtomicPackage,
            &IdempotencySupport::PackageToken,
        )
        .unwrap();
    assert_eq!(plan.kernel.kernel.disposition, WriteDisposition::Append);
    assert!(plan.target_ddl.iter().any(|statement| {
        statement.sql == "ALTER TABLE \"raw\".\"orders\" ADD COLUMN \"age\" BIGINT"
    }));
    assert!(
        plan.target_ddl
            .iter()
            .any(|statement| { statement.sql.contains("CREATE UNIQUE INDEX IF NOT EXISTS") })
    );
    assert!(plan.create_stage.dry_run_safe);
    assert!(plan.update_sql[0].sql.contains("_cdf_row_key"));
    assert!(plan.update_sql[0].sql.contains("_cdf_variant"));
    assert!(
        plan.transactional_statements()
            .iter()
            .all(|statement| statement.dry_run_safe)
    );
}

#[test]
fn correction_plan_rejects_nullable_provenance_address() {
    let destination = PostgresDestination::new();
    let error = destination
        .plan_addressed_correction(PostgresCorrectionPlanInput {
            request: correction_request_for_test(),
            existing_table: correction_existing_table(true),
        })
        .unwrap_err();
    assert!(error.to_string().contains("_cdf_row_key to be NOT NULL"));
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
fn framework_variant_column_is_system_owned_but_user_prefixed_columns_stay_rejected() {
    let variant = cdf_kernel::with_semantic(
        Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true),
        cdf_contract::VARIANT_SEMANTIC_TAG,
    );
    let mut metadata = variant.metadata().clone();
    metadata.insert(
        cdf_contract::RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
        cdf_contract::RESIDUAL_ENCODING_NAME.to_owned(),
    );
    let variant = variant.with_metadata(metadata);
    let columns = postgres_columns_for_schema(&Schema::new(vec![variant])).unwrap();
    assert_eq!(columns[0].name.as_str(), cdf_contract::VARIANT_COLUMN_NAME);

    let impostors = [
        Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true),
        cdf_kernel::with_semantic(
            Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true),
            "wrong",
        ),
        cdf_kernel::with_semantic(
            Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true),
            cdf_contract::VARIANT_SEMANTIC_TAG,
        ),
        cdf_kernel::with_semantic(
            Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Int64, true),
            cdf_contract::VARIANT_SEMANTIC_TAG,
        )
        .with_metadata(std::collections::HashMap::from([
            (
                cdf_kernel::SEMANTIC_METADATA_KEY.to_owned(),
                cdf_contract::VARIANT_SEMANTIC_TAG.to_owned(),
            ),
            (
                cdf_contract::RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
                cdf_contract::RESIDUAL_ENCODING_NAME.to_owned(),
            ),
        ])),
        cdf_kernel::with_semantic(
            Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, false),
            cdf_contract::VARIANT_SEMANTIC_TAG,
        )
        .with_metadata(std::collections::HashMap::from([
            (
                cdf_kernel::SEMANTIC_METADATA_KEY.to_owned(),
                cdf_contract::VARIANT_SEMANTIC_TAG.to_owned(),
            ),
            (
                cdf_contract::RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
                cdf_contract::RESIDUAL_ENCODING_NAME.to_owned(),
            ),
        ])),
        cdf_kernel::with_semantic(
            Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true),
            cdf_contract::VARIANT_SEMANTIC_TAG,
        )
        .with_metadata(std::collections::HashMap::from([
            (
                cdf_kernel::SEMANTIC_METADATA_KEY.to_owned(),
                cdf_contract::VARIANT_SEMANTIC_TAG.to_owned(),
            ),
            (
                cdf_contract::RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
                "wrong".to_owned(),
            ),
        ])),
    ];
    for impostor in impostors {
        assert!(postgres_columns_for_schema(&Schema::new(vec![impostor])).is_err());
    }
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
    assert!(merge_sql.contains("ORDER BY \"_cdf_row_key\" DESC"));
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
fn zero_data_append_and_replace_plans_have_only_receipt_and_state_effects() {
    let destination = PostgresDestination::new();
    for disposition in [WriteDisposition::Append, WriteDisposition::Replace] {
        let plan = destination.plan_load(zero_data_input(disposition)).unwrap();
        assert!(plan.target_ddl.is_empty());
        assert!(plan.write_sql.is_empty());
        assert!(
            plan.kernel
                .migrations
                .iter()
                .all(|migration| migration.migration_id.starts_with("postgres.create_cdf_"))
        );
        let receipt = build_receipt(
            &plan,
            PostgresReceiptInput {
                receipt_id: ReceiptId::new("receipt-zero").unwrap(),
                xid: "1".to_owned(),
                committed_at_ms: 1,
                counts: CommitCounts::default(),
                duplicate: false,
            },
        )
        .unwrap();
        assert!(receipt.segment_acks.is_empty());
        assert_eq!(receipt.counts, CommitCounts::default());
    }
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
    assert!(first_sql.contains("ORDER BY \"_cdf_row_key\" ASC"));

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
    assert!(plan.target_ddl.iter().any(|statement| statement.sql
        == "ALTER TABLE \"raw\".\"orders\" ADD COLUMN \"_cdf_row_key\" BIGINT"));

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
