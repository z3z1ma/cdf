use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{
    CommitSegment, CommitSession, CursorPosition, CursorValue, DestinationCommitRequest,
    DestinationProtocol, ErrorKind, IdempotencyToken, PackageHash, SchemaHash, ScopeKey, SegmentId,
    SourcePosition, StateSegment, WriteDisposition,
};

use crate::{
    ClickHouseDestination,
    mapping::{PACKAGE_HASH_COLUMN, columns_for_schema, physical_columns},
    models::{
        ClickHouseCommitRequest, ClickHouseExpectedSegment, ClickHouseLoadPlanInput,
        ClickHouseMergeMode, ClickHouseSessionSegments,
    },
    plan::{ensure_supported_disposition, plan_clickhouse_load},
    runtime::{clickhouse_runtime_capabilities, parse_uri_for_test},
    session::{ClickHouseCommitSession, verify_receipt},
};

#[test]
fn sheet_and_bulk_path_are_truthful() {
    let destination = ClickHouseDestination::new().unwrap();
    assert!(Arc::ptr_eq(
        &destination.client,
        &destination.clone().client
    ));
    let sheet = destination.sheet();
    assert_eq!(sheet.destination.as_str(), "clickhouse");
    assert_eq!(sheet.identifier_rules.normalizer, "namecase-v1");
    assert_eq!(sheet.identifier_rules.max_length, Some(255));
    cdf_contract::identifier_policy_from_destination_rules(&sheet.identifier_rules).unwrap();
    cdf_contract::validate_destination_schema_mappings(
        &cdf_contract::TypePolicy::strict_fidelity(),
        sheet,
        &Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]),
    )
    .unwrap();
    assert_eq!(
        sheet.supported_dispositions,
        [
            WriteDisposition::Append,
            WriteDisposition::Replace,
            WriteDisposition::Merge
        ]
    );
    let capabilities = clickhouse_runtime_capabilities();
    capabilities.validate().unwrap();
    assert_eq!(capabilities.bulk_path.as_deref(), Some("arrowstream"));
    assert_eq!(capabilities.max_in_flight_segments, Some(1));
    assert_eq!(capabilities.max_in_flight_bytes, Some(64 * 1024 * 1024));
}

#[test]
fn mapping_adds_exact_compact_provenance_columns() {
    let logical = Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, true),
    ]);
    let columns = columns_for_schema(&logical).unwrap();
    let physical = physical_columns(&columns).unwrap();
    assert_eq!(physical.len(), 4);
    assert_eq!(physical[2].name.as_str(), PACKAGE_HASH_COLUMN);
    assert_eq!(physical[2].clickhouse_type, "FixedString(32)");
    assert_eq!(
        physical[3].name.as_str(),
        cdf_package_contract::CDF_PACKAGE_ROW_ORD_FIELD
    );
    assert_eq!(physical[3].clickhouse_type, "UInt64");
}

#[test]
fn uri_and_disposition_boundaries_fail_closed() {
    let parsed = parse_uri_for_test("clickhouse://localhost:8123/analytics", false).unwrap();
    assert_eq!(parsed.endpoint, "http://localhost:8123");
    assert_eq!(parsed.database.as_str(), "analytics");
    let credentials =
        parse_uri_for_test("clickhouse://user:password@localhost:8123/analytics", false)
            .unwrap_err();
    assert_eq!(credentials.kind, ErrorKind::Auth);
    assert!(ensure_supported_disposition(&WriteDisposition::Merge).is_ok());
    assert!(ensure_supported_disposition(&WriteDisposition::CdcApply).is_err());
}

#[test]
fn merge_mode_defaults_native_and_accepts_only_the_two_ratified_values() {
    assert_eq!(
        ClickHouseMergeMode::parse(None).unwrap(),
        ClickHouseMergeMode::ReplacingMergeTree
    );
    assert_eq!(
        ClickHouseMergeMode::parse(Some("atomic_copy_on_write")).unwrap(),
        ClickHouseMergeMode::AtomicCopyOnWrite
    );
    assert!(ClickHouseMergeMode::parse(Some("mutation")).is_err());
}

#[test]
fn merge_plan_requires_non_nullable_keys_and_binds_mode_into_identity() {
    let native = merge_plan(ClickHouseMergeMode::ReplacingMergeTree, false).unwrap();
    let atomic = merge_plan(ClickHouseMergeMode::AtomicCopyOnWrite, false).unwrap();
    assert_ne!(native.kernel.plan_id, atomic.kernel.plan_id);
    assert_eq!(
        native.kernel.delivery_guarantee,
        cdf_kernel::DeliveryGuarantee::EffectivelyOncePerKey
    );
    assert_eq!(native.merge_keys[0].as_str(), "id");
    let nullable = merge_plan(ClickHouseMergeMode::ReplacingMergeTree, true).unwrap_err();
    assert_eq!(nullable.kind, ErrorKind::Contract);

    let package_hash = PackageHash::new("12".repeat(32)).unwrap();
    let request = DestinationCommitRequest {
        package_hash: package_hash.clone(),
        content: cdf_kernel::PackageContentAuthority::rows(
            SchemaHash::new("schema-clickhouse-merge").unwrap(),
        ),
        target: cdf_kernel::TargetName::new("events").unwrap(),
        disposition: WriteDisposition::Merge,
        segments: Vec::new(),
        idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
    };
    let native_protocol = ClickHouseDestination::new()
        .unwrap()
        .plan_commit(&request)
        .unwrap();
    let mut atomic_destination = ClickHouseDestination::new().unwrap();
    atomic_destination.merge_mode = ClickHouseMergeMode::AtomicCopyOnWrite;
    let atomic_protocol = atomic_destination.plan_commit(&request).unwrap();
    assert_ne!(native_protocol.plan_id, atomic_protocol.plan_id);
}

fn merge_plan(
    merge_mode: ClickHouseMergeMode,
    nullable_key: bool,
) -> cdf_kernel::Result<crate::models::ClickHouseLoadPlan> {
    let hash = "11".repeat(32);
    let package_hash = PackageHash::new(hash).unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt64, nullable_key),
        Field::new("name", DataType::Utf8, true),
    ]);
    plan_clickhouse_load(ClickHouseLoadPlanInput {
        package_hash: package_hash.clone(),
        content: cdf_kernel::PackageContentAuthority::rows(
            SchemaHash::new("schema-clickhouse-merge").unwrap(),
        ),
        idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
        target: crate::identifier::ClickHouseIdentifier::user("events").unwrap(),
        disposition: WriteDisposition::Merge,
        schema_hash: SchemaHash::new("schema-clickhouse-merge").unwrap(),
        segments: Vec::new(),
        columns: columns_for_schema(&schema).unwrap(),
        merge_keys: vec![crate::identifier::ClickHouseIdentifier::user("id").unwrap()],
        merge_mode,
        resource_id: None,
        state_delta: None,
    })
}

#[test]
#[ignore = "requires digest-pinned ClickHouse via CDF_CLICKHOUSE_DESTINATION_ENDPOINT"]
fn live_native_and_atomic_merge_contract() {
    let uri = std::env::var("CDF_CLICKHOUSE_DESTINATION_ENDPOINT").expect(
        "set CDF_CLICKHOUSE_DESTINATION_ENDPOINT=clickhouse://host:port/cdf_destination_live",
    );
    let connection = parse_uri_for_test(&uri, false).unwrap();
    let setup = clickhouse::Client::default().with_url(&connection.endpoint);
    let (_, live_execution) =
        cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    live_execution
        .run_io({
            let setup = setup.clone();
            async move {
                setup
                    .query("DROP DATABASE IF EXISTS cdf_destination_live SYNC")
                    .execute()
                    .await
                    .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))?;
                setup
                    .query("CREATE DATABASE cdf_destination_live ENGINE = Atomic")
                    .execute()
                    .await
                    .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))?;
                let database = setup.with_database("cdf_destination_live");
                for table in [
                    "atomic_events",
                    "atomic_recovery",
                    "replace_events",
                    "settlement_recovery",
                    "view_events",
                    "view_sink",
                ] {
                    database
                        .query(&format!(
                            "CREATE TABLE {table} (id Int64, name String, _cdf_package_hash FixedString(32), _cdf_package_row_ord UInt64) ENGINE = MergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100000"
                        ))
                        .execute()
                        .await
                        .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))?;
                }
                database
                    .query(
                        "ALTER TABLE atomic_events MODIFY COMMENT 'operator-owned comment'",
                    )
                    .execute()
                    .await
                    .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))?;
                database
                    .query(
                        "CREATE TABLE native_events (id Int64, name String, _cdf_package_hash FixedString(32), _cdf_package_row_ord UInt64) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS non_replicated_deduplication_window = 100000",
                    )
                    .execute()
                    .await
                    .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))?;
                database
                    .query(
                        "CREATE TABLE versioned_events (id Int64, name String, _cdf_package_hash FixedString(32), _cdf_package_row_ord UInt64) ENGINE = ReplacingMergeTree(_cdf_package_row_ord) ORDER BY id SETTINGS non_replicated_deduplication_window = 100000",
                    )
                    .execute()
                    .await
                    .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))?;
                database
                    .query(
                        "CREATE TABLE memory_events (id Int64, name String, _cdf_package_hash FixedString(32), _cdf_package_row_ord UInt64) ENGINE = Memory",
                    )
                    .execute()
                    .await
                    .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))?;
                database
                    .query(
                        "CREATE MATERIALIZED VIEW view_dependency TO view_sink AS SELECT * FROM view_events",
                    )
                    .execute()
                    .await
                    .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))?;
                Ok(())
            }
        })
        .unwrap();

    let first_native = live_commit(
        connection.clone(),
        "native_events",
        "11".repeat(32),
        WriteDisposition::Merge,
        ClickHouseMergeMode::ReplacingMergeTree,
        live_batch(&[1, 2], &["old", "two"]),
    );
    let duplicate_native = live_commit(
        connection.clone(),
        "native_events",
        "11".repeat(32),
        WriteDisposition::Merge,
        ClickHouseMergeMode::ReplacingMergeTree,
        live_batch(&[1, 2], &["old", "two"]),
    );
    assert_eq!(first_native, duplicate_native);
    live_commit(
        connection.clone(),
        "native_events",
        "22".repeat(32),
        WriteDisposition::Merge,
        ClickHouseMergeMode::ReplacingMergeTree,
        live_batch(&[1, 3], &["new", "three"]),
    );

    live_commit(
        connection.clone(),
        "atomic_events",
        "33".repeat(32),
        WriteDisposition::Append,
        ClickHouseMergeMode::ReplacingMergeTree,
        live_batch(&[1, 2], &["old", "two"]),
    );
    let atomic = live_commit(
        connection.clone(),
        "atomic_events",
        "44".repeat(32),
        WriteDisposition::Merge,
        ClickHouseMergeMode::AtomicCopyOnWrite,
        live_batch(&[1, 3], &["new", "three"]),
    );
    assert_eq!(atomic.counts.rows_inserted, Some(1));
    assert_eq!(atomic.counts.rows_updated, Some(1));
    assert_eq!(
        atomic,
        live_commit(
            connection.clone(),
            "atomic_events",
            "44".repeat(32),
            WriteDisposition::Merge,
            ClickHouseMergeMode::AtomicCopyOnWrite,
            live_batch(&[1, 3], &["new", "three"]),
        )
    );

    live_commit(
        connection.clone(),
        "replace_events",
        "77".repeat(32),
        WriteDisposition::Append,
        ClickHouseMergeMode::ReplacingMergeTree,
        live_batch(&[1, 2], &["old", "two"]),
    );
    let replacement = live_commit(
        connection.clone(),
        "replace_events",
        "88".repeat(32),
        WriteDisposition::Replace,
        ClickHouseMergeMode::ReplacingMergeTree,
        live_batch(&[3], &["three"]),
    );
    assert_eq!(replacement.counts.rows_inserted, Some(1));
    assert_eq!(replacement.counts.rows_deleted, Some(2));
    assert_eq!(
        replacement,
        live_commit(
            connection.clone(),
            "replace_events",
            "88".repeat(32),
            WriteDisposition::Replace,
            ClickHouseMergeMode::ReplacingMergeTree,
            live_batch(&[3], &["three"]),
        )
    );

    let zero_replacement = live_commit(
        connection.clone(),
        "replace_events",
        "99".repeat(32),
        WriteDisposition::Replace,
        ClickHouseMergeMode::ReplacingMergeTree,
        live_batch(&[], &[]),
    );
    assert_eq!(zero_replacement.counts.rows_written, 0);
    assert_eq!(zero_replacement.counts.rows_deleted, Some(1));
    let replacement_verifier = ClickHouseDestination::for_runtime(
        connection.clone(),
        crate::identifier::ClickHouseIdentifier::user("replace_events").unwrap(),
        None,
        ClickHouseMergeMode::ReplacingMergeTree,
    )
    .unwrap()
    .with_execution_services(Some(live_execution.clone()));
    verify_receipt(&replacement_verifier, &replacement).unwrap();
    verify_receipt(&replacement_verifier, &zero_replacement).unwrap();

    let atomic_verifier = ClickHouseDestination::for_runtime(
        connection.clone(),
        crate::identifier::ClickHouseIdentifier::user("atomic_events").unwrap(),
        None,
        ClickHouseMergeMode::AtomicCopyOnWrite,
    )
    .unwrap()
    .with_execution_services(Some(live_execution.clone()));
    verify_receipt(&atomic_verifier, &atomic).unwrap();

    let settlement_hash = "aa".repeat(32);
    let settlement_batch = live_batch(&[7, 8], &["seven", "eight"]);
    let (settlement_plan, settlement_state, settlement_expected, settlement_canonical) =
        live_request(
            "settlement_recovery",
            settlement_hash.clone(),
            WriteDisposition::Append,
            ClickHouseMergeMode::ReplacingMergeTree,
            settlement_batch.clone(),
        );
    let (_, settlement_execution) =
        cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let mut interrupted_settlement = ClickHouseCommitSession::new(
        connection.clone(),
        settlement_execution,
        Default::default(),
        ClickHouseCommitRequest {
            plan: settlement_plan,
            segments: ClickHouseSessionSegments {
                expected: [(settlement_state.segment_id.clone(), settlement_expected)]
                    .into_iter()
                    .collect(),
            },
        },
    );
    interrupted_settlement.apply_migrations().unwrap();
    let duplicate_error = interrupted_settlement
        .write_segments(Box::new(
            [
                Ok(CommitSegment::new(
                    settlement_state.clone(),
                    settlement_state.byte_count,
                    vec![settlement_canonical.clone()],
                )),
                Ok(CommitSegment::new(
                    settlement_state,
                    32,
                    vec![settlement_canonical],
                )),
            ]
            .into_iter(),
        ))
        .unwrap_err();
    assert_eq!(duplicate_error.kind, ErrorKind::Data);
    drop(interrupted_settlement);
    live_commit(
        connection.clone(),
        "settlement_recovery",
        settlement_hash,
        WriteDisposition::Append,
        ClickHouseMergeMode::ReplacingMergeTree,
        settlement_batch,
    );

    for (target, disposition, mode) in [
        (
            "versioned_events",
            WriteDisposition::Merge,
            ClickHouseMergeMode::ReplacingMergeTree,
        ),
        (
            "memory_events",
            WriteDisposition::Append,
            ClickHouseMergeMode::ReplacingMergeTree,
        ),
        (
            "view_events",
            WriteDisposition::Append,
            ClickHouseMergeMode::ReplacingMergeTree,
        ),
    ] {
        let error = live_commit_result(
            connection.clone(),
            target,
            "cc".repeat(32),
            disposition,
            mode,
            live_batch(&[1], &["rejected"]),
        )
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Contract, "target {target}");
    }

    live_commit(
        connection.clone(),
        "atomic_recovery",
        "55".repeat(32),
        WriteDisposition::Append,
        ClickHouseMergeMode::ReplacingMergeTree,
        live_batch(&[1, 2], &["old", "two"]),
    );
    let recovery_hash = "66".repeat(32);
    let recovery_batch = live_batch(&[1, 3], &["new", "three"]);
    let (recovery_plan, recovery_state, recovery_expected, _) = live_request(
        "atomic_recovery",
        recovery_hash.clone(),
        WriteDisposition::Merge,
        ClickHouseMergeMode::AtomicCopyOnWrite,
        recovery_batch.clone(),
    );
    let (_, recovery_execution) =
        cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let mut interrupted = ClickHouseCommitSession::new(
        connection.clone(),
        recovery_execution.clone(),
        Default::default(),
        ClickHouseCommitRequest {
            plan: recovery_plan.clone(),
            segments: ClickHouseSessionSegments {
                expected: [(recovery_state.segment_id.clone(), recovery_expected.clone())]
                    .into_iter()
                    .collect(),
            },
        },
    );
    interrupted.apply_migrations().unwrap();
    let incoming = recovery_plan.incoming_stage.quoted();
    let publish = recovery_plan.stage.quoted();
    let target = recovery_plan.target.quoted();
    let segment_token = crate::plan::segment_token(&recovery_plan, &recovery_state.segment_id);
    let marker = format!("cdf:package:{recovery_hash}");
    let recovery_database = setup.clone().with_database("cdf_destination_live");
    let staged_recovery_hash = recovery_hash.clone();
    recovery_execution
        .run_io(async move {
            recovery_database
                .query(&format!(
                    "INSERT INTO {incoming} VALUES (1, 'new', unhex('{staged_recovery_hash}'), 0), (3, 'three', unhex('{staged_recovery_hash}'), 1)"
                ))
                .with_setting("insert_deduplication_token", segment_token)
                .execute()
                .await
                .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))?;
            recovery_database
                .query(&format!(
                    "INSERT INTO {publish} SELECT target.* FROM {target} AS target LEFT ANTI JOIN {incoming} AS incoming USING (id)"
                ))
                .execute()
                .await
                .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))?;
            recovery_database
                .query(&format!("INSERT INTO {publish} SELECT * FROM {incoming}"))
                .execute()
                .await
                .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))?;
            recovery_database
                .query(&format!(
                    "ALTER TABLE {publish} MODIFY COMMENT '{}'",
                    marker.replace('\'', "\\'")
                ))
                .execute()
                .await
                .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))?;
            Ok(())
        })
        .unwrap();
    drop(interrupted);
    let recovered = live_commit(
        connection.clone(),
        "atomic_recovery",
        recovery_hash,
        WriteDisposition::Merge,
        ClickHouseMergeMode::AtomicCopyOnWrite,
        recovery_batch,
    );
    assert_eq!(recovered.counts.rows_inserted, Some(1));
    assert_eq!(recovered.counts.rows_updated, Some(1));

    #[derive(Debug, serde::Deserialize, clickhouse::Row)]
    struct LiveValue {
        id: i64,
        name: String,
    }
    let database = setup.clone().with_database("cdf_destination_live");
    for (table, final_clause) in [
        ("native_events", " FINAL"),
        ("atomic_events", ""),
        ("atomic_recovery", ""),
    ] {
        let database = database.clone();
        let values = live_execution
            .run_io(async move {
                database
                    .query(&format!(
                        "SELECT id, name FROM {table}{final_clause} ORDER BY id"
                    ))
                    .fetch_all::<LiveValue>()
                    .await
                    .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))
            })
            .unwrap();
        assert_eq!(
            values
                .into_iter()
                .map(|value| (value.id, value.name))
                .collect::<Vec<_>>(),
            [
                (1, "new".to_owned()),
                (2, "two".to_owned()),
                (3, "three".to_owned())
            ]
        );
    }
    #[derive(Debug, serde::Deserialize, clickhouse::Row)]
    struct LiveComment {
        comment: String,
    }
    let atomic_comment = live_execution
        .run_io({
            let database = database.clone();
            async move {
                database
                    .query(
                        "SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 'atomic_events'",
                    )
                    .fetch_one::<LiveComment>()
                    .await
                    .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))
            }
        })
        .unwrap();
    assert!(
        atomic_comment
            .comment
            .starts_with("operator-owned comment cdf:package:")
    );
    let replacement_rows = live_execution
        .run_io(async move {
            database
                .query("SELECT count() AS rows FROM replace_events")
                .fetch_one::<LiveCount>()
                .await
                .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))
        })
        .unwrap();
    assert_eq!(replacement_rows.rows, 0);
    let unsettled_stages = live_execution
        .run_io(async move {
            setup
                .query(
                    "SELECT count() AS rows FROM system.tables WHERE database = 'cdf_destination_live' AND (startsWith(name, '_cdf_publish_') OR startsWith(name, '_cdf_incoming_'))",
                )
                .fetch_one::<LiveCount>()
                .await
                .map_err(|error| cdf_kernel::CdfError::destination(error.to_string()))
        })
        .unwrap();
    assert_eq!(unsettled_stages.rows, 0);
}

#[derive(Debug, serde::Deserialize, clickhouse::Row)]
struct LiveCount {
    rows: u64,
}

fn live_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(names.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn live_commit(
    connection: crate::client::ClickHouseConnectionOptions,
    target: &str,
    hash: String,
    disposition: WriteDisposition,
    merge_mode: ClickHouseMergeMode,
    batch: RecordBatch,
) -> cdf_kernel::Receipt {
    live_commit_result(connection, target, hash, disposition, merge_mode, batch).unwrap()
}

fn live_commit_result(
    connection: crate::client::ClickHouseConnectionOptions,
    target: &str,
    hash: String,
    disposition: WriteDisposition,
    merge_mode: ClickHouseMergeMode,
    batch: RecordBatch,
) -> cdf_kernel::Result<cdf_kernel::Receipt> {
    let (plan, state, expected, canonical) =
        live_request(target, hash, disposition, merge_mode, batch);
    let rows = state.row_count;
    let (_, execution) = cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024)?;
    let mut session = ClickHouseCommitSession::new(
        connection,
        execution,
        Default::default(),
        ClickHouseCommitRequest {
            plan,
            segments: ClickHouseSessionSegments {
                expected: [(state.segment_id.clone(), expected)].into_iter().collect(),
            },
        },
    );
    session.apply_migrations()?;
    session.write_segments(Box::new(std::iter::once(Ok(CommitSegment::new(
        state,
        rows * 16,
        vec![canonical],
    )))))?;
    Box::new(session).finalize()
}

fn live_request(
    target: &str,
    hash: String,
    disposition: WriteDisposition,
    merge_mode: ClickHouseMergeMode,
    batch: RecordBatch,
) -> (
    crate::models::ClickHouseLoadPlan,
    StateSegment,
    ClickHouseExpectedSegment,
    RecordBatch,
) {
    let rows = u64::try_from(batch.num_rows()).unwrap();
    let state = StateSegment {
        kind: cdf_kernel::PackageSegmentKind::Row,
        segment_id: SegmentId::new("segment-1").unwrap(),
        scope: ScopeKey::Resource,
        output_position: SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "id".to_owned(),
            value: CursorValue::I64(i64::try_from(rows).unwrap()),
        }),
        row_count: rows,
        byte_count: rows * 16,
    };
    let package_hash = PackageHash::new(hash).unwrap();
    let plan = plan_clickhouse_load(ClickHouseLoadPlanInput {
        package_hash: package_hash.clone(),
        content: cdf_kernel::PackageContentAuthority::rows(
            SchemaHash::new("schema-clickhouse-live").unwrap(),
        ),
        idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
        target: crate::identifier::ClickHouseIdentifier::user(target).unwrap(),
        disposition: disposition.clone(),
        schema_hash: SchemaHash::new("schema-clickhouse-live").unwrap(),
        segments: vec![state.clone()],
        columns: columns_for_schema(batch.schema().as_ref()).unwrap(),
        merge_keys: if disposition == WriteDisposition::Merge {
            vec![crate::identifier::ClickHouseIdentifier::user("id").unwrap()]
        } else {
            Vec::new()
        },
        merge_mode,
        resource_id: None,
        state_delta: None,
    })
    .unwrap();
    let expected = ClickHouseExpectedSegment {
        state: state.clone(),
        package_byte_count: rows * 16,
        package_row_ord_start: 0,
    };
    let canonical = cdf_package_contract::append_package_row_ord(vec![batch], 0)
        .unwrap()
        .remove(0);
    (plan, state, expected, canonical)
}
