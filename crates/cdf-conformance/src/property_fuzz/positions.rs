use std::collections::BTreeMap;

use cdf_kernel::{
    CommittedLogPosition, CompositePosition, CursorPosition, CursorValue, FileManifest,
    FilePosition, ForeignState, MongoChangeStreamResumeToken, MongoChangeStreamScope,
    MongoResumeMode, MongoResumeTokenSource, MongoWatchLevel, MySqlCommitPosition, MySqlLogScope,
    PageToken, PostgresCommitPosition, PostgresLogScope, ResumeTokenPosition,
    SOURCE_POSITION_VERSION, SourcePosition, TableSnapshotPosition, TableSnapshotSelector,
};
use proptest::prelude::*;
use serde_json::Value;

fn cursor_position(value: CursorValue) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: SOURCE_POSITION_VERSION,
        field: "updated_at".to_owned(),
        value,
    })
}

fn active_source_positions() -> Vec<SourcePosition> {
    let mut composite_positions = BTreeMap::new();
    composite_positions.insert("cursor".to_owned(), cursor_position(CursorValue::I64(42)));
    composite_positions.insert(
        "page".to_owned(),
        SourcePosition::PageToken(PageToken {
            version: SOURCE_POSITION_VERSION,
            token: "page-2".to_owned(),
        }),
    );

    vec![
        cursor_position(CursorValue::String("2026-07-08T00:00:00Z".to_owned())),
        cursor_position(CursorValue::I64(i64::MIN)),
        cursor_position(CursorValue::U64(u64::MAX)),
        cursor_position(CursorValue::DecimalString("-1234567890.000001".to_owned())),
        cursor_position(CursorValue::TimestampMicros {
            micros: 1_783_468_800_000_000,
            timezone: Some("America/Phoenix".to_owned()),
        }),
        cursor_position(CursorValue::TimestampMicros {
            micros: -1,
            timezone: None,
        }),
        SourcePosition::Log(CommittedLogPosition::PostgreSql(PostgresCommitPosition {
            version: SOURCE_POSITION_VERSION,
            scope: PostgresLogScope {
                system_identifier: "7421938841407953395".to_owned(),
                database_oid: 16_384,
                slot: "cdf_orders".to_owned(),
                output_plugin: "pgoutput".to_owned(),
                semantics_sha256:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_owned(),
            },
            commit_lsn: u64::MAX - 1,
            end_lsn: u64::MAX,
            xid: u32::MAX,
        })),
        SourcePosition::Log(CommittedLogPosition::MySql(MySqlCommitPosition {
            version: SOURCE_POSITION_VERSION,
            scope: MySqlLogScope {
                source_binding: "orders-primary".to_owned(),
                active_server_uuid: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_owned(),
                binlog_basename: "mysql-bin".to_owned(),
                semantics_sha256:
                    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_owned(),
            },
            binlog_file: "mysql-bin.000042".to_owned(),
            file_sequence: 42,
            end_log_position: u64::MAX,
            executed_gtid_set: concat!(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-7,",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:blue:1:4-9"
            )
            .to_owned(),
            transaction_gtid: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:7".to_owned(),
        })),
        SourcePosition::ResumeToken(ResumeTokenPosition::MongoChangeStream(
            MongoChangeStreamResumeToken {
                version: SOURCE_POSITION_VERSION,
                scope: MongoChangeStreamScope {
                    source_binding: "orders-stream".to_owned(),
                    watch_level: MongoWatchLevel::Collection,
                    database: Some("sales".to_owned()),
                    collection: Some("orders".to_owned()),
                    pipeline_sha256:
                        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                            .to_owned(),
                    options_sha256:
                        "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                            .to_owned(),
                },
                token_bson_base64: "FgAAAAJfZGF0YQAGAAAAdG9rZW4AAA==".to_owned(),
                token_sha256:
                    "sha256:2861e1850c87f3c48b875671d9fc0ca97b9c268ad17ff0b713a116989f2a68a2"
                        .to_owned(),
                resume_mode: MongoResumeMode::ResumeAfter,
                token_source: MongoResumeTokenSource::PostBatch,
            },
        )),
        SourcePosition::FileManifest(FileManifest {
            version: SOURCE_POSITION_VERSION,
            files: vec![
                FilePosition {
                    path: "orders/a.ndjson".to_owned(),
                    size_bytes: 0,
                    source_generation: None,
                    etag: None,
                    object_version: None,
                    sha256: Some("sha256-a".to_owned()),
                },
                FilePosition {
                    path: "orders/b.ndjson".to_owned(),
                    size_bytes: u64::MAX,
                    source_generation: None,
                    etag: Some("etag-b".to_owned()),
                    object_version: None,
                    sha256: None,
                },
            ],
        }),
        SourcePosition::TableSnapshot(Box::new(TableSnapshotPosition {
            version: SOURCE_POSITION_VERSION,
            protocol: "iceberg".to_owned(),
            catalog: "glue:us-east-1:123456789012".to_owned(),
            namespace: vec!["analytics".to_owned(), "curated".to_owned()],
            table: "orders".to_owned(),
            selector: TableSnapshotSelector::Branch {
                name: "main".to_owned(),
            },
            snapshot_id: i64::MAX,
            sequence_number: i64::MAX,
            parent_snapshot_id: Some(i64::MAX - 1),
            metadata_location: "s3://warehouse/analytics/orders/metadata/v42.json".to_owned(),
            metadata_generation: "version-id:v42".to_owned(),
        })),
        SourcePosition::PageToken(PageToken {
            version: SOURCE_POSITION_VERSION,
            token: "opaque-page-token".to_owned(),
        }),
        SourcePosition::Composite(CompositePosition {
            version: SOURCE_POSITION_VERSION,
            positions: composite_positions,
        }),
        SourcePosition::ForeignState(ForeignState {
            version: SOURCE_POSITION_VERSION,
            protocol: "singer".to_owned(),
            opaque_blob: br#"{"bookmarks":{"orders":{"cursor":42}}}"#.to_vec(),
            blob_sha256: "sha256:state".to_owned(),
        }),
    ]
}

fn cursor_value_strategy() -> impl Strategy<Value = CursorValue> {
    prop_oneof![
        ".{0,128}".prop_map(CursorValue::String),
        any::<i64>().prop_map(CursorValue::I64),
        any::<u64>().prop_map(CursorValue::U64),
        "-?[0-9]{1,20}(\\.[0-9]{1,6})?".prop_map(CursorValue::DecimalString),
        (any::<i64>(), prop::option::of("[A-Za-z_./+-]{1,32}"))
            .prop_map(|(micros, timezone)| CursorValue::TimestampMicros { micros, timezone }),
    ]
}

fn assert_json_round_trip(position: &SourcePosition) {
    assert_eq!(position.version(), SOURCE_POSITION_VERSION);

    let value = serde_json::to_value(position).unwrap();
    assert_embedded_versions(&value);

    let from_value: SourcePosition = serde_json::from_value(value).unwrap();
    assert_eq!(from_value, *position);

    let text = serde_json::to_string(position).unwrap();
    let from_text: SourcePosition = serde_json::from_str(&text).unwrap();
    assert_eq!(from_text, *position);
}

fn assert_embedded_versions(value: &Value) {
    let object = value
        .as_object()
        .expect("source position serializes as object");
    assert_eq!(
        object.get("version").and_then(Value::as_u64),
        Some(u64::from(SOURCE_POSITION_VERSION))
    );

    if let Some(positions) = object.get("positions").and_then(Value::as_object) {
        for nested in positions.values() {
            assert_embedded_versions(nested);
        }
    }
}

#[test]
fn property_fuzz_source_positions_round_trip_all_active_variants() {
    assert_eq!(SOURCE_POSITION_VERSION, 2);

    for position in active_source_positions() {
        assert_json_round_trip(&position);
    }
}

proptest! {
    #[test]
    fn property_fuzz_generated_cursor_positions_round_trip(value in cursor_value_strategy()) {
        assert_json_round_trip(&cursor_position(value));
    }
}
