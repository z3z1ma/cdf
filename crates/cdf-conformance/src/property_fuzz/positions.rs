use std::collections::BTreeMap;

use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CompositePosition, CursorPosition, CursorValue, FileManifest,
    FilePosition, ForeignState, LogPosition, PageToken, SourcePosition,
};
use proptest::prelude::*;
use serde_json::Value;

fn cursor_position(value: CursorValue) -> SourcePosition {
    SourcePosition::Cursor(CursorPosition {
        version: CHECKPOINT_STATE_VERSION,
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
            version: CHECKPOINT_STATE_VERSION,
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
        SourcePosition::Log(LogPosition {
            version: CHECKPOINT_STATE_VERSION,
            log: "orders-log".to_owned(),
            offset: 9_223_372_036_854_775,
            sequence: Some("seq-0001".to_owned()),
        }),
        SourcePosition::FileManifest(FileManifest {
            version: CHECKPOINT_STATE_VERSION,
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
        SourcePosition::PageToken(PageToken {
            version: CHECKPOINT_STATE_VERSION,
            token: "opaque-page-token".to_owned(),
        }),
        SourcePosition::Composite(CompositePosition {
            version: CHECKPOINT_STATE_VERSION,
            positions: composite_positions,
        }),
        SourcePosition::ForeignState(ForeignState {
            version: CHECKPOINT_STATE_VERSION,
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
    assert_eq!(position.version(), CHECKPOINT_STATE_VERSION);

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
        Some(u64::from(CHECKPOINT_STATE_VERSION))
    );

    if let Some(positions) = object.get("positions").and_then(Value::as_object) {
        for nested in positions.values() {
            assert_embedded_versions(nested);
        }
    }
}

#[test]
fn property_fuzz_source_positions_round_trip_all_active_variants() {
    assert_eq!(CHECKPOINT_STATE_VERSION, 1);

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
