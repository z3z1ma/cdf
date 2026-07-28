use super::*;
use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
};

pub(super) fn document(report: &AddReport) -> RenderDocument {
    let field_table = report.fields.iter().fold(
        Table::new(["field", "type", "nullable", "source"]),
        |table, field| {
            table.row([
                field.name.clone(),
                field_type_label(&field.data_type),
                yes_no(field.nullable).to_owned(),
                field.source_name.clone().unwrap_or_else(|| "-".to_owned()),
            ])
        },
    );
    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            if report.writes.resource_config {
                format!("added resource {}", report.resource_id)
            } else {
                format!("prepared resource {} (dry run)", report.resource_id)
            },
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Resource")
                .row("id", report.resource_id.clone())
                .row("driver", report.source_driver.clone())
                .row("config", report.config_path.clone())
                .row("location", report.location.clone())
                .row("selection", report.selection.clone())
                .row("disposition", report.write_disposition.to_owned())
                .row("schema", report.schema_hash.clone())
                .row("snapshot", report.schema_snapshot_path.clone()),
        )
        .push(
            KeyValuePanel::new("Suggestions")
                .row(
                    "cursor",
                    report.cursor.clone().unwrap_or_else(|| "none".to_owned()),
                )
                .row(
                    "cursor candidates",
                    if report.cursor_candidates.is_empty() {
                        "none".to_owned()
                    } else {
                        format!("{} (not selected)", report.cursor_candidates.join(", "))
                    },
                ),
        )
        .blank_line()
        .push(field_table)
        .blank_line()
        .push(NextCommand::new(report.next_command.clone()))
}

fn field_type_label(data_type: &SchemaSnapshotDataType) -> String {
    match data_type {
        SchemaSnapshotDataType::Null => "null".to_owned(),
        SchemaSnapshotDataType::Boolean => "bool".to_owned(),
        SchemaSnapshotDataType::Int { signed, bits } => {
            format!("{}int{bits}", if *signed { "" } else { "u" })
        }
        SchemaSnapshotDataType::Float { bits } => format!("float{bits}"),
        SchemaSnapshotDataType::Decimal {
            bits,
            precision,
            scale,
        } => format!("decimal{bits}({precision},{scale})"),
        SchemaSnapshotDataType::Timestamp { unit, timezone } => match timezone {
            Some(timezone) => format!("timestamp({unit:?}, {timezone})").to_lowercase(),
            None => format!("timestamp({unit:?})").to_lowercase(),
        },
        SchemaSnapshotDataType::Date { unit } => format!("date({unit:?})").to_lowercase(),
        SchemaSnapshotDataType::Time { unit, bits } => {
            format!("time{bits}({unit:?})").to_lowercase()
        }
        SchemaSnapshotDataType::Duration { unit } => format!("duration({unit:?})").to_lowercase(),
        SchemaSnapshotDataType::Interval { unit } => format!("interval({unit:?})").to_lowercase(),
        SchemaSnapshotDataType::Binary { offset_width } => format!("binary{offset_width}"),
        SchemaSnapshotDataType::FixedSizeBinary { byte_width } => {
            format!("fixed_size_binary({byte_width})")
        }
        SchemaSnapshotDataType::BinaryView => "binary_view".to_owned(),
        SchemaSnapshotDataType::Utf8 { offset_width } => {
            if *offset_width == 64 {
                "large_utf8".to_owned()
            } else {
                "utf8".to_owned()
            }
        }
        SchemaSnapshotDataType::Utf8View => "utf8_view".to_owned(),
        SchemaSnapshotDataType::List { field, .. } => {
            format!("list<{}>", field_type_label(&field.data_type))
        }
        SchemaSnapshotDataType::FixedSizeList { field, length } => {
            format!(
                "fixed_size_list<{}; {length}>",
                field_type_label(&field.data_type)
            )
        }
        SchemaSnapshotDataType::Struct { fields } => format!("struct<{} fields>", fields.len()),
        SchemaSnapshotDataType::Union { mode, fields } => {
            format!("union({mode:?}, {} fields)", fields.len()).to_lowercase()
        }
        SchemaSnapshotDataType::Dictionary {
            key_type,
            value_type,
        } => format!(
            "dictionary<{}, {}>",
            field_type_label(key_type),
            field_type_label(value_type)
        ),
        SchemaSnapshotDataType::Map { field, .. } => {
            format!("map<{}>", field_type_label(&field.data_type))
        }
        SchemaSnapshotDataType::RunEndEncoded { run_ends, values } => format!(
            "run_end_encoded<{}, {}>",
            field_type_label(&run_ends.data_type),
            field_type_label(&values.data_type)
        ),
        SchemaSnapshotDataType::Other { display } => display.clone(),
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
