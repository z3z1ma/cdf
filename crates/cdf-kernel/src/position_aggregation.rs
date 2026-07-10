use std::collections::BTreeMap;

use arrow_schema::{DataType, Schema, TimeUnit};

use crate::{
    CdfError, CompositePosition, CursorOrderingClaim, CursorPosition, CursorValue, FileManifest,
    FilePosition, ResourceDescriptor, Result, SourcePosition, WriteDisposition,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CursorArithmetic {
    I64,
    U64,
    TimestampMicros,
    Date32,
}

pub fn aggregate_resource_output_position(
    descriptor: &ResourceDescriptor,
    schema: &Schema,
    input: Option<&SourcePosition>,
    positions: &[SourcePosition],
) -> Result<SourcePosition> {
    if positions.is_empty() {
        return Err(CdfError::data(
            "source-position aggregation requires at least one processed position",
        ));
    }
    if descriptor.cursor.is_some() {
        return aggregate_cursor(descriptor, schema, positions);
    }
    aggregate_position_set(
        descriptor.resource_id.as_ref(),
        input,
        positions,
        &descriptor.write_disposition,
    )
}

pub fn aggregate_position_set(
    resource_id: &str,
    input: Option<&SourcePosition>,
    positions: &[SourcePosition],
    disposition: &WriteDisposition,
) -> Result<SourcePosition> {
    if positions.is_empty() {
        return Err(CdfError::data(
            "source-position aggregation requires at least one processed position",
        ));
    }
    if positions
        .iter()
        .all(|position| matches!(position, SourcePosition::FileManifest(_)))
    {
        let current = aggregate_file_manifests(resource_id, positions)?;
        if disposition != &WriteDisposition::Append {
            return Ok(current);
        }
        return merge_file_manifest_input(resource_id, input, current);
    }
    let first = &positions[0];
    if positions.iter().any(|position| position != first) {
        return Err(CdfError::data(
            "single resource run produced divergent segment source positions",
        ));
    }
    Ok(first.clone())
}

fn aggregate_file_manifests(
    resource_id: &str,
    positions: &[SourcePosition],
) -> Result<SourcePosition> {
    let mut version = None;
    let mut files = BTreeMap::<String, FilePosition>::new();
    for position in positions {
        let SourcePosition::FileManifest(manifest) = position else {
            unreachable!();
        };
        if version.is_some_and(|value| value != manifest.version) {
            return Err(CdfError::data(format!(
                "resource `{resource_id}` produced mixed file manifest versions"
            )));
        }
        version = Some(manifest.version);
        for file in &manifest.files {
            match files.get(&file.path) {
                Some(existing) if existing != file => {
                    return Err(CdfError::data(format!(
                        "resource `{resource_id}` produced conflicting file manifest evidence for `{}`",
                        file.path
                    )));
                }
                Some(_) => {}
                None => {
                    files.insert(file.path.clone(), file.clone());
                }
            }
        }
    }
    if files.is_empty() {
        return Err(CdfError::data(format!(
            "resource `{resource_id}` produced file manifest positions with no entries"
        )));
    }
    Ok(SourcePosition::FileManifest(FileManifest {
        version: version.expect("positions are non-empty"),
        files: files.into_values().collect(),
    }))
}

fn merge_file_manifest_input(
    resource_id: &str,
    input: Option<&SourcePosition>,
    current: SourcePosition,
) -> Result<SourcePosition> {
    let Some(SourcePosition::FileManifest(previous)) = input else {
        return Ok(current);
    };
    let SourcePosition::FileManifest(current) = current else {
        unreachable!();
    };
    if previous.version != current.version {
        return Err(CdfError::data(format!(
            "resource `{resource_id}` cannot merge file manifest versions {} and {}",
            previous.version, current.version
        )));
    }
    let mut files = BTreeMap::new();
    for file in &previous.files {
        files.insert(file.path.clone(), file.clone());
    }
    for file in current.files {
        files.insert(file.path.clone(), file);
    }
    Ok(SourcePosition::FileManifest(FileManifest {
        version: previous.version,
        files: files.into_values().collect(),
    }))
}

fn aggregate_cursor(
    descriptor: &ResourceDescriptor,
    schema: &Schema,
    positions: &[SourcePosition],
) -> Result<SourcePosition> {
    let cursor = descriptor.cursor.as_ref().expect("cursor is present");
    if cursor.ordering == CursorOrderingClaim::Unordered {
        return Err(CdfError::contract(format!(
            "resource `{}` cursor field `{}` is unordered and cannot advance checkpoints",
            descriptor.resource_id, cursor.field
        )));
    }
    let field = schema.field_with_name(&cursor.field).map_err(|_| {
        CdfError::contract(format!(
            "resource `{}` cursor field `{}` is missing from the declared schema",
            descriptor.resource_id, cursor.field
        ))
    })?;
    let arithmetic = match field.data_type() {
        DataType::Int64 => CursorArithmetic::I64,
        DataType::UInt64 => CursorArithmetic::U64,
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
            _,
        ) => CursorArithmetic::TimestampMicros,
        DataType::Date32 => CursorArithmetic::Date32,
        other => {
            return Err(CdfError::contract(format!(
                "resource `{}` cursor field `{}` has unsupported cursor value kind {other}",
                descriptor.resource_id, cursor.field
            )));
        }
    };
    let cursors = cursor_positions(descriptor, positions)?;
    let mut maximum = None::<&CursorPosition>;
    for position in cursors {
        if position.field != cursor.field {
            return Err(CdfError::data(format!(
                "source position cursor field `{}` does not match resource cursor field `{}`",
                position.field, cursor.field
            )));
        }
        ensure_cursor_kind(descriptor, arithmetic, &position.value)?;
        if maximum.is_none_or(|current| greater(arithmetic, &position.value, &current.value)) {
            maximum = Some(position);
        }
    }
    let maximum = maximum.expect("cursor positions are non-empty");
    Ok(SourcePosition::Cursor(CursorPosition {
        version: maximum.version,
        field: cursor.field.clone(),
        value: close_cursor(
            descriptor,
            arithmetic,
            &maximum.value,
            cursor.lag_tolerance_ms,
        )?,
    }))
}

fn cursor_positions<'a>(
    descriptor: &ResourceDescriptor,
    positions: &'a [SourcePosition],
) -> Result<Vec<&'a CursorPosition>> {
    let mut cursors = Vec::new();
    let mut saw_cursor = false;
    let mut saw_page = false;
    let mut saw_other = false;
    for position in positions {
        match position {
            SourcePosition::Cursor(cursor) => {
                saw_cursor = true;
                cursors.push(cursor);
            }
            SourcePosition::PageToken(_) => saw_page = true,
            SourcePosition::Composite(composite) => {
                saw_other = true;
                let (cursor, page) = composite_summary(composite);
                saw_cursor |= cursor;
                saw_page |= page;
            }
            _ => saw_other = true,
        }
    }
    if saw_page && saw_cursor {
        return Err(CdfError::data(format!(
            "resource `{}` produced mixed cursor/page-token source positions",
            descriptor.resource_id
        )));
    }
    if saw_page && !saw_cursor && !saw_other {
        return Err(CdfError::data(format!(
            "resource `{}` produced page-token-only checkpoint positions",
            descriptor.resource_id
        )));
    }
    if saw_other && saw_cursor {
        return Err(CdfError::data(format!(
            "resource `{}` produced divergent source-position variants",
            descriptor.resource_id
        )));
    }
    if saw_page || saw_other || cursors.len() != positions.len() {
        return Err(CdfError::data(format!(
            "resource `{}` produced non-cursor checkpoint positions",
            descriptor.resource_id
        )));
    }
    Ok(cursors)
}

fn composite_summary(composite: &CompositePosition) -> (bool, bool) {
    let mut cursor = false;
    let mut page = false;
    for position in composite.positions.values() {
        match position {
            SourcePosition::Cursor(_) => cursor = true,
            SourcePosition::PageToken(_) => page = true,
            SourcePosition::Composite(nested) => {
                let nested = composite_summary(nested);
                cursor |= nested.0;
                page |= nested.1;
            }
            _ => {}
        }
    }
    (cursor, page)
}

fn ensure_cursor_kind(
    descriptor: &ResourceDescriptor,
    arithmetic: CursorArithmetic,
    value: &CursorValue,
) -> Result<()> {
    if matches!(
        (arithmetic, value),
        (
            CursorArithmetic::I64 | CursorArithmetic::Date32,
            CursorValue::I64(_)
        ) | (CursorArithmetic::U64, CursorValue::U64(_))
            | (
                CursorArithmetic::TimestampMicros,
                CursorValue::TimestampMicros { .. }
            )
    ) {
        return Ok(());
    }
    Err(CdfError::data(format!(
        "resource `{}` cursor produced a value incompatible with its declared schema",
        descriptor.resource_id
    )))
}

fn greater(arithmetic: CursorArithmetic, left: &CursorValue, right: &CursorValue) -> bool {
    match (arithmetic, left, right) {
        (
            CursorArithmetic::I64 | CursorArithmetic::Date32,
            CursorValue::I64(left),
            CursorValue::I64(right),
        ) => left > right,
        (CursorArithmetic::U64, CursorValue::U64(left), CursorValue::U64(right)) => left > right,
        (
            CursorArithmetic::TimestampMicros,
            CursorValue::TimestampMicros { micros: left, .. },
            CursorValue::TimestampMicros { micros: right, .. },
        ) => left > right,
        _ => false,
    }
}

fn close_cursor(
    descriptor: &ResourceDescriptor,
    arithmetic: CursorArithmetic,
    value: &CursorValue,
    lag_ms: u64,
) -> Result<CursorValue> {
    let incompatible = || {
        CdfError::data(format!(
            "resource `{}` has incompatible cursor lag {}ms",
            descriptor.resource_id, lag_ms
        ))
    };
    match (arithmetic, value) {
        (CursorArithmetic::I64, CursorValue::I64(value)) => value
            .checked_sub(i64::try_from(lag_ms).map_err(|_| incompatible())?)
            .map(CursorValue::I64)
            .ok_or_else(incompatible),
        (CursorArithmetic::U64, CursorValue::U64(value)) => value
            .checked_sub(lag_ms)
            .map(CursorValue::U64)
            .ok_or_else(incompatible),
        (CursorArithmetic::TimestampMicros, CursorValue::TimestampMicros { micros, timezone }) => {
            micros
                .checked_sub(
                    i64::try_from(lag_ms.checked_mul(1_000).ok_or_else(incompatible)?)
                        .map_err(|_| incompatible())?,
                )
                .map(|micros| CursorValue::TimestampMicros {
                    micros,
                    timezone: timezone.clone(),
                })
                .ok_or_else(incompatible)
        }
        (CursorArithmetic::Date32, CursorValue::I64(value)) => {
            const DAY_MS: u64 = 86_400_000;
            if !lag_ms.is_multiple_of(DAY_MS) {
                return Err(incompatible());
            }
            value
                .checked_sub(i64::try_from(lag_ms / DAY_MS).map_err(|_| incompatible())?)
                .map(CursorValue::I64)
                .ok_or_else(incompatible)
        }
        _ => Err(incompatible()),
    }
}
