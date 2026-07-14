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

/// Merges evidence about the same logical file without permitting its source
/// generation to change. A cryptographic checksum may enrich metadata-only
/// evidence after extraction has consumed the file.
pub fn merge_file_position_evidence(
    existing: &FilePosition,
    observed: &FilePosition,
) -> Result<FilePosition> {
    if existing.path != observed.path
        || existing.size_bytes != observed.size_bytes
        || existing.source_generation != observed.source_generation
        || existing.etag != observed.etag
        || existing.object_version != observed.object_version
    {
        return Err(CdfError::data(format!(
            "file manifest evidence changed generation for `{}`",
            existing.path
        )));
    }
    let sha256 = match (&existing.sha256, &observed.sha256) {
        (Some(left), Some(right)) if left != right => {
            return Err(CdfError::data(format!(
                "file manifest evidence produced conflicting content hashes for `{}`",
                existing.path
            )));
        }
        (Some(value), _) | (_, Some(value)) => Some(value.clone()),
        (None, None) => None,
    };
    Ok(FilePosition {
        path: existing.path.clone(),
        size_bytes: existing.size_bytes,
        source_generation: existing.source_generation.clone(),
        etag: existing.etag.clone(),
        object_version: existing.object_version.clone(),
        sha256,
    })
}

/// Enriches a segment's source position with evidence available only after its source stream
/// reached EOF. This is the sole source-position authority for terminal enrichment; orchestration
/// must not branch on source kind.
pub fn merge_terminal_position_evidence(
    existing: &SourcePosition,
    terminal: &SourcePosition,
) -> Result<SourcePosition> {
    match (existing, terminal) {
        (SourcePosition::FileManifest(existing), SourcePosition::FileManifest(terminal)) => {
            if existing.version != terminal.version {
                return Err(CdfError::data(
                    "terminal file evidence changed the manifest version",
                ));
            }
            let mut terminal_by_path = BTreeMap::new();
            for file in &terminal.files {
                if terminal_by_path.insert(file.path.as_str(), file).is_some() {
                    return Err(CdfError::data(format!(
                        "terminal file evidence repeats path `{}`",
                        file.path
                    )));
                }
            }
            let mut existing_paths = BTreeMap::new();
            let mut files = Vec::with_capacity(existing.files.len());
            for file in &existing.files {
                if existing_paths.insert(file.path.as_str(), ()).is_some() {
                    return Err(CdfError::data(format!(
                        "segment file evidence repeats path `{}`",
                        file.path
                    )));
                }
                let terminal = terminal_by_path.remove(file.path.as_str()).ok_or_else(|| {
                    CdfError::data(format!(
                        "terminal file evidence omitted segment path `{}`",
                        file.path
                    ))
                })?;
                files.push(merge_file_position_evidence(file, terminal)?);
            }
            if let Some(extra) = terminal_by_path.into_values().next() {
                return Err(CdfError::data(format!(
                    "terminal file evidence introduced path `{}` absent from the segment",
                    extra.path
                )));
            }
            Ok(SourcePosition::FileManifest(FileManifest {
                version: existing.version,
                files,
            }))
        }
        (SourcePosition::Composite(existing), SourcePosition::Composite(terminal)) => {
            if existing.version != terminal.version
                || existing.positions.keys().ne(terminal.positions.keys())
            {
                return Err(CdfError::data(
                    "terminal composite evidence changed its version or position keys",
                ));
            }
            let positions = existing
                .positions
                .iter()
                .map(|(key, position)| {
                    let terminal = terminal.positions.get(key).ok_or_else(|| {
                        CdfError::internal("validated composite terminal key disappeared")
                    })?;
                    Ok((
                        key.clone(),
                        merge_terminal_position_evidence(position, terminal)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            Ok(SourcePosition::Composite(CompositePosition {
                version: existing.version,
                positions,
            }))
        }
        _ if existing == terminal => Ok(existing.clone()),
        _ => Err(CdfError::data(
            "terminal source-position evidence changed position kind or value",
        )),
    }
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
                Some(existing) => {
                    files.insert(
                        file.path.clone(),
                        merge_file_position_evidence(existing, file).map_err(|error| {
                            CdfError::data(format!(
                                "resource `{resource_id}` produced conflicting file manifest evidence for `{}`: {error}",
                                file.path
                            ))
                        })?,
                    );
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn file(sha256: Option<&str>) -> FilePosition {
        FilePosition {
            path: "events.ndjson".to_owned(),
            size_bytes: 42,
            source_generation: Some("local-v1:generation".to_owned()),
            etag: None,
            object_version: None,
            sha256: sha256.map(str::to_owned),
        }
    }

    #[test]
    fn extraction_checksum_enriches_metadata_only_file_evidence() {
        let merged =
            merge_file_position_evidence(&file(None), &file(Some("sha256:content"))).unwrap();
        assert_eq!(merged.sha256.as_deref(), Some("sha256:content"));
    }

    #[test]
    fn conflicting_generation_or_checksum_cannot_be_merged() {
        let mut changed_generation = file(Some("sha256:content"));
        changed_generation.source_generation = Some("local-v1:changed".to_owned());
        assert!(merge_file_position_evidence(&file(None), &changed_generation).is_err());
        assert!(
            merge_file_position_evidence(
                &file(Some("sha256:first")),
                &file(Some("sha256:second")),
            )
            .is_err()
        );
    }

    #[test]
    fn terminal_position_merge_is_total_and_recurses_through_composites() {
        let existing = SourcePosition::Composite(CompositePosition {
            version: 1,
            positions: BTreeMap::from([
                (
                    "file".to_owned(),
                    SourcePosition::FileManifest(FileManifest {
                        version: 1,
                        files: vec![file(None)],
                    }),
                ),
                (
                    "cursor".to_owned(),
                    SourcePosition::Cursor(CursorPosition {
                        version: 1,
                        field: "id".to_owned(),
                        value: CursorValue::I64(42),
                    }),
                ),
            ]),
        });
        let terminal = SourcePosition::Composite(CompositePosition {
            version: 1,
            positions: BTreeMap::from([
                (
                    "file".to_owned(),
                    SourcePosition::FileManifest(FileManifest {
                        version: 1,
                        files: vec![file(Some("sha256:content"))],
                    }),
                ),
                (
                    "cursor".to_owned(),
                    SourcePosition::Cursor(CursorPosition {
                        version: 1,
                        field: "id".to_owned(),
                        value: CursorValue::I64(42),
                    }),
                ),
            ]),
        });

        let merged = merge_terminal_position_evidence(&existing, &terminal).unwrap();
        let SourcePosition::Composite(merged) = merged else {
            panic!("expected composite terminal evidence");
        };
        let SourcePosition::FileManifest(manifest) = &merged.positions["file"] else {
            panic!("expected nested file manifest");
        };
        assert_eq!(manifest.files[0].sha256.as_deref(), Some("sha256:content"));
        assert_eq!(
            merged.positions["cursor"],
            terminal_position(&terminal, "cursor")
        );
    }

    #[test]
    fn terminal_position_merge_rejects_kind_value_and_path_changes() {
        let existing = SourcePosition::FileManifest(FileManifest {
            version: 1,
            files: vec![file(None)],
        });
        let mut changed = file(Some("sha256:content"));
        changed.path = "other.ndjson".to_owned();
        assert!(
            merge_terminal_position_evidence(
                &existing,
                &SourcePosition::FileManifest(FileManifest {
                    version: 1,
                    files: vec![changed],
                }),
            )
            .is_err()
        );
        assert!(
            merge_terminal_position_evidence(
                &SourcePosition::Cursor(CursorPosition {
                    version: 1,
                    field: "id".to_owned(),
                    value: CursorValue::I64(1),
                }),
                &SourcePosition::Cursor(CursorPosition {
                    version: 1,
                    field: "id".to_owned(),
                    value: CursorValue::I64(2),
                }),
            )
            .is_err()
        );
        assert!(
            merge_terminal_position_evidence(
                &existing,
                &SourcePosition::FileManifest(FileManifest {
                    version: 1,
                    files: vec![file(None), file(Some("sha256:content"))],
                }),
            )
            .is_err()
        );
    }

    fn terminal_position<'a>(position: &'a SourcePosition, key: &str) -> SourcePosition {
        let SourcePosition::Composite(composite) = position else {
            panic!("expected composite position");
        };
        composite.positions[key].clone()
    }
}
