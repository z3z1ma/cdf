use std::collections::{BTreeMap, BTreeSet};

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_kernel::{CdfError, ResourceDescriptor, Result, SchemaHash, SchemaSource, source_name};
use rusqlite::types::ValueRef;
use serde::{Deserialize, Serialize};

use crate::{catalog::SQLITE_TEMPORAL_ENCODING_METADATA_KEY, identifier::SqliteIdentifier};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SqliteTemporalEncoding {
    Iso8601Text,
    UnixSeconds,
    UnixMilliseconds,
    UnixMicroseconds,
    UnixNanoseconds,
}

pub(crate) fn validate_sqlite_table_resource_shape(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    _table: &SqliteIdentifier,
    stable_key: Option<&SqliteIdentifier>,
    temporal_encodings: &BTreeMap<String, SqliteTemporalEncoding>,
) -> Result<()> {
    execution_schema_hash(descriptor)?;
    if schema.fields().is_empty() {
        return Err(CdfError::data(
            "SQLite table source execution requires a declared schema with at least one field",
        ));
    }
    let mut names = BTreeSet::new();
    for field in schema.fields() {
        if !names.insert(field.name().to_owned()) {
            return Err(CdfError::contract(format!(
                "SQLite table source schema declares duplicate field `{}`",
                field.name()
            )));
        }
        validate_supported_field(field)?;
        SqliteIdentifier::new(field.name().as_str())?;
        source_column_identifier(field)?;
        if matches!(
            field.data_type(),
            DataType::Date32 | DataType::Timestamp(..)
        ) && temporal_encoding(field, temporal_encodings).is_none()
        {
            return Err(CdfError::contract(format!(
                "SQLite temporal field `{}` requires an explicit encoding",
                field.name()
            )));
        }
    }
    for field in temporal_encodings.keys() {
        let schema_field = field_by_name(schema, field).ok_or_else(|| {
            CdfError::contract(format!(
                "SQLite temporal encoding names unknown field `{field}`"
            ))
        })?;
        if !matches!(
            schema_field.data_type(),
            DataType::Date32 | DataType::Timestamp(..)
        ) {
            return Err(CdfError::contract(format!(
                "SQLite temporal encoding for `{field}` requires a date or timestamp Arrow field"
            )));
        }
    }
    match (&descriptor.cursor, stable_key) {
        (None, Some(_)) => {
            return Err(CdfError::contract(
                "SQLite stable_key is valid only for a cursor resource",
            ));
        }
        (Some(_), None) => {
            return Err(CdfError::contract(
                "SQLite cursor resources require a stable_key tie-breaker",
            ));
        }
        _ => {}
    }
    if let Some(cursor) = &descriptor.cursor {
        let cursor_field = field_by_name(schema, &cursor.field).ok_or_else(|| {
            CdfError::data(format!(
                "SQLite cursor field `{}` is missing from the declared schema",
                cursor.field
            ))
        })?;
        if !matches!(
            cursor_field.data_type(),
            DataType::Int64 | DataType::UInt64 | DataType::Date32 | DataType::Timestamp(..)
        ) {
            return Err(CdfError::contract(format!(
                "SQLite cursor field `{}` must be int64, uint64, date32, or timestamp",
                cursor.field
            )));
        }
        let stable_key = stable_key.expect("checked above");
        if stable_key.as_str() == cursor.field {
            return Err(CdfError::contract(
                "SQLite cursor stable_key must differ from the cursor field",
            ));
        }
        let stable_field =
            field_by_source_or_output_name(schema, stable_key.as_str()).ok_or_else(|| {
                CdfError::contract(format!(
                    "SQLite stable_key `{stable_key}` is missing from the declared schema"
                ))
            })?;
        if stable_field.is_nullable() {
            return Err(CdfError::contract(format!(
                "SQLite stable_key `{stable_key}` must be non-nullable"
            )));
        }
        if !matches!(
            stable_field.data_type(),
            DataType::Int64 | DataType::UInt64 | DataType::Utf8
        ) {
            return Err(CdfError::contract(format!(
                "SQLite stable_key `{stable_key}` must be int64, uint64, or string"
            )));
        }
    }
    Ok(())
}

fn validate_supported_field(field: &Field) -> Result<()> {
    if matches!(
        field.data_type(),
        DataType::Boolean
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Utf8
            | DataType::Binary
            | DataType::Date32
            | DataType::Timestamp(
                TimeUnit::Second
                    | TimeUnit::Millisecond
                    | TimeUnit::Microsecond
                    | TimeUnit::Nanosecond,
                _
            )
    ) {
        Ok(())
    } else {
        Err(CdfError::data(format!(
            "SQLite table source does not support Arrow type {:?} for field `{}`",
            field.data_type(),
            field.name()
        )))
    }
}

pub(super) fn source_column_identifier(field: &Field) -> Result<SqliteIdentifier> {
    SqliteIdentifier::new(source_name(field).unwrap_or_else(|| field.name().as_str()))
}

pub(super) fn field_by_name<'a>(schema: &'a Schema, name: &str) -> Option<&'a Field> {
    schema
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .map(AsRef::as_ref)
}

pub(super) fn field_by_source_or_output_name<'a>(
    schema: &'a Schema,
    name: &str,
) -> Option<&'a Field> {
    schema
        .fields()
        .iter()
        .map(AsRef::as_ref)
        .find(|field| field.name() == name || source_name(field) == Some(name))
}

pub(super) fn temporal_encoding(
    field: &Field,
    encodings: &BTreeMap<String, SqliteTemporalEncoding>,
) -> Option<SqliteTemporalEncoding> {
    encodings.get(field.name()).copied().or_else(|| {
        field
            .metadata()
            .get(SQLITE_TEMPORAL_ENCODING_METADATA_KEY)
            .and_then(|value| serde_json::from_value(serde_json::Value::String(value.clone())).ok())
    })
}

pub(super) fn type_mismatch<T>(field: &Field, value: ValueRef<'_>, expected: &str) -> Result<T> {
    Err(storage_class_mismatch(field, value.data_type(), expected))
}

pub(super) fn storage_class_mismatch(
    field: &Field,
    observed: impl std::fmt::Display,
    expected: &str,
) -> CdfError {
    CdfError::data(format!(
        "SQLite field `{}` has dynamic storage class {observed} outside pinned Arrow type {:?}; expected {expected}",
        field.name(),
        field.data_type()
    ))
}

fn execution_schema_hash(descriptor: &ResourceDescriptor) -> Result<SchemaHash> {
    match &descriptor.schema_source {
        SchemaSource::Declared { schema_hash, .. } => Ok(schema_hash.clone()),
        SchemaSource::Active { schema_hash } => Ok(schema_hash.clone()),
        SchemaSource::Discovered { snapshot } => Ok(snapshot.schema_hash.clone()),
        _ => Err(CdfError::data(
            "SQLite table source execution requires an active, declared, or discovered schema hash",
        )),
    }
}
