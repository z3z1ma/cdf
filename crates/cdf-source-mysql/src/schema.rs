use std::collections::{BTreeSet, HashMap};

use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{
    CdfError, ForeignState, ResourceDescriptor, ResourceId, Result, SourcePosition,
    with_physical_type,
};
use cdf_runtime::artifact_hash;
use mysql_async::{Column, consts::ColumnFlags, consts::ColumnType};

pub(crate) const MYSQL_GENERATION_SCHEMA_KEY: &str = "cdf.source.mysql.generation";
pub(crate) const MYSQL_GENERATION_PROTOCOL: &str = "cdf.mysql.prepared-schema.v1";

pub(crate) fn schema_from_columns(resource_id: &ResourceId, columns: &[Column]) -> Result<Schema> {
    if columns.is_empty() {
        return Err(CdfError::data(format!(
            "MySQL resource `{resource_id}` produced no columns"
        )));
    }
    let mut names = BTreeSet::new();
    let mut descriptors = Vec::with_capacity(columns.len());
    let fields = columns
        .iter()
        .map(|column| {
            let name = std::str::from_utf8(column.name_ref()).map_err(|_| {
                CdfError::data(format!(
                    "MySQL resource `{resource_id}` produced a non-UTF-8 column name"
                ))
            })?;
            if !names.insert(name.to_owned()) {
                return Err(CdfError::data(format!(
                    "MySQL resource `{resource_id}` produced duplicate column `{name}`; alias every output column uniquely"
                )));
            }
            crate::identifier::MySqlIdentifier::user(name)?;
            let data_type = arrow_type(column)?;
            let physical = physical_type(column);
            descriptors.push((
                name.to_owned(),
                physical.clone(),
                column.flags().bits(),
                column.character_set(),
                column.column_length(),
                column.decimals(),
            ));
            Ok(with_physical_type(
                Field::new(
                    name,
                    data_type,
                    !column.flags().contains(ColumnFlags::NOT_NULL_FLAG),
                ),
                physical,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let generation = artifact_hash(&(MYSQL_GENERATION_PROTOCOL, descriptors))?;
    Ok(Schema::new_with_metadata(
        fields,
        HashMap::from([(MYSQL_GENERATION_SCHEMA_KEY.to_owned(), generation)]),
    ))
}

pub(crate) fn generation_from_schema(schema: &Schema) -> Result<&str> {
    schema
        .metadata()
        .get(MYSQL_GENERATION_SCHEMA_KEY)
        .map(String::as_str)
        .ok_or_else(|| {
            CdfError::data(
                "MySQL schema omitted prepared-statement generation evidence; refresh discovery",
            )
        })
}

pub(crate) fn generation_position(
    descriptor: &ResourceDescriptor,
    input_identity: &str,
    schema: &Schema,
) -> Result<SourcePosition> {
    let authority = (
        MYSQL_GENERATION_PROTOCOL,
        descriptor.resource_id.as_str(),
        input_identity,
        generation_from_schema(schema)?,
    );
    let opaque_blob = serde_json::to_vec(&authority).map_err(|error| {
        CdfError::internal(format!(
            "serialize MySQL prepared-schema authority: {error}"
        ))
    })?;
    let position = SourcePosition::ForeignState(ForeignState {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        protocol: MYSQL_GENERATION_PROTOCOL.to_owned(),
        blob_sha256: artifact_hash(&authority)?,
        opaque_blob,
    });
    position.validate()?;
    Ok(position)
}

pub(crate) fn arrow_type(column: &Column) -> Result<DataType> {
    use ColumnType::*;
    let unsigned = column.flags().contains(ColumnFlags::UNSIGNED_FLAG);
    let data_type = match column.column_type() {
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_LONG | MYSQL_TYPE_INT24
        | MYSQL_TYPE_LONGLONG | MYSQL_TYPE_YEAR => {
            if unsigned {
                DataType::UInt64
            } else {
                DataType::Int64
            }
        }
        MYSQL_TYPE_FLOAT => DataType::Float32,
        MYSQL_TYPE_DOUBLE => DataType::Float64,
        MYSQL_TYPE_DECIMAL
        | MYSQL_TYPE_NEWDECIMAL
        | MYSQL_TYPE_JSON
        | MYSQL_TYPE_DATE
        | MYSQL_TYPE_NEWDATE
        | MYSQL_TYPE_DATETIME
        | MYSQL_TYPE_DATETIME2
        | MYSQL_TYPE_TIMESTAMP
        | MYSQL_TYPE_TIMESTAMP2
        | MYSQL_TYPE_TIME
        | MYSQL_TYPE_TIME2
        | MYSQL_TYPE_ENUM
        | MYSQL_TYPE_SET => DataType::Utf8,
        MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_VAR_STRING
        | MYSQL_TYPE_STRING
        | MYSQL_TYPE_TINY_BLOB
        | MYSQL_TYPE_MEDIUM_BLOB
        | MYSQL_TYPE_LONG_BLOB
        | MYSQL_TYPE_BLOB => {
            if column.character_set() == 63 || column.flags().contains(ColumnFlags::BINARY_FLAG) {
                DataType::Binary
            } else {
                DataType::Utf8
            }
        }
        MYSQL_TYPE_BIT | MYSQL_TYPE_GEOMETRY | MYSQL_TYPE_VECTOR => DataType::Binary,
        MYSQL_TYPE_NULL | MYSQL_TYPE_TYPED_ARRAY | MYSQL_TYPE_UNKNOWN => {
            return Err(CdfError::data(format!(
                "MySQL column `{}` has unsupported prepared type {:?}; cast it to an exact supported type in the native query",
                column.name_str(),
                column.column_type()
            )));
        }
    };
    Ok(data_type)
}

pub(crate) fn physical_type(column: &Column) -> String {
    format!(
        "{:?};unsigned={};charset={};length={};decimals={}",
        column.column_type(),
        column.flags().contains(ColumnFlags::UNSIGNED_FLAG),
        column.character_set(),
        column.column_length(),
        column.decimals()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_mysql_binary_metadata_without_guessing_nested_domains() {
        let columns = vec![
            Column::new(ColumnType::MYSQL_TYPE_LONGLONG)
                .with_name(b"signed_id")
                .with_flags(ColumnFlags::NOT_NULL_FLAG),
            Column::new(ColumnType::MYSQL_TYPE_LONGLONG)
                .with_name(b"unsigned_id")
                .with_flags(ColumnFlags::UNSIGNED_FLAG),
            Column::new(ColumnType::MYSQL_TYPE_NEWDECIMAL)
                .with_name(b"amount")
                .with_decimals(9),
            Column::new(ColumnType::MYSQL_TYPE_JSON).with_name(b"payload"),
            Column::new(ColumnType::MYSQL_TYPE_BLOB)
                .with_name(b"raw")
                .with_character_set(63),
        ];
        let schema =
            schema_from_columns(&ResourceId::new("mysql.types").unwrap(), &columns).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(4).data_type(), &DataType::Binary);
        assert!(schema.metadata().contains_key(MYSQL_GENERATION_SCHEMA_KEY));
    }
}
