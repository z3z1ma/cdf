use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use cdf_kernel::{CdfError, Result};
use prost_reflect::{Cardinality, FieldDescriptor, Kind, MessageDescriptor};

#[derive(Clone, Debug)]
pub(crate) struct MessagePlan {
    pub(crate) descriptor: MessageDescriptor,
    pub(crate) descriptor_fields: BTreeMap<u32, FieldDescriptor>,
    pub(crate) fields: Vec<FieldPlan>,
    pub(crate) arrow_schema: SchemaRef,
}

#[derive(Clone, Debug)]
pub(crate) struct FieldPlan {
    pub(crate) descriptor: FieldDescriptor,
    pub(crate) arrow_field: FieldRef,
    pub(crate) value: ValuePlan,
}

#[derive(Clone, Debug)]
pub(crate) enum ValuePlan {
    Scalar(ScalarPlan),
    Message(Box<MessagePlan>),
    OpaqueMessage {
        message_name: String,
    },
    Timestamp,
    Duration,
    Wrapper(ScalarPlan),
    Any,
    FieldMask,
    Empty,
    List(Box<ValuePlan>),
    Map {
        key: Box<ValuePlan>,
        value: Box<ValuePlan>,
    },
}

#[derive(Clone, Debug)]
pub(crate) enum ScalarPlan {
    Double,
    Float,
    Int32,
    Int64,
    Uint32,
    Uint64,
    Sint32,
    Sint64,
    Fixed32,
    Fixed64,
    Sfixed32,
    Sfixed64,
    Bool,
    String,
    Bytes,
    Enum {
        full_name: String,
        values: BTreeMap<i32, String>,
    },
}

impl MessagePlan {
    pub(crate) fn compile(descriptor: MessageDescriptor) -> Result<Self> {
        let mut stack = Vec::new();
        compile_message(descriptor, &mut stack)
    }

    pub(crate) fn projected(&self, projection: Option<&[String]>) -> Result<Self> {
        let Some(projection) = projection else {
            return Ok(self.clone());
        };
        let mut fields = Vec::with_capacity(projection.len());
        for name in projection {
            let field = self
                .fields
                .iter()
                .find(|field| field.descriptor.name() == name)
                .ok_or_else(|| {
                    CdfError::contract(format!(
                        "Protobuf projection references unknown top-level field `{name}`"
                    ))
                })?;
            if fields.iter().any(|existing: &FieldPlan| {
                existing.descriptor.number() == field.descriptor.number()
            }) {
                return Err(CdfError::contract(format!(
                    "Protobuf projection repeats top-level field `{name}`"
                )));
            }
            fields.push(field.clone());
        }
        let arrow_schema = Arc::new(Schema::new_with_metadata(
            fields
                .iter()
                .map(|field| Arc::clone(&field.arrow_field))
                .collect::<Vec<_>>(),
            self.arrow_schema.metadata().clone(),
        ));
        Ok(Self {
            descriptor: self.descriptor.clone(),
            descriptor_fields: self.descriptor_fields.clone(),
            fields,
            arrow_schema,
        })
    }
}

fn compile_message(descriptor: MessageDescriptor, stack: &mut Vec<String>) -> Result<MessagePlan> {
    if descriptor.is_map_entry() {
        return Err(CdfError::contract(format!(
            "Protobuf root message `{}` is a synthetic map entry and cannot be decoded as a resource row",
            descriptor.full_name()
        )));
    }
    stack.push(descriptor.full_name().to_owned());
    let mut fields = Vec::with_capacity(descriptor.fields().len());
    for field in descriptor.fields() {
        fields.push(compile_field(field, stack)?);
    }
    stack.pop();
    fields.sort_by_key(|field| field.descriptor.number());
    let descriptor_fields = descriptor
        .fields()
        .map(|field| (field.number(), field))
        .collect();
    let mut metadata = HashMap::new();
    metadata.insert(
        "cdf:protobuf_message".to_owned(),
        descriptor.full_name().to_owned(),
    );
    let arrow_schema = Arc::new(Schema::new_with_metadata(
        fields
            .iter()
            .map(|field| Arc::clone(&field.arrow_field))
            .collect::<Vec<_>>(),
        metadata,
    ));
    Ok(MessagePlan {
        descriptor,
        descriptor_fields,
        fields,
        arrow_schema,
    })
}

fn compile_field(field: FieldDescriptor, stack: &mut Vec<String>) -> Result<FieldPlan> {
    let value = if field.is_map() {
        let Kind::Message(entry) = field.kind() else {
            return Err(CdfError::internal(
                "Protobuf map field did not resolve to a message",
            ));
        };
        ValuePlan::Map {
            key: Box::new(compile_kind(entry.map_entry_key_field(), stack)?),
            value: Box::new(compile_kind(entry.map_entry_value_field(), stack)?),
        }
    } else if field.is_list() {
        ValuePlan::List(Box::new(compile_kind(field.clone(), stack)?))
    } else {
        compile_kind(field.clone(), stack)?
    };
    let nullable = !field.is_list()
        && !field.is_map()
        && field.cardinality() != Cardinality::Required
        && field.supports_presence();
    let mut metadata = HashMap::new();
    metadata.insert(
        "cdf:protobuf_field_number".to_owned(),
        field.number().to_string(),
    );
    metadata.insert(
        "cdf:protobuf_json_name".to_owned(),
        field.json_name().to_owned(),
    );
    metadata.insert(
        "cdf:protobuf_presence".to_owned(),
        if field.supports_presence() {
            "explicit"
        } else {
            "implicit"
        }
        .to_owned(),
    );
    if let Some(oneof) = field.containing_oneof() {
        metadata.insert(
            "cdf:protobuf_oneof".to_owned(),
            oneof.full_name().to_owned(),
        );
    }
    if field.is_packed() {
        metadata.insert("cdf:protobuf_packed".to_owned(), "true".to_owned());
    }
    if field.is_map() {
        metadata.insert(
            "cdf:protobuf_map_key_order".to_owned(),
            "canonical".to_owned(),
        );
    }
    value.extend_metadata(&mut metadata)?;
    let arrow_field =
        Arc::new(Field::new(field.name(), value.data_type()?, nullable).with_metadata(metadata));
    Ok(FieldPlan {
        descriptor: field,
        arrow_field,
        value,
    })
}

fn compile_kind(field: FieldDescriptor, stack: &mut Vec<String>) -> Result<ValuePlan> {
    Ok(match field.kind() {
        Kind::Double => ValuePlan::Scalar(ScalarPlan::Double),
        Kind::Float => ValuePlan::Scalar(ScalarPlan::Float),
        Kind::Int32 => ValuePlan::Scalar(ScalarPlan::Int32),
        Kind::Int64 => ValuePlan::Scalar(ScalarPlan::Int64),
        Kind::Uint32 => ValuePlan::Scalar(ScalarPlan::Uint32),
        Kind::Uint64 => ValuePlan::Scalar(ScalarPlan::Uint64),
        Kind::Sint32 => ValuePlan::Scalar(ScalarPlan::Sint32),
        Kind::Sint64 => ValuePlan::Scalar(ScalarPlan::Sint64),
        Kind::Fixed32 => ValuePlan::Scalar(ScalarPlan::Fixed32),
        Kind::Fixed64 => ValuePlan::Scalar(ScalarPlan::Fixed64),
        Kind::Sfixed32 => ValuePlan::Scalar(ScalarPlan::Sfixed32),
        Kind::Sfixed64 => ValuePlan::Scalar(ScalarPlan::Sfixed64),
        Kind::Bool => ValuePlan::Scalar(ScalarPlan::Bool),
        Kind::String => ValuePlan::Scalar(ScalarPlan::String),
        Kind::Bytes => ValuePlan::Scalar(ScalarPlan::Bytes),
        Kind::Enum(descriptor) => ValuePlan::Scalar(ScalarPlan::Enum {
            full_name: descriptor.full_name().to_owned(),
            values: descriptor
                .values()
                .map(|value| (value.number(), value.name().to_owned()))
                .collect(),
        }),
        Kind::Message(message) => compile_message_kind(message, stack)?,
    })
}

fn compile_message_kind(message: MessageDescriptor, stack: &mut Vec<String>) -> Result<ValuePlan> {
    Ok(match message.full_name() {
        "google.protobuf.Timestamp" => {
            validate_well_known(
                &message,
                &[(1, "seconds", "int64", false), (2, "nanos", "int32", false)],
            )?;
            ValuePlan::Timestamp
        }
        "google.protobuf.Duration" => {
            validate_well_known(
                &message,
                &[(1, "seconds", "int64", false), (2, "nanos", "int32", false)],
            )?;
            ValuePlan::Duration
        }
        "google.protobuf.DoubleValue" => wrapper_plan(&message, ScalarPlan::Double, "double")?,
        "google.protobuf.FloatValue" => wrapper_plan(&message, ScalarPlan::Float, "float")?,
        "google.protobuf.Int64Value" => wrapper_plan(&message, ScalarPlan::Int64, "int64")?,
        "google.protobuf.UInt64Value" => wrapper_plan(&message, ScalarPlan::Uint64, "uint64")?,
        "google.protobuf.Int32Value" => wrapper_plan(&message, ScalarPlan::Int32, "int32")?,
        "google.protobuf.UInt32Value" => wrapper_plan(&message, ScalarPlan::Uint32, "uint32")?,
        "google.protobuf.BoolValue" => wrapper_plan(&message, ScalarPlan::Bool, "bool")?,
        "google.protobuf.StringValue" => wrapper_plan(&message, ScalarPlan::String, "string")?,
        "google.protobuf.BytesValue" => wrapper_plan(&message, ScalarPlan::Bytes, "bytes")?,
        "google.protobuf.Any" => {
            validate_well_known(
                &message,
                &[
                    (1, "type_url", "string", false),
                    (2, "value", "bytes", false),
                ],
            )?;
            ValuePlan::Any
        }
        "google.protobuf.FieldMask" => {
            validate_well_known(&message, &[(1, "paths", "string", true)])?;
            ValuePlan::FieldMask
        }
        "google.protobuf.Empty" => {
            validate_well_known(&message, &[])?;
            ValuePlan::Empty
        }
        name if stack.iter().any(|active| active == name) => ValuePlan::OpaqueMessage {
            message_name: name.to_owned(),
        },
        _ => ValuePlan::Message(Box::new(compile_message(message, stack)?)),
    })
}

fn wrapper_plan(
    message: &MessageDescriptor,
    scalar: ScalarPlan,
    kind: &'static str,
) -> Result<ValuePlan> {
    validate_well_known(message, &[(1, "value", kind, false)])?;
    Ok(ValuePlan::Wrapper(scalar))
}

fn validate_well_known(
    message: &MessageDescriptor,
    expected: &[(u32, &str, &str, bool)],
) -> Result<()> {
    let actual = message.fields().collect::<Vec<_>>();
    let valid = actual.len() == expected.len()
        && expected.iter().all(|(number, name, kind, repeated)| {
            actual.iter().any(|field| {
                field.number() == *number
                    && field.name() == *name
                    && kind_name(&field.kind()) == *kind
                    && field.is_list() == *repeated
            })
        });
    if !valid {
        return Err(CdfError::contract(format!(
            "Protobuf descriptor definition for well-known type `{}` does not match its canonical field layout",
            message.full_name()
        )));
    }
    Ok(())
}

fn kind_name(kind: &Kind) -> &'static str {
    match kind {
        Kind::Double => "double",
        Kind::Float => "float",
        Kind::Int64 => "int64",
        Kind::Uint64 => "uint64",
        Kind::Int32 => "int32",
        Kind::Fixed64 => "fixed64",
        Kind::Fixed32 => "fixed32",
        Kind::Bool => "bool",
        Kind::String => "string",
        Kind::Bytes => "bytes",
        Kind::Uint32 => "uint32",
        Kind::Sfixed32 => "sfixed32",
        Kind::Sfixed64 => "sfixed64",
        Kind::Sint32 => "sint32",
        Kind::Sint64 => "sint64",
        Kind::Message(_) => "message",
        Kind::Enum(_) => "enum",
    }
}

impl ValuePlan {
    fn extend_metadata(&self, metadata: &mut HashMap<String, String>) -> Result<()> {
        match self {
            Self::Scalar(ScalarPlan::Enum { full_name, values }) => {
                metadata.insert("cdf:protobuf_enum".to_owned(), full_name.clone());
                metadata.insert(
                    "cdf:protobuf_enum_values".to_owned(),
                    serde_json::to_string(values).map_err(|error| {
                        CdfError::internal(format!("encode Protobuf enum metadata: {error}"))
                    })?,
                );
            }
            Self::OpaqueMessage { message_name } => {
                metadata.insert("cdf:protobuf_message".to_owned(), message_name.clone());
                metadata.insert(
                    "cdf:protobuf_message_encoding".to_owned(),
                    "wire".to_owned(),
                );
            }
            Self::Timestamp => {
                metadata.insert(
                    "cdf:protobuf_well_known_type".to_owned(),
                    "google.protobuf.Timestamp".to_owned(),
                );
            }
            Self::Duration => {
                metadata.insert(
                    "cdf:protobuf_well_known_type".to_owned(),
                    "google.protobuf.Duration".to_owned(),
                );
            }
            Self::Wrapper(scalar) => {
                metadata.insert(
                    "cdf:protobuf_well_known_type".to_owned(),
                    scalar.wrapper_name()?.to_owned(),
                );
            }
            Self::Any => {
                metadata.insert(
                    "cdf:protobuf_well_known_type".to_owned(),
                    "google.protobuf.Any".to_owned(),
                );
            }
            Self::FieldMask => {
                metadata.insert(
                    "cdf:protobuf_well_known_type".to_owned(),
                    "google.protobuf.FieldMask".to_owned(),
                );
            }
            Self::Empty => {
                metadata.insert(
                    "cdf:protobuf_well_known_type".to_owned(),
                    "google.protobuf.Empty".to_owned(),
                );
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) fn data_type(&self) -> Result<DataType> {
        Ok(match self {
            Self::Scalar(scalar) | Self::Wrapper(scalar) => scalar.data_type(),
            Self::Message(message) => DataType::Struct(
                message
                    .arrow_schema
                    .fields()
                    .iter()
                    .cloned()
                    .collect::<Fields>(),
            ),
            Self::OpaqueMessage { .. } => DataType::Binary,
            // The legal Protobuf ranges exceed Arrow's nanosecond timestamp/duration range.
            // Preserve both components rather than truncating nanos or rejecting legal values.
            Self::Timestamp | Self::Duration => DataType::Struct(
                vec![
                    Arc::new(Field::new("seconds", DataType::Int64, false)),
                    Arc::new(Field::new("nanos", DataType::Int32, false)),
                ]
                .into(),
            ),
            Self::Any => DataType::Struct(
                vec![
                    Arc::new(Field::new("type_url", DataType::Utf8, false)),
                    Arc::new(Field::new("value", DataType::Binary, false)),
                ]
                .into(),
            ),
            Self::FieldMask => DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            Self::Empty => DataType::Struct(Fields::empty()),
            Self::List(value) => {
                DataType::List(Arc::new(Field::new("item", value.data_type()?, false)))
            }
            Self::Map { key, value } => {
                let entries = Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", key.data_type()?, false)),
                            Arc::new(Field::new("value", value.data_type()?, false)),
                        ]
                        .into(),
                    ),
                    false,
                ));
                // Arrow's generic MapBuilder materializes the standard unsorted type. CDF still
                // canonicalizes entries by key before append; metadata records that stronger
                // deterministic ordering without claiming a builder flag it cannot emit.
                DataType::Map(entries, false)
            }
        })
    }
}

impl ScalarPlan {
    fn wrapper_name(&self) -> Result<&'static str> {
        Ok(match self {
            Self::Double => "google.protobuf.DoubleValue",
            Self::Float => "google.protobuf.FloatValue",
            Self::Int64 => "google.protobuf.Int64Value",
            Self::Uint64 => "google.protobuf.UInt64Value",
            Self::Int32 => "google.protobuf.Int32Value",
            Self::Uint32 => "google.protobuf.UInt32Value",
            Self::Bool => "google.protobuf.BoolValue",
            Self::String => "google.protobuf.StringValue",
            Self::Bytes => "google.protobuf.BytesValue",
            Self::Sint32
            | Self::Sint64
            | Self::Fixed32
            | Self::Fixed64
            | Self::Sfixed32
            | Self::Sfixed64
            | Self::Enum { .. } => {
                return Err(CdfError::internal(
                    "non-wrapper Protobuf scalar reached wrapper metadata",
                ));
            }
        })
    }

    pub(crate) fn data_type(&self) -> DataType {
        match self {
            Self::Double => DataType::Float64,
            Self::Float => DataType::Float32,
            Self::Int32 | Self::Sint32 | Self::Sfixed32 | Self::Enum { .. } => DataType::Int32,
            Self::Int64 | Self::Sint64 | Self::Sfixed64 => DataType::Int64,
            Self::Uint32 | Self::Fixed32 => DataType::UInt32,
            Self::Uint64 | Self::Fixed64 => DataType::UInt64,
            Self::Bool => DataType::Boolean,
            Self::String => DataType::Utf8,
            Self::Bytes => DataType::Binary,
        }
    }
}
