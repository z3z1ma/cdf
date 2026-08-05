use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float64Array, Int32Array, Int64Array, ListArray, NullArray, RecordBatch, StringArray,
    StructArray, TimestampMillisecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use cdf_kernel::{
    CanonicalArrowType, CdfError, PHYSICAL_TYPE_METADATA_KEY, PreContractPhysicalReconciliation,
    PreContractResidualCandidate, Result, SEMANTIC_METADATA_KEY, SourceMaterializationRule,
    physical_type, semantic, source_name, with_physical_type, with_semantic, with_source_name,
};
use mongodb::bson::{RawBsonRef, RawDocument};

pub(crate) const MONGODB_OBJECT_ID_SEMANTIC: &str = cdf_semantic::MONGODB_OBJECT_ID_SEMANTIC;
pub(crate) const MONGODB_DECIMAL_TEXT_SEMANTIC: &str =
    cdf_semantic::MONGODB_DECIMAL128_TEXT_SEMANTIC;
pub(crate) const MONGODB_DECIMAL128_MATERIALIZER: &str =
    "mongodb.bson_decimal128_to_arrow_decimal128.v1";
const MAXIMUM_SCHEMA_FIELDS: usize = 4_096;
const MAXIMUM_SCHEMA_DEPTH: usize = 32;
const MAXIMUM_RESIDUAL_CANDIDATES: usize = 65_536;
const MAXIMUM_PHYSICAL_RECONCILIATION_GROUPS: usize = MAXIMUM_SCHEMA_FIELDS;
const MAXIMUM_DOCUMENT_SHAPE_ELEMENTS: usize = 65_536;
const MAXIMUM_DOCUMENT_SHAPE_BYTES: u64 = 64 * 1024 * 1024;
const DOCUMENT_SHAPE_ELEMENT_OVERHEAD_BYTES: u64 = 256;
const MAXIMUM_RESIDUAL_PATH_SEGMENT_BYTES: usize = 1_024;
const MAXIMUM_RESIDUAL_EVIDENCE_BYTES: u64 = 64 * 1024 * 1024;
const MAXIMUM_COLUMN_ACCUMULATOR_BYTES: u64 = 32 * 1024 * 1024;
const RESIDUAL_CANDIDATE_ALLOCATION_OVERHEAD_BYTES: u64 = 512;

pub(crate) fn compile_source_materializations(
    schema: &Schema,
) -> Result<Vec<SourceMaterializationRule>> {
    let mut rules = Vec::new();
    for field in schema.fields() {
        collect_source_materializations(field, &mut Vec::new(), &mut rules)?;
    }
    rules.sort_by(|left, right| left.field_path.cmp(&right.field_path));
    Ok(rules)
}

fn collect_source_materializations(
    field: &Field,
    parent_path: &mut Vec<String>,
    rules: &mut Vec<SourceMaterializationRule>,
) -> Result<()> {
    parent_path.push(
        source_name(field)
            .unwrap_or_else(|| field.name())
            .to_owned(),
    );
    match field.data_type() {
        DataType::Decimal128(_, _) => rules.push(SourceMaterializationRule::new(
            MONGODB_DECIMAL128_MATERIALIZER,
            parent_path.clone(),
            CanonicalArrowType::from_arrow(&DataType::Utf8)?,
            BTreeMap::from([
                (
                    PHYSICAL_TYPE_METADATA_KEY.to_owned(),
                    "bson:decimal128".to_owned(),
                ),
                (
                    SEMANTIC_METADATA_KEY.to_owned(),
                    MONGODB_DECIMAL_TEXT_SEMANTIC.to_owned(),
                ),
            ]),
            CanonicalArrowType::from_arrow(field.data_type())?,
        )?),
        DataType::Struct(children) => {
            for child in children {
                collect_source_materializations(child, parent_path, rules)?;
            }
        }
        DataType::List(child) => {
            collect_source_materializations(child, parent_path, rules)?;
        }
        _ => {}
    }
    parent_path.pop();
    Ok(())
}

pub(crate) fn attach_expected_physical_types(
    logical_schema: &Schema,
    observed_schema: &Schema,
) -> Result<SchemaRef> {
    let fields = logical_schema
        .fields()
        .iter()
        .map(|logical| {
            let source = source_name(logical).unwrap_or_else(|| logical.name());
            let observed = observed_schema
                .fields()
                .iter()
                .find(|field| source_name(field).unwrap_or_else(|| field.name()) == source)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "MongoDB physical schema observation omitted pinned field `{source}`"
                    ))
                })?;
            attach_expected_field_physical_type(logical, observed)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        logical_schema.metadata().clone(),
    )))
}

fn attach_expected_field_physical_type(logical: &Field, observed: &Field) -> Result<Field> {
    let physical = physical_type(observed).ok_or_else(|| {
        CdfError::data(format!(
            "MongoDB physical schema observation omitted exact BSON type metadata for `{}`",
            source_name(observed).unwrap_or_else(|| observed.name())
        ))
    })?;
    validate_observed_physical_type(observed, physical)?;
    match (logical.data_type(), physical) {
        (DataType::Utf8, "bson:decimal128")
            if semantic(logical) != Some(MONGODB_DECIMAL_TEXT_SEMANTIC) =>
        {
            return Err(CdfError::data(format!(
                "MongoDB physical Decimal128 observation for `{}` requires the exact tagged-text semantic on a Utf8 pin",
                logical.name()
            )));
        }
        (DataType::Utf8, "bson:string")
            if semantic(logical) == Some(MONGODB_DECIMAL_TEXT_SEMANTIC) =>
        {
            return Err(CdfError::data(format!(
                "MongoDB physical string observation for `{}` contradicts its Decimal128 tagged-text semantic",
                logical.name()
            )));
        }
        _ => {}
    }
    let data_type = match (logical.data_type(), observed.data_type()) {
        (DataType::Struct(logical_children), DataType::Struct(observed_children)) => {
            let children = logical_children
                .iter()
                .map(|logical_child| {
                    let source = source_name(logical_child)
                        .unwrap_or_else(|| logical_child.name());
                    let observed_child = observed_children
                        .iter()
                        .find(|field| {
                            source_name(field).unwrap_or_else(|| field.name()) == source
                        })
                        .ok_or_else(|| {
                            CdfError::data(format!(
                                "MongoDB physical schema observation omitted pinned nested field `{source}`"
                            ))
                        })?;
                    attach_expected_field_physical_type(logical_child, observed_child)
                        .map(Arc::new)
                })
                .collect::<Result<Vec<_>>>()?;
            DataType::Struct(children.into())
        }
        (DataType::List(logical_child), DataType::List(observed_child)) => {
            DataType::List(Arc::new(attach_expected_field_physical_type(
                logical_child,
                observed_child,
            )?))
        }
        (logical_type, observed_type) if logical_type == observed_type => logical_type.clone(),
        (DataType::Int64, DataType::Int32) => DataType::Int64,
        (DataType::Decimal128(precision, scale), DataType::Utf8)
            if physical == "bson:decimal128" =>
        {
            DataType::Decimal128(*precision, *scale)
        }
        (logical_type, observed_type) => {
            return Err(CdfError::data(format!(
                "MongoDB physical schema observation type {observed_type} cannot materialize pinned type {logical_type} for `{}`",
                logical.name()
            )));
        }
    };
    Ok(with_physical_type(
        logical.clone().with_data_type(data_type),
        physical,
    ))
}

fn validate_observed_physical_type(field: &Field, physical: &str) -> Result<()> {
    let matches = match field.data_type() {
        DataType::Boolean => physical == "bson:boolean",
        DataType::Int32 => physical == "bson:int32",
        DataType::Int64 => physical == "bson:int64",
        DataType::Float64 => physical == "bson:double",
        DataType::Utf8 => match physical {
            "bson:string" => semantic(field) != Some(MONGODB_DECIMAL_TEXT_SEMANTIC),
            "bson:decimal128" => semantic(field) == Some(MONGODB_DECIMAL_TEXT_SEMANTIC),
            _ => false,
        },
        DataType::Binary => physical == "bson:binary",
        DataType::FixedSizeBinary(12) => physical == "bson:object_id",
        DataType::Date32 | DataType::Timestamp(TimeUnit::Millisecond, _) => {
            physical == "bson:date_time"
        }
        DataType::Decimal128(_, _) => physical == "bson:decimal128",
        DataType::List(_) => physical == "bson:array",
        DataType::Struct(_) => physical == "bson:document",
        DataType::Null => physical == "bson:null",
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(CdfError::data(format!(
            "MongoDB physical schema observation binds BSON type `{physical}` to incompatible Arrow type {} for `{}`",
            field.data_type(),
            source_name(field).unwrap_or_else(|| field.name())
        )))
    }
}

#[derive(Clone, Debug)]
enum InferredType {
    Boolean,
    Int32,
    Int64,
    Float64,
    Utf8,
    Binary,
    ObjectId,
    DateTime,
    DecimalText,
    List(Box<InferredType>),
    Struct {
        fields: BTreeMap<String, InferredField>,
        observed_documents: usize,
    },
    Null,
}

#[derive(Clone, Debug)]
struct InferredField {
    value: InferredType,
    nullable: bool,
    observed_documents: usize,
}

#[derive(Default)]
pub(crate) struct SchemaInference {
    fields: BTreeMap<String, InferredField>,
    retained_shape_elements: usize,
    documents: usize,
    bytes: u64,
}

impl SchemaInference {
    pub(crate) fn observe(&mut self, document: &RawDocument) -> Result<()> {
        validate_document_shape(document, 0)?;
        self.documents = self
            .documents
            .checked_add(1)
            .ok_or_else(|| CdfError::internal("MongoDB discovery document count overflow"))?;
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(document.as_bytes().len()).map_err(|_| {
                CdfError::internal("MongoDB discovery document byte count exceeds u64")
            })?)
            .ok_or_else(|| CdfError::internal("MongoDB discovery byte count overflow"))?;
        let mut observed = BTreeSet::new();
        for element in document {
            let (name, value) = element.map_err(|error| {
                CdfError::data(format!(
                    "MongoDB discovery encountered malformed BSON: {error}"
                ))
            })?;
            let name = name.to_string();
            if !observed.insert(name.clone()) {
                return Err(CdfError::data(format!(
                    "MongoDB document repeats field `{name}`"
                )));
            }
            if !self.fields.contains_key(&name) && self.fields.len() >= MAXIMUM_SCHEMA_FIELDS {
                return Err(CdfError::data(format!(
                    "MongoDB discovery exceeds the {MAXIMUM_SCHEMA_FIELDS}-field schema bound"
                )));
            }
            let inferred = infer_value(value, 0)?;
            match self.fields.get_mut(&name) {
                Some(field) => {
                    self.retained_shape_elements = self
                        .retained_shape_elements
                        .checked_add(merge_types(&mut field.value, inferred, &name)?)
                        .ok_or_else(|| CdfError::data("MongoDB discovery shape overflow"))?;
                    field.nullable |= matches!(value, RawBsonRef::Null);
                    field.observed_documents =
                        field.observed_documents.checked_add(1).ok_or_else(|| {
                            CdfError::internal("MongoDB discovery field count overflow")
                        })?;
                }
                None => {
                    let added = 1_usize
                        .checked_add(inferred_nested_field_count(&inferred)?)
                        .ok_or_else(|| CdfError::data("MongoDB discovery shape overflow"))?;
                    self.retained_shape_elements = self
                        .retained_shape_elements
                        .checked_add(added)
                        .ok_or_else(|| CdfError::data("MongoDB discovery shape overflow"))?;
                    self.fields.insert(
                        name,
                        InferredField {
                            value: inferred,
                            nullable: self.documents > 1 || matches!(value, RawBsonRef::Null),
                            observed_documents: 1,
                        },
                    );
                }
            }
            if self.retained_shape_elements > MAXIMUM_SCHEMA_FIELDS {
                return Err(CdfError::data(format!(
                    "MongoDB discovery exceeds the {MAXIMUM_SCHEMA_FIELDS}-field retained schema bound"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<(Schema, usize, u64)> {
        if self.documents == 0 || self.fields.is_empty() {
            return Err(CdfError::data(
                "MongoDB schema discovery observed no documents with fields",
            ));
        }
        for field in self.fields.values_mut() {
            if field.observed_documents != self.documents {
                field.nullable = true;
            }
        }
        let fields = self
            .fields
            .into_iter()
            .map(|(name, field)| inferred_field(name, field, 0))
            .collect::<Result<Vec<_>>>()?;
        Ok((
            Schema::new_with_metadata(
                fields,
                BTreeMap::from([
                    (
                        "cdf:mongodb_sample_documents".to_owned(),
                        self.documents.to_string(),
                    ),
                    (
                        "cdf:mongodb_sample_bytes".to_owned(),
                        self.bytes.to_string(),
                    ),
                ])
                .into_iter()
                .collect(),
            ),
            self.documents,
            self.bytes,
        ))
    }
}

fn infer_value(value: RawBsonRef<'_>, depth: usize) -> Result<InferredType> {
    if depth >= MAXIMUM_SCHEMA_DEPTH {
        return Err(CdfError::data(format!(
            "MongoDB BSON nesting exceeds the {MAXIMUM_SCHEMA_DEPTH}-level schema bound"
        )));
    }
    Ok(match value {
        RawBsonRef::Boolean(_) => InferredType::Boolean,
        RawBsonRef::Int32(_) => InferredType::Int32,
        RawBsonRef::Int64(_) => InferredType::Int64,
        RawBsonRef::Double(_) => InferredType::Float64,
        RawBsonRef::String(_) => InferredType::Utf8,
        RawBsonRef::Binary(_) => InferredType::Binary,
        RawBsonRef::ObjectId(_) => InferredType::ObjectId,
        RawBsonRef::DateTime(_) => InferredType::DateTime,
        RawBsonRef::Decimal128(_) => InferredType::DecimalText,
        RawBsonRef::Null => InferredType::Null,
        RawBsonRef::Array(array) => {
            let mut element = InferredType::Null;
            for value in array {
                let value = value.map_err(|error| {
                    CdfError::data(format!(
                        "MongoDB discovery encountered malformed array BSON: {error}"
                    ))
                })?;
                merge_types(
                    &mut element,
                    infer_value(value, depth + 1)?,
                    "array element",
                )?;
            }
            InferredType::List(Box::new(element))
        }
        RawBsonRef::Document(document) => {
            let mut fields = BTreeMap::new();
            let mut names = BTreeSet::new();
            for element in document {
                let (name, value) = element.map_err(|error| {
                    CdfError::data(format!(
                        "MongoDB discovery encountered malformed nested BSON: {error}"
                    ))
                })?;
                let name = name.to_string();
                if !names.insert(name.clone()) {
                    return Err(CdfError::data(format!(
                        "MongoDB nested document repeats field `{name}`"
                    )));
                }
                fields.insert(
                    name,
                    InferredField {
                        value: infer_value(value, depth + 1)?,
                        nullable: matches!(value, RawBsonRef::Null),
                        observed_documents: 1,
                    },
                );
            }
            InferredType::Struct {
                fields,
                observed_documents: 1,
            }
        }
        other => {
            return Err(CdfError::data(format!(
                "MongoDB BSON type {:?} requires explicit variant or quarantine policy",
                other.element_type()
            )));
        }
    })
}

fn merge_types(left: &mut InferredType, right: InferredType, path: &str) -> Result<usize> {
    if matches!(right, InferredType::Null) {
        return Ok(0);
    }
    if matches!(left, InferredType::Null) {
        let added = inferred_nested_field_count(&right)?;
        *left = right;
        return Ok(added);
    }
    match (left, right) {
        (left @ InferredType::Int32, InferredType::Int64) => {
            *left = InferredType::Int64;
            Ok(0)
        }
        (InferredType::Int64, InferredType::Int32) => Ok(0),
        (InferredType::List(left), InferredType::List(right)) => merge_types(left, *right, path),
        (
            InferredType::Struct {
                fields: left,
                observed_documents: left_documents,
            },
            InferredType::Struct {
                fields: right,
                observed_documents: right_documents,
            },
        ) => {
            *left_documents = left_documents
                .checked_add(right_documents)
                .ok_or_else(|| CdfError::internal("MongoDB nested document count overflow"))?;
            let mut added = 0_usize;
            for (name, right) in right {
                match left.get_mut(&name) {
                    Some(existing) => {
                        added = added
                            .checked_add(merge_types(
                                &mut existing.value,
                                right.value,
                                &format!("{path}.{name}"),
                            )?)
                            .ok_or_else(|| CdfError::data("MongoDB discovery shape overflow"))?;
                        existing.nullable |= right.nullable;
                        existing.observed_documents = existing
                            .observed_documents
                            .checked_add(right.observed_documents)
                            .ok_or_else(|| {
                                CdfError::internal("MongoDB nested field count overflow")
                            })?;
                    }
                    None => {
                        added = added
                            .checked_add(1)
                            .and_then(|count| {
                                inferred_nested_field_count(&right.value)
                                    .ok()
                                    .and_then(|nested| count.checked_add(nested))
                            })
                            .ok_or_else(|| CdfError::data("MongoDB discovery shape overflow"))?;
                        left.insert(name, right);
                    }
                }
            }
            Ok(added)
        }
        (left, right) if std::mem::discriminant(left) == std::mem::discriminant(&right) => Ok(0),
        (left, right) => Err(CdfError::data(format!(
            "MongoDB discovery observed heterogeneous BSON types at `{path}`: {left:?} and {right:?}; select an explicit variant or quarantine policy"
        ))),
    }
}

fn inferred_nested_field_count(value: &InferredType) -> Result<usize> {
    match value {
        InferredType::List(child) => inferred_nested_field_count(child),
        InferredType::Struct { fields, .. } => fields.values().try_fold(0_usize, |total, field| {
            let nested = inferred_nested_field_count(&field.value)?;
            total
                .checked_add(1)
                .and_then(|count| count.checked_add(nested))
                .ok_or_else(|| CdfError::data("MongoDB discovery shape overflow"))
        }),
        _ => Ok(0),
    }
}

fn inferred_field(name: String, inferred: InferredField, depth: usize) -> Result<Field> {
    let (data_type, physical_type, semantic_reference) = inferred_data_type(inferred.value, depth)?;
    let mut field = Field::new(&name, data_type, inferred.nullable);
    field = with_source_name(field, name);
    field = with_physical_type(field, physical_type);
    if let Some(reference) = semantic_reference {
        let reference = reference
            .parse::<cdf_kernel::SemanticReference>()
            .map_err(|error| {
                CdfError::contract(format!(
                    "MongoDB inferred invalid semantic reference `{reference}`: {error}"
                ))
            })?;
        field = with_semantic(field, &reference);
    }
    Ok(field)
}

fn inferred_data_type(
    value: InferredType,
    depth: usize,
) -> Result<(DataType, String, Option<String>)> {
    if depth >= MAXIMUM_SCHEMA_DEPTH {
        return Err(CdfError::data(
            "MongoDB inferred schema nesting is too deep",
        ));
    }
    Ok(match value {
        InferredType::Boolean => (DataType::Boolean, "bson:boolean".to_owned(), None),
        InferredType::Int32 => (DataType::Int32, "bson:int32".to_owned(), None),
        InferredType::Int64 => (DataType::Int64, "bson:int64".to_owned(), None),
        InferredType::Float64 => (DataType::Float64, "bson:double".to_owned(), None),
        InferredType::Utf8 => (DataType::Utf8, "bson:string".to_owned(), None),
        InferredType::Binary => (DataType::Binary, "bson:binary".to_owned(), None),
        InferredType::ObjectId => (
            DataType::FixedSizeBinary(12),
            "bson:object_id".to_owned(),
            Some(MONGODB_OBJECT_ID_SEMANTIC.to_owned()),
        ),
        InferredType::DateTime => (
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            "bson:date_time".to_owned(),
            None,
        ),
        InferredType::DecimalText => (
            DataType::Utf8,
            "bson:decimal128".to_owned(),
            Some(MONGODB_DECIMAL_TEXT_SEMANTIC.to_owned()),
        ),
        InferredType::List(element) => {
            let (data_type, physical, semantic) = inferred_data_type(*element, depth + 1)?;
            let mut child = Field::new("item", data_type, true);
            child = with_physical_type(child, physical);
            if let Some(reference) = semantic {
                let reference = reference
                    .parse::<cdf_kernel::SemanticReference>()
                    .map_err(|error| {
                        CdfError::contract(format!(
                            "MongoDB inferred invalid nested semantic reference `{reference}`: {error}"
                        ))
                    })?;
                child = with_semantic(child, &reference);
            }
            (
                DataType::List(Arc::new(child)),
                "bson:array".to_owned(),
                None,
            )
        }
        InferredType::Struct {
            fields,
            observed_documents,
        } => {
            let fields = fields
                .into_iter()
                .map(|(name, mut field)| {
                    if field.observed_documents != observed_documents {
                        field.nullable = true;
                    }
                    inferred_field(name, field, depth + 1)
                })
                .collect::<Result<Vec<_>>>()?;
            (
                DataType::Struct(fields.into()),
                "bson:document".to_owned(),
                None,
            )
        }
        InferredType::Null => (DataType::Null, "bson:null".to_owned(), None),
    })
}

pub(crate) fn validate_mongodb_schema(schema: &Schema) -> Result<()> {
    if schema.fields().is_empty() || schema.fields().len() > MAXIMUM_SCHEMA_FIELDS {
        return Err(CdfError::data(format!(
            "MongoDB execution requires 1..={MAXIMUM_SCHEMA_FIELDS} pinned fields"
        )));
    }
    for field in schema.fields() {
        validate_field(field, 0)?;
    }
    Ok(())
}

fn validate_field(field: &Field, depth: usize) -> Result<()> {
    if depth >= MAXIMUM_SCHEMA_DEPTH {
        return Err(CdfError::data("MongoDB schema nesting is too deep"));
    }
    crate::identifier::validate_field_path(field.name())?;
    match field.data_type() {
        DataType::Boolean
        | DataType::Int32
        | DataType::Int64
        | DataType::Float64
        | DataType::Utf8
        | DataType::Binary
        | DataType::FixedSizeBinary(12)
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Millisecond, _)
        | DataType::Decimal128(_, _)
        | DataType::Null => Ok(()),
        DataType::List(child) => validate_field(child, depth + 1),
        DataType::Struct(fields) => fields
            .iter()
            .try_for_each(|field| validate_field(field, depth + 1)),
        data_type => Err(CdfError::contract(format!(
            "MongoDB field `{}` uses unsupported Arrow type {data_type:?}",
            field.name()
        ))),
    }
}

#[derive(Debug)]
pub(crate) struct DecodedMongoBatch {
    pub(crate) record_batch: RecordBatch,
    pub(crate) physical_schema: Schema,
    pub(crate) residual_candidates: Vec<PreContractResidualCandidate>,
    pub(crate) physical_reconciliations: Vec<PreContractPhysicalReconciliation>,
    pub(crate) pre_contract_evidence_bytes: u64,
}

#[cfg(test)]
pub(crate) fn decode_batch(schema: SchemaRef, documents: &[&RawDocument]) -> Result<RecordBatch> {
    let decoded = decode_batch_with_evidence(Arc::clone(&schema), schema, documents, 0)?;
    if let Some(candidate) = decoded.residual_candidates.first() {
        return Err(CdfError::data(format!(
            "MongoDB value at `{}` contradicted the pinned Arrow schema",
            candidate.source_path().join(".")
        )));
    }
    Ok(decoded.record_batch)
}

#[cfg(test)]
pub(crate) fn decode_batch_with_evidence(
    full_schema: SchemaRef,
    output_schema: SchemaRef,
    documents: &[&RawDocument],
    source_row_offset: u64,
) -> Result<DecodedMongoBatch> {
    decode_batch_with_physical_schema(
        Arc::clone(&full_schema),
        Arc::clone(&output_schema),
        output_schema,
        full_schema,
        documents,
        source_row_offset,
    )
}

pub(crate) fn decode_batch_with_physical_schema(
    full_schema: SchemaRef,
    decoder_schema: SchemaRef,
    output_schema: SchemaRef,
    physical_schema: SchemaRef,
    documents: &[&RawDocument],
    source_row_offset: u64,
) -> Result<DecodedMongoBatch> {
    validate_mongodb_schema(full_schema.as_ref())?;
    validate_mongodb_schema(decoder_schema.as_ref())?;
    validate_mongodb_schema(output_schema.as_ref())?;
    validate_mongodb_schema(physical_schema.as_ref())?;
    if decoder_schema.fields().len() != output_schema.fields().len()
        || physical_schema.fields().len() != output_schema.fields().len()
        || physical_schema
            .fields()
            .iter()
            .zip(decoder_schema.fields())
            .zip(output_schema.fields())
            .any(|((physical, decoder), output)| {
                let physical_source = source_name(physical).unwrap_or_else(|| physical.name());
                let decoder_source = source_name(decoder).unwrap_or_else(|| decoder.name());
                let output_source = source_name(output).unwrap_or_else(|| output.name());
                physical_source != output_source || decoder_source != output_source
            })
    {
        return Err(CdfError::internal(
            "MongoDB physical observation projection does not align with decoder output",
        ));
    }
    preflight_column_accumulator_bytes(decoder_schema.as_ref(), documents)?;
    let mut columns = decoder_schema
        .fields()
        .iter()
        .zip(output_schema.fields())
        .map(|(decoder, output)| ColumnAccumulator::new_for_output(decoder, output))
        .collect::<Result<Vec<_>>>()?;
    let known_sources = full_schema
        .fields()
        .iter()
        .map(|field| {
            source_name(field)
                .unwrap_or_else(|| field.name())
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    let mut residual_candidates = Vec::new();
    let mut physical_reconciliation_accumulators = BTreeMap::new();
    let physical_reconciliation_fields = decoder_schema
        .fields()
        .iter()
        .map(|field| field_can_produce_physical_reconciliation(field))
        .collect::<Vec<_>>();
    let mut pre_contract_evidence_bytes = 0_u64;
    for (row, document) in documents.iter().enumerate() {
        for ((field, column), reconcile_physical) in decoder_schema
            .fields()
            .iter()
            .zip(&mut columns)
            .zip(&physical_reconciliation_fields)
        {
            let source = source_name(field).unwrap_or_else(|| field.name());
            let value = raw_value_at_path(document, source)?;
            if value_matches_field(field, value)? {
                let mut source_path = source.split('.').map(str::to_owned).collect::<Vec<_>>();
                if *reconcile_physical {
                    collect_physical_reconciliations(
                        &mut physical_reconciliation_accumulators,
                        row,
                        &mut source_path,
                        value,
                        field,
                        &mut pre_contract_evidence_bytes,
                    )?;
                }
                column.append(value)?;
                collect_nested_unknown_fields(
                    field,
                    value,
                    source_row_offset.saturating_add(row as u64),
                    row,
                    &mut source_path,
                    &mut residual_candidates,
                    &mut pre_contract_evidence_bytes,
                )?;
            } else {
                column.append(None)?;
                let candidate = residual_candidate(
                    source_row_offset.saturating_add(row as u64),
                    row,
                    source,
                    value,
                    Some(field.as_ref().clone()),
                    residual_candidates.len(),
                    pre_contract_evidence_bytes,
                )?;
                push_residual_candidate(
                    &mut residual_candidates,
                    &mut pre_contract_evidence_bytes,
                    candidate,
                )?;
            }
        }
        for element in document.iter() {
            let (name, value) = element.map_err(|error| {
                CdfError::data(format!("MongoDB source returned malformed BSON: {error}"))
            })?;
            let name = name.to_string();
            if known_sources.contains(&name) {
                continue;
            }
            let candidate = residual_candidate(
                source_row_offset.saturating_add(row as u64),
                row,
                &name,
                Some(value),
                None,
                residual_candidates.len(),
                pre_contract_evidence_bytes,
            )?;
            push_residual_candidate(
                &mut residual_candidates,
                &mut pre_contract_evidence_bytes,
                candidate,
            )?;
        }
    }
    let materialized_fields = output_schema
        .fields()
        .iter()
        .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
        .collect::<Vec<_>>();
    let materialized_schema = Arc::new(Schema::new_with_metadata(
        materialized_fields,
        output_schema.metadata().clone(),
    ));
    let arrays = columns
        .into_iter()
        .map(ColumnAccumulator::finish)
        .collect::<Result<Vec<_>>>()?;
    let record_batch = RecordBatch::try_new(materialized_schema, arrays).map_err(|error| {
        CdfError::data(format!(
            "MongoDB decoded batch contradicted the pinned Arrow schema: {error}"
        ))
    })?;
    let physical_reconciliations = physical_reconciliation_accumulators
        .into_values()
        .map(PhysicalReconciliationAccumulator::finish)
        .collect::<Result<Vec<_>>>()?;
    let exact_evidence_bytes =
        retained_pre_contract_evidence_bytes(&residual_candidates, &physical_reconciliations)?;
    validate_residual_evidence_bound(0, exact_evidence_bytes)?;
    pre_contract_evidence_bytes = pre_contract_evidence_bytes.max(exact_evidence_bytes);
    Ok(DecodedMongoBatch {
        record_batch,
        physical_schema: physical_schema.as_ref().clone(),
        residual_candidates,
        physical_reconciliations,
        pre_contract_evidence_bytes,
    })
}

fn preflight_column_accumulator_bytes(schema: &Schema, documents: &[&RawDocument]) -> Result<()> {
    let mut budget = DecodeAllocationBudget::default();
    budget.charge(
        schema
            .fields()
            .len()
            .checked_mul(std::mem::size_of::<ColumnAccumulator>())
            .ok_or_else(|| CdfError::data("MongoDB decoder column allocation overflow"))?,
    )?;
    if !schema
        .fields()
        .iter()
        .any(|field| field_contains_list(field))
    {
        for document in documents {
            validate_document_shape(document, 0)?;
        }
        let rows = documents.len();
        for field in schema.fields() {
            estimate_fixed_field_appends(field, rows, &mut budget)?;
        }
        let payload_multiplier = schema
            .fields()
            .iter()
            .map(|field| field_payload_multiplier(field))
            .max()
            .unwrap_or(0);
        if payload_multiplier > 0 {
            let raw_document_bytes = documents.iter().try_fold(0_usize, |total, document| {
                total
                    .checked_add(document.as_bytes().len())
                    .ok_or_else(|| CdfError::data("MongoDB decoder payload estimate overflow"))
            })?;
            budget.charge(
                raw_document_bytes
                    .checked_mul(payload_multiplier)
                    .ok_or_else(|| CdfError::data("MongoDB decoder payload estimate overflow"))?,
            )?;
        }
        return Ok(());
    }
    for document in documents {
        validate_document_shape(document, 0)?;
        for field in schema.fields() {
            let source = source_name(field).unwrap_or_else(|| field.name());
            estimate_field_append(field, raw_value_at_path(document, source)?, &mut budget)?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct DecodeAllocationBudget {
    bytes: u64,
}

impl DecodeAllocationBudget {
    fn charge(&mut self, bytes: usize) -> Result<()> {
        let bytes = u64::try_from(bytes)
            .map_err(|_| CdfError::data("MongoDB decoder allocation exceeds u64"))?;
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| CdfError::data("MongoDB decoder allocation overflow"))?;
        if self.bytes > MAXIMUM_COLUMN_ACCUMULATOR_BYTES {
            return Err(CdfError::data(format!(
                "MongoDB projected batch requires more than the {MAXIMUM_COLUMN_ACCUMULATOR_BYTES}-byte progressive decode bound; reduce batch_rows or project fewer fields"
            )));
        }
        Ok(())
    }

    fn charge_growable_entry<T>(&mut self) -> Result<()> {
        self.charge(std::mem::size_of::<T>().saturating_mul(2))
    }

    fn charge_growable_entries<T>(&mut self, entries: usize) -> Result<()> {
        let entry_bytes = std::mem::size_of::<T>()
            .checked_mul(2)
            .ok_or_else(|| CdfError::data("MongoDB decoder allocation overflow"))?;
        self.charge(
            entry_bytes
                .checked_mul(entries)
                .ok_or_else(|| CdfError::data("MongoDB decoder allocation overflow"))?,
        )
    }
}

fn field_contains_list(field: &Field) -> bool {
    match field.data_type() {
        DataType::List(_) => true,
        DataType::Struct(fields) => fields.iter().any(|field| field_contains_list(field)),
        _ => false,
    }
}

fn field_payload_multiplier(field: &Field) -> usize {
    match field.data_type() {
        DataType::Utf8 if semantic(field) == Some(MONGODB_DECIMAL_TEXT_SEMANTIC) => 3,
        DataType::Utf8 | DataType::Binary => 1,
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| field_payload_multiplier(field))
            .max()
            .unwrap_or(0),
        _ => 0,
    }
}

fn estimate_fixed_field_appends(
    field: &Field,
    rows: usize,
    budget: &mut DecodeAllocationBudget,
) -> Result<()> {
    match field.data_type() {
        DataType::Boolean => budget.charge_growable_entries::<Option<bool>>(rows),
        DataType::Int32 | DataType::Date32 => budget.charge_growable_entries::<Option<i32>>(rows),
        DataType::Int64 | DataType::Timestamp(TimeUnit::Millisecond, _) => {
            budget.charge_growable_entries::<Option<i64>>(rows)
        }
        DataType::Float64 => budget.charge_growable_entries::<Option<f64>>(rows),
        DataType::Utf8 => budget.charge_growable_entries::<Option<String>>(rows),
        DataType::Binary => budget.charge_growable_entries::<Option<Vec<u8>>>(rows),
        DataType::FixedSizeBinary(12) => budget.charge_growable_entries::<Option<[u8; 12]>>(rows),
        DataType::Decimal128(_, _) => budget.charge_growable_entries::<Option<i128>>(rows),
        DataType::Struct(fields) => {
            budget.charge_growable_entries::<bool>(rows)?;
            for child in fields {
                estimate_fixed_field_appends(child, rows, budget)?;
            }
            Ok(())
        }
        DataType::Null => Ok(()),
        DataType::List(_) => Err(CdfError::internal(
            "MongoDB fixed-cardinality preflight received a list field",
        )),
        other => Err(CdfError::contract(format!(
            "MongoDB decoder cannot preflight Arrow type {other:?}"
        ))),
    }
}

fn estimate_field_append(
    field: &Field,
    value: Option<RawBsonRef<'_>>,
    budget: &mut DecodeAllocationBudget,
) -> Result<()> {
    let value = value.filter(|value| !matches!(value, RawBsonRef::Null));
    match field.data_type() {
        DataType::Boolean => budget.charge_growable_entry::<Option<bool>>(),
        DataType::Int32 | DataType::Date32 => budget.charge_growable_entry::<Option<i32>>(),
        DataType::Int64 | DataType::Timestamp(TimeUnit::Millisecond, _) => {
            budget.charge_growable_entry::<Option<i64>>()
        }
        DataType::Float64 => budget.charge_growable_entry::<Option<f64>>(),
        DataType::Utf8 => {
            budget.charge_growable_entry::<Option<String>>()?;
            match value {
                Some(RawBsonRef::String(value)) => budget.charge(value.len()),
                Some(RawBsonRef::Decimal128(value)) => budget.charge(value.to_string().len()),
                _ => Ok(()),
            }
        }
        DataType::Binary => {
            budget.charge_growable_entry::<Option<Vec<u8>>>()?;
            match value {
                Some(RawBsonRef::Binary(value)) => budget.charge(value.bytes.len()),
                _ => Ok(()),
            }
        }
        DataType::FixedSizeBinary(12) => budget.charge_growable_entry::<Option<[u8; 12]>>(),
        DataType::Decimal128(_, _) => budget.charge_growable_entry::<Option<i128>>(),
        DataType::List(child) => {
            budget.charge_growable_entry::<usize>()?;
            budget.charge_growable_entry::<bool>()?;
            if let Some(RawBsonRef::Array(array)) = value {
                for value in array {
                    estimate_field_append(
                        child,
                        Some(value.map_err(|error| {
                            CdfError::data(format!("MongoDB array value is malformed: {error}"))
                        })?),
                        budget,
                    )?;
                }
            }
            Ok(())
        }
        DataType::Struct(fields) => {
            budget.charge_growable_entry::<bool>()?;
            let document = match value {
                Some(RawBsonRef::Document(document)) => Some(document),
                _ => None,
            };
            for child in fields {
                let child_value = match document {
                    Some(document) => {
                        let source = source_name(child).unwrap_or_else(|| child.name());
                        raw_value_at_path(document, source)?
                    }
                    None => None,
                };
                estimate_field_append(child, child_value, budget)?;
            }
            Ok(())
        }
        DataType::Null => Ok(()),
        other => Err(CdfError::contract(format!(
            "MongoDB decoder cannot preflight Arrow type {other:?}"
        ))),
    }
}

fn value_matches_field(field: &Field, value: Option<RawBsonRef<'_>>) -> Result<bool> {
    let Some(value) = value.filter(|value| !matches!(value, RawBsonRef::Null)) else {
        return Ok(field.is_nullable());
    };
    Ok(match (field.data_type(), value) {
        (DataType::Boolean, RawBsonRef::Boolean(_))
        | (DataType::Int32, RawBsonRef::Int32(_))
        | (DataType::Int64, RawBsonRef::Int32(_) | RawBsonRef::Int64(_))
        | (DataType::Utf8, RawBsonRef::String(_))
        | (DataType::Binary, RawBsonRef::Binary(_))
        | (DataType::FixedSizeBinary(12), RawBsonRef::ObjectId(_))
        | (DataType::Timestamp(TimeUnit::Millisecond, _), RawBsonRef::DateTime(_)) => true,
        (DataType::Float64, RawBsonRef::Double(_)) => true,
        (DataType::Utf8, RawBsonRef::Decimal128(_)) => {
            semantic(field) == Some(MONGODB_DECIMAL_TEXT_SEMANTIC)
        }
        (DataType::Date32, RawBsonRef::DateTime(value)) => {
            value.timestamp_millis().rem_euclid(86_400_000) == 0
                && i32::try_from(value.timestamp_millis().div_euclid(86_400_000)).is_ok()
        }
        (DataType::Decimal128(precision, scale), RawBsonRef::Decimal128(value)) => {
            parse_decimal128(&value.to_string(), *precision, *scale).is_ok()
        }
        (DataType::List(child), RawBsonRef::Array(array)) => {
            for value in array {
                let value = value.map_err(|error| {
                    CdfError::data(format!("MongoDB array value is malformed: {error}"))
                })?;
                if !value_matches_field(child, Some(value))? {
                    return Ok(false);
                }
            }
            true
        }
        (DataType::Struct(fields), RawBsonRef::Document(document)) => {
            validate_unique_document(document)?;
            for child in fields {
                let source = source_name(child).unwrap_or_else(|| child.name());
                if !value_matches_field(child, raw_value_at_path(document, source)?)? {
                    return Ok(false);
                }
            }
            true
        }
        (DataType::Null, _) => false,
        _ => false,
    })
}

fn residual_candidate(
    source_row_ordinal: u64,
    batch_row_ordinal: usize,
    source: &str,
    value: Option<RawBsonRef<'_>>,
    expected: Option<Field>,
    existing_candidates: usize,
    retained_bytes: u64,
) -> Result<AccountedResidualCandidate> {
    let source_path = source.split('.').map(str::to_owned).collect::<Vec<_>>();
    residual_candidate_at_path(
        source_row_ordinal,
        batch_row_ordinal,
        source_path,
        value,
        expected,
        existing_candidates,
        retained_bytes,
    )
}

struct AccountedResidualCandidate {
    candidate: PreContractResidualCandidate,
    retained_bytes: u64,
}

struct PhysicalReconciliationAccumulator {
    source_path: Vec<String>,
    observed_field: Field,
    expected_field: Field,
    values: ColumnAccumulator,
    batch_row_ordinals: Vec<usize>,
}

impl PhysicalReconciliationAccumulator {
    fn new(
        source_path: Vec<String>,
        observed_field: Field,
        expected_field: Field,
        batch_row_ordinal: usize,
        value: RawBsonRef<'_>,
    ) -> Result<Self> {
        let mut values = ColumnAccumulator::new(&observed_field)?;
        values.append(Some(value))?;
        Ok(Self {
            source_path,
            observed_field,
            expected_field,
            values,
            batch_row_ordinals: vec![batch_row_ordinal],
        })
    }

    fn push(&mut self, batch_row_ordinal: usize, value: RawBsonRef<'_>) -> Result<()> {
        if self
            .batch_row_ordinals
            .last()
            .is_some_and(|previous| *previous >= batch_row_ordinal)
        {
            return Err(CdfError::internal(
                "MongoDB physical reconciliation rows are not strictly increasing",
            ));
        }
        self.values.append(Some(value))?;
        self.batch_row_ordinals.push(batch_row_ordinal);
        Ok(())
    }

    fn finish(self) -> Result<PreContractPhysicalReconciliation> {
        PreContractPhysicalReconciliation::new(
            self.source_path,
            self.observed_field,
            self.expected_field,
            self.values.finish()?,
            self.batch_row_ordinals,
        )
    }
}

fn field_can_produce_physical_reconciliation(field: &Field) -> bool {
    match field.data_type() {
        DataType::Int64 => matches!(physical_type(field), Some("bson:int32" | "bson:int64")),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| field_can_produce_physical_reconciliation(field)),
        DataType::List(child) => field_can_produce_physical_reconciliation(child),
        _ => false,
    }
}

fn collect_physical_reconciliations(
    accumulators: &mut BTreeMap<Vec<String>, PhysicalReconciliationAccumulator>,
    batch_row_ordinal: usize,
    source_path: &mut Vec<String>,
    value: Option<RawBsonRef<'_>>,
    expected_field: &Field,
    retained_bytes: &mut u64,
) -> Result<()> {
    let Some(value) = value.filter(|value| !matches!(value, RawBsonRef::Null)) else {
        return Ok(());
    };
    match (expected_field.data_type(), value) {
        (DataType::Struct(fields), RawBsonRef::Document(document)) => {
            for child in fields {
                let source = source_name(child).unwrap_or_else(|| child.name());
                source_path.push(source.to_owned());
                collect_physical_reconciliations(
                    accumulators,
                    batch_row_ordinal,
                    source_path,
                    raw_value_at_path(document, source)?,
                    child,
                    retained_bytes,
                )?;
                source_path.pop();
            }
            return Ok(());
        }
        (DataType::List(child), RawBsonRef::Array(array)) => {
            for (index, value) in array.into_iter().enumerate() {
                source_path.push(index.to_string());
                collect_physical_reconciliations(
                    accumulators,
                    batch_row_ordinal,
                    source_path,
                    Some(value.map_err(|error| {
                        CdfError::data(format!("MongoDB array value is malformed: {error}"))
                    })?),
                    child,
                    retained_bytes,
                )?;
                source_path.pop();
            }
            return Ok(());
        }
        _ if value_matches_pinned_physical(expected_field, Some(value))? => return Ok(()),
        _ => {}
    }
    record_physical_reconciliation(
        accumulators,
        batch_row_ordinal,
        source_path,
        value,
        expected_field.clone(),
        retained_bytes,
    )
}

fn record_physical_reconciliation(
    accumulators: &mut BTreeMap<Vec<String>, PhysicalReconciliationAccumulator>,
    batch_row_ordinal: usize,
    source_path: &[String],
    value: RawBsonRef<'_>,
    expected_field: Field,
    retained_bytes: &mut u64,
) -> Result<()> {
    let observed_field = observed_value_field(expected_field.name(), value)?;
    let source_path = source_path.to_vec();
    let row_bytes = residual_value_allocation_floor(Some(value))?
        .checked_add(std::mem::size_of::<usize>() as u64)
        .and_then(|bytes| residual_path_bytes(&source_path).ok()?.checked_add(bytes))
        .and_then(|bytes| bytes.checked_add(RESIDUAL_CANDIDATE_ALLOCATION_OVERHEAD_BYTES))
        .ok_or_else(|| CdfError::internal("MongoDB physical evidence memory overflow"))?;
    validate_residual_evidence_bound(*retained_bytes, row_bytes)?;
    *retained_bytes = retained_bytes
        .checked_add(row_bytes)
        .ok_or_else(|| CdfError::internal("MongoDB physical evidence memory overflow"))?;
    if let Some(existing) = accumulators.get_mut(&source_path) {
        if existing.observed_field != observed_field || existing.expected_field != expected_field {
            return Err(CdfError::data(format!(
                "MongoDB batch observed multiple incompatible physical projections at `{}`",
                source_path.join(".")
            )));
        }
        return existing.push(batch_row_ordinal, value);
    }
    if accumulators.len() >= MAXIMUM_PHYSICAL_RECONCILIATION_GROUPS {
        return Err(CdfError::data(format!(
            "MongoDB batch exceeds the {MAXIMUM_PHYSICAL_RECONCILIATION_GROUPS}-group physical reconciliation bound; reduce batch_rows or narrow the source shape"
        )));
    }
    accumulators.insert(
        source_path.clone(),
        PhysicalReconciliationAccumulator::new(
            source_path,
            observed_field,
            expected_field,
            batch_row_ordinal,
            value,
        )?,
    );
    Ok(())
}

fn retained_pre_contract_evidence_bytes(
    residual_candidates: &[PreContractResidualCandidate],
    physical_reconciliations: &[PreContractPhysicalReconciliation],
) -> Result<u64> {
    let residual_capacity = if residual_candidates.is_empty() {
        0
    } else {
        residual_candidates.len().next_power_of_two().max(4)
    };
    let reconciliation_capacity = if physical_reconciliations.is_empty() {
        0
    } else {
        physical_reconciliations.len().next_power_of_two().max(4)
    };
    let container_bytes = residual_capacity
        .checked_mul(std::mem::size_of::<PreContractResidualCandidate>())
        .and_then(|bytes| {
            reconciliation_capacity
                .checked_mul(std::mem::size_of::<PreContractPhysicalReconciliation>())
                .and_then(|physical| bytes.checked_add(physical))
        })
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| CdfError::internal("MongoDB evidence container memory overflow"))?;
    residual_candidates
        .iter()
        .map(PreContractResidualCandidate::retained_bytes)
        .chain(
            physical_reconciliations
                .iter()
                .map(PreContractPhysicalReconciliation::retained_bytes),
        )
        .try_fold(container_bytes, |total, bytes| {
            total
                .checked_add(bytes?)
                .ok_or_else(|| CdfError::internal("MongoDB pre-contract evidence memory overflow"))
        })
}

fn residual_candidate_at_path(
    source_row_ordinal: u64,
    batch_row_ordinal: usize,
    source_path: Vec<String>,
    value: Option<RawBsonRef<'_>>,
    expected: Option<Field>,
    existing_candidates: usize,
    retained_bytes: u64,
) -> Result<AccountedResidualCandidate> {
    if source_path
        .iter()
        .any(|segment| segment.is_empty() || segment.len() > MAXIMUM_RESIDUAL_PATH_SEGMENT_BYTES)
    {
        return Err(CdfError::data(format!(
            "MongoDB residual field path exceeds the {MAXIMUM_RESIDUAL_PATH_SEGMENT_BYTES}-byte segment bound"
        )));
    }
    let source = source_path
        .last()
        .ok_or_else(|| CdfError::internal("MongoDB residual source path is empty"))?;
    if existing_candidates >= MAXIMUM_RESIDUAL_CANDIDATES {
        return Err(CdfError::data(format!(
            "MongoDB batch exceeds the {MAXIMUM_RESIDUAL_CANDIDATES}-candidate residual evidence bound; reduce batch_rows"
        )));
    }
    let path_bytes = residual_path_bytes(&source_path)?;
    let allocation_floor = residual_value_allocation_floor(value)?
        .checked_add(path_bytes)
        .and_then(|bytes| bytes.checked_add(RESIDUAL_CANDIDATE_ALLOCATION_OVERHEAD_BYTES))
        .ok_or_else(|| CdfError::internal("MongoDB residual evidence memory overflow"))?;
    validate_residual_evidence_bound(retained_bytes, allocation_floor)?;
    let (observed_field, value) = observed_value_evidence(source, value)?;
    let candidate = PreContractResidualCandidate::new(
        source_row_ordinal,
        batch_row_ordinal,
        source_path,
        observed_field,
        expected,
        value,
        0,
    )?;
    let array_bytes = u64::try_from(candidate.value().get_array_memory_size())
        .map_err(|_| CdfError::internal("MongoDB residual evidence memory exceeds u64"))?;
    let retained_bytes = allocation_floor.max(
        array_bytes
            .checked_add(path_bytes)
            .and_then(|bytes| bytes.checked_add(RESIDUAL_CANDIDATE_ALLOCATION_OVERHEAD_BYTES))
            .ok_or_else(|| CdfError::internal("MongoDB residual evidence memory overflow"))?,
    );
    Ok(AccountedResidualCandidate {
        candidate,
        retained_bytes,
    })
}

fn push_residual_candidate(
    candidates: &mut Vec<PreContractResidualCandidate>,
    retained_bytes: &mut u64,
    accounted: AccountedResidualCandidate,
) -> Result<()> {
    let next = retained_bytes
        .checked_add(accounted.retained_bytes)
        .ok_or_else(|| CdfError::internal("MongoDB residual evidence memory overflow"))?;
    validate_residual_evidence_bound(*retained_bytes, accounted.retained_bytes)?;
    *retained_bytes = next;
    candidates.push(accounted.candidate);
    Ok(())
}

fn residual_path_bytes(source_path: &[String]) -> Result<u64> {
    source_path.iter().try_fold(0_u64, |total, segment| {
        total
            .checked_add(
                u64::try_from(segment.len())
                    .map_err(|_| CdfError::internal("MongoDB residual path length exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::internal("MongoDB residual path memory overflow"))
    })
}

fn validate_residual_evidence_bound(retained_bytes: u64, candidate_bytes: u64) -> Result<()> {
    if retained_bytes
        .checked_add(candidate_bytes)
        .is_none_or(|next| next > MAXIMUM_RESIDUAL_EVIDENCE_BYTES)
    {
        return Err(CdfError::data(format!(
            "MongoDB batch residual evidence exceeds the {MAXIMUM_RESIDUAL_EVIDENCE_BYTES}-byte bound; reduce batch_rows or narrow the source shape"
        )));
    }
    Ok(())
}

fn residual_value_allocation_floor(value: Option<RawBsonRef<'_>>) -> Result<u64> {
    let raw_bytes = match value {
        None | Some(RawBsonRef::Null) => 1,
        Some(
            RawBsonRef::String(value)
            | RawBsonRef::JavaScriptCode(value)
            | RawBsonRef::Symbol(value),
        ) => u64::try_from(value.len())
            .map_err(|_| CdfError::internal("MongoDB residual string length exceeds u64"))?,
        Some(RawBsonRef::Document(value)) => u64::try_from(value.as_bytes().len())
            .map_err(|_| CdfError::internal("MongoDB residual document length exceeds u64"))?,
        Some(RawBsonRef::Array(value)) => u64::try_from(value.as_bytes().len())
            .map_err(|_| CdfError::internal("MongoDB residual array length exceeds u64"))?,
        Some(RawBsonRef::Binary(value)) => u64::try_from(value.bytes.len())
            .map_err(|_| CdfError::internal("MongoDB residual binary length exceeds u64"))?,
        Some(RawBsonRef::RegularExpression(value)) => {
            let bytes = value
                .pattern
                .as_str()
                .len()
                .checked_add(value.options.as_str().len())
                .ok_or_else(|| CdfError::internal("MongoDB residual regex length overflow"))?;
            u64::try_from(bytes)
                .map_err(|_| CdfError::internal("MongoDB residual regex length exceeds u64"))?
        }
        Some(RawBsonRef::JavaScriptCodeWithScope(value)) => {
            let code = u64::try_from(value.code.len()).map_err(|_| {
                CdfError::internal("MongoDB residual JavaScript length exceeds u64")
            })?;
            code.checked_add(u64::try_from(value.scope.as_bytes().len()).map_err(|_| {
                CdfError::internal("MongoDB residual JavaScript scope length exceeds u64")
            })?)
            .ok_or_else(|| CdfError::internal("MongoDB residual JavaScript length overflow"))?
        }
        // RawDbPointerRef intentionally hides its namespace bytes. Charging the complete evidence
        // budget fails a second candidate closed while leaving enough decode headroom for one
        // maximum-size BSON value to be copied into exact evidence.
        Some(RawBsonRef::DbPointer(_)) => MAXIMUM_RESIDUAL_EVIDENCE_BYTES / 4,
        Some(_) => 32,
    };
    raw_bytes
        .max(1)
        .checked_mul(4)
        .ok_or_else(|| CdfError::internal("MongoDB residual allocation estimate overflow"))
}

#[allow(clippy::too_many_arguments)]
fn collect_nested_unknown_fields(
    field: &Field,
    value: Option<RawBsonRef<'_>>,
    source_row_ordinal: u64,
    batch_row_ordinal: usize,
    source_path: &mut Vec<String>,
    candidates: &mut Vec<PreContractResidualCandidate>,
    retained_bytes: &mut u64,
) -> Result<()> {
    match (field.data_type(), value) {
        (DataType::Struct(fields), Some(RawBsonRef::Document(document))) => {
            let expected = fields
                .iter()
                .map(|child| source_name(child).unwrap_or_else(|| child.name()))
                .collect::<BTreeSet<_>>();
            for element in document {
                let (name, value) = element.map_err(|error| {
                    CdfError::data(format!("MongoDB source returned malformed BSON: {error}"))
                })?;
                if !expected.contains(name.as_str()) {
                    source_path.push(name.to_string());
                    let candidate = residual_candidate_at_path(
                        source_row_ordinal,
                        batch_row_ordinal,
                        source_path.clone(),
                        Some(value),
                        None,
                        candidates.len(),
                        *retained_bytes,
                    )?;
                    source_path.pop();
                    push_residual_candidate(candidates, retained_bytes, candidate)?;
                }
            }
            for child in fields {
                let name = source_name(child).unwrap_or_else(|| child.name());
                source_path.push(name.to_owned());
                collect_nested_unknown_fields(
                    child,
                    raw_value_at_path(document, name)?,
                    source_row_ordinal,
                    batch_row_ordinal,
                    source_path,
                    candidates,
                    retained_bytes,
                )?;
                source_path.pop();
            }
        }
        (DataType::List(child), Some(RawBsonRef::Array(array))) => {
            for (index, item) in array.into_iter().enumerate() {
                let item = item.map_err(|error| {
                    CdfError::data(format!("MongoDB array value is malformed: {error}"))
                })?;
                source_path.push(index.to_string());
                collect_nested_unknown_fields(
                    child,
                    Some(item),
                    source_row_ordinal,
                    batch_row_ordinal,
                    source_path,
                    candidates,
                    retained_bytes,
                )?;
                source_path.pop();
            }
        }
        _ => {}
    }
    Ok(())
}

fn observed_value_evidence(
    source: &str,
    value: Option<RawBsonRef<'_>>,
) -> Result<(Field, ArrayRef)> {
    let Some(value) = value.filter(|value| !matches!(value, RawBsonRef::Null)) else {
        return Ok((
            with_source_name(Field::new(source, DataType::Null, true), source),
            Arc::new(NullArray::new(1)),
        ));
    };
    match observed_value_field(source, value) {
        Ok(field) => {
            let mut column = ColumnAccumulator::new(&field)?;
            column.append(Some(value))?;
            Ok((field, column.finish()?))
        }
        Err(_) => {
            let owned = mongodb::bson::Bson::try_from(value).map_err(|error| {
                CdfError::data(format!(
                    "encode unsupported MongoDB value evidence: {error}"
                ))
            })?;
            let bytes = mongodb::bson::serialize_to_vec(&mongodb::bson::doc! {"value": owned})
                .map_err(|error| {
                    CdfError::data(format!(
                        "encode unsupported MongoDB value evidence: {error}"
                    ))
                })?;
            Ok((
                with_physical_type(
                    with_source_name(Field::new(source, DataType::Binary, true), source),
                    format!("bson:{:?}", value.element_type()),
                ),
                Arc::new(BinaryArray::from(vec![Some(bytes.as_slice())])),
            ))
        }
    }
}

fn observed_value_field(source: &str, value: RawBsonRef<'_>) -> Result<Field> {
    inferred_field(
        source.to_owned(),
        InferredField {
            value: infer_value(value, 0)?,
            nullable: true,
            observed_documents: 1,
        },
        0,
    )
}

fn value_matches_pinned_physical(field: &Field, value: Option<RawBsonRef<'_>>) -> Result<bool> {
    let Some(value) = value.filter(|value| !matches!(value, RawBsonRef::Null)) else {
        return Ok(field.is_nullable());
    };
    let Some(physical) = physical_type(field) else {
        // A declared Arrow-only schema has no BSON subtype contract to contradict. Logical
        // compatibility remains authoritative until discovery pins exact physical metadata.
        return Ok(true);
    };
    Ok(match (physical, value) {
        ("bson:boolean", RawBsonRef::Boolean(_))
        | ("bson:int32", RawBsonRef::Int32(_))
        | ("bson:int64", RawBsonRef::Int64(_))
        | ("bson:double", RawBsonRef::Double(_))
        | ("bson:string", RawBsonRef::String(_))
        | ("bson:binary", RawBsonRef::Binary(_))
        | ("bson:object_id", RawBsonRef::ObjectId(_))
        | ("bson:date_time", RawBsonRef::DateTime(_))
        | ("bson:decimal128", RawBsonRef::Decimal128(_)) => true,
        ("bson:array", RawBsonRef::Array(array)) => {
            let DataType::List(child) = field.data_type() else {
                return Ok(false);
            };
            for value in array {
                let value = value.map_err(|error| {
                    CdfError::data(format!("MongoDB array value is malformed: {error}"))
                })?;
                if !value_matches_pinned_physical(child, Some(value))? {
                    return Ok(false);
                }
            }
            true
        }
        ("bson:document", RawBsonRef::Document(document)) => {
            let DataType::Struct(fields) = field.data_type() else {
                return Ok(false);
            };
            for child in fields {
                let source = source_name(child).unwrap_or_else(|| child.name());
                if !value_matches_pinned_physical(child, raw_value_at_path(document, source)?)? {
                    return Ok(false);
                }
            }
            true
        }
        _ => false,
    })
}

fn validate_unique_document(document: &RawDocument) -> Result<()> {
    let mut names = BTreeSet::new();
    for element in document {
        let (name, _) = element.map_err(|error| {
            CdfError::data(format!("MongoDB source returned malformed BSON: {error}"))
        })?;
        if !names.insert(name) {
            return Err(CdfError::data(format!(
                "MongoDB source document repeats field `{name}`"
            )));
        }
    }
    Ok(())
}

fn validate_document_shape(document: &RawDocument, depth: usize) -> Result<()> {
    let mut budget = DocumentShapeBudget::default();
    validate_document_shape_with_budget(document, depth, &mut budget)
}

#[derive(Default)]
struct DocumentShapeBudget {
    elements: usize,
    estimated_bytes: u64,
}

impl DocumentShapeBudget {
    fn admit(&mut self, name_bytes: usize) -> Result<()> {
        if self.elements >= MAXIMUM_DOCUMENT_SHAPE_ELEMENTS {
            return Err(CdfError::data(format!(
                "MongoDB document exceeds the {MAXIMUM_DOCUMENT_SHAPE_ELEMENTS}-element structural bound"
            )));
        }
        let name_bytes = u64::try_from(name_bytes)
            .map_err(|_| CdfError::internal("MongoDB field name length exceeds u64"))?;
        let next = self
            .estimated_bytes
            .checked_add(name_bytes)
            .and_then(|bytes| bytes.checked_add(DOCUMENT_SHAPE_ELEMENT_OVERHEAD_BYTES))
            .ok_or_else(|| CdfError::internal("MongoDB structural memory estimate overflow"))?;
        if next > MAXIMUM_DOCUMENT_SHAPE_BYTES {
            return Err(CdfError::data(format!(
                "MongoDB document shape exceeds the {MAXIMUM_DOCUMENT_SHAPE_BYTES}-byte structural decode bound"
            )));
        }
        self.elements += 1;
        self.estimated_bytes = next;
        Ok(())
    }
}

fn validate_document_shape_with_budget(
    document: &RawDocument,
    depth: usize,
    budget: &mut DocumentShapeBudget,
) -> Result<()> {
    if depth > MAXIMUM_SCHEMA_DEPTH {
        return Err(CdfError::data("MongoDB document nesting is too deep"));
    }
    let mut names = BTreeSet::new();
    for element in document {
        let (name, value) = element.map_err(|error| {
            CdfError::data(format!("MongoDB source returned malformed BSON: {error}"))
        })?;
        budget.admit(name.len())?;
        if !names.insert(name.as_str()) {
            return Err(CdfError::data(format!(
                "MongoDB source document repeats field `{name}`"
            )));
        }
        if name.len() > MAXIMUM_RESIDUAL_PATH_SEGMENT_BYTES {
            return Err(CdfError::data(format!(
                "MongoDB field name exceeds the {MAXIMUM_RESIDUAL_PATH_SEGMENT_BYTES}-byte residual path bound"
            )));
        }
        if name.as_str().contains('.') {
            return Err(CdfError::data(format!(
                "MongoDB field `{name}` contains a literal dot and cannot be represented as an unambiguous CDF field path; rename the source field"
            )));
        }
        validate_nested_value_shape(value, depth + 1, budget)?;
    }
    Ok(())
}

fn validate_nested_value_shape(
    value: RawBsonRef<'_>,
    depth: usize,
    budget: &mut DocumentShapeBudget,
) -> Result<()> {
    if depth > MAXIMUM_SCHEMA_DEPTH {
        return Err(CdfError::data("MongoDB value nesting is too deep"));
    }
    match value {
        RawBsonRef::Document(document) => {
            validate_document_shape_with_budget(document, depth, budget)
        }
        RawBsonRef::JavaScriptCodeWithScope(value) => {
            validate_document_shape_with_budget(value.scope, depth, budget)
        }
        RawBsonRef::Array(array) => {
            for value in array {
                budget.admit(0)?;
                validate_nested_value_shape(
                    value.map_err(|error| {
                        CdfError::data(format!("MongoDB array value is malformed: {error}"))
                    })?,
                    depth + 1,
                    budget,
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn raw_value_at_path<'a>(document: &'a RawDocument, path: &str) -> Result<Option<RawBsonRef<'a>>> {
    let mut current = document;
    let mut parts = path.split('.').peekable();
    while let Some(part) = parts.next() {
        let value = current.get(part).map_err(|error| {
            CdfError::data(format!(
                "MongoDB field `{path}` could not be decoded: {error}"
            ))
        })?;
        if parts.peek().is_none() {
            return Ok(value);
        }
        current = match value {
            Some(RawBsonRef::Document(document)) => document,
            Some(RawBsonRef::Null) | None => return Ok(None),
            Some(_) => {
                return Err(CdfError::data(format!(
                    "MongoDB field path `{path}` traverses a non-document value"
                )));
            }
        };
    }
    Ok(None)
}

enum ColumnAccumulator {
    Boolean(Vec<Option<bool>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Utf8 {
        values: Vec<Option<String>>,
        decimal_text: bool,
    },
    Binary(Vec<Option<Vec<u8>>>),
    ObjectId(Vec<Option<[u8; 12]>>),
    Date32(Vec<Option<i32>>),
    TimestampMillis(Vec<Option<i64>>),
    Decimal {
        values: Vec<Option<i128>>,
        precision: u8,
        scale: i8,
    },
    List {
        field: Arc<Field>,
        lengths: Vec<usize>,
        valid: Vec<bool>,
        child: Box<ColumnAccumulator>,
    },
    Struct {
        fields: Fields,
        output_fields: Fields,
        valid: Vec<bool>,
        children: Vec<ColumnAccumulator>,
    },
    Null(usize),
}

impl ColumnAccumulator {
    fn new(field: &Field) -> Result<Self> {
        Self::new_for_output(field, field)
    }

    fn new_for_output(field: &Field, output: &Field) -> Result<Self> {
        let field_source = source_name(field).unwrap_or_else(|| field.name());
        let output_source = source_name(output).unwrap_or_else(|| output.name());
        if field.name() != output.name() || field_source != output_source {
            return Err(CdfError::internal(
                "MongoDB decoder field does not align with its logical output field",
            ));
        }
        Ok(match (field.data_type(), output.data_type()) {
            (DataType::Boolean, DataType::Boolean) => Self::Boolean(Vec::new()),
            (DataType::Int32, DataType::Int32) => Self::Int32(Vec::new()),
            (DataType::Int64, DataType::Int64) => Self::Int64(Vec::new()),
            (DataType::Float64, DataType::Float64) => Self::Float64(Vec::new()),
            (DataType::Utf8, DataType::Utf8) => Self::Utf8 {
                values: Vec::new(),
                decimal_text: semantic(field) == Some(MONGODB_DECIMAL_TEXT_SEMANTIC),
            },
            (DataType::Binary, DataType::Binary) => Self::Binary(Vec::new()),
            (DataType::FixedSizeBinary(12), DataType::FixedSizeBinary(12)) => {
                Self::ObjectId(Vec::new())
            }
            (DataType::Date32, DataType::Date32) => Self::Date32(Vec::new()),
            (
                DataType::Timestamp(TimeUnit::Millisecond, decoder_timezone),
                DataType::Timestamp(TimeUnit::Millisecond, output_timezone),
            ) if decoder_timezone == output_timezone => Self::TimestampMillis(Vec::new()),
            (
                DataType::Decimal128(precision, scale),
                DataType::Decimal128(output_precision, output_scale),
            ) if precision == output_precision && scale == output_scale => Self::Decimal {
                values: Vec::new(),
                precision: *precision,
                scale: *scale,
            },
            (DataType::List(child), DataType::List(output_child)) => Self::List {
                field: Arc::clone(output_child),
                lengths: Vec::new(),
                valid: Vec::new(),
                child: Box::new(Self::new_for_output(child, output_child)?),
            },
            (DataType::Struct(fields), DataType::Struct(output_fields))
                if fields.len() == output_fields.len() =>
            {
                Self::Struct {
                    fields: fields.clone(),
                    output_fields: output_fields.clone(),
                    valid: Vec::new(),
                    children: fields
                        .iter()
                        .zip(output_fields)
                        .map(|(field, output)| Self::new_for_output(field, output))
                        .collect::<Result<Vec<_>>>()?,
                }
            }
            (DataType::Null, DataType::Null) => Self::Null(0),
            (decoder, output) => {
                return Err(CdfError::internal(format!(
                    "MongoDB decoder Arrow type {decoder:?} does not align with logical output type {output:?}"
                )));
            }
        })
    }

    fn append(&mut self, value: Option<RawBsonRef<'_>>) -> Result<()> {
        let value = value.filter(|value| !matches!(value, RawBsonRef::Null));
        match self {
            Self::Boolean(values) => values.push(match value {
                None => None,
                Some(RawBsonRef::Boolean(value)) => Some(value),
                Some(other) => return type_mismatch("Boolean", other),
            }),
            Self::Int32(values) => values.push(match value {
                None => None,
                Some(RawBsonRef::Int32(value)) => Some(value),
                Some(other) => return type_mismatch("Int32", other),
            }),
            Self::Int64(values) => values.push(match value {
                None => None,
                Some(RawBsonRef::Int32(value)) => Some(i64::from(value)),
                Some(RawBsonRef::Int64(value)) => Some(value),
                Some(other) => return type_mismatch("Int64", other),
            }),
            Self::Float64(values) => values.push(match value {
                None => None,
                Some(RawBsonRef::Double(value)) => Some(value),
                Some(other) => return type_mismatch("Float64", other),
            }),
            Self::Utf8 {
                values,
                decimal_text,
            } => values.push(match value {
                None => None,
                Some(RawBsonRef::String(value)) if !*decimal_text => Some(value.to_owned()),
                Some(RawBsonRef::Decimal128(value)) if *decimal_text => Some(value.to_string()),
                Some(other) => return type_mismatch("Utf8", other),
            }),
            Self::Binary(values) => values.push(match value {
                None => None,
                Some(RawBsonRef::Binary(value)) => Some(value.bytes.to_vec()),
                Some(other) => return type_mismatch("Binary", other),
            }),
            Self::ObjectId(values) => values.push(match value {
                None => None,
                Some(RawBsonRef::ObjectId(value)) => Some(value.bytes()),
                Some(other) => return type_mismatch("MongoDB ObjectId", other),
            }),
            Self::Date32(values) => {
                values.push(match value {
                    None => None,
                    Some(RawBsonRef::DateTime(value)) => {
                        let millis = value.timestamp_millis();
                        if millis.rem_euclid(86_400_000) != 0 {
                            return Err(CdfError::data(
                                "MongoDB Date32 field contains a non-midnight UTC DateTime",
                            ));
                        }
                        Some(i32::try_from(millis.div_euclid(86_400_000)).map_err(|_| {
                            CdfError::data("MongoDB Date32 value exceeds Arrow range")
                        })?)
                    }
                    Some(other) => return type_mismatch("Date32", other),
                })
            }
            Self::TimestampMillis(values) => values.push(match value {
                None => None,
                Some(RawBsonRef::DateTime(value)) => Some(value.timestamp_millis()),
                Some(other) => return type_mismatch("Timestamp(Millisecond)", other),
            }),
            Self::Decimal {
                values,
                precision,
                scale,
            } => values.push(match value {
                None => None,
                Some(RawBsonRef::Decimal128(value)) => {
                    Some(parse_decimal128(&value.to_string(), *precision, *scale)?)
                }
                Some(other) => return type_mismatch("Decimal128", other),
            }),
            Self::List {
                lengths,
                valid,
                child,
                ..
            } => match value {
                None => {
                    lengths.push(0);
                    valid.push(false);
                }
                Some(RawBsonRef::Array(array)) => {
                    let mut length = 0usize;
                    for value in array {
                        child.append(Some(value.map_err(|error| {
                            CdfError::data(format!("MongoDB array value is malformed: {error}"))
                        })?))?;
                        length += 1;
                    }
                    lengths.push(length);
                    valid.push(true);
                }
                Some(other) => return type_mismatch("List", other),
            },
            Self::Struct {
                fields,
                valid,
                children,
                ..
            } => match value {
                None => {
                    valid.push(false);
                    for child in children {
                        child.append(None)?;
                    }
                }
                Some(RawBsonRef::Document(document)) => {
                    validate_unique_document(document)?;
                    valid.push(true);
                    for (field, child) in fields.iter().zip(children) {
                        let source = source_name(field).unwrap_or_else(|| field.name());
                        child.append(raw_value_at_path(document, source)?)?;
                    }
                }
                Some(other) => return type_mismatch("Struct", other),
            },
            Self::Null(length) => {
                if value.is_some() {
                    return Err(CdfError::data(
                        "MongoDB field pinned as Null produced a non-null value",
                    ));
                }
                *length += 1;
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<ArrayRef> {
        Ok(match self {
            Self::Boolean(values) => Arc::new(BooleanArray::from(values)),
            Self::Int32(values) => Arc::new(Int32Array::from(values)),
            Self::Int64(values) => Arc::new(Int64Array::from(values)),
            Self::Float64(values) => Arc::new(Float64Array::from(values)),
            Self::Utf8 { values, .. } => {
                Arc::new(StringArray::from_iter(values.iter().map(Option::as_deref)))
            }
            Self::Binary(values) => Arc::new(BinaryArray::from_iter(
                values.iter().map(|value| value.as_deref()),
            )),
            Self::ObjectId(values) => Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    values
                        .iter()
                        .map(|value| value.as_ref().map(<[u8; 12]>::as_slice)),
                    12,
                )
                .map_err(|error| CdfError::data(format!("build ObjectId array: {error}")))?,
            ),
            Self::Date32(values) => Arc::new(Date32Array::from(values)),
            Self::TimestampMillis(values) => {
                Arc::new(TimestampMillisecondArray::from(values).with_timezone("UTC"))
            }
            Self::Decimal {
                values,
                precision,
                scale,
            } => Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(precision, scale)
                    .map_err(|error| CdfError::data(format!("build Decimal128 array: {error}")))?,
            ),
            Self::List {
                field,
                lengths,
                valid,
                child,
            } => Arc::new(
                ListArray::try_new(
                    field,
                    OffsetBuffer::from_lengths(lengths),
                    child.finish()?,
                    null_buffer(valid),
                )
                .map_err(|error| CdfError::data(format!("build MongoDB list array: {error}")))?,
            ),
            Self::Struct {
                output_fields,
                valid,
                children,
                ..
            } => Arc::new(
                StructArray::try_new(
                    output_fields,
                    children
                        .into_iter()
                        .map(Self::finish)
                        .collect::<Result<Vec<_>>>()?,
                    null_buffer(valid),
                )
                .map_err(|error| CdfError::data(format!("build MongoDB struct array: {error}")))?,
            ),
            Self::Null(length) => Arc::new(NullArray::new(length)),
        })
    }
}

fn null_buffer(valid: Vec<bool>) -> Option<NullBuffer> {
    (!valid.iter().all(|value| *value)).then(|| NullBuffer::from(valid))
}

fn type_mismatch<T>(expected: &str, actual: RawBsonRef<'_>) -> Result<T> {
    Err(CdfError::data(format!(
        "MongoDB value has BSON type {:?}, expected {expected} under the pinned schema",
        actual.element_type()
    )))
}

pub(crate) fn parse_decimal128(value: &str, precision: u8, scale: i8) -> Result<i128> {
    if matches!(value, "NaN" | "sNaN" | "Infinity" | "-Infinity") {
        return Err(CdfError::data(
            "MongoDB Decimal128 special value cannot enter an Arrow decimal; use the exact tagged text mapping",
        ));
    }
    let (mantissa, exponent) = value
        .split_once(['e', 'E'])
        .map_or((value, 0_i32), |(mantissa, exponent)| {
            (mantissa, exponent.parse::<i32>().unwrap_or(i32::MIN))
        });
    if exponent == i32::MIN {
        return Err(CdfError::data("MongoDB Decimal128 exponent is invalid"));
    }
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.trim_start_matches(['-', '+']);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return Err(CdfError::data("MongoDB Decimal128 value is invalid"));
    }
    let digits = format!("{whole}{fraction}")
        .trim_start_matches('0')
        .to_owned();
    if digits.is_empty() {
        return Ok(0);
    }
    let digits = digits.as_str();
    let adjustment =
        exponent - i32::try_from(fraction.len()).unwrap_or(i32::MAX) + i32::from(scale);
    let (significand, trailing_zeros) = if adjustment >= 0 {
        (
            digits,
            usize::try_from(adjustment)
                .map_err(|_| CdfError::data("MongoDB Decimal128 scale is out of range"))?,
        )
    } else {
        let remove = usize::try_from(-adjustment)
            .map_err(|_| CdfError::data("MongoDB Decimal128 scale is out of range"))?;
        if remove > digits.len()
            || !digits[digits.len() - remove..]
                .bytes()
                .all(|byte| byte == b'0')
        {
            return Err(CdfError::data(
                "MongoDB Decimal128 value cannot fit the pinned Arrow scale exactly",
            ));
        }
        (&digits[..digits.len() - remove], 0)
    };
    let total_digits = significand.trim_start_matches('0').len() + trailing_zeros;
    if total_digits > usize::from(precision) {
        return Err(CdfError::data(
            "MongoDB Decimal128 value exceeds the pinned Arrow precision",
        ));
    }
    let mut unscaled = significand
        .parse::<i128>()
        .map_err(|_| CdfError::data("MongoDB Decimal128 value exceeds Arrow Decimal128 range"))?;
    for _ in 0..trailing_zeros {
        unscaled = unscaled.checked_mul(10).ok_or_else(|| {
            CdfError::data("MongoDB Decimal128 value exceeds Arrow Decimal128 range")
        })?;
    }
    Ok(if negative { -unscaled } else { unscaled })
}
