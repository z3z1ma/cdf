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
    CdfError, PreContractResidualCandidate, Result, semantic, source_name, with_physical_type,
    with_semantic, with_source_name,
};
use mongodb::bson::{RawBsonRef, RawDocument};

pub(crate) const MONGODB_OBJECT_ID_SEMANTIC: &str = cdf_semantic::MONGODB_OBJECT_ID_SEMANTIC;
pub(crate) const MONGODB_DECIMAL_TEXT_SEMANTIC: &str =
    cdf_semantic::MONGODB_DECIMAL128_TEXT_SEMANTIC;
const MAXIMUM_SCHEMA_FIELDS: usize = 4_096;
const MAXIMUM_SCHEMA_DEPTH: usize = 32;

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
    Struct(BTreeMap<String, InferredField>),
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
    documents: usize,
    bytes: u64,
}

impl SchemaInference {
    pub(crate) fn observe(&mut self, document: &RawDocument) -> Result<()> {
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
                    field.value = merge_types(field.value.clone(), inferred, &name)?;
                    field.nullable |= matches!(value, RawBsonRef::Null);
                    field.observed_documents += 1;
                }
                None => {
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
        }
        for (name, field) in &mut self.fields {
            if !observed.contains(name) {
                field.nullable = true;
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
                element = merge_types(element, infer_value(value, depth + 1)?, "array element")?;
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
            InferredType::Struct(fields)
        }
        other => {
            return Err(CdfError::data(format!(
                "MongoDB BSON type {:?} requires explicit variant or quarantine policy",
                other.element_type()
            )));
        }
    })
}

fn merge_types(left: InferredType, right: InferredType, path: &str) -> Result<InferredType> {
    Ok(match (left, right) {
        (InferredType::Null, other) | (other, InferredType::Null) => other,
        (InferredType::Int32, InferredType::Int64) | (InferredType::Int64, InferredType::Int32) => {
            InferredType::Int64
        }
        (InferredType::List(left), InferredType::List(right)) => {
            InferredType::List(Box::new(merge_types(*left, *right, path)?))
        }
        (InferredType::Struct(mut left), InferredType::Struct(right)) => {
            let right_names = right.keys().cloned().collect::<BTreeSet<_>>();
            for (name, right) in right {
                match left.get_mut(&name) {
                    Some(existing) => {
                        existing.value = merge_types(
                            existing.value.clone(),
                            right.value,
                            &format!("{path}.{name}"),
                        )?;
                        existing.nullable |= right.nullable;
                    }
                    None => {
                        left.insert(
                            name,
                            InferredField {
                                nullable: true,
                                ..right
                            },
                        );
                    }
                }
            }
            for (name, field) in &mut left {
                if !right_names.contains(name) {
                    field.nullable = true;
                }
            }
            InferredType::Struct(left)
        }
        (left, right) if std::mem::discriminant(&left) == std::mem::discriminant(&right) => left,
        (left, right) => {
            return Err(CdfError::data(format!(
                "MongoDB discovery observed heterogeneous BSON types at `{path}`: {left:?} and {right:?}; select an explicit variant or quarantine policy"
            )));
        }
    })
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
        InferredType::Struct(fields) => {
            let fields = fields
                .into_iter()
                .map(|(name, field)| inferred_field(name, field, depth + 1))
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

pub(crate) struct DecodedMongoBatch {
    pub(crate) record_batch: RecordBatch,
    pub(crate) physical_schema: Schema,
    pub(crate) residual_candidates: Vec<PreContractResidualCandidate>,
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

pub(crate) fn decode_batch_with_evidence(
    full_schema: SchemaRef,
    output_schema: SchemaRef,
    documents: &[&RawDocument],
    source_row_offset: u64,
) -> Result<DecodedMongoBatch> {
    validate_mongodb_schema(full_schema.as_ref())?;
    validate_mongodb_schema(output_schema.as_ref())?;
    let mut columns = output_schema
        .fields()
        .iter()
        .map(|field| ColumnAccumulator::new(field))
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
    let projected_sources = output_schema
        .fields()
        .iter()
        .map(|field| {
            source_name(field)
                .unwrap_or_else(|| field.name())
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    let mut residual_candidates = Vec::new();
    for (row, document) in documents.iter().enumerate() {
        validate_unique_document(document)?;
        for (field, column) in output_schema.fields().iter().zip(&mut columns) {
            let source = source_name(field).unwrap_or_else(|| field.name());
            let value = raw_value_at_path(document, source)?;
            if value_matches_field(field, value)? {
                column.append(value)?;
            } else {
                column.append(None)?;
                residual_candidates.push(residual_candidate(
                    source_row_offset.saturating_add(row as u64),
                    row,
                    source,
                    value,
                    Some(field.as_ref().clone()),
                )?);
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
            residual_candidates.push(residual_candidate(
                source_row_offset.saturating_add(row as u64),
                row,
                &name,
                Some(value),
                None,
            )?);
        }
    }
    let residual_sources = residual_candidates
        .iter()
        .filter(|candidate| candidate.expected_field().is_some())
        .filter_map(|candidate| candidate.source_path().first().cloned())
        .collect::<BTreeSet<_>>();
    let materialized_fields = output_schema
        .fields()
        .iter()
        .map(|field| {
            let source = source_name(field).unwrap_or_else(|| field.name());
            if residual_sources.contains(source) {
                Arc::new(field.as_ref().clone().with_nullable(true))
            } else {
                Arc::clone(field)
            }
        })
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
    let physical_schema = execution_physical_schema(
        &full_schema,
        &output_schema,
        documents,
        &projected_sources,
        &known_sources,
        &residual_candidates,
    )?;
    Ok(DecodedMongoBatch {
        record_batch,
        physical_schema,
        residual_candidates,
    })
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
        (DataType::Float64, RawBsonRef::Double(value)) => value.is_finite(),
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
) -> Result<PreContractResidualCandidate> {
    let (observed_field, value) = observed_value_evidence(source, value)?;
    PreContractResidualCandidate::new(
        source_row_ordinal,
        batch_row_ordinal,
        source.split('.').map(str::to_owned).collect(),
        observed_field,
        expected,
        value,
        0,
    )
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
    match infer_value(value, 0).and_then(|inferred| {
        inferred_field(
            source.to_owned(),
            InferredField {
                value: inferred,
                nullable: true,
                observed_documents: 1,
            },
            0,
        )
    }) {
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

fn execution_physical_schema(
    full_schema: &Schema,
    output_schema: &Schema,
    documents: &[&RawDocument],
    projected_sources: &BTreeSet<String>,
    known_sources: &BTreeSet<String>,
    residual_candidates: &[PreContractResidualCandidate],
) -> Result<Schema> {
    let inferred = (|| {
        let mut inference = SchemaInference::default();
        for document in documents {
            inference.observe(document)?;
        }
        inference.finish().map(|(schema, _, _)| schema)
    })();
    if let Ok(inferred) = inferred {
        let mut fields = inferred
            .fields()
            .iter()
            .filter(|field| {
                let source = source_name(field).unwrap_or_else(|| field.name());
                projected_sources.contains(source) || !known_sources.contains(source)
            })
            .cloned()
            .collect::<Vec<_>>();
        for output in output_schema.fields() {
            let source = source_name(output).unwrap_or_else(|| output.name());
            if !fields
                .iter()
                .any(|field| source_name(field).unwrap_or_else(|| field.name()) == source)
            {
                fields.push(Arc::new(with_source_name(
                    Field::new(source, DataType::Null, true),
                    source,
                )));
            }
        }
        fields.sort_by(|left, right| left.name().cmp(right.name()));
        return Ok(Schema::new_with_metadata(
            fields,
            full_schema.metadata().clone(),
        ));
    }
    let mut fields = output_schema.fields().to_vec();
    for candidate in residual_candidates
        .iter()
        .filter(|candidate| candidate.expected_field().is_none())
    {
        let source = candidate
            .source_path()
            .first()
            .ok_or_else(|| CdfError::internal("MongoDB residual source path disappeared"))?;
        if !fields
            .iter()
            .any(|field| source_name(field).unwrap_or_else(|| field.name()) == source)
        {
            fields.push(Arc::new(candidate.observed_field().clone()));
        }
    }
    fields.sort_by(|left, right| left.name().cmp(right.name()));
    Ok(Schema::new_with_metadata(
        fields,
        full_schema.metadata().clone(),
    ))
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
        valid: Vec<bool>,
        children: Vec<ColumnAccumulator>,
    },
    Null(usize),
}

impl ColumnAccumulator {
    fn new(field: &Field) -> Result<Self> {
        Ok(match field.data_type() {
            DataType::Boolean => Self::Boolean(Vec::new()),
            DataType::Int32 => Self::Int32(Vec::new()),
            DataType::Int64 => Self::Int64(Vec::new()),
            DataType::Float64 => Self::Float64(Vec::new()),
            DataType::Utf8 => Self::Utf8 {
                values: Vec::new(),
                decimal_text: semantic(field) == Some(MONGODB_DECIMAL_TEXT_SEMANTIC),
            },
            DataType::Binary => Self::Binary(Vec::new()),
            DataType::FixedSizeBinary(12) => Self::ObjectId(Vec::new()),
            DataType::Date32 => Self::Date32(Vec::new()),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Self::TimestampMillis(Vec::new()),
            DataType::Decimal128(precision, scale) => Self::Decimal {
                values: Vec::new(),
                precision: *precision,
                scale: *scale,
            },
            DataType::List(child) => Self::List {
                field: Arc::clone(child),
                lengths: Vec::new(),
                valid: Vec::new(),
                child: Box::new(Self::new(child)?),
            },
            DataType::Struct(fields) => Self::Struct {
                fields: fields.clone(),
                valid: Vec::new(),
                children: fields
                    .iter()
                    .map(|field| Self::new(field))
                    .collect::<Result<Vec<_>>>()?,
            },
            DataType::Null => Self::Null(0),
            other => {
                return Err(CdfError::contract(format!(
                    "MongoDB decoder does not support Arrow type {other:?}"
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
                Some(RawBsonRef::Double(value)) if value.is_finite() => Some(value),
                Some(RawBsonRef::Double(_)) => {
                    return Err(CdfError::data("MongoDB double is not finite"));
                }
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
                fields,
                valid,
                children,
            } => Arc::new(
                StructArray::try_new(
                    fields,
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
