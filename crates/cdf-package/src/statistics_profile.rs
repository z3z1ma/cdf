use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, RecordBatch, StringArray,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_kernel::{
    BatchStats, CdfError, IncompleteStatisticsReason, Result, STATISTICS_MODEL_VERSION,
    StatisticsArrowType, StatisticsCompleteness, TypedScalar,
};
use cdf_package_contract::FileEntry;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};

use crate::{PackageBuilder, PackageReader, StreamingIdentityArtifact, VerifiedPackage};

pub const STATISTICS_PROFILE_FILE: &str = "stats/profile.parquet";
const STATISTICS_PROFILE_ARTIFACT_VERSION: u16 = 1;
const SCALAR_DECIMAL_PRECISION: u8 = 38;
const SCALAR_DECIMAL_SCALE: i8 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatisticsProfileGrain {
    Segment,
    Package,
}

impl StatisticsProfileGrain {
    fn as_str(self) -> &'static str {
        match self {
            Self::Segment => "segment",
            Self::Package => "package",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "segment" => Ok(Self::Segment),
            "package" => Ok(Self::Package),
            _ => Err(CdfError::data(format!(
                "unknown statistics profile grain {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatisticsProfileRow {
    pub grain: StatisticsProfileGrain,
    pub container_ordinal: u64,
    pub container_id: String,
    pub schema_hash: String,
    pub field_ordinal: u32,
    pub field_path: Box<[Box<str>]>,
    pub data_type: StatisticsArrowType,
    pub row_count: u64,
    pub null_count: u64,
    pub completeness: StatisticsCompleteness,
    pub minimum: Option<TypedScalar>,
    pub maximum: Option<TypedScalar>,
}

pub struct StatisticsProfileWriter {
    writer: ArrowWriter<StreamingIdentityArtifact>,
    row_count: u64,
}

impl PackageBuilder {
    pub fn begin_statistics_profile(&self) -> Result<StatisticsProfileWriter> {
        let artifact = self.begin_streaming_identity_artifact(STATISTICS_PROFILE_FILE)?;
        let properties = WriterProperties::builder()
            .set_created_by("cdf native typed statistics profile v1".to_owned())
            .build();
        let writer = ArrowWriter::try_new(artifact, statistics_profile_schema(), Some(properties))
            .map_err(|error| {
                CdfError::data(format!("create statistics profile Parquet writer: {error}"))
            })?;
        Ok(StatisticsProfileWriter {
            writer,
            row_count: 0,
        })
    }
}

impl StatisticsProfileWriter {
    pub fn write_stats(
        &mut self,
        grain: StatisticsProfileGrain,
        container_ordinal: u64,
        container_id: &str,
        schema_hash: &str,
        stats: &BatchStats,
    ) -> Result<()> {
        let batch =
            statistics_profile_batch(grain, container_ordinal, container_id, schema_hash, stats)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.writer.write(&batch).map_err(|error| {
            CdfError::data(format!("write statistics profile Parquet batch: {error}"))
        })?;
        self.row_count = self
            .row_count
            .checked_add(
                u64::try_from(batch.num_rows())
                    .map_err(|_| CdfError::data("statistics profile row count exceeds u64"))?,
            )
            .ok_or_else(|| CdfError::data("statistics profile row count overflow"))?;
        Ok(())
    }

    pub fn finish(self) -> Result<FileEntry> {
        let artifact = self.writer.into_inner().map_err(|error| {
            CdfError::data(format!("finish statistics profile Parquet writer: {error}"))
        })?;
        artifact.finish()
    }
}

impl PackageReader {
    pub fn verified_statistics_profile(
        &self,
        verified: &VerifiedPackage,
    ) -> Result<Vec<StatisticsProfileRow>> {
        let bytes = self.verified_identity_bytes(verified, STATISTICS_PROFILE_FILE)?;
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .map_err(|error| {
                CdfError::data(format!("open verified statistics profile Parquet: {error}"))
            })?
            .build()
            .map_err(|error| {
                CdfError::data(format!("build verified statistics profile reader: {error}"))
            })?;
        let mut rows = Vec::new();
        for batch in &mut reader {
            let batch = batch.map_err(|error| {
                CdfError::data(format!(
                    "read verified statistics profile row group: {error}"
                ))
            })?;
            rows.extend(statistics_profile_rows(&batch)?);
        }
        self.validate_statistics_profile_rows(&rows)?;
        Ok(rows)
    }

    fn validate_statistics_profile_rows(&self, rows: &[StatisticsProfileRow]) -> Result<()> {
        if rows.is_empty() {
            if self.manifest().identity.segments.is_empty() {
                return Ok(());
            }
            return Err(CdfError::data(
                "statistics profile is empty for a package with data segments",
            ));
        }
        let mut expected_segment = 0_u64;
        let mut expected_field = 0_u32;
        let mut seen_package = false;
        let mut package_rows = 0_u64;
        let mut prior: Option<(StatisticsProfileGrain, u64, u32)> = None;
        for row in rows {
            if let Some((prior_grain, prior_container, prior_field)) = prior {
                let ordered = match (prior_grain, row.grain) {
                    (StatisticsProfileGrain::Segment, StatisticsProfileGrain::Segment) => {
                        (row.container_ordinal, row.field_ordinal) > (prior_container, prior_field)
                    }
                    (StatisticsProfileGrain::Segment, StatisticsProfileGrain::Package) => true,
                    (StatisticsProfileGrain::Package, StatisticsProfileGrain::Package) => {
                        row.container_ordinal == 0 && row.field_ordinal > prior_field
                    }
                    (StatisticsProfileGrain::Package, StatisticsProfileGrain::Segment) => false,
                };
                if !ordered {
                    return Err(CdfError::data(
                        "statistics profile rows are not in canonical grain/container/field order",
                    ));
                }
            }
            prior = Some((row.grain, row.container_ordinal, row.field_ordinal));
            match row.grain {
                StatisticsProfileGrain::Segment => {
                    if seen_package {
                        return Err(CdfError::data(
                            "statistics profile segment row appears after package rows",
                        ));
                    }
                    if row.container_ordinal != expected_segment {
                        expected_segment = row.container_ordinal;
                        expected_field = 0;
                    }
                    let segment = self
                        .manifest()
                        .identity
                        .segments
                        .get(usize::try_from(row.container_ordinal).map_err(|_| {
                            CdfError::data("statistics profile segment ordinal exceeds usize")
                        })?)
                        .ok_or_else(|| {
                            CdfError::data(format!(
                                "statistics profile references missing segment ordinal {}",
                                row.container_ordinal
                            ))
                        })?;
                    if row.container_id != segment.segment_id.as_str() {
                        return Err(CdfError::data(format!(
                            "statistics profile segment ordinal {} names {:?} but manifest names {:?}",
                            row.container_ordinal,
                            row.container_id,
                            segment.segment_id.as_str()
                        )));
                    }
                    if row.row_count != segment.row_count {
                        return Err(CdfError::data(format!(
                            "statistics profile segment {} row count {} differs from manifest {}",
                            segment.segment_id.as_str(),
                            row.row_count,
                            segment.row_count
                        )));
                    }
                    if row.field_ordinal != expected_field {
                        return Err(CdfError::data(
                            "statistics profile segment fields are not contiguous",
                        ));
                    }
                    expected_field = expected_field.checked_add(1).ok_or_else(|| {
                        CdfError::data("statistics profile field ordinal overflow")
                    })?;
                }
                StatisticsProfileGrain::Package => {
                    seen_package = true;
                    if row.container_ordinal != 0
                        || row.container_id != self.manifest().identity.package_id
                    {
                        return Err(CdfError::data(
                            "statistics profile package row does not bind the manifest package id",
                        ));
                    }
                    package_rows = package_rows.checked_add(1).ok_or_else(|| {
                        CdfError::data("statistics profile package row count overflow")
                    })?;
                }
            }
            if row.null_count > row.row_count {
                return Err(CdfError::data(
                    "statistics profile null count exceeds row count",
                ));
            }
        }
        if package_rows == 0 {
            return Err(CdfError::data(
                "statistics profile requires package-grain rows",
            ));
        }
        Ok(())
    }
}

fn statistics_profile_batch(
    grain: StatisticsProfileGrain,
    container_ordinal: u64,
    container_id: &str,
    schema_hash: &str,
    stats: &BatchStats,
) -> Result<RecordBatch> {
    let rows = stats
        .columns
        .iter()
        .enumerate()
        .map(|(field_ordinal, column)| {
            let field_ordinal = u32::try_from(field_ordinal)
                .map_err(|_| CdfError::data("statistics profile field ordinal exceeds u32"))?;
            Ok((
                field_ordinal,
                column,
                column_scalar(&column.minimum),
                column_scalar(&column.maximum),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let row_count = rows.len();
    let versions = UInt16Array::from(vec![Some(STATISTICS_PROFILE_ARTIFACT_VERSION); row_count]);
    let model_versions = UInt16Array::from(vec![Some(STATISTICS_MODEL_VERSION); row_count]);
    let grains = StringArray::from(vec![Some(grain.as_str()); row_count]);
    let container_ordinals = UInt64Array::from(vec![Some(container_ordinal); row_count]);
    let container_ids = StringArray::from(vec![Some(container_id); row_count]);
    let schema_hashes = StringArray::from(vec![Some(schema_hash); row_count]);
    let field_ordinals = UInt32Array::from(
        rows.iter()
            .map(|(ordinal, _, _, _)| Some(*ordinal))
            .collect::<Vec<_>>(),
    );
    let field_paths = string_array(
        rows.iter()
            .map(|(_, column, _, _)| canonical_string(&column.field_path).map(Some)),
    )?;
    let data_types = string_array(
        rows.iter()
            .map(|(_, column, _, _)| canonical_string(&column.data_type).map(Some)),
    )?;
    let row_counts = UInt64Array::from(
        rows.iter()
            .map(|(_, column, _, _)| Some(column.row_count))
            .collect::<Vec<_>>(),
    );
    let null_counts = UInt64Array::from(
        rows.iter()
            .map(|(_, column, _, _)| Some(column.null_count))
            .collect::<Vec<_>>(),
    );
    let completeness = string_array(rows.iter().map(|(_, column, _, _)| {
        Ok(Some(match column.completeness {
            StatisticsCompleteness::Complete => "complete".to_owned(),
            StatisticsCompleteness::Incomplete { .. } => "incomplete".to_owned(),
        }))
    }))?;
    let incomplete_reasons = string_array(rows.iter().map(|(_, column, _, _)| {
        Ok(match &column.completeness {
            StatisticsCompleteness::Complete => None,
            StatisticsCompleteness::Incomplete { reason } => {
                Some(incomplete_reason(reason).to_owned())
            }
        })
    }))?;
    let minimum = scalar_arrays(rows.iter().map(|(_, _, minimum, _)| minimum))?;
    let maximum = scalar_arrays(rows.iter().map(|(_, _, _, maximum)| maximum))?;

    RecordBatch::try_new(
        statistics_profile_schema(),
        vec![
            Arc::new(versions) as ArrayRef,
            Arc::new(model_versions),
            Arc::new(grains),
            Arc::new(container_ordinals),
            Arc::new(container_ids),
            Arc::new(schema_hashes),
            Arc::new(field_ordinals),
            Arc::new(field_paths),
            Arc::new(data_types),
            Arc::new(row_counts),
            Arc::new(null_counts),
            Arc::new(completeness),
            Arc::new(incomplete_reasons),
            minimum.kind,
            minimum.boolean,
            minimum.signed,
            minimum.unsigned,
            minimum.decimal,
            minimum.utf8,
            minimum.binary,
            maximum.kind,
            maximum.boolean,
            maximum.signed,
            maximum.unsigned,
            maximum.decimal,
            maximum.utf8,
            maximum.binary,
        ],
    )
    .map_err(CdfError::from)
}

fn statistics_profile_rows(batch: &RecordBatch) -> Result<Vec<StatisticsProfileRow>> {
    if batch.schema().as_ref() != statistics_profile_schema().as_ref() {
        return Err(CdfError::data(
            "statistics profile Parquet schema does not match the current profile schema",
        ));
    }
    let version = column::<UInt16Array>(batch, 0)?;
    let model_version = column::<UInt16Array>(batch, 1)?;
    let grain = column::<StringArray>(batch, 2)?;
    let container_ordinal = column::<UInt64Array>(batch, 3)?;
    let container_id = column::<StringArray>(batch, 4)?;
    let schema_hash = column::<StringArray>(batch, 5)?;
    let field_ordinal = column::<UInt32Array>(batch, 6)?;
    let field_path = column::<StringArray>(batch, 7)?;
    let data_type = column::<StringArray>(batch, 8)?;
    let row_count = column::<UInt64Array>(batch, 9)?;
    let null_count = column::<UInt64Array>(batch, 10)?;
    let completeness = column::<StringArray>(batch, 11)?;
    let incomplete_reason = column::<StringArray>(batch, 12)?;
    let min = ScalarArrayColumns::from_batch(batch, 13)?;
    let max = ScalarArrayColumns::from_batch(batch, 20)?;

    (0..batch.num_rows())
        .map(|row| {
            let version = required_value(version, row, "profile_version")?;
            if version != STATISTICS_PROFILE_ARTIFACT_VERSION {
                return Err(CdfError::data(format!(
                    "unsupported statistics profile artifact version {version}"
                )));
            }
            let model_version = required_value(model_version, row, "statistics_model_version")?;
            if model_version != STATISTICS_MODEL_VERSION {
                return Err(CdfError::data(format!(
                    "unsupported statistics model version {model_version}"
                )));
            }
            let completeness = match required_str(completeness, row, "completeness")? {
                "complete" => {
                    if optional(incomplete_reason, row).is_some() {
                        return Err(CdfError::data(
                            "complete statistics profile row carried an incomplete reason",
                        ));
                    }
                    StatisticsCompleteness::Complete
                }
                "incomplete" => StatisticsCompleteness::Incomplete {
                    reason: parse_incomplete_reason(optional(incomplete_reason, row)).ok_or_else(
                        || CdfError::data("incomplete statistics profile row omitted its reason"),
                    )?,
                },
                other => {
                    return Err(CdfError::data(format!(
                        "unknown statistics profile completeness {other:?}"
                    )));
                }
            };
            Ok(StatisticsProfileRow {
                grain: StatisticsProfileGrain::parse(required_str(grain, row, "grain")?)?,
                container_ordinal: required_value(container_ordinal, row, "container_ordinal")?,
                container_id: required_str(container_id, row, "container_id")?.to_owned(),
                schema_hash: required_str(schema_hash, row, "schema_hash")?.to_owned(),
                field_ordinal: required_value(field_ordinal, row, "field_ordinal")?,
                field_path: serde_json::from_str(required_str(field_path, row, "field_path_json")?)
                    .map_err(|error| {
                        CdfError::data(format!("decode statistics field path JSON: {error}"))
                    })?,
                data_type: serde_json::from_str(required_str(data_type, row, "arrow_type_json")?)
                    .map_err(|error| {
                    CdfError::data(format!("decode statistics Arrow type JSON: {error}"))
                })?,
                row_count: required_value(row_count, row, "row_count")?,
                null_count: required_value(null_count, row, "null_count")?,
                completeness,
                minimum: min.scalar(row)?,
                maximum: max.scalar(row)?,
            })
        })
        .collect()
}

fn statistics_profile_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("profile_version", DataType::UInt16, false),
        Field::new("statistics_model_version", DataType::UInt16, false),
        Field::new("grain", DataType::Utf8, false),
        Field::new("container_ordinal", DataType::UInt64, false),
        Field::new("container_id", DataType::Utf8, false),
        Field::new("schema_hash", DataType::Utf8, false),
        Field::new("field_ordinal", DataType::UInt32, false),
        Field::new("field_path_json", DataType::Utf8, false),
        Field::new("arrow_type_json", DataType::Utf8, false),
        Field::new("row_count", DataType::UInt64, false),
        Field::new("null_count", DataType::UInt64, false),
        Field::new("completeness", DataType::Utf8, false),
        Field::new("incomplete_reason", DataType::Utf8, true),
        Field::new("minimum_kind", DataType::Utf8, true),
        Field::new("minimum_bool", DataType::Boolean, true),
        Field::new("minimum_i64", DataType::Int64, true),
        Field::new("minimum_u64", DataType::UInt64, true),
        Field::new(
            "minimum_i128",
            DataType::Decimal128(SCALAR_DECIMAL_PRECISION, SCALAR_DECIMAL_SCALE),
            true,
        ),
        Field::new("minimum_utf8", DataType::Utf8, true),
        Field::new("minimum_binary", DataType::Binary, true),
        Field::new("maximum_kind", DataType::Utf8, true),
        Field::new("maximum_bool", DataType::Boolean, true),
        Field::new("maximum_i64", DataType::Int64, true),
        Field::new("maximum_u64", DataType::UInt64, true),
        Field::new(
            "maximum_i128",
            DataType::Decimal128(SCALAR_DECIMAL_PRECISION, SCALAR_DECIMAL_SCALE),
            true,
        ),
        Field::new("maximum_utf8", DataType::Utf8, true),
        Field::new("maximum_binary", DataType::Binary, true),
    ]))
}

struct ScalarArrayRefs {
    kind: ArrayRef,
    boolean: ArrayRef,
    signed: ArrayRef,
    unsigned: ArrayRef,
    decimal: ArrayRef,
    utf8: ArrayRef,
    binary: ArrayRef,
}

struct ScalarColumns<'a> {
    kind: Option<&'static str>,
    boolean: Option<bool>,
    signed: Option<i64>,
    unsigned: Option<u64>,
    decimal: Option<i128>,
    utf8: Option<&'a str>,
    binary: Option<&'a [u8]>,
}

fn column_scalar(value: &Option<TypedScalar>) -> ScalarColumns<'_> {
    match value {
        None => ScalarColumns {
            kind: None,
            boolean: None,
            signed: None,
            unsigned: None,
            decimal: None,
            utf8: None,
            binary: None,
        },
        Some(TypedScalar::Boolean(value)) => ScalarColumns {
            kind: Some("boolean"),
            boolean: Some(*value),
            signed: None,
            unsigned: None,
            decimal: None,
            utf8: None,
            binary: None,
        },
        Some(TypedScalar::Signed(value)) => ScalarColumns {
            kind: Some("signed"),
            boolean: None,
            signed: Some(*value),
            unsigned: None,
            decimal: None,
            utf8: None,
            binary: None,
        },
        Some(TypedScalar::Unsigned(value)) => ScalarColumns {
            kind: Some("unsigned"),
            boolean: None,
            signed: None,
            unsigned: Some(*value),
            decimal: None,
            utf8: None,
            binary: None,
        },
        Some(TypedScalar::Float32Bits(value)) => ScalarColumns {
            kind: Some("float32_bits"),
            boolean: None,
            signed: None,
            unsigned: Some(u64::from(*value)),
            decimal: None,
            utf8: None,
            binary: None,
        },
        Some(TypedScalar::Float64Bits(value)) => ScalarColumns {
            kind: Some("float64_bits"),
            boolean: None,
            signed: None,
            unsigned: Some(*value),
            decimal: None,
            utf8: None,
            binary: None,
        },
        Some(TypedScalar::Decimal32(value)) => ScalarColumns {
            kind: Some("decimal32"),
            boolean: None,
            signed: None,
            unsigned: None,
            decimal: Some(i128::from(*value)),
            utf8: None,
            binary: None,
        },
        Some(TypedScalar::Decimal64(value)) => ScalarColumns {
            kind: Some("decimal64"),
            boolean: None,
            signed: None,
            unsigned: None,
            decimal: Some(i128::from(*value)),
            utf8: None,
            binary: None,
        },
        Some(TypedScalar::Decimal128(value)) => ScalarColumns {
            kind: Some("decimal128"),
            boolean: None,
            signed: None,
            unsigned: None,
            decimal: Some(*value),
            utf8: None,
            binary: None,
        },
        Some(TypedScalar::Utf8(value)) => ScalarColumns {
            kind: Some("utf8"),
            boolean: None,
            signed: None,
            unsigned: None,
            decimal: None,
            utf8: Some(value.as_ref()),
            binary: None,
        },
        Some(TypedScalar::Binary(value)) => ScalarColumns {
            kind: Some("binary"),
            boolean: None,
            signed: None,
            unsigned: None,
            decimal: None,
            utf8: None,
            binary: Some(value.as_ref()),
        },
    }
}

fn scalar_arrays<'a>(
    values: impl Iterator<Item = &'a ScalarColumns<'a>>,
) -> Result<ScalarArrayRefs> {
    let values = values.collect::<Vec<_>>();
    let kind = StringArray::from_iter(values.iter().map(|value| value.kind));
    let boolean = BooleanArray::from(values.iter().map(|value| value.boolean).collect::<Vec<_>>());
    let signed =
        arrow_array::Int64Array::from(values.iter().map(|value| value.signed).collect::<Vec<_>>());
    let unsigned = UInt64Array::from(
        values
            .iter()
            .map(|value| value.unsigned)
            .collect::<Vec<_>>(),
    );
    let decimal =
        Decimal128Array::from(values.iter().map(|value| value.decimal).collect::<Vec<_>>())
            .with_precision_and_scale(SCALAR_DECIMAL_PRECISION, SCALAR_DECIMAL_SCALE)
            .map_err(CdfError::from)?;
    let utf8 = StringArray::from_iter(values.iter().map(|value| value.utf8));
    let binary = BinaryArray::from_iter(values.iter().map(|value| value.binary));
    Ok(ScalarArrayRefs {
        kind: Arc::new(kind),
        boolean: Arc::new(boolean),
        signed: Arc::new(signed),
        unsigned: Arc::new(unsigned),
        decimal: Arc::new(decimal),
        utf8: Arc::new(utf8),
        binary: Arc::new(binary),
    })
}

struct ScalarArrayColumns<'a> {
    kind: &'a StringArray,
    boolean: &'a BooleanArray,
    signed: &'a arrow_array::Int64Array,
    unsigned: &'a UInt64Array,
    decimal: &'a Decimal128Array,
    utf8: &'a StringArray,
    binary: &'a BinaryArray,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScalarValueColumn {
    Boolean,
    Signed,
    Unsigned,
    Decimal,
    Utf8,
    Binary,
}

impl<'a> ScalarArrayColumns<'a> {
    fn from_batch(batch: &'a RecordBatch, offset: usize) -> Result<Self> {
        Ok(Self {
            kind: column(batch, offset)?,
            boolean: column(batch, offset + 1)?,
            signed: column(batch, offset + 2)?,
            unsigned: column(batch, offset + 3)?,
            decimal: column(batch, offset + 4)?,
            utf8: column(batch, offset + 5)?,
            binary: column(batch, offset + 6)?,
        })
    }

    fn scalar(&self, row: usize) -> Result<Option<TypedScalar>> {
        Ok(match optional(self.kind, row) {
            None => {
                self.require_only_value_column(row, None)?;
                None
            }
            Some("boolean") => {
                self.require_only_value_column(row, Some(ScalarValueColumn::Boolean))?;
                Some(TypedScalar::Boolean(required_value(
                    self.boolean,
                    row,
                    "bool",
                )?))
            }
            Some("signed") => {
                self.require_only_value_column(row, Some(ScalarValueColumn::Signed))?;
                Some(TypedScalar::Signed(required_value(
                    self.signed,
                    row,
                    "i64",
                )?))
            }
            Some("unsigned") => {
                self.require_only_value_column(row, Some(ScalarValueColumn::Unsigned))?;
                Some(TypedScalar::Unsigned(required_value(
                    self.unsigned,
                    row,
                    "u64",
                )?))
            }
            Some("float32_bits") => {
                self.require_only_value_column(row, Some(ScalarValueColumn::Unsigned))?;
                let value = required_value(self.unsigned, row, "u64")?;
                Some(TypedScalar::Float32Bits(u32::try_from(value).map_err(
                    |_| CdfError::data("statistics profile float32 bits exceed u32"),
                )?))
            }
            Some("float64_bits") => {
                self.require_only_value_column(row, Some(ScalarValueColumn::Unsigned))?;
                Some(TypedScalar::Float64Bits(required_value(
                    self.unsigned,
                    row,
                    "u64",
                )?))
            }
            Some("decimal32") => {
                self.require_only_value_column(row, Some(ScalarValueColumn::Decimal))?;
                let value = required_value(self.decimal, row, "i128")?;
                Some(TypedScalar::Decimal32(i32::try_from(value).map_err(
                    |_| CdfError::data("statistics profile decimal32 value exceeds i32"),
                )?))
            }
            Some("decimal64") => {
                self.require_only_value_column(row, Some(ScalarValueColumn::Decimal))?;
                let value = required_value(self.decimal, row, "i128")?;
                Some(TypedScalar::Decimal64(i64::try_from(value).map_err(
                    |_| CdfError::data("statistics profile decimal64 value exceeds i64"),
                )?))
            }
            Some("decimal128") => {
                self.require_only_value_column(row, Some(ScalarValueColumn::Decimal))?;
                Some(TypedScalar::Decimal128(required_value(
                    self.decimal,
                    row,
                    "i128",
                )?))
            }
            Some("utf8") => {
                self.require_only_value_column(row, Some(ScalarValueColumn::Utf8))?;
                Some(TypedScalar::Utf8(
                    required_str(self.utf8, row, "utf8")?.into(),
                ))
            }
            Some("binary") => {
                self.require_only_value_column(row, Some(ScalarValueColumn::Binary))?;
                Some(TypedScalar::Binary(
                    required_binary(self.binary, row, "binary")?.into(),
                ))
            }
            Some(other) => {
                return Err(CdfError::data(format!(
                    "unknown statistics scalar kind {other:?}"
                )));
            }
        })
    }

    fn require_only_value_column(
        &self,
        row: usize,
        allowed: Option<ScalarValueColumn>,
    ) -> Result<()> {
        let columns = [
            (
                ScalarValueColumn::Boolean,
                "bool",
                !self.boolean.is_null(row),
            ),
            (ScalarValueColumn::Signed, "i64", !self.signed.is_null(row)),
            (
                ScalarValueColumn::Unsigned,
                "u64",
                !self.unsigned.is_null(row),
            ),
            (
                ScalarValueColumn::Decimal,
                "i128",
                !self.decimal.is_null(row),
            ),
            (ScalarValueColumn::Utf8, "utf8", !self.utf8.is_null(row)),
            (
                ScalarValueColumn::Binary,
                "binary",
                !self.binary.is_null(row),
            ),
        ];
        for (column, name, present) in columns {
            if present && Some(column) != allowed {
                return Err(CdfError::data(format!(
                    "statistics profile scalar kind carried unexpected {name} value"
                )));
            }
        }
        Ok(())
    }
}

trait RequiredValue: Array {
    type Value: Copy;
    fn value_at(&self, row: usize) -> Self::Value;
}

impl RequiredValue for UInt16Array {
    type Value = u16;
    fn value_at(&self, row: usize) -> Self::Value {
        self.value(row)
    }
}

impl RequiredValue for UInt32Array {
    type Value = u32;
    fn value_at(&self, row: usize) -> Self::Value {
        self.value(row)
    }
}

impl RequiredValue for UInt64Array {
    type Value = u64;
    fn value_at(&self, row: usize) -> Self::Value {
        self.value(row)
    }
}

impl RequiredValue for arrow_array::Int64Array {
    type Value = i64;
    fn value_at(&self, row: usize) -> Self::Value {
        self.value(row)
    }
}

impl RequiredValue for BooleanArray {
    type Value = bool;
    fn value_at(&self, row: usize) -> Self::Value {
        self.value(row)
    }
}

impl RequiredValue for Decimal128Array {
    type Value = i128;
    fn value_at(&self, row: usize) -> Self::Value {
        self.value(row)
    }
}

fn required_str<'a>(array: &'a StringArray, row: usize, name: &str) -> Result<&'a str> {
    if array.is_null(row) {
        return Err(CdfError::data(format!(
            "statistics profile required field {name} is null"
        )));
    }
    Ok(array.value(row))
}

fn required_binary<'a>(array: &'a BinaryArray, row: usize, name: &str) -> Result<&'a [u8]> {
    if array.is_null(row) {
        return Err(CdfError::data(format!(
            "statistics profile required field {name} is null"
        )));
    }
    Ok(array.value(row))
}

fn required_value<T: RequiredValue>(array: &T, row: usize, name: &str) -> Result<T::Value> {
    if array.is_null(row) {
        return Err(CdfError::data(format!(
            "statistics profile required field {name} is null"
        )));
    }
    Ok(array.value_at(row))
}

fn optional(array: &StringArray, row: usize) -> Option<&str> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn column<T: 'static>(batch: &RecordBatch, index: usize) -> Result<&T> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| CdfError::data("statistics profile column had unexpected Arrow type"))
}

fn canonical_string<T: serde::Serialize>(value: &T) -> Result<String> {
    String::from_utf8(crate::canonical_json_bytes(value)?)
        .map_err(|error| CdfError::internal(format!("canonical JSON was not UTF-8: {error}")))
}

fn string_array(values: impl Iterator<Item = Result<Option<String>>>) -> Result<StringArray> {
    values.collect::<Result<Vec<_>>>().map(StringArray::from)
}

fn incomplete_reason(reason: &IncompleteStatisticsReason) -> &'static str {
    match reason {
        IncompleteStatisticsReason::UnsupportedType => "unsupported_type",
        IncompleteStatisticsReason::NanObserved => "nan_observed",
        IncompleteStatisticsReason::NonFiniteObserved => "non_finite_observed",
    }
}

fn parse_incomplete_reason(value: Option<&str>) -> Option<IncompleteStatisticsReason> {
    Some(match value? {
        "unsupported_type" => IncompleteStatisticsReason::UnsupportedType,
        "nan_observed" => IncompleteStatisticsReason::NanObserved,
        "non_finite_observed" => IncompleteStatisticsReason::NonFiniteObserved,
        _ => return None,
    })
}
