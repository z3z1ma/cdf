use std::{collections::BTreeMap, fmt, str::FromStr, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, DurationMicrosecondArray,
    DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray,
    FixedSizeListArray, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
    IntervalYearMonthArray, LargeBinaryArray, LargeListArray, LargeListViewArray, LargeStringArray,
    ListArray, ListViewArray, MapArray, StringArray, StringViewArray, StructArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array, new_empty_array,
    new_null_array,
};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, OffsetBuffer, ScalarBuffer, i256};
use arrow_schema::{DataType, FieldRef, Fields, IntervalUnit, TimeUnit};
use arrow_select::concat::concat;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use half::f16;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::RedactionDecision;

pub use cdf_kernel::{
    CanonicalArrowDateUnit, CanonicalArrowField, CanonicalArrowIntervalUnit,
    CanonicalArrowTimeUnit, CanonicalArrowType, CanonicalArrowUnionField, CanonicalArrowUnionMode,
};

pub const RESIDUAL_JSON_V1: u64 = 1;
pub const RESIDUAL_ENCODING_NAME: &str = "residual-json-v1";
pub const RESIDUAL_ENCODING_METADATA_KEY: &str = "cdf:variant_encoding";
pub const RESIDUAL_ENCODE_UNSUPPORTED_CODE: &str = "cdf.residual_encode_unsupported";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResidualCodecError {
    EncodeUnsupported { data_type: String, reason: String },
    InvalidPath { path: String, reason: String },
    InvalidEnvelope { reason: String },
    UnsupportedVersion { version: u64 },
    ExactDecode { path: String, reason: String },
}

impl ResidualCodecError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::EncodeUnsupported { .. } => RESIDUAL_ENCODE_UNSUPPORTED_CODE,
            Self::InvalidPath { .. } => "cdf.residual_path_invalid",
            Self::InvalidEnvelope { .. } => "cdf.residual_envelope_invalid",
            Self::UnsupportedVersion { .. } => "cdf.residual_version_unsupported",
            Self::ExactDecode { .. } => "cdf.residual_decode_inexact",
        }
    }
}

impl fmt::Display for ResidualCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EncodeUnsupported { data_type, reason } => {
                write!(formatter, "unsupported Arrow type {data_type}: {reason}")
            }
            Self::InvalidPath { path, reason } => {
                write!(formatter, "invalid residual path {path:?}: {reason}")
            }
            Self::InvalidEnvelope { reason } => {
                write!(formatter, "invalid residual envelope: {reason}")
            }
            Self::UnsupportedVersion { version } => {
                write!(formatter, "unsupported residual envelope version {version}")
            }
            Self::ExactDecode { path, reason } => {
                write!(
                    formatter,
                    "residual value at {path:?} cannot decode exactly: {reason}"
                )
            }
        }
    }
}

impl std::error::Error for ResidualCodecError {}

pub type ResidualArrowField = CanonicalArrowField;
pub type ResidualArrowType = CanonicalArrowType;
pub type ResidualUnionField = CanonicalArrowUnionField;
pub type ResidualUnionMode = CanonicalArrowUnionMode;
pub type ResidualTimeUnit = CanonicalArrowTimeUnit;
pub type ResidualDateUnit = CanonicalArrowDateUnit;
pub type ResidualIntervalUnit = CanonicalArrowIntervalUnit;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ResidualValueEncoding {
    Json,
    Base10,
    FloatString,
    Base64url,
    StorageInteger,
    Nested,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResidualValue {
    arrow_type: ResidualArrowType,
    encoding: ResidualValueEncoding,
    value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    redaction: Option<ResidualRedaction>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ResidualRedaction {
    Hash { algorithm: String },
    Omit,
    Mask,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResidualEnvelope {
    v: u64,
    fields: BTreeMap<String, ResidualValue>,
}

pub struct ResidualFieldRef<'a> {
    path: String,
    array: &'a dyn Array,
    row: usize,
}

pub struct ResidualFieldWithRedaction<'a> {
    field: ResidualFieldRef<'a>,
    redaction: &'a RedactionDecision,
}

impl<'a> ResidualFieldWithRedaction<'a> {
    pub fn new(field: ResidualFieldRef<'a>, redaction: &'a RedactionDecision) -> Self {
        Self { field, redaction }
    }
}

impl<'a> ResidualFieldRef<'a> {
    pub fn new<I, S>(
        path_segments: I,
        array: &'a dyn Array,
        row: usize,
    ) -> Result<Self, ResidualCodecError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let segments = path_segments
            .into_iter()
            .map(|segment| segment.as_ref().to_owned())
            .collect::<Vec<_>>();
        if segments.is_empty() {
            return Err(ResidualCodecError::InvalidPath {
                path: String::new(),
                reason: "at least one original-source path segment is required".to_owned(),
            });
        }
        if row >= array.len() {
            return Err(ResidualCodecError::InvalidEnvelope {
                reason: format!(
                    "residual row {row} is outside Arrow array length {}",
                    array.len()
                ),
            });
        }
        Ok(Self {
            path: residual_json_pointer(segments.iter().map(String::as_str)),
            array,
            row,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

#[derive(Clone)]
pub struct DecodedResidualField {
    pub path: String,
    pub array: ArrayRef,
}

impl fmt::Debug for DecodedResidualField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedResidualField")
            .field("path", &self.path)
            .field("data_type", self.array.data_type())
            .field("is_null", &self.array.is_null(0))
            .finish()
    }
}

pub fn residual_json_pointer<I, S>(segments: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut pointer = String::new();
    for segment in segments {
        pointer.push('/');
        for character in segment.as_ref().chars() {
            match character {
                '~' => pointer.push_str("~0"),
                '/' => pointer.push_str("~1"),
                other => pointer.push(other),
            }
        }
    }
    pointer
}

pub fn encode_residual_json_v1<'a, I>(fields: I) -> Result<Vec<u8>, ResidualCodecError>
where
    I: IntoIterator<Item = ResidualFieldRef<'a>>,
{
    let mut encoded = BTreeMap::new();
    for field in fields {
        let arrow_type = ResidualArrowType::from_arrow(field.array.data_type())
            .map_err(|reason| unsupported_type(field.array.data_type(), &reason.to_string()))?;
        let encoding = encoding_for_type(field.array.data_type())?;
        let value = encode_arrow_value(field.array, field.row)?;
        if encoded
            .insert(
                field.path.clone(),
                ResidualValue {
                    arrow_type,
                    encoding,
                    value,
                    redaction: None,
                },
            )
            .is_some()
        {
            return Err(ResidualCodecError::InvalidPath {
                path: field.path,
                reason: "duplicate canonical residual path".to_owned(),
            });
        }
    }
    if encoded.is_empty() {
        return Err(ResidualCodecError::InvalidEnvelope {
            reason: "a residual envelope must contain at least one field".to_owned(),
        });
    }
    serde_json::to_vec(&ResidualEnvelope {
        v: RESIDUAL_JSON_V1,
        fields: encoded,
    })
    .map_err(|error| ResidualCodecError::InvalidEnvelope {
        reason: error.to_string(),
    })
}

pub fn encode_residual_json_v1_redacted<'a, I>(fields: I) -> Result<Vec<u8>, ResidualCodecError>
where
    I: IntoIterator<Item = ResidualFieldWithRedaction<'a>>,
{
    let mut encoded = BTreeMap::new();
    for field in fields {
        let arrow_type =
            ResidualArrowType::from_arrow(field.field.array.data_type()).map_err(|reason| {
                unsupported_type(field.field.array.data_type(), &reason.to_string())
            })?;
        let encoding = encoding_for_type(field.field.array.data_type())?;
        let exact = encode_arrow_value(field.field.array, field.field.row)?;
        let (value, redaction) = match field.redaction {
            RedactionDecision::Preserve => (exact, None),
            RedactionDecision::Hash { algorithm } if algorithm == "sha256" => {
                let canonical = serde_json::to_vec(&exact).map_err(|error| {
                    ResidualCodecError::InvalidEnvelope {
                        reason: error.to_string(),
                    }
                })?;
                (
                    Value::String(format!("sha256:{:x}", Sha256::digest(canonical))),
                    Some(ResidualRedaction::Hash {
                        algorithm: algorithm.clone(),
                    }),
                )
            }
            RedactionDecision::Hash { algorithm } => {
                return Err(ResidualCodecError::EncodeUnsupported {
                    data_type: field.field.array.data_type().to_string(),
                    reason: format!("unsupported residual hash algorithm {algorithm:?}"),
                });
            }
            RedactionDecision::Omit => (Value::Null, Some(ResidualRedaction::Omit)),
            RedactionDecision::Mask { replacement } => (
                Value::String(replacement.clone()),
                Some(ResidualRedaction::Mask),
            ),
        };
        if encoded
            .insert(
                field.field.path.clone(),
                ResidualValue {
                    arrow_type,
                    encoding,
                    value,
                    redaction,
                },
            )
            .is_some()
        {
            return Err(ResidualCodecError::InvalidPath {
                path: field.field.path,
                reason: "duplicate canonical residual path".to_owned(),
            });
        }
    }
    if encoded.is_empty() {
        return Err(ResidualCodecError::InvalidEnvelope {
            reason: "a residual envelope must contain at least one field".to_owned(),
        });
    }
    serde_json::to_vec(&ResidualEnvelope {
        v: RESIDUAL_JSON_V1,
        fields: encoded,
    })
    .map_err(|error| ResidualCodecError::InvalidEnvelope {
        reason: error.to_string(),
    })
}

pub fn decode_residual_json_v1(
    bytes: &[u8],
) -> Result<Vec<DecodedResidualField>, ResidualCodecError> {
    let envelope: ResidualEnvelope =
        serde_json::from_slice(bytes).map_err(|error| ResidualCodecError::InvalidEnvelope {
            reason: error.to_string(),
        })?;
    if envelope.v != RESIDUAL_JSON_V1 {
        return Err(ResidualCodecError::UnsupportedVersion {
            version: envelope.v,
        });
    }
    if envelope.fields.is_empty() {
        return Err(ResidualCodecError::InvalidEnvelope {
            reason: "a residual envelope must contain at least one field".to_owned(),
        });
    }
    let canonical =
        serde_json::to_vec(&envelope).map_err(|error| ResidualCodecError::InvalidEnvelope {
            reason: error.to_string(),
        })?;
    if canonical != bytes {
        return Err(ResidualCodecError::InvalidEnvelope {
            reason: "bytes are not canonical residual-json-v1".to_owned(),
        });
    }

    envelope
        .fields
        .into_iter()
        .map(|(path, residual)| {
            validate_canonical_pointer(&path)?;
            if let Some(redaction) = residual.redaction {
                return Err(ResidualCodecError::ExactDecode {
                    path,
                    reason: format!(
                        "typed residual value is intentionally redacted ({redaction:?})"
                    ),
                });
            }
            let data_type = residual.arrow_type.to_arrow().map_err(|reason| {
                ResidualCodecError::ExactDecode {
                    path: path.clone(),
                    reason: reason.to_string(),
                }
            })?;
            let expected_encoding =
                encoding_for_type(&data_type).map_err(|error| ResidualCodecError::ExactDecode {
                    path: path.clone(),
                    reason: error.to_string(),
                })?;
            if residual.encoding != expected_encoding {
                return Err(ResidualCodecError::ExactDecode {
                    path,
                    reason: format!(
                        "encoding {:?} does not match Arrow type {data_type}",
                        residual.encoding
                    ),
                });
            }
            let array = decode_arrow_value(&path, &data_type, &residual.value)?;
            Ok(DecodedResidualField { path, array })
        })
        .collect()
}

pub fn remove_residual_json_v1_path(
    bytes: &[u8],
    pointer: &str,
) -> Result<Option<Vec<u8>>, ResidualCodecError> {
    validate_canonical_pointer(pointer)?;
    let mut envelope: ResidualEnvelope =
        serde_json::from_slice(bytes).map_err(|error| ResidualCodecError::InvalidEnvelope {
            reason: error.to_string(),
        })?;
    if envelope.v != RESIDUAL_JSON_V1 {
        return Err(ResidualCodecError::UnsupportedVersion {
            version: envelope.v,
        });
    }
    if envelope.fields.is_empty() {
        return Err(ResidualCodecError::InvalidEnvelope {
            reason: "a residual envelope must contain at least one field".to_owned(),
        });
    }
    for path in envelope.fields.keys() {
        validate_canonical_pointer(path)?;
    }
    let canonical =
        serde_json::to_vec(&envelope).map_err(|error| ResidualCodecError::InvalidEnvelope {
            reason: error.to_string(),
        })?;
    if canonical != bytes {
        return Err(ResidualCodecError::InvalidEnvelope {
            reason: "bytes are not canonical residual-json-v1".to_owned(),
        });
    }
    if envelope.fields.remove(pointer).is_none() {
        return Err(ResidualCodecError::InvalidPath {
            path: pointer.to_owned(),
            reason: "promoted path is absent from the residual envelope".to_owned(),
        });
    }
    if envelope.fields.is_empty() {
        return Ok(None);
    }
    serde_json::to_vec(&envelope)
        .map(Some)
        .map_err(|error| ResidualCodecError::InvalidEnvelope {
            reason: error.to_string(),
        })
}

fn unsupported_type(data_type: &DataType, reason: &str) -> ResidualCodecError {
    ResidualCodecError::EncodeUnsupported {
        data_type: data_type.to_string(),
        reason: reason.to_owned(),
    }
}

fn encoding_for_type(data_type: &DataType) -> Result<ResidualValueEncoding, ResidualCodecError> {
    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => Ok(ResidualValueEncoding::Json),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => Ok(ResidualValueEncoding::Base10),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            Ok(ResidualValueEncoding::FloatString)
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::BinaryView => Ok(ResidualValueEncoding::Base64url),
        DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => Ok(ResidualValueEncoding::StorageInteger),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => Ok(ResidualValueEncoding::Nested),
        other => Err(unsupported_type(
            other,
            "no exact residual-json-v1 encoding",
        )),
    }
}

fn encode_arrow_value(array: &dyn Array, row: usize) -> Result<Value, ResidualCodecError> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    macro_rules! string_value {
        ($array_type:ty) => {
            Value::String(downcast::<$array_type>(array)?.value(row).to_string())
        };
    }
    match array.data_type() {
        DataType::Null => Ok(Value::Null),
        DataType::Boolean => Ok(Value::Bool(downcast::<BooleanArray>(array)?.value(row))),
        DataType::Int8 => Ok(string_value!(Int8Array)),
        DataType::Int16 => Ok(string_value!(Int16Array)),
        DataType::Int32 => Ok(string_value!(Int32Array)),
        DataType::Int64 => Ok(string_value!(Int64Array)),
        DataType::UInt8 => Ok(string_value!(UInt8Array)),
        DataType::UInt16 => Ok(string_value!(UInt16Array)),
        DataType::UInt32 => Ok(string_value!(UInt32Array)),
        DataType::UInt64 => Ok(string_value!(UInt64Array)),
        DataType::Float16 => Ok(Value::String(canonical_float16(
            downcast::<Float16Array>(array)?.value(row),
        ))),
        DataType::Float32 => Ok(Value::String(canonical_float32(
            downcast::<Float32Array>(array)?.value(row),
        ))),
        DataType::Float64 => Ok(Value::String(canonical_float64(
            downcast::<Float64Array>(array)?.value(row),
        ))),
        DataType::Decimal32(_, _) => Ok(string_value!(Decimal32Array)),
        DataType::Decimal64(_, _) => Ok(string_value!(Decimal64Array)),
        DataType::Decimal128(_, _) => Ok(string_value!(Decimal128Array)),
        DataType::Decimal256(_, _) => Ok(string_value!(Decimal256Array)),
        DataType::Utf8 => Ok(Value::String(
            downcast::<StringArray>(array)?.value(row).to_owned(),
        )),
        DataType::LargeUtf8 => Ok(Value::String(
            downcast::<LargeStringArray>(array)?.value(row).to_owned(),
        )),
        DataType::Utf8View => Ok(Value::String(
            downcast::<StringViewArray>(array)?.value(row).to_owned(),
        )),
        DataType::Binary => Ok(binary_json(downcast::<BinaryArray>(array)?.value(row))),
        DataType::LargeBinary => Ok(binary_json(downcast::<LargeBinaryArray>(array)?.value(row))),
        DataType::FixedSizeBinary(_) => Ok(binary_json(
            downcast::<FixedSizeBinaryArray>(array)?.value(row),
        )),
        DataType::BinaryView => Ok(binary_json(downcast::<BinaryViewArray>(array)?.value(row))),
        DataType::Timestamp(TimeUnit::Second, _) => Ok(string_value!(TimestampSecondArray)),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Ok(string_value!(TimestampMillisecondArray))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(string_value!(TimestampMicrosecondArray))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(string_value!(TimestampNanosecondArray)),
        DataType::Date32 => Ok(string_value!(Date32Array)),
        DataType::Date64 => Ok(string_value!(Date64Array)),
        DataType::Time32(TimeUnit::Second) => Ok(string_value!(Time32SecondArray)),
        DataType::Time32(TimeUnit::Millisecond) => Ok(string_value!(Time32MillisecondArray)),
        DataType::Time64(TimeUnit::Microsecond) => Ok(string_value!(Time64MicrosecondArray)),
        DataType::Time64(TimeUnit::Nanosecond) => Ok(string_value!(Time64NanosecondArray)),
        DataType::Duration(TimeUnit::Second) => Ok(string_value!(DurationSecondArray)),
        DataType::Duration(TimeUnit::Millisecond) => Ok(string_value!(DurationMillisecondArray)),
        DataType::Duration(TimeUnit::Microsecond) => Ok(string_value!(DurationMicrosecondArray)),
        DataType::Duration(TimeUnit::Nanosecond) => Ok(string_value!(DurationNanosecondArray)),
        DataType::Interval(IntervalUnit::YearMonth) => Ok(string_value!(IntervalYearMonthArray)),
        DataType::Interval(IntervalUnit::DayTime) => {
            let value = downcast::<IntervalDayTimeArray>(array)?.value(row);
            Ok(Value::String(pack_day_time(value).to_string()))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let value = downcast::<IntervalMonthDayNanoArray>(array)?.value(row);
            Ok(Value::String(pack_month_day_nano(value).to_string()))
        }
        DataType::List(_) => encode_list(downcast::<ListArray>(array)?.value(row).as_ref()),
        DataType::LargeList(_) => {
            encode_list(downcast::<LargeListArray>(array)?.value(row).as_ref())
        }
        DataType::ListView(_) => encode_list(downcast::<ListViewArray>(array)?.value(row).as_ref()),
        DataType::LargeListView(_) => {
            encode_list(downcast::<LargeListViewArray>(array)?.value(row).as_ref())
        }
        DataType::FixedSizeList(_, _) => {
            encode_list(downcast::<FixedSizeListArray>(array)?.value(row).as_ref())
        }
        DataType::Struct(_) => encode_struct(downcast::<StructArray>(array)?, row),
        DataType::Map(_, _) => encode_map(downcast::<MapArray>(array)?, row),
        other => Err(unsupported_type(
            other,
            "no exact residual-json-v1 value encoder",
        )),
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, ResidualCodecError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| ResidualCodecError::InvalidEnvelope {
            reason: format!(
                "Arrow array cannot downcast from declared type {}",
                array.data_type()
            ),
        })
}

fn binary_json(bytes: &[u8]) -> Value {
    Value::String(URL_SAFE_NO_PAD.encode(bytes))
}

fn canonical_float16(value: f16) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f16::INFINITY {
        "Infinity".to_owned()
    } else if value == f16::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        let mut candidates = vec![value.to_string()];
        for precision in 0..=8 {
            candidates.push(normalize_decimal(format!(
                "{:.*}",
                precision,
                value.to_f32()
            )));
            candidates.push(normalize_decimal(format!(
                "{:.*e}",
                precision,
                value.to_f32()
            )));
        }
        candidates
            .into_iter()
            .filter(|candidate| {
                f16::from_str(candidate)
                    .map(|parsed| parsed.to_bits() == value.to_bits())
                    .unwrap_or(false)
            })
            .min_by(|left, right| left.len().cmp(&right.len()).then_with(|| left.cmp(right)))
            .expect("finite float16 display is round-trippable")
    }
}

fn normalize_decimal(value: String) -> String {
    let (mantissa, exponent) = match value.split_once('e') {
        Some((mantissa, exponent)) => (mantissa, Some(exponent)),
        None => (value.as_str(), None),
    };
    let mantissa = if mantissa.contains('.') {
        let trimmed = mantissa.trim_end_matches('0').trim_end_matches('.');
        if trimmed.is_empty() || trimmed == "-" {
            format!("{trimmed}0")
        } else {
            trimmed.to_owned()
        }
    } else {
        mantissa.to_owned()
    };
    match exponent {
        Some(exponent) => exponent
            .parse::<i32>()
            .map(|exponent| format!("{mantissa}e{exponent}"))
            .unwrap_or_else(|_| format!("{mantissa}e{exponent}")),
        None => mantissa,
    }
}

fn canonical_float32(value: f32) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f32::INFINITY {
        "Infinity".to_owned()
    } else if value == f32::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        value.to_string()
    }
}

fn canonical_float64(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "Infinity".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        value.to_string()
    }
}

fn encode_list(values: &dyn Array) -> Result<Value, ResidualCodecError> {
    (0..values.len())
        .map(|row| encode_arrow_value(values, row))
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn encode_struct(array: &StructArray, row: usize) -> Result<Value, ResidualCodecError> {
    let mut object = serde_json::Map::new();
    for (field, column) in array.fields().iter().zip(array.columns()) {
        if object
            .insert(
                field.name().clone(),
                encode_arrow_value(column.as_ref(), row)?,
            )
            .is_some()
        {
            return Err(unsupported_type(
                array.data_type(),
                &format!("duplicate struct field name {:?}", field.name()),
            ));
        }
    }
    let sorted = object.into_iter().collect::<BTreeMap<_, _>>();
    serde_json::to_value(sorted).map_err(|error| ResidualCodecError::InvalidEnvelope {
        reason: error.to_string(),
    })
}

fn encode_map(array: &MapArray, row: usize) -> Result<Value, ResidualCodecError> {
    let entries = array.value(row);
    let keys = entries.column(0);
    let values = entries.column(1);
    let mut encoded = Vec::with_capacity(entries.len());
    for index in 0..entries.len() {
        let mut entry = serde_json::Map::new();
        entry.insert("key".to_owned(), encode_arrow_value(keys.as_ref(), index)?);
        entry.insert(
            "value".to_owned(),
            encode_arrow_value(values.as_ref(), index)?,
        );
        encoded.push(Value::Object(entry));
    }
    Ok(Value::Array(encoded))
}

fn decode_arrow_value(
    path: &str,
    data_type: &DataType,
    value: &Value,
) -> Result<ArrayRef, ResidualCodecError> {
    if value.is_null() {
        return Ok(new_null_array(data_type, 1));
    }
    macro_rules! parsed_array {
        ($array_type:ty, $value_type:ty) => {{
            let parsed = parse_canonical_integer::<$value_type>(path, value)?;
            Ok(Arc::new(<$array_type>::from(vec![parsed])) as ArrayRef)
        }};
    }
    match data_type {
        DataType::Null => Err(exact_error(path, "non-null value for Arrow Null")),
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(vec![Some(
            value
                .as_bool()
                .ok_or_else(|| exact_error(path, "expected JSON boolean"))?,
        )]))),
        DataType::Int8 => parsed_array!(Int8Array, i8),
        DataType::Int16 => parsed_array!(Int16Array, i16),
        DataType::Int32 => parsed_array!(Int32Array, i32),
        DataType::Int64 => parsed_array!(Int64Array, i64),
        DataType::UInt8 => parsed_array!(UInt8Array, u8),
        DataType::UInt16 => parsed_array!(UInt16Array, u16),
        DataType::UInt32 => parsed_array!(UInt32Array, u32),
        DataType::UInt64 => parsed_array!(UInt64Array, u64),
        DataType::Float16 => {
            let parsed = parse_float16(path, value)?;
            Ok(Arc::new(Float16Array::from(vec![parsed])))
        }
        DataType::Float32 => {
            let parsed = parse_float32(path, value)?;
            Ok(Arc::new(Float32Array::from(vec![parsed])))
        }
        DataType::Float64 => {
            let parsed = parse_float64(path, value)?;
            Ok(Arc::new(Float64Array::from(vec![parsed])))
        }
        DataType::Decimal32(precision, scale) => {
            let parsed = parse_canonical_integer::<i32>(path, value)?;
            Ok(Arc::new(
                Decimal32Array::from(vec![parsed])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| exact_error(path, error.to_string()))?,
            ))
        }
        DataType::Decimal64(precision, scale) => {
            let parsed = parse_canonical_integer::<i64>(path, value)?;
            Ok(Arc::new(
                Decimal64Array::from(vec![parsed])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| exact_error(path, error.to_string()))?,
            ))
        }
        DataType::Decimal128(precision, scale) => {
            let parsed = parse_canonical_integer::<i128>(path, value)?;
            Ok(Arc::new(
                Decimal128Array::from(vec![parsed])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| exact_error(path, error.to_string()))?,
            ))
        }
        DataType::Decimal256(precision, scale) => {
            let text = canonical_string(path, value)?;
            let parsed =
                i256::from_str(text).map_err(|error| exact_error(path, error.to_string()))?;
            if parsed.to_string() != text {
                return Err(exact_error(path, "non-canonical decimal256 integer"));
            }
            Ok(Arc::new(
                Decimal256Array::from(vec![parsed])
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| exact_error(path, error.to_string()))?,
            ))
        }
        DataType::Utf8 => Ok(Arc::new(StringArray::from(vec![Some(canonical_string(
            path, value,
        )?)]))),
        DataType::LargeUtf8 => Ok(Arc::new(LargeStringArray::from(vec![Some(
            canonical_string(path, value)?,
        )]))),
        DataType::Utf8View => Ok(Arc::new(StringViewArray::from(vec![Some(
            canonical_string(path, value)?,
        )]))),
        DataType::Binary => Ok(Arc::new(BinaryArray::from(vec![Some(
            decode_binary(path, value)?.as_slice(),
        )]))),
        DataType::LargeBinary => Ok(Arc::new(LargeBinaryArray::from(vec![Some(
            decode_binary(path, value)?.as_slice(),
        )]))),
        DataType::FixedSizeBinary(width) => {
            let bytes = decode_binary(path, value)?;
            if bytes.len() != *width as usize {
                return Err(exact_error(path, "fixed-size binary width mismatch"));
            }
            Ok(Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![Some(bytes.as_slice())].into_iter(),
                    *width,
                )
                .map_err(|error| exact_error(path, error.to_string()))?,
            ))
        }
        DataType::BinaryView => Ok(Arc::new(BinaryViewArray::from(vec![Some(
            decode_binary(path, value)?.as_slice(),
        )]))),
        DataType::Timestamp(TimeUnit::Second, timezone) => Ok(Arc::new(
            TimestampSecondArray::from(vec![parse_canonical_integer::<i64>(path, value)?])
                .with_timezone_opt(timezone.clone()),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => Ok(Arc::new(
            TimestampMillisecondArray::from(vec![parse_canonical_integer::<i64>(path, value)?])
                .with_timezone_opt(timezone.clone()),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => Ok(Arc::new(
            TimestampMicrosecondArray::from(vec![parse_canonical_integer::<i64>(path, value)?])
                .with_timezone_opt(timezone.clone()),
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, timezone) => Ok(Arc::new(
            TimestampNanosecondArray::from(vec![parse_canonical_integer::<i64>(path, value)?])
                .with_timezone_opt(timezone.clone()),
        )),
        DataType::Date32 => parsed_array!(Date32Array, i32),
        DataType::Date64 => parsed_array!(Date64Array, i64),
        DataType::Time32(TimeUnit::Second) => parsed_array!(Time32SecondArray, i32),
        DataType::Time32(TimeUnit::Millisecond) => parsed_array!(Time32MillisecondArray, i32),
        DataType::Time64(TimeUnit::Microsecond) => parsed_array!(Time64MicrosecondArray, i64),
        DataType::Time64(TimeUnit::Nanosecond) => parsed_array!(Time64NanosecondArray, i64),
        DataType::Duration(TimeUnit::Second) => parsed_array!(DurationSecondArray, i64),
        DataType::Duration(TimeUnit::Millisecond) => parsed_array!(DurationMillisecondArray, i64),
        DataType::Duration(TimeUnit::Microsecond) => parsed_array!(DurationMicrosecondArray, i64),
        DataType::Duration(TimeUnit::Nanosecond) => parsed_array!(DurationNanosecondArray, i64),
        DataType::Interval(IntervalUnit::YearMonth) => parsed_array!(IntervalYearMonthArray, i32),
        DataType::Interval(IntervalUnit::DayTime) => {
            let raw = parse_canonical_integer::<i64>(path, value)?;
            Ok(Arc::new(IntervalDayTimeArray::from(vec![unpack_day_time(
                raw,
            )])))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let raw = parse_canonical_integer::<i128>(path, value)?;
            Ok(Arc::new(IntervalMonthDayNanoArray::from(vec![
                unpack_month_day_nano(raw),
            ])))
        }
        DataType::List(field) => decode_list_i32(path, field, value, false),
        DataType::LargeList(field) => decode_list_i64(path, field, value, false),
        DataType::ListView(field) => decode_list_i32(path, field, value, true),
        DataType::LargeListView(field) => decode_list_i64(path, field, value, true),
        DataType::FixedSizeList(field, length) => {
            decode_fixed_size_list(path, field, *length, value)
        }
        DataType::Struct(fields) => decode_struct(path, fields, value),
        DataType::Map(field, sorted) => decode_map(path, field, *sorted, value),
        other => Err(exact_error(
            path,
            format!("unsupported decoded Arrow type {other}"),
        )),
    }
}

fn canonical_string<'a>(path: &str, value: &'a Value) -> Result<&'a str, ResidualCodecError> {
    value
        .as_str()
        .ok_or_else(|| exact_error(path, "expected JSON string"))
}

fn parse_canonical_integer<T>(path: &str, value: &Value) -> Result<T, ResidualCodecError>
where
    T: FromStr + ToString,
    T::Err: fmt::Display,
{
    let text = canonical_string(path, value)?;
    let parsed = T::from_str(text).map_err(|error| exact_error(path, error.to_string()))?;
    if parsed.to_string() != text {
        return Err(exact_error(path, "non-canonical base-10 integer"));
    }
    Ok(parsed)
}

fn parse_float16(path: &str, value: &Value) -> Result<f16, ResidualCodecError> {
    let text = canonical_string(path, value)?;
    let parsed = match text {
        "NaN" => f16::NAN,
        "Infinity" => f16::INFINITY,
        "-Infinity" => f16::NEG_INFINITY,
        _ => f16::from_str(text).map_err(|error| exact_error(path, error.to_string()))?,
    };
    if canonical_float16(parsed) != text {
        return Err(exact_error(path, "non-canonical float16 string"));
    }
    Ok(parsed)
}

fn parse_float32(path: &str, value: &Value) -> Result<f32, ResidualCodecError> {
    let text = canonical_string(path, value)?;
    let parsed = match text {
        "NaN" => f32::NAN,
        "Infinity" => f32::INFINITY,
        "-Infinity" => f32::NEG_INFINITY,
        _ => f32::from_str(text).map_err(|error| exact_error(path, error.to_string()))?,
    };
    if canonical_float32(parsed) != text {
        return Err(exact_error(path, "non-canonical float32 string"));
    }
    Ok(parsed)
}

fn parse_float64(path: &str, value: &Value) -> Result<f64, ResidualCodecError> {
    let text = canonical_string(path, value)?;
    let parsed = match text {
        "NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        "-Infinity" => f64::NEG_INFINITY,
        _ => f64::from_str(text).map_err(|error| exact_error(path, error.to_string()))?,
    };
    if canonical_float64(parsed) != text {
        return Err(exact_error(path, "non-canonical float64 string"));
    }
    Ok(parsed)
}

fn decode_binary(path: &str, value: &Value) -> Result<Vec<u8>, ResidualCodecError> {
    let text = canonical_string(path, value)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(text)
        .map_err(|error| exact_error(path, error.to_string()))?;
    if URL_SAFE_NO_PAD.encode(&bytes) != text {
        return Err(exact_error(path, "non-canonical base64url"));
    }
    Ok(bytes)
}

fn decode_values(
    path: &str,
    data_type: &DataType,
    values: &[Value],
) -> Result<ArrayRef, ResidualCodecError> {
    if values.is_empty() {
        return Ok(new_empty_array(data_type));
    }
    let arrays = values
        .iter()
        .enumerate()
        .map(|(index, value)| decode_arrow_value(&format!("{path}/{index}"), data_type, value))
        .collect::<Result<Vec<_>, _>>()?;
    let refs = arrays
        .iter()
        .map(|array| array.as_ref())
        .collect::<Vec<_>>();
    concat(&refs).map_err(|error| exact_error(path, error.to_string()))
}

fn decode_list_i32(
    path: &str,
    field: &FieldRef,
    value: &Value,
    view: bool,
) -> Result<ArrayRef, ResidualCodecError> {
    let values = value
        .as_array()
        .ok_or_else(|| exact_error(path, "expected nested list array"))?;
    let child = decode_values(path, field.data_type(), values)?;
    let length =
        i32::try_from(values.len()).map_err(|error| exact_error(path, error.to_string()))?;
    if view {
        Ok(Arc::new(
            ListViewArray::try_new(
                Arc::clone(field),
                ScalarBuffer::from(vec![0_i32]),
                ScalarBuffer::from(vec![length]),
                child,
                None,
            )
            .map_err(|error| exact_error(path, error.to_string()))?,
        ))
    } else {
        Ok(Arc::new(
            ListArray::try_new(
                Arc::clone(field),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, length])),
                child,
                None,
            )
            .map_err(|error| exact_error(path, error.to_string()))?,
        ))
    }
}

fn decode_list_i64(
    path: &str,
    field: &FieldRef,
    value: &Value,
    view: bool,
) -> Result<ArrayRef, ResidualCodecError> {
    let values = value
        .as_array()
        .ok_or_else(|| exact_error(path, "expected nested list array"))?;
    let child = decode_values(path, field.data_type(), values)?;
    let length =
        i64::try_from(values.len()).map_err(|error| exact_error(path, error.to_string()))?;
    if view {
        Ok(Arc::new(
            LargeListViewArray::try_new(
                Arc::clone(field),
                ScalarBuffer::from(vec![0_i64]),
                ScalarBuffer::from(vec![length]),
                child,
                None,
            )
            .map_err(|error| exact_error(path, error.to_string()))?,
        ))
    } else {
        Ok(Arc::new(
            LargeListArray::try_new(
                Arc::clone(field),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, length])),
                child,
                None,
            )
            .map_err(|error| exact_error(path, error.to_string()))?,
        ))
    }
}

fn decode_fixed_size_list(
    path: &str,
    field: &FieldRef,
    length: i32,
    value: &Value,
) -> Result<ArrayRef, ResidualCodecError> {
    let values = value
        .as_array()
        .ok_or_else(|| exact_error(path, "expected fixed-size list array"))?;
    if values.len() != length as usize {
        return Err(exact_error(path, "fixed-size list length mismatch"));
    }
    Ok(Arc::new(
        FixedSizeListArray::try_new(
            Arc::clone(field),
            length,
            decode_values(path, field.data_type(), values)?,
            None,
        )
        .map_err(|error| exact_error(path, error.to_string()))?,
    ))
}

fn decode_struct(
    path: &str,
    fields: &Fields,
    value: &Value,
) -> Result<ArrayRef, ResidualCodecError> {
    let object = value
        .as_object()
        .ok_or_else(|| exact_error(path, "expected struct object"))?;
    if object.len() != fields.len() {
        return Err(exact_error(path, "struct field count mismatch"));
    }
    let columns = fields
        .iter()
        .map(|field| {
            let child = object.get(field.name()).ok_or_else(|| {
                exact_error(path, format!("missing struct field {:?}", field.name()))
            })?;
            decode_arrow_value(
                &format!("{path}/{}", escape_pointer_segment(field.name())),
                field.data_type(),
                child,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(
        StructArray::try_new(fields.clone(), columns, None)
            .map_err(|error| exact_error(path, error.to_string()))?,
    ))
}

fn decode_map(
    path: &str,
    entries_field: &FieldRef,
    sorted: bool,
    value: &Value,
) -> Result<ArrayRef, ResidualCodecError> {
    let entries = value
        .as_array()
        .ok_or_else(|| exact_error(path, "expected ordered map-entry array"))?;
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return Err(exact_error(path, "Arrow map entries field is not a struct"));
    };
    if entry_fields.len() != 2 {
        return Err(exact_error(
            path,
            "Arrow map entries struct must contain key and value",
        ));
    }
    let mut keys = Vec::with_capacity(entries.len());
    let mut values = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let object = entry
            .as_object()
            .ok_or_else(|| exact_error(path, format!("map entry {index} is not an object")))?;
        if object.len() != 2 || !object.contains_key("key") || !object.contains_key("value") {
            return Err(exact_error(
                path,
                format!("map entry {index} must contain only key/value"),
            ));
        }
        keys.push(object["key"].clone());
        values.push(object["value"].clone());
    }
    let key_array = decode_values(path, entry_fields[0].data_type(), &keys)?;
    if key_array.null_count() != 0 {
        return Err(exact_error(path, "Arrow map keys cannot be null"));
    }
    let value_array = decode_values(path, entry_fields[1].data_type(), &values)?;
    let entry_struct =
        StructArray::try_new(entry_fields.clone(), vec![key_array, value_array], None)
            .map_err(|error| exact_error(path, error.to_string()))?;
    let length =
        i32::try_from(entries.len()).map_err(|error| exact_error(path, error.to_string()))?;
    Ok(Arc::new(
        MapArray::try_new(
            Arc::clone(entries_field),
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, length])),
            entry_struct,
            None,
            sorted,
        )
        .map_err(|error| exact_error(path, error.to_string()))?,
    ))
}

fn exact_error(path: &str, reason: impl Into<String>) -> ResidualCodecError {
    ResidualCodecError::ExactDecode {
        path: path.to_owned(),
        reason: reason.into(),
    }
}

fn pack_day_time(value: IntervalDayTime) -> i64 {
    let bits = u64::from(value.days as u32) | (u64::from(value.milliseconds as u32) << 32);
    bits as i64
}

fn unpack_day_time(value: i64) -> IntervalDayTime {
    let bits = value as u64;
    IntervalDayTime::new(bits as u32 as i32, (bits >> 32) as u32 as i32)
}

fn pack_month_day_nano(value: IntervalMonthDayNano) -> i128 {
    let bits = u128::from(value.months as u32)
        | (u128::from(value.days as u32) << 32)
        | ((value.nanoseconds as u64 as u128) << 64);
    bits as i128
}

fn unpack_month_day_nano(value: i128) -> IntervalMonthDayNano {
    let bits = value as u128;
    IntervalMonthDayNano::new(
        bits as u32 as i32,
        (bits >> 32) as u32 as i32,
        (bits >> 64) as u64 as i64,
    )
}

fn validate_canonical_pointer(path: &str) -> Result<(), ResidualCodecError> {
    if !path.starts_with('/') {
        return Err(ResidualCodecError::InvalidPath {
            path: path.to_owned(),
            reason: "RFC 6901 pointer must start with `/`".to_owned(),
        });
    }
    let mut decoded = Vec::new();
    for encoded in path[1..].split('/') {
        let mut segment = String::new();
        let mut characters = encoded.chars();
        while let Some(character) = characters.next() {
            if character != '~' {
                segment.push(character);
                continue;
            }
            match characters.next() {
                Some('0') => segment.push('~'),
                Some('1') => segment.push('/'),
                _ => {
                    return Err(ResidualCodecError::InvalidPath {
                        path: path.to_owned(),
                        reason: "invalid RFC 6901 escape".to_owned(),
                    });
                }
            }
        }
        decoded.push(segment);
    }
    if residual_json_pointer(decoded.iter().map(String::as_str)) != path {
        return Err(ResidualCodecError::InvalidPath {
            path: path.to_owned(),
            reason: "pointer is not canonically escaped".to_owned(),
        });
    }
    Ok(())
}

fn escape_pointer_segment(segment: &str) -> String {
    residual_json_pointer([segment])
        .trim_start_matches('/')
        .to_owned()
}

#[cfg(test)]
mod tests;
