use std::{
    fmt::Write as _,
    io::{BufRead, ErrorKind, Read},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{
        BooleanBuilder, Date32Builder, Decimal128Builder, Decimal256Builder, Float64Builder,
        Int64Builder, StringBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
        UInt64Builder,
    },
};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use cdf_kernel::{CdfError, Result};
use postgres::types::Type;

const COPY_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";
const COPY_TARGET_ROWS: usize = 64 * 1024;
const UTF8_SCRATCH_BYTES: usize = 32 * 1024;
const MAX_NUMERIC_BINARY_BYTES: usize = 8 + 2 * 20;
const NUMERIC_POSITIVE: u16 = 0x0000;
const NUMERIC_NEGATIVE: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_POSITIVE_INFINITY: u16 = 0xD000;
const NUMERIC_NEGATIVE_INFINITY: u16 = 0xF000;

pub(crate) struct PostgresBinaryCopyDecoder<R> {
    reader: R,
    schema: SchemaRef,
    builders: Vec<ColumnBuilder>,
    rows: usize,
    estimated_bytes: u64,
    maximum_batch_bytes: u64,
    target_batch_bytes: u64,
    value_scratch: Vec<u8>,
    finished: bool,
}

impl<R: BufRead> PostgresBinaryCopyDecoder<R> {
    pub(crate) fn new(mut reader: R, schema: SchemaRef, maximum_batch_bytes: u64) -> Result<Self> {
        if schema.fields().is_empty() {
            return Err(CdfError::contract(
                "Postgres binary COPY requires at least one projected field",
            ));
        }
        read_copy_header(&mut reader, maximum_batch_bytes)?;
        let builders = schema
            .fields()
            .iter()
            .map(|field| ColumnBuilder::new(field.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            reader,
            schema,
            builders,
            rows: 0,
            estimated_bytes: 0,
            maximum_batch_bytes,
            target_batch_bytes: maximum_batch_bytes.saturating_mul(3) / 4,
            value_scratch: vec![0; UTF8_SCRATCH_BYTES + 4],
            finished: false,
        })
    }

    pub(crate) fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }

        while self.rows < COPY_TARGET_ROWS && self.estimated_bytes < self.target_batch_bytes {
            let field_count = read_i16(&mut self.reader, "tuple field count")?;
            if field_count == -1 {
                ensure_copy_eof(&mut self.reader)?;
                self.finished = true;
                break;
            }
            let field_count = usize::try_from(field_count).map_err(|_| {
                CdfError::data(format!(
                    "Postgres binary COPY tuple has invalid field count {field_count}"
                ))
            })?;
            if field_count != self.builders.len() {
                return Err(CdfError::data(format!(
                    "Postgres binary COPY tuple has {field_count} fields, expected {}",
                    self.builders.len()
                )));
            }

            for (index, builder) in self.builders.iter_mut().enumerate() {
                let length = read_i32(&mut self.reader, "field length")?;
                let field = self.schema.field(index);
                if length == -1 {
                    if !field.is_nullable() {
                        return Err(CdfError::data(format!(
                            "Postgres binary COPY has NULL for non-nullable field `{}`",
                            field.name()
                        )));
                    }
                    admit_bytes(
                        &mut self.estimated_bytes,
                        builder.estimated_retained_bytes(0),
                        self.maximum_batch_bytes,
                        field,
                    )?;
                    builder.append_null();
                    continue;
                }
                let length = usize::try_from(length).map_err(|_| {
                    CdfError::data(format!(
                        "Postgres binary COPY field `{}` has invalid length {length}",
                        field.name()
                    ))
                })?;
                let retained = builder.estimated_retained_bytes(length);
                admit_bytes(
                    &mut self.estimated_bytes,
                    retained,
                    self.maximum_batch_bytes,
                    field,
                )?;
                builder.append_value(&mut self.reader, length, field, &mut self.value_scratch)?;
            }
            self.rows = self.rows.checked_add(1).ok_or_else(|| {
                CdfError::data("Postgres binary COPY batch row count overflowed usize")
            })?;
        }

        if self.rows == 0 {
            return Ok(None);
        }
        let arrays = self
            .builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect::<Vec<_>>();
        let batch =
            RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(CdfError::from)?;
        self.rows = 0;
        self.estimated_bytes = 0;
        Ok(Some(batch))
    }
}

pub(crate) fn expected_postgres_type(data_type: &DataType) -> Result<Type> {
    match data_type {
        DataType::Boolean => Ok(Type::BOOL),
        DataType::Int64 | DataType::Timestamp(TimeUnit::Millisecond | TimeUnit::Microsecond, _) => {
            Ok(Type::INT8)
        }
        DataType::UInt64 | DataType::Utf8 => Ok(Type::TEXT),
        DataType::Float64 => Ok(Type::FLOAT8),
        DataType::Date32 => Ok(Type::INT4),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Ok(Type::NUMERIC),
        other => Err(CdfError::data(format!(
            "Postgres binary COPY does not support Arrow type {other:?}"
        ))),
    }
}

enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Int64(Int64Builder),
    UInt64(UInt64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Date32(Date32Builder),
    TimestampMilliseconds(TimestampMillisecondBuilder),
    TimestampMicroseconds(TimestampMicrosecondBuilder),
    Decimal128(Decimal128Builder, u8, i8),
    Decimal256(Decimal256Builder, u8, i8),
}

impl ColumnBuilder {
    fn new(field: &Field) -> Result<Self> {
        let capacity = 8 * 1024;
        Ok(match field.data_type() {
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::UInt64 => Self::UInt64(UInt64Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, 128 * 1024)),
            DataType::Date32 => Self::Date32(Date32Builder::with_capacity(capacity)),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Self::TimestampMilliseconds(
                TimestampMillisecondBuilder::with_capacity(capacity)
                    .with_data_type(field.data_type().clone()),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Self::TimestampMicroseconds(
                TimestampMicrosecondBuilder::with_capacity(capacity)
                    .with_data_type(field.data_type().clone()),
            ),
            DataType::Decimal128(precision, scale) => {
                validate_decimal_type(*precision, *scale, 38, field)?;
                Self::Decimal128(
                    Decimal128Builder::with_capacity(capacity)
                        .with_data_type(field.data_type().clone()),
                    *precision,
                    *scale,
                )
            }
            DataType::Decimal256(precision, scale) => {
                validate_decimal_type(*precision, *scale, 76, field)?;
                Self::Decimal256(
                    Decimal256Builder::with_capacity(capacity)
                        .with_data_type(field.data_type().clone()),
                    *precision,
                    *scale,
                )
            }
            other => {
                return Err(CdfError::data(format!(
                    "Postgres binary COPY does not support Arrow type {other:?} for field `{}`",
                    field.name()
                )));
            }
        })
    }

    fn estimated_retained_bytes(&self, utf8_length: usize) -> u64 {
        match self {
            Self::Boolean(_) => 1,
            Self::Int64(_)
            | Self::UInt64(_)
            | Self::Float64(_)
            | Self::TimestampMilliseconds(_)
            | Self::TimestampMicroseconds(_) => 9,
            Self::Utf8(_) => u64::try_from(utf8_length)
                .unwrap_or(u64::MAX)
                .saturating_add(5),
            Self::Date32(_) => 5,
            Self::Decimal128(_, _, _) => 17,
            Self::Decimal256(_, _, _) => 33,
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Boolean(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::UInt64(builder) => builder.append_null(),
            Self::Float64(builder) => builder.append_null(),
            Self::Utf8(builder) => builder.append_null(),
            Self::Date32(builder) => builder.append_null(),
            Self::TimestampMilliseconds(builder) => builder.append_null(),
            Self::TimestampMicroseconds(builder) => builder.append_null(),
            Self::Decimal128(builder, _, _) => builder.append_null(),
            Self::Decimal256(builder, _, _) => builder.append_null(),
        }
    }

    fn append_value<R: BufRead>(
        &mut self,
        reader: &mut R,
        length: usize,
        field: &Field,
        value_scratch: &mut [u8],
    ) -> Result<()> {
        match self {
            Self::Boolean(builder) => {
                require_length(field, length, 1)?;
                let value = read_u8(reader, "boolean")?;
                match value {
                    0 => builder.append_value(false),
                    1 => builder.append_value(true),
                    _ => {
                        return Err(field_data_error(
                            field,
                            format!("invalid binary boolean value {value}"),
                        ));
                    }
                }
            }
            Self::Int64(builder) => {
                require_length(field, length, 8)?;
                builder.append_value(read_i64(reader, "int64")?);
            }
            Self::UInt64(builder) => builder.append_value(read_u64_text(reader, length, field)?),
            Self::Float64(builder) => {
                require_length(field, length, 8)?;
                let value = f64::from_bits(read_u64(reader, "float64")?);
                if !value.is_finite() {
                    return Err(field_data_error(field, "non-finite float64"));
                }
                builder.append_value(value);
            }
            Self::Utf8(builder) => append_utf8(reader, length, field, builder, value_scratch)?,
            Self::Date32(builder) => {
                require_length(field, length, 4)?;
                builder.append_value(read_i32(reader, "date32")?);
            }
            Self::TimestampMilliseconds(builder) => {
                require_length(field, length, 8)?;
                builder.append_value(read_i64(reader, "timestamp")?);
            }
            Self::TimestampMicroseconds(builder) => {
                require_length(field, length, 8)?;
                builder.append_value(read_i64(reader, "timestamp")?);
            }
            Self::Decimal128(builder, precision, scale) => {
                let value = read_numeric(reader, length, *precision, *scale, field, value_scratch)?;
                let value = value.to_i128().ok_or_else(|| {
                    field_data_error(field, "numeric value exceeds Arrow Decimal128")
                })?;
                builder.append_value(value);
            }
            Self::Decimal256(builder, precision, scale) => {
                builder.append_value(read_numeric(
                    reader,
                    length,
                    *precision,
                    *scale,
                    field,
                    value_scratch,
                )?);
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Boolean(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::UInt64(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::Date32(builder) => Arc::new(builder.finish()),
            Self::TimestampMilliseconds(builder) => Arc::new(builder.finish()),
            Self::TimestampMicroseconds(builder) => Arc::new(builder.finish()),
            Self::Decimal128(builder, _, _) => Arc::new(builder.finish()),
            Self::Decimal256(builder, _, _) => Arc::new(builder.finish()),
        }
    }
}

fn read_copy_header<R: BufRead>(reader: &mut R, maximum_batch_bytes: u64) -> Result<()> {
    let mut signature = [0_u8; COPY_SIGNATURE.len()];
    read_exact(reader, &mut signature, "signature")?;
    if &signature != COPY_SIGNATURE {
        return Err(CdfError::data("Postgres binary COPY signature is invalid"));
    }
    let flags = read_u32(reader, "flags")?;
    if flags != 0 {
        return Err(CdfError::data(format!(
            "Postgres binary COPY flags {flags:#010x} are unsupported"
        )));
    }
    let extension_length = read_i32(reader, "header extension length")?;
    let extension_length = usize::try_from(extension_length).map_err(|_| {
        CdfError::data(format!(
            "Postgres binary COPY header extension length {extension_length} is invalid"
        ))
    })?;
    if u64::try_from(extension_length).unwrap_or(u64::MAX) > maximum_batch_bytes {
        return Err(CdfError::data(format!(
            "Postgres binary COPY header extension has {extension_length} bytes above the admitted {maximum_batch_bytes}-byte source window"
        )));
    }
    discard_exact(reader, extension_length, "header extension")
}

fn ensure_copy_eof<R: BufRead>(reader: &mut R) -> Result<()> {
    let mut byte = [0_u8; 1];
    loop {
        match reader.read(&mut byte) {
            Ok(0) => return Ok(()),
            Ok(_) => {
                return Err(CdfError::data(
                    "Postgres binary COPY contains bytes after the stream trailer",
                ));
            }
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => {
                return Err(CdfError::data(format!(
                    "read Postgres binary COPY completion: {error}"
                )));
            }
        }
    }
}

fn append_utf8<R: BufRead>(
    reader: &mut R,
    length: usize,
    field: &Field,
    builder: &mut StringBuilder,
    scratch: &mut [u8],
) -> Result<()> {
    if length == 0 {
        builder.append_value("");
        return Ok(());
    }
    {
        let available = reader.fill_buf().map_err(|error| {
            CdfError::data(format!("read Postgres binary COPY UTF-8 field: {error}"))
        })?;
        if available.len() >= length {
            let value = std::str::from_utf8(&available[..length])
                .map_err(|_| field_data_error(field, "invalid UTF-8"))?;
            builder.append_value(value);
            reader.consume(length);
            return Ok(());
        }
    }
    debug_assert!(scratch.len() >= UTF8_SCRATCH_BYTES + 4);
    let mut remaining = length;
    let mut carry = 0_usize;
    while remaining > 0 {
        let read_length = remaining.min(UTF8_SCRATCH_BYTES);
        read_exact(
            reader,
            &mut scratch[carry..carry + read_length],
            "UTF-8 field",
        )?;
        remaining -= read_length;
        let available = carry + read_length;
        match std::str::from_utf8(&scratch[..available]) {
            Ok(value) => {
                builder
                    .write_str(value)
                    .map_err(|_| CdfError::internal("append Postgres UTF-8 to Arrow builder"))?;
                carry = 0;
            }
            Err(error) if error.error_len().is_none() => {
                let valid = error.valid_up_to();
                let prefix = std::str::from_utf8(&scratch[..valid]).map_err(|_| {
                    CdfError::internal("Postgres UTF-8 valid prefix failed revalidation")
                })?;
                builder
                    .write_str(prefix)
                    .map_err(|_| CdfError::internal("append Postgres UTF-8 to Arrow builder"))?;
                let trailing = available - valid;
                if trailing > 3 {
                    return Err(field_data_error(field, "invalid UTF-8"));
                }
                scratch.copy_within(valid..available, 0);
                carry = trailing;
            }
            Err(_) => return Err(field_data_error(field, "invalid UTF-8")),
        }
    }
    if carry != 0 {
        return Err(field_data_error(field, "truncated UTF-8 code point"));
    }
    builder.append_value("");
    Ok(())
}

fn read_u64_text<R: BufRead>(reader: &mut R, length: usize, field: &Field) -> Result<u64> {
    if length == 0 || length > 20 {
        return Err(field_data_error(
            field,
            format!("uint64 text length {length} is invalid"),
        ));
    }
    let mut bytes = [0_u8; 20];
    read_exact(reader, &mut bytes[..length], "uint64 text")?;
    let text = std::str::from_utf8(&bytes[..length])
        .map_err(|_| field_data_error(field, "uint64 text is not UTF-8"))?;
    text.parse::<u64>()
        .map_err(|error| field_data_error(field, format!("cannot parse uint64 text: {error}")))
}

fn read_numeric<R: BufRead>(
    reader: &mut R,
    length: usize,
    precision: u8,
    scale: i8,
    field: &Field,
    scratch: &mut [u8],
) -> Result<i256> {
    if length > MAX_NUMERIC_BINARY_BYTES {
        return Err(field_data_error(
            field,
            format!(
                "numeric binary length {length} exceeds the {MAX_NUMERIC_BINARY_BYTES}-byte Arrow Decimal256 domain"
            ),
        ));
    }
    debug_assert!(scratch.len() >= MAX_NUMERIC_BINARY_BYTES);
    {
        let available = reader.fill_buf().map_err(|error| {
            CdfError::data(format!("read Postgres binary COPY numeric field: {error}"))
        })?;
        if available.len() >= length {
            let value = decode_numeric(&available[..length], precision, scale, field)?;
            reader.consume(length);
            return Ok(value);
        }
    }
    read_exact(reader, &mut scratch[..length], "numeric field")?;
    decode_numeric(&scratch[..length], precision, scale, field)
}

fn decode_numeric(raw: &[u8], precision: u8, scale: i8, field: &Field) -> Result<i256> {
    let length = raw.len();
    if length < 8 || !(length - 8).is_multiple_of(2) {
        return Err(field_data_error(
            field,
            format!("numeric binary length {length} is invalid"),
        ));
    }
    let digits = i16::from_be_bytes(raw[0..2].try_into().expect("numeric header length checked"));
    let digits = usize::try_from(digits)
        .map_err(|_| field_data_error(field, "numeric digit count is negative"))?;
    let weight = i16::from_be_bytes(raw[2..4].try_into().expect("numeric header length checked"));
    let sign = u16::from_be_bytes(raw[4..6].try_into().expect("numeric header length checked"));
    let _display_scale =
        u16::from_be_bytes(raw[6..8].try_into().expect("numeric header length checked"));
    let expected_length =
        8_usize
            .checked_add(digits.checked_mul(2).ok_or_else(|| {
                field_data_error(field, "numeric digit byte length overflowed usize")
            })?)
            .ok_or_else(|| field_data_error(field, "numeric binary length overflowed usize"))?;
    if length != expected_length {
        return Err(field_data_error(
            field,
            format!("numeric binary length {length} does not match {digits} base-10000 digits"),
        ));
    }
    if matches!(
        sign,
        NUMERIC_NAN | NUMERIC_POSITIVE_INFINITY | NUMERIC_NEGATIVE_INFINITY
    ) {
        let special = match sign {
            NUMERIC_NAN => "NaN",
            NUMERIC_POSITIVE_INFINITY => "Infinity",
            NUMERIC_NEGATIVE_INFINITY => "-Infinity",
            _ => unreachable!("matched above"),
        };
        return Err(field_data_error(
            field,
            format!(
                "numeric special value {special} cannot be represented by Arrow Decimal; declare this field as Utf8"
            ),
        ));
    }
    if !matches!(sign, NUMERIC_POSITIVE | NUMERIC_NEGATIVE) {
        return Err(field_data_error(
            field,
            format!("numeric sign {sign:#06x} is invalid"),
        ));
    }

    for index in 0..digits {
        let digit = numeric_digit(raw, index);
        if digit >= 10_000 {
            return Err(field_data_error(
                field,
                format!("numeric base-10000 digit {digit} is invalid"),
            ));
        }
    }

    let exponent = i32::from(weight)
        .checked_sub(
            i32::try_from(digits)
                .map_err(|_| field_data_error(field, "numeric digit count exceeds i32"))?,
        )
        .and_then(|value| value.checked_add(1))
        .and_then(|value| value.checked_mul(4))
        .and_then(|value| value.checked_add(i32::from(scale)))
        .ok_or_else(|| field_data_error(field, "numeric decimal exponent overflowed i32"))?;
    let mut value = if exponent < 0 {
        accumulate_numeric_with_exact_scale_reduction(
            raw,
            digits,
            exponent.unsigned_abs(),
            scale,
            field,
        )?
    } else {
        let value = accumulate_numeric_digits(raw, digits, field)?;
        let multiplier = power_of_ten(exponent.unsigned_abs(), field)?;
        value
            .checked_mul(multiplier)
            .ok_or_else(|| field_data_error(field, "numeric value exceeds Decimal256"))?
    };
    if sign == NUMERIC_NEGATIVE {
        value = value
            .checked_neg()
            .ok_or_else(|| field_data_error(field, "numeric sign exceeds Decimal256"))?;
    }

    let limit = power_of_ten(u32::from(precision), field)?;
    let negative_limit = limit
        .checked_neg()
        .ok_or_else(|| field_data_error(field, "numeric precision limit overflowed"))?;
    if value >= limit || value <= negative_limit {
        return Err(field_data_error(
            field,
            format!("numeric value exceeds Arrow decimal precision {precision}"),
        ));
    }
    Ok(value)
}

fn numeric_digit(raw: &[u8], index: usize) -> u16 {
    let offset = 8 + index * 2;
    u16::from_be_bytes(
        raw[offset..offset + 2]
            .try_into()
            .expect("numeric digit range was length-checked"),
    )
}

fn accumulate_numeric_digits(raw: &[u8], digits: usize, field: &Field) -> Result<i256> {
    let mut value = i256::ZERO;
    for index in 0..digits {
        value = value
            .checked_mul(i256::from_i128(10_000))
            .and_then(|value| {
                value.checked_add(i256::from_i128(i128::from(numeric_digit(raw, index))))
            })
            .ok_or_else(|| field_data_error(field, "numeric value exceeds Decimal256"))?;
    }
    Ok(value)
}

fn accumulate_numeric_with_exact_scale_reduction(
    raw: &[u8],
    digits: usize,
    discarded_decimal_digits: u32,
    scale: i8,
    field: &Field,
) -> Result<i256> {
    let discarded_groups = usize::try_from(discarded_decimal_digits / 4)
        .map_err(|_| field_data_error(field, "numeric scale reduction exceeds usize"))?;
    let partial_digits = discarded_decimal_digits % 4;
    let retained_groups = digits.saturating_sub(discarded_groups);
    if discarded_groups > digits
        || (retained_groups..digits).any(|index| numeric_digit(raw, index) != 0)
    {
        return Err(nonzero_discarded_numeric(field, scale));
    }
    if retained_groups == 0 {
        return Ok(i256::ZERO);
    }
    if partial_digits == 0 {
        return accumulate_numeric_digits(raw, retained_groups, field);
    }

    let boundary_index = retained_groups - 1;
    let divisor = 10_u16.pow(partial_digits);
    let boundary = numeric_digit(raw, boundary_index);
    if !boundary.is_multiple_of(divisor) {
        return Err(nonzero_discarded_numeric(field, scale));
    }
    let mut value = accumulate_numeric_digits(raw, boundary_index, field)?;
    value = value
        .checked_mul(i256::from_i128(i128::from(10_u16.pow(4 - partial_digits))))
        .and_then(|value| value.checked_add(i256::from_i128(i128::from(boundary / divisor))))
        .ok_or_else(|| field_data_error(field, "numeric value exceeds Decimal256"))?;
    Ok(value)
}

fn nonzero_discarded_numeric(field: &Field, scale: i8) -> CdfError {
    field_data_error(
        field,
        format!(
            "numeric value has nonzero digits below Arrow scale {scale}; declare this field as Utf8"
        ),
    )
}

fn power_of_ten(exponent: u32, field: &Field) -> Result<i256> {
    i256::from_i128(10).checked_pow(exponent).ok_or_else(|| {
        field_data_error(
            field,
            format!("numeric decimal exponent {exponent} exceeds Decimal256"),
        )
    })
}

fn validate_decimal_type(precision: u8, scale: i8, maximum: u8, field: &Field) -> Result<()> {
    if precision == 0
        || precision > maximum
        || scale > 0 && scale.unsigned_abs() > precision
        || scale.unsigned_abs() > maximum
    {
        return Err(CdfError::contract(format!(
            "Postgres field `{}` has invalid Arrow decimal precision/scale ({precision},{scale})",
            field.name()
        )));
    }
    Ok(())
}

fn admit_bytes(admitted: &mut u64, bytes: u64, maximum: u64, field: &Field) -> Result<()> {
    let next = admitted
        .checked_add(bytes)
        .ok_or_else(|| field_data_error(field, "decoded batch byte count overflowed u64"))?;
    if next > maximum {
        return Err(field_data_error(
            field,
            format!(
                "decoded batch would exceed the admitted {maximum}-byte source window; reduce projected row width"
            ),
        ));
    }
    *admitted = next;
    Ok(())
}

fn require_length(field: &Field, actual: usize, expected: usize) -> Result<()> {
    if actual != expected {
        return Err(field_data_error(
            field,
            format!("binary length is {actual}, expected {expected}"),
        ));
    }
    Ok(())
}

fn field_data_error(field: &Field, detail: impl std::fmt::Display) -> CdfError {
    CdfError::data(format!(
        "Postgres binary COPY field `{}` ({:?}) {detail}",
        field.name(),
        field.data_type()
    ))
}

fn discard_exact<R: Read>(reader: &mut R, mut length: usize, label: &str) -> Result<()> {
    let mut scratch = [0_u8; 8 * 1024];
    while length > 0 {
        let chunk = length.min(scratch.len());
        read_exact(reader, &mut scratch[..chunk], label)?;
        length -= chunk;
    }
    Ok(())
}

fn read_exact<R: Read>(reader: &mut R, bytes: &mut [u8], label: &str) -> Result<()> {
    reader
        .read_exact(bytes)
        .map_err(|error| CdfError::data(format!("read Postgres binary COPY {label}: {error}")))
}

fn read_u8<R: Read>(reader: &mut R, label: &str) -> Result<u8> {
    let mut bytes = [0_u8; 1];
    read_exact(reader, &mut bytes, label)?;
    Ok(bytes[0])
}

fn read_i16<R: BufRead>(reader: &mut R, label: &str) -> Result<i16> {
    Ok(i16::from_be_bytes(read_array(reader, label)?))
}

fn read_i32<R: BufRead>(reader: &mut R, label: &str) -> Result<i32> {
    Ok(i32::from_be_bytes(read_array(reader, label)?))
}

fn read_u32<R: BufRead>(reader: &mut R, label: &str) -> Result<u32> {
    Ok(u32::from_be_bytes(read_array(reader, label)?))
}

fn read_i64<R: BufRead>(reader: &mut R, label: &str) -> Result<i64> {
    Ok(i64::from_be_bytes(read_array(reader, label)?))
}

fn read_u64<R: BufRead>(reader: &mut R, label: &str) -> Result<u64> {
    Ok(u64::from_be_bytes(read_array(reader, label)?))
}

fn read_array<R: BufRead, const N: usize>(reader: &mut R, label: &str) -> Result<[u8; N]> {
    let mut bytes = [0_u8; N];
    {
        let available = reader.fill_buf().map_err(|error| {
            CdfError::data(format!("read Postgres binary COPY {label}: {error}"))
        })?;
        if available.len() >= N {
            bytes.copy_from_slice(&available[..N]);
            reader.consume(N);
            return Ok(bytes);
        }
    }
    read_exact(reader, &mut bytes, label)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, Cursor, Read};

    use arrow_array::{Array, Decimal128Array, Decimal256Array, Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

    use super::*;

    #[test]
    fn fragmented_stream_decodes_directly_to_arrow() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(12, 4), false),
        ]));
        let mut bytes = copy_header();
        push_i16(&mut bytes, 3);
        push_field(&mut bytes, &42_i64.to_be_bytes());
        push_field(&mut bytes, "snowman ☃".as_bytes());
        push_field(
            &mut bytes,
            &numeric(&[1, 2345, 6700], 1, NUMERIC_POSITIVE, 4),
        );
        push_i16(&mut bytes, 3);
        push_field(&mut bytes, &(-7_i64).to_be_bytes());
        push_i32(&mut bytes, -1);
        push_field(
            &mut bytes,
            &numeric(&[9, 8765, 4300], 1, NUMERIC_NEGATIVE, 4),
        );
        push_i16(&mut bytes, -1);

        let reader = Fragmented {
            inner: Cursor::new(bytes),
            maximum: 3,
        };
        let mut decoder =
            PostgresBinaryCopyDecoder::new(reader, Arc::clone(&schema), 1024 * 1024).unwrap();
        let batch = decoder.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[42, -7]
        );
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(labels.value(0), "snowman ☃");
        assert!(labels.is_null(1));
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .values(),
            &[123_456_700, -987_654_300]
        );
        assert!(decoder.next_batch().unwrap().is_none());
    }

    #[test]
    fn malformed_framing_and_utf8_fail_closed() {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
        let mut invalid_signature = copy_header();
        invalid_signature[0] = b'X';
        assert!(
            PostgresBinaryCopyDecoder::new(Cursor::new(invalid_signature), Arc::clone(&schema), 64)
                .is_err()
        );

        let mut invalid_utf8 = copy_header();
        push_i16(&mut invalid_utf8, 1);
        push_field(&mut invalid_utf8, &[0xff]);
        push_i16(&mut invalid_utf8, -1);
        let mut decoder =
            PostgresBinaryCopyDecoder::new(Cursor::new(invalid_utf8), schema, 64).unwrap();
        assert!(decoder.next_batch().is_err());
    }

    #[test]
    fn numeric_rejects_special_values_and_discarded_digits() {
        let field = Field::new("amount", DataType::Decimal128(8, 2), false);
        let special = numeric(&[], 0, NUMERIC_NAN, 0);
        let error = decode_numeric(&special, 8, 2, &field).unwrap_err();
        assert!(error.to_string().contains("declare this field as Utf8"));

        let fractional = numeric(&[1, 2345], 0, NUMERIC_POSITIVE, 4);
        let error = decode_numeric(&fractional, 8, 2, &field).unwrap_err();
        assert!(error.to_string().contains("nonzero digits"));

        let overflow = numeric(&[1234, 5678, 9000], 2, NUMERIC_POSITIVE, 2);
        let error = decode_numeric(&overflow, 8, 2, &field).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("exceeds Arrow decimal precision 8")
        );
    }

    #[test]
    fn decimal256_accepts_twenty_base_10000_groups_at_precision_76() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wide",
            DataType::Decimal256(76, 1),
            false,
        )]));
        let mut digits = vec![123];
        digits.extend(std::iter::repeat_n(4567, 18));
        digits.push(3000);
        let value = numeric(&digits, 18, NUMERIC_POSITIVE, 1);
        assert_eq!(value.len(), MAX_NUMERIC_BINARY_BYTES);
        let mut bytes = copy_header();
        push_i16(&mut bytes, 1);
        push_field(&mut bytes, &value);
        push_i16(&mut bytes, -1);
        let mut decoder =
            PostgresBinaryCopyDecoder::new(Cursor::new(bytes), schema, 1024 * 1024).unwrap();
        let batch = decoder.next_batch().unwrap().unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap()
                .len(),
            1
        );
        assert!(decoder.next_batch().unwrap().is_none());
    }

    #[test]
    fn trailer_requires_true_eof_and_batches_are_bounded() {
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
        let mut bytes = copy_header();
        push_i16(&mut bytes, -1);
        bytes.push(0);
        let mut decoder =
            PostgresBinaryCopyDecoder::new(Cursor::new(bytes), Arc::clone(&schema), 64).unwrap();
        assert!(decoder.next_batch().is_err());

        let mut bytes = copy_header();
        push_i16(&mut bytes, 1);
        push_field(&mut bytes, &[b'x'; 64]);
        push_i16(&mut bytes, -1);
        let mut decoder = PostgresBinaryCopyDecoder::new(Cursor::new(bytes), schema, 32).unwrap();
        assert!(decoder.next_batch().is_err());
    }

    fn copy_header() -> Vec<u8> {
        let mut bytes = COPY_SIGNATURE.to_vec();
        bytes.extend_from_slice(&0_u32.to_be_bytes());
        bytes.extend_from_slice(&0_i32.to_be_bytes());
        bytes
    }

    fn push_field(output: &mut Vec<u8>, value: &[u8]) {
        push_i32(output, i32::try_from(value.len()).unwrap());
        output.extend_from_slice(value);
    }

    fn numeric(digits: &[u16], weight: i16, sign: u16, display_scale: u16) -> Vec<u8> {
        let mut bytes = Vec::new();
        push_i16(&mut bytes, i16::try_from(digits.len()).unwrap());
        push_i16(&mut bytes, weight);
        bytes.extend_from_slice(&sign.to_be_bytes());
        bytes.extend_from_slice(&display_scale.to_be_bytes());
        for digit in digits {
            bytes.extend_from_slice(&digit.to_be_bytes());
        }
        bytes
    }

    fn push_i16(output: &mut Vec<u8>, value: i16) {
        output.extend_from_slice(&value.to_be_bytes());
    }

    fn push_i32(output: &mut Vec<u8>, value: i32) {
        output.extend_from_slice(&value.to_be_bytes());
    }

    struct Fragmented<R> {
        inner: R,
        maximum: usize,
    }

    impl<R: Read> Read for Fragmented<R> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let length = buffer.len().min(self.maximum);
            self.inner.read(&mut buffer[..length])
        }
    }

    impl<R: BufRead> BufRead for Fragmented<R> {
        fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
            let available = self.inner.fill_buf()?;
            Ok(&available[..available.len().min(self.maximum)])
        }

        fn consume(&mut self, amount: usize) {
            self.inner.consume(amount);
        }
    }
}
