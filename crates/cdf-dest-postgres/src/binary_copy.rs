use std::io::{BufWriter, Write};

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, TimeUnit};

use crate::{commit::io_error, *};

const HEADER: &[u8] = b"PGCOPY\n\xFF\r\n\0";
pub(crate) const BINARY_COPY_BUFFER_BYTES: usize = 1024 * 1024;
const POSTGRES_EPOCH_DAYS: i32 = 10_957;
const POSTGRES_EPOCH_MICROS: i64 = 946_684_800_000_000;

enum BinaryColumn<'a> {
    Boolean(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Decimal128(&'a Decimal128Array),
    Decimal256(&'a Decimal256Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
    Date32(&'a Date32Array),
    Time64Microsecond(&'a Time64MicrosecondArray),
    TimestampMicrosecond(&'a TimestampMicrosecondArray),
}

impl<'a> BinaryColumn<'a> {
    fn new(array: &'a dyn Array, data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
            DataType::Boolean => Self::Boolean(typed(array, data_type)?),
            DataType::Int8 => Self::Int8(typed(array, data_type)?),
            DataType::Int16 => Self::Int16(typed(array, data_type)?),
            DataType::Int32 => Self::Int32(typed(array, data_type)?),
            DataType::Int64 => Self::Int64(typed(array, data_type)?),
            DataType::UInt8 => Self::UInt8(typed(array, data_type)?),
            DataType::UInt16 => Self::UInt16(typed(array, data_type)?),
            DataType::UInt32 => Self::UInt32(typed(array, data_type)?),
            DataType::UInt64 => Self::UInt64(typed(array, data_type)?),
            DataType::Decimal128(_, _) => Self::Decimal128(typed(array, data_type)?),
            DataType::Decimal256(_, _) => Self::Decimal256(typed(array, data_type)?),
            DataType::Float32 => Self::Float32(typed(array, data_type)?),
            DataType::Float64 => Self::Float64(typed(array, data_type)?),
            DataType::Utf8 => Self::Utf8(typed(array, data_type)?),
            DataType::LargeUtf8 => Self::LargeUtf8(typed(array, data_type)?),
            DataType::Binary => Self::Binary(typed(array, data_type)?),
            DataType::LargeBinary => Self::LargeBinary(typed(array, data_type)?),
            DataType::Date32 => Self::Date32(typed(array, data_type)?),
            DataType::Time64(TimeUnit::Microsecond) => {
                Self::Time64Microsecond(typed(array, data_type)?)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Self::TimestampMicrosecond(typed(array, data_type)?)
            }
            other => {
                return Err(CdfError::contract(format!(
                    "Postgres binary COPY does not support Arrow type {other:?}"
                )));
            }
        })
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Boolean(array) => array.is_null(row),
            Self::Int8(array) => array.is_null(row),
            Self::Int16(array) => array.is_null(row),
            Self::Int32(array) => array.is_null(row),
            Self::Int64(array) => array.is_null(row),
            Self::UInt8(array) => array.is_null(row),
            Self::UInt16(array) => array.is_null(row),
            Self::UInt32(array) => array.is_null(row),
            Self::UInt64(array) => array.is_null(row),
            Self::Decimal128(array) => array.is_null(row),
            Self::Decimal256(array) => array.is_null(row),
            Self::Float32(array) => array.is_null(row),
            Self::Float64(array) => array.is_null(row),
            Self::Utf8(array) => array.is_null(row),
            Self::LargeUtf8(array) => array.is_null(row),
            Self::Binary(array) => array.is_null(row),
            Self::LargeBinary(array) => array.is_null(row),
            Self::Date32(array) => array.is_null(row),
            Self::Time64Microsecond(array) => array.is_null(row),
            Self::TimestampMicrosecond(array) => array.is_null(row),
        }
    }
}

pub(crate) struct BinaryCopyEncoder<W: Write> {
    writer: BufWriter<W>,
    field_count: i16,
    scratch: Vec<u8>,
    rows: u64,
}

impl<W: Write> BinaryCopyEncoder<W> {
    pub(crate) fn new(writer: W, user_fields: usize) -> Result<Self> {
        let field_count = i16::try_from(user_fields.saturating_add(2))
            .map_err(|_| CdfError::contract("Postgres binary COPY field count exceeds i16"))?;
        let mut writer = BufWriter::with_capacity(BINARY_COPY_BUFFER_BYTES, writer);
        writer
            .write_all(HEADER)
            .and_then(|_| writer.write_all(&0_i32.to_be_bytes()))
            .and_then(|_| writer.write_all(&0_i32.to_be_bytes()))
            .map_err(|error| io_error("write Postgres binary COPY header", error))?;
        Ok(Self {
            writer,
            field_count,
            scratch: Vec::with_capacity(128),
            rows: 0,
        })
    }

    pub(crate) fn write_batch(
        &mut self,
        batch: &RecordBatch,
        package_row_key_start: i64,
        loaded_at_ms: i64,
    ) -> Result<()> {
        let package_row_ord = cdf_package_contract::package_row_ord_array(batch)?.clone();
        let logical_batch = cdf_package_contract::strip_package_row_ord(batch.clone())?;
        let columns = logical_batch
            .columns()
            .iter()
            .zip(logical_batch.schema().fields())
            .map(|(array, field)| BinaryColumn::new(array.as_ref(), field.data_type()))
            .collect::<Result<Vec<_>>>()?;
        for row in 0..logical_batch.num_rows() {
            self.writer
                .write_all(&self.field_count.to_be_bytes())
                .map_err(|error| io_error("write Postgres binary COPY row header", error))?;
            for column in &columns {
                self.write_arrow_field(column, row)?;
            }
            let row_key =
                package_row_key_start
                    .checked_add(i64::try_from(package_row_ord.value(row)).map_err(|_| {
                        CdfError::data("Postgres package row ordinal exceeds BIGINT")
                    })?)
                    .ok_or_else(|| CdfError::data("Postgres row key overflowed BIGINT"))?;
            self.write_bytes(Some(&row_key.to_be_bytes()))?;
            self.write_bytes(Some(&loaded_at_ms.to_be_bytes()))?;
            self.rows = self
                .rows
                .checked_add(1)
                .ok_or_else(|| CdfError::data("Postgres binary COPY row count overflowed"))?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<(W, u64)> {
        self.writer
            .write_all(&(-1_i16).to_be_bytes())
            .map_err(|error| io_error("write Postgres binary COPY trailer", error))?;
        let writer = self.writer.into_inner().map_err(|error| {
            io_error(
                "flush Postgres binary COPY aggregate buffer",
                error.into_error(),
            )
        })?;
        Ok((writer, self.rows))
    }

    fn write_arrow_field(&mut self, column: &BinaryColumn<'_>, row: usize) -> Result<()> {
        if column.is_null(row) {
            return self.write_bytes(None);
        }
        self.scratch.clear();
        match column {
            BinaryColumn::Boolean(array) => self.scratch.push(u8::from(array.value(row))),
            BinaryColumn::Int8(array) => self
                .scratch
                .extend_from_slice(&i16::from(array.value(row)).to_be_bytes()),
            BinaryColumn::Int16(array) => self
                .scratch
                .extend_from_slice(&array.value(row).to_be_bytes()),
            BinaryColumn::Int32(array) => self
                .scratch
                .extend_from_slice(&array.value(row).to_be_bytes()),
            BinaryColumn::Int64(array) => self
                .scratch
                .extend_from_slice(&array.value(row).to_be_bytes()),
            BinaryColumn::UInt8(array) => self
                .scratch
                .extend_from_slice(&i16::from(array.value(row)).to_be_bytes()),
            BinaryColumn::UInt16(array) => self
                .scratch
                .extend_from_slice(&i32::from(array.value(row)).to_be_bytes()),
            BinaryColumn::UInt32(array) => self
                .scratch
                .extend_from_slice(&i64::from(array.value(row)).to_be_bytes()),
            BinaryColumn::UInt64(array) => {
                encode_numeric_text(&array.value(row).to_string(), &mut self.scratch)?;
            }
            BinaryColumn::Decimal128(array) => {
                encode_numeric_text(&array.value_as_string(row), &mut self.scratch)?;
            }
            BinaryColumn::Decimal256(array) => {
                encode_numeric_text(&array.value_as_string(row), &mut self.scratch)?;
            }
            BinaryColumn::Float32(array) => self
                .scratch
                .extend_from_slice(&array.value(row).to_bits().to_be_bytes()),
            BinaryColumn::Float64(array) => self
                .scratch
                .extend_from_slice(&array.value(row).to_bits().to_be_bytes()),
            BinaryColumn::Utf8(array) => {
                self.scratch.extend_from_slice(array.value(row).as_bytes())
            }
            BinaryColumn::LargeUtf8(array) => {
                self.scratch.extend_from_slice(array.value(row).as_bytes());
            }
            BinaryColumn::Binary(array) => self.scratch.extend_from_slice(array.value(row)),
            BinaryColumn::LargeBinary(array) => self.scratch.extend_from_slice(array.value(row)),
            BinaryColumn::Date32(array) => {
                let days = array
                    .value(row)
                    .checked_sub(POSTGRES_EPOCH_DAYS)
                    .ok_or_else(|| CdfError::data("Postgres DATE epoch conversion overflowed"))?;
                self.scratch.extend_from_slice(&days.to_be_bytes());
            }
            BinaryColumn::Time64Microsecond(array) => self
                .scratch
                .extend_from_slice(&array.value(row).to_be_bytes()),
            BinaryColumn::TimestampMicrosecond(array) => {
                let micros = array
                    .value(row)
                    .checked_sub(POSTGRES_EPOCH_MICROS)
                    .ok_or_else(|| {
                        CdfError::data("Postgres timestamp epoch conversion overflowed")
                    })?;
                self.scratch.extend_from_slice(&micros.to_be_bytes());
            }
        }
        let bytes = std::mem::take(&mut self.scratch);
        let result = self.write_bytes(Some(&bytes));
        self.scratch = bytes;
        result
    }

    fn write_bytes(&mut self, value: Option<&[u8]>) -> Result<()> {
        match value {
            None => self
                .writer
                .write_all(&(-1_i32).to_be_bytes())
                .map_err(|error| io_error("write Postgres binary COPY null", error)),
            Some(bytes) => {
                let length = i32::try_from(bytes.len())
                    .map_err(|_| CdfError::data("Postgres binary COPY field exceeds i32 bytes"))?;
                self.writer
                    .write_all(&length.to_be_bytes())
                    .and_then(|_| self.writer.write_all(bytes))
                    .map_err(|error| io_error("write Postgres binary COPY field", error))
            }
        }
    }
}

fn typed<'a, T: 'static>(array: &'a dyn Array, data_type: &DataType) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        CdfError::internal(format!(
            "Arrow array for {data_type:?} had unexpected concrete type"
        ))
    })
}

fn encode_numeric_text(value: &str, output: &mut Vec<u8>) -> Result<()> {
    let (negative, value) = value
        .strip_prefix('-')
        .map_or((false, value), |v| (true, v));
    let (integer, fraction) = value.split_once('.').unwrap_or((value, ""));
    if !integer
        .bytes()
        .chain(fraction.bytes())
        .all(|b| b.is_ascii_digit())
    {
        return Err(CdfError::data("Arrow decimal produced non-numeric text"));
    }
    let dscale = u16::try_from(fraction.len())
        .map_err(|_| CdfError::data("Postgres NUMERIC scale exceeds u16"))?;
    let integer = integer.trim_start_matches('0');
    let integer_groups = integer.len().div_ceil(4);
    let mut digits = String::new();
    digits.extend(std::iter::repeat_n('0', integer_groups * 4 - integer.len()));
    digits.push_str(integer);
    digits.push_str(fraction);
    digits.extend(std::iter::repeat_n(
        '0',
        fraction.len().next_multiple_of(4) - fraction.len(),
    ));
    let mut groups = digits
        .as_bytes()
        .chunks_exact(4)
        .map(|chunk| {
            std::str::from_utf8(chunk)
                .map_err(|_| CdfError::internal("numeric group is not UTF-8"))?
                .parse::<u16>()
                .map_err(|_| CdfError::data("numeric group is invalid"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut weight = i16::try_from(integer_groups)
        .map_err(|_| CdfError::data("Postgres NUMERIC weight exceeds i16"))?
        - 1;
    while groups.first() == Some(&0) {
        groups.remove(0);
        weight = weight.saturating_sub(1);
    }
    while groups.last() == Some(&0) {
        groups.pop();
    }
    if groups.is_empty() {
        weight = 0;
    }
    let count = i16::try_from(groups.len())
        .map_err(|_| CdfError::data("Postgres NUMERIC digit count exceeds i16"))?;
    output.extend_from_slice(&count.to_be_bytes());
    output.extend_from_slice(&weight.to_be_bytes());
    output.extend_from_slice(
        &(if negative && !groups.is_empty() {
            0x4000_u16
        } else {
            0
        })
        .to_be_bytes(),
    );
    output.extend_from_slice(&dscale.to_be_bytes());
    for group in groups {
        output.extend_from_slice(&group.to_be_bytes());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Instant};

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

    use super::*;

    #[test]
    fn numeric_binary_uses_base_10000_weight_sign_and_scale() {
        let mut encoded = Vec::new();
        encode_numeric_text("-12345.6700", &mut encoded).unwrap();
        assert_eq!(
            encoded,
            vec![
                0, 3, // ndigits
                0, 1, // weight
                0x40, 0, // negative
                0, 4, // dscale
                0, 1, // 1
                0x09, 0x29, // 2345
                0x1a, 0x2c, // 6700
            ]
        );
    }

    #[test]
    fn binary_copy_derives_row_key_from_canonical_package_ordinal() {
        let logical = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![42_i64]))],
        )
        .unwrap();
        let canonical = cdf_package_contract::append_package_row_ord(vec![logical], 7)
            .unwrap()
            .pop()
            .unwrap();
        let mut encoder = BinaryCopyEncoder::new(Vec::new(), 1).unwrap();
        encoder
            .write_batch(&canonical, 100, 1_700_000_000_000)
            .unwrap();
        let (bytes, rows) = encoder.finish().unwrap();

        assert_eq!(rows, 1);
        let mut offset = HEADER.len() + 8;
        assert_eq!(
            i16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap()),
            3
        );
        offset += 2;
        for expected in [42_i64, 107, 1_700_000_000_000] {
            assert_eq!(
                i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()),
                8
            );
            offset += 4;
            assert_eq!(
                i64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap()),
                expected
            );
            offset += 8;
        }
    }

    #[test]
    #[ignore = "release-mode D3 binary-vs-CSV encoder benchmark"]
    fn binary_copy_encoder_is_at_least_twice_csv() {
        const ROWS: usize = 262_144;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("amount", DataType::Float64, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(0..ROWS as i64)),
                Arc::new(StringArray::from_iter(
                    (0..ROWS).map(|row| (row % 11 != 0).then_some("yellow-taxi")),
                )),
                Arc::new(Float64Array::from_iter_values(
                    (0..ROWS).map(|row| row as f64 / 100.0),
                )),
            ],
        )
        .unwrap();
        let batch = cdf_package_contract::append_package_row_ord(vec![batch], 0)
            .unwrap()
            .pop()
            .unwrap();
        let started = Instant::now();
        let mut binary = BinaryCopyEncoder::new(Vec::new(), 3).unwrap();
        binary.write_batch(&batch, 1, 1_700_000_000_000).unwrap();
        let (binary_bytes, rows) = binary.finish().unwrap();
        let binary_elapsed = started.elapsed();
        assert_eq!(rows, ROWS as u64);

        let logical_batch = cdf_package_contract::strip_package_row_ord(batch.clone()).unwrap();
        let ids = logical_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = logical_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amounts = logical_batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let started = Instant::now();
        let mut csv = Vec::new();
        for row in 0..ROWS {
            let name = if names.is_null(row) {
                "\\N"
            } else {
                names.value(row)
            };
            writeln!(
                csv,
                "{},{},{},sha256:package,seg-000001,{row},1700000000000",
                ids.value(row),
                name,
                amounts.value(row)
            )
            .unwrap();
        }
        let csv_elapsed = started.elapsed();
        let speedup = csv_elapsed.as_secs_f64() / binary_elapsed.as_secs_f64();
        eprintln!(
            "postgres_copy_encoder binary_rows_per_second={:.0} csv_rows_per_second={:.0} speedup={speedup:.2}x binary_bytes={} csv_bytes={}",
            ROWS as f64 / binary_elapsed.as_secs_f64(),
            ROWS as f64 / csv_elapsed.as_secs_f64(),
            binary_bytes.len(),
            csv.len(),
        );
        assert!(speedup >= 2.0, "binary encoder speedup was {speedup:.2}x");
    }
}
