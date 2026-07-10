use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionMode};
use cdf_kernel::{Result, SchemaHash};
use sha2::{Digest, Sha256};

pub fn canonical_arrow_schema_hash(schema: &Schema) -> Result<SchemaHash> {
    let mut encoder = StructuralEncoder::new();
    encoder.tag(0x01);
    encoder.metadata(schema.metadata());
    encoder.len(schema.fields().len());
    for field in schema.fields() {
        encoder.field(field.as_ref());
    }
    SchemaHash::new(format!("sha256:{}", hex::encode(encoder.finish())))
}

struct StructuralEncoder {
    hasher: Sha256,
}

impl StructuralEncoder {
    fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }

    fn finish(self) -> impl AsRef<[u8]> {
        self.hasher.finalize()
    }

    fn tag(&mut self, value: u8) {
        self.hasher.update([value]);
    }

    fn len(&mut self, value: usize) {
        self.hasher.update((value as u64).to_be_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.len(value.len());
        self.hasher.update(value);
    }

    fn string(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    fn bool(&mut self, value: bool) {
        self.tag(u8::from(value));
    }

    fn i32(&mut self, value: i32) {
        self.hasher.update(value.to_be_bytes());
    }

    fn metadata(&mut self, metadata: &std::collections::HashMap<String, String>) {
        let mut entries = metadata.iter().collect::<Vec<_>>();
        entries.sort_by_key(|(key, _)| *key);
        self.len(entries.len());
        for (key, value) in entries {
            self.string(key);
            self.string(value);
        }
    }

    fn field(&mut self, field: &Field) {
        self.tag(0x02);
        self.string(field.name());
        self.bool(field.is_nullable());
        self.metadata(field.metadata());
        self.data_type(field.data_type());
    }

    fn data_type(&mut self, data_type: &DataType) {
        match data_type {
            DataType::Null => self.tag(0x10),
            DataType::Boolean => self.tag(0x11),
            DataType::Int8 => self.tag(0x12),
            DataType::Int16 => self.tag(0x13),
            DataType::Int32 => self.tag(0x14),
            DataType::Int64 => self.tag(0x15),
            DataType::UInt8 => self.tag(0x16),
            DataType::UInt16 => self.tag(0x17),
            DataType::UInt32 => self.tag(0x18),
            DataType::UInt64 => self.tag(0x19),
            DataType::Float16 => self.tag(0x1a),
            DataType::Float32 => self.tag(0x1b),
            DataType::Float64 => self.tag(0x1c),
            DataType::Timestamp(unit, timezone) => {
                self.tag(0x1d);
                self.time_unit(unit);
                match timezone {
                    Some(timezone) => {
                        self.bool(true);
                        self.string(timezone);
                    }
                    None => self.bool(false),
                }
            }
            DataType::Date32 => self.tag(0x1e),
            DataType::Date64 => self.tag(0x1f),
            DataType::Time32(unit) => {
                self.tag(0x20);
                self.time_unit(unit);
            }
            DataType::Time64(unit) => {
                self.tag(0x21);
                self.time_unit(unit);
            }
            DataType::Duration(unit) => {
                self.tag(0x22);
                self.time_unit(unit);
            }
            DataType::Interval(unit) => {
                self.tag(0x23);
                self.interval_unit(unit);
            }
            DataType::Binary => self.tag(0x24),
            DataType::FixedSizeBinary(width) => {
                self.tag(0x25);
                self.i32(*width);
            }
            DataType::LargeBinary => self.tag(0x26),
            DataType::BinaryView => self.tag(0x27),
            DataType::Utf8 => self.tag(0x28),
            DataType::LargeUtf8 => self.tag(0x29),
            DataType::Utf8View => self.tag(0x2a),
            DataType::List(field) => self.child_type(0x2b, field),
            DataType::ListView(field) => self.child_type(0x2c, field),
            DataType::FixedSizeList(field, length) => {
                self.tag(0x2d);
                self.i32(*length);
                self.field(field);
            }
            DataType::LargeList(field) => self.child_type(0x2e, field),
            DataType::LargeListView(field) => self.child_type(0x2f, field),
            DataType::Struct(fields) => {
                self.tag(0x30);
                self.len(fields.len());
                for field in fields {
                    self.field(field);
                }
            }
            DataType::Union(fields, mode) => {
                self.tag(0x31);
                self.union_mode(mode);
                self.len(fields.len());
                for (type_id, field) in fields.iter() {
                    self.tag(type_id as u8);
                    self.field(field);
                }
            }
            DataType::Dictionary(key, value) => {
                self.tag(0x32);
                self.data_type(key);
                self.data_type(value);
            }
            DataType::Decimal32(precision, scale) => self.decimal(0x33, *precision, *scale),
            DataType::Decimal64(precision, scale) => self.decimal(0x34, *precision, *scale),
            DataType::Decimal128(precision, scale) => self.decimal(0x35, *precision, *scale),
            DataType::Decimal256(precision, scale) => self.decimal(0x36, *precision, *scale),
            DataType::Map(field, sorted) => {
                self.tag(0x37);
                self.bool(*sorted);
                self.field(field);
            }
            DataType::RunEndEncoded(run_ends, values) => {
                self.tag(0x38);
                self.field(run_ends);
                self.field(values);
            }
        }
    }

    fn child_type(&mut self, tag: u8, field: &Field) {
        self.tag(tag);
        self.field(field);
    }

    fn decimal(&mut self, tag: u8, precision: u8, scale: i8) {
        self.tag(tag);
        self.tag(precision);
        self.tag(scale as u8);
    }

    fn time_unit(&mut self, unit: &TimeUnit) {
        self.tag(match unit {
            TimeUnit::Second => 0,
            TimeUnit::Millisecond => 1,
            TimeUnit::Microsecond => 2,
            TimeUnit::Nanosecond => 3,
        });
    }

    fn interval_unit(&mut self, unit: &IntervalUnit) {
        self.tag(match unit {
            IntervalUnit::YearMonth => 0,
            IntervalUnit::DayTime => 1,
            IntervalUnit::MonthDayNano => 2,
        });
    }

    fn union_mode(&mut self, mode: &UnionMode) {
        self.tag(match mode {
            UnionMode::Sparse => 0,
            UnionMode::Dense => 1,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_schema::{DataType, Field, Schema};

    use super::canonical_arrow_schema_hash;

    #[test]
    fn metadata_encoding_is_delimiter_safe_and_map_order_independent() {
        let left = Schema::new_with_metadata(
            vec![Field::new("value", DataType::Utf8, true)],
            HashMap::from([
                ("a".to_owned(), "b=c\nd".to_owned()),
                ("a=b".to_owned(), "c\nd".to_owned()),
            ]),
        );
        let right = Schema::new_with_metadata(
            vec![Field::new("value", DataType::Utf8, true)],
            HashMap::from([
                ("a=b".to_owned(), "c\nd".to_owned()),
                ("a".to_owned(), "b=c\nd".to_owned()),
            ]),
        );
        assert_eq!(
            canonical_arrow_schema_hash(&left).unwrap(),
            canonical_arrow_schema_hash(&right).unwrap()
        );

        let collision_attempt = Schema::new_with_metadata(
            vec![Field::new("value", DataType::Utf8, true)],
            HashMap::from([("a".to_owned(), "b\na=b=c\nd".to_owned())]),
        );
        assert_ne!(
            canonical_arrow_schema_hash(&left).unwrap(),
            canonical_arrow_schema_hash(&collision_attempt).unwrap()
        );
    }

    #[test]
    fn nested_child_identity_nullability_and_metadata_change_the_hash() {
        fn schema(child: Field) -> Schema {
            Schema::new(vec![Field::new(
                "items",
                DataType::List(Arc::new(child)),
                true,
            )])
        }

        let base = schema(
            Field::new("item", DataType::Int64, false)
                .with_metadata(HashMap::from([("owner".to_owned(), "source".to_owned())])),
        );
        let renamed = schema(
            Field::new("entry", DataType::Int64, false)
                .with_metadata(HashMap::from([("owner".to_owned(), "source".to_owned())])),
        );
        let nullable = schema(
            Field::new("item", DataType::Int64, true)
                .with_metadata(HashMap::from([("owner".to_owned(), "source".to_owned())])),
        );
        let metadata = schema(
            Field::new("item", DataType::Int64, false)
                .with_metadata(HashMap::from([("owner".to_owned(), "other".to_owned())])),
        );
        let base_hash = canonical_arrow_schema_hash(&base).unwrap();
        assert_ne!(base_hash, canonical_arrow_schema_hash(&renamed).unwrap());
        assert_ne!(base_hash, canonical_arrow_schema_hash(&nullable).unwrap());
        assert_ne!(base_hash, canonical_arrow_schema_hash(&metadata).unwrap());
    }
}
