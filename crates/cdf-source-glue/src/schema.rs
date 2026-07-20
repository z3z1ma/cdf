use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use cdf_kernel::{CdfError, Result};

use crate::model::GlueColumn;

pub fn glue_arrow_schema(columns: &[GlueColumn], partition_keys: &[GlueColumn]) -> Result<Schema> {
    let mut fields = Vec::with_capacity(columns.len() + partition_keys.len());
    for column in columns.iter().chain(partition_keys) {
        if fields
            .iter()
            .any(|field: &Arc<Field>| field.name() == &column.name)
        {
            return Err(CdfError::data(format!(
                "Glue schema repeats column `{}`",
                column.name
            )));
        }
        let mut metadata = std::collections::HashMap::from([
            ("cdf:source_name".to_owned(), column.name.clone()),
            ("cdf:glue_type".to_owned(), column.type_name.clone()),
        ]);
        if let Some(comment) = &column.comment {
            metadata.insert("cdf:glue_comment".to_owned(), comment.clone());
        }
        fields.push(Arc::new(
            Field::new(
                column.name.clone(),
                parse_glue_type(&column.type_name)?,
                true,
            )
            .with_metadata(metadata),
        ));
    }
    Ok(Schema::new(fields))
}

pub fn parse_glue_type(input: &str) -> Result<DataType> {
    let mut parser = TypeParser::new(input);
    let parsed = parser.parse_type()?;
    parser.skip_space();
    if !parser.remaining().is_empty() {
        return Err(parser.error("unexpected trailing type syntax"));
    }
    Ok(parsed)
}

struct TypeParser<'a> {
    input: &'a str,
    offset: usize,
}

impl<'a> TypeParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, offset: 0 }
    }

    fn remaining(&self) -> &'a str {
        &self.input[self.offset..]
    }

    fn skip_space(&mut self) {
        while self.remaining().starts_with(char::is_whitespace) {
            self.offset += self.remaining().chars().next().unwrap().len_utf8();
        }
    }

    fn parse_type(&mut self) -> Result<DataType> {
        self.skip_space();
        let name = self.parse_identifier()?.to_ascii_lowercase();
        match name.as_str() {
            "boolean" => Ok(DataType::Boolean),
            "tinyint" => Ok(DataType::Int8),
            "smallint" => Ok(DataType::Int16),
            "int" | "integer" => Ok(DataType::Int32),
            "bigint" => Ok(DataType::Int64),
            "float" => Ok(DataType::Float32),
            "double" => Ok(DataType::Float64),
            "string" | "varchar" => {
                if self
                    .consume_optional_parenthesized()?
                    .is_some_and(|length| length == 0)
                {
                    return Err(self.error("varchar length must be nonzero"));
                }
                Ok(DataType::Utf8)
            }
            "char" => {
                if self
                    .consume_optional_parenthesized()?
                    .is_some_and(|length| !(1..=255).contains(&length))
                {
                    return Err(self.error("char length must be between 1 and 255"));
                }
                Ok(DataType::Utf8)
            }
            "binary" => Ok(DataType::Binary),
            "date" => Ok(DataType::Date32),
            "timestamp" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
            "decimal" => self.parse_decimal(),
            "array" | "list" => {
                self.expect('<')?;
                let element = self.parse_type()?;
                self.expect('>')?;
                Ok(DataType::List(Arc::new(Field::new("item", element, true))))
            }
            "map" => {
                self.expect('<')?;
                let key = self.parse_type()?;
                self.expect(',')?;
                let value = self.parse_type()?;
                self.expect('>')?;
                let entries = DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("key", key, false)),
                    Arc::new(Field::new("value", value, true)),
                ]));
                Ok(DataType::Map(
                    Arc::new(Field::new("entries", entries, false)),
                    false,
                ))
            }
            "struct" => self.parse_struct(),
            other => Err(self.error(&format!("unsupported Glue type `{other}`"))),
        }
    }

    fn parse_decimal(&mut self) -> Result<DataType> {
        self.expect('(')?;
        let precision = self.parse_u8()?;
        self.expect(',')?;
        let scale = self.parse_i8()?;
        self.expect(')')?;
        if precision == 0 || precision > 76 || scale.unsigned_abs() > precision {
            return Err(self.error("decimal precision/scale are outside Arrow bounds"));
        }
        if precision <= 38 {
            Ok(DataType::Decimal128(precision, scale))
        } else {
            Ok(DataType::Decimal256(precision, scale))
        }
    }

    fn parse_struct(&mut self) -> Result<DataType> {
        self.expect('<')?;
        let mut fields = Vec::new();
        loop {
            self.skip_space();
            if self.consume('>') {
                break;
            }
            let name = self.parse_field_name()?;
            self.expect(':')?;
            let data_type = self.parse_type()?;
            fields.push(Arc::new(Field::new(name, data_type, true)));
            self.skip_space();
            if self.consume('>') {
                break;
            }
            self.expect(',')?;
        }
        if fields.is_empty() {
            return Err(self.error("Glue struct type requires at least one field"));
        }
        Ok(DataType::Struct(Fields::from(fields)))
    }

    fn parse_identifier(&mut self) -> Result<&'a str> {
        self.skip_space();
        let length = self
            .remaining()
            .bytes()
            .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
            .count();
        if length == 0 {
            return Err(self.error("expected type identifier"));
        }
        let start = self.offset;
        self.offset += length;
        Ok(&self.input[start..self.offset])
    }

    fn parse_field_name(&mut self) -> Result<String> {
        self.skip_space();
        if self.consume('`') {
            let mut value = String::new();
            loop {
                let Some(character) = self.remaining().chars().next() else {
                    return Err(self.error("unterminated quoted struct field"));
                };
                self.offset += character.len_utf8();
                if character != '`' {
                    value.push(character);
                    continue;
                }
                if self.consume('`') {
                    value.push('`');
                    continue;
                }
                break;
            }
            if value.is_empty() {
                return Err(self.error("struct field name cannot be empty"));
            }
            return Ok(value);
        }
        Ok(self.parse_identifier()?.to_owned())
    }

    fn parse_u8(&mut self) -> Result<u8> {
        self.skip_space();
        let length = self
            .remaining()
            .bytes()
            .take_while(u8::is_ascii_digit)
            .count();
        let value = self.remaining()[..length]
            .parse::<u8>()
            .map_err(|_| self.error("expected unsigned 8-bit integer"))?;
        self.offset += length;
        Ok(value)
    }

    fn parse_i8(&mut self) -> Result<i8> {
        self.skip_space();
        let length = self
            .remaining()
            .bytes()
            .take_while(|byte| byte.is_ascii_digit() || *byte == b'-')
            .count();
        let value = self.remaining()[..length]
            .parse::<i8>()
            .map_err(|_| self.error("expected signed 8-bit integer"))?;
        self.offset += length;
        Ok(value)
    }

    fn consume_optional_parenthesized(&mut self) -> Result<Option<u32>> {
        self.skip_space();
        if !self.consume('(') {
            return Ok(None);
        }
        let length = self.parse_u32()?;
        self.expect(')')?;
        Ok(Some(length))
    }

    fn parse_u32(&mut self) -> Result<u32> {
        self.skip_space();
        let length = self
            .remaining()
            .bytes()
            .take_while(u8::is_ascii_digit)
            .count();
        let value = self.remaining()[..length]
            .parse::<u32>()
            .map_err(|_| self.error("expected unsigned 32-bit integer"))?;
        self.offset += length;
        Ok(value)
    }

    fn expect(&mut self, expected: char) -> Result<()> {
        self.skip_space();
        if self.consume(expected) {
            Ok(())
        } else {
            Err(self.error(&format!("expected `{expected}`")))
        }
    }

    fn consume(&mut self, expected: char) -> bool {
        if self.remaining().starts_with(expected) {
            self.offset += expected.len_utf8();
            true
        } else {
            false
        }
    }

    fn error(&self, detail: &str) -> CdfError {
        CdfError::contract(format!(
            "invalid Glue type {:?} at byte {}: {detail}",
            self.input, self.offset
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glue_type_parser_covers_nested_and_wide_decimal_types() {
        assert_eq!(parse_glue_type("bigint").unwrap(), DataType::Int64);
        assert_eq!(
            parse_glue_type("decimal(40, 9)").unwrap(),
            DataType::Decimal256(40, 9)
        );
        assert!(matches!(
            parse_glue_type("array<struct<id:bigint,label:string>>").unwrap(),
            DataType::List(_)
        ));
        assert!(matches!(
            parse_glue_type("map<string,array<int>>").unwrap(),
            DataType::Map(_, false)
        ));
        assert_eq!(parse_glue_type("varchar(65535)").unwrap(), DataType::Utf8);
        assert_eq!(
            parse_glue_type("struct<`tick``name`:int>").unwrap(),
            DataType::Struct(Fields::from(vec![Arc::new(Field::new(
                "tick`name",
                DataType::Int32,
                true,
            ))]))
        );
        assert!(parse_glue_type("char(256)").is_err());
    }
}
