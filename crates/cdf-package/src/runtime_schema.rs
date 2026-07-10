use std::io::Cursor;

use arrow_ipc::{reader::FileReader, writer::FileWriter};
use arrow_schema::{Schema, SchemaRef};
use cdf_kernel::{CdfError, Result};

pub const RUNTIME_ARROW_SCHEMA_FILE: &str = "schema/runtime-arrow-schema.arrow";

pub(crate) fn runtime_schema_bytes(schema: &Schema) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut bytes, schema).map_err(CdfError::from)?;
        writer.finish().map_err(CdfError::from)?;
    }
    Ok(bytes)
}

pub(crate) fn runtime_schema_from_bytes(bytes: Vec<u8>) -> Result<SchemaRef> {
    let reader = FileReader::try_new(Cursor::new(bytes), None).map_err(CdfError::from)?;
    Ok(reader.schema())
}
