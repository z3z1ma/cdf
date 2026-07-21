use std::io::Read;

use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{Schema, SchemaRef};
use cdf_kernel::{CdfError, Result};

pub const RUNTIME_ARROW_SCHEMA_FILE: &str = "schema/runtime-arrow-schema.arrow";

pub(crate) fn runtime_schema_bytes(schema: &Schema) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, schema).map_err(CdfError::from)?;
        writer.finish().map_err(CdfError::from)?;
    }
    Ok(bytes)
}

pub(crate) fn runtime_schema_from_reader(reader: impl Read) -> Result<SchemaRef> {
    let mut reader = StreamReader::try_new_buffered(reader, None).map_err(CdfError::from)?;
    let schema = reader.schema();
    if reader.next().transpose().map_err(CdfError::from)?.is_some() {
        return Err(CdfError::data(
            "runtime Arrow schema artifact must not contain record batches",
        ));
    }
    Ok(schema)
}
