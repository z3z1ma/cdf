#![doc = "Declarative resource authoring boundary for cdf."]

mod compiled;
mod declarations;
#[cfg(test)]
mod tests;

pub use compiled::{
    CompiledResource, compile_document, compile_document_with_project_root, parse_arrow_field_type,
    physical_arrow_schema_hash, validate_document,
};
pub use declarations::*;
