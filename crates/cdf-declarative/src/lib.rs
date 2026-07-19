#![doc = "Declarative resource authoring boundary for cdf."]

mod compiled;
mod declarations;
#[cfg(test)]
mod tests;

pub use cdf_kernel::parse_arrow_field_type;
pub use compiled::{
    CompiledResource, compile_document, compile_document_with_project_root,
    compile_execution_extent, physical_arrow_schema_hash, validate_document,
};
pub use declarations::*;
