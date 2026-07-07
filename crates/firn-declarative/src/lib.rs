#![doc = "Declarative resource authoring boundary for firn."]

mod compiled;
mod declarations;
mod file_runtime;
#[cfg(test)]
mod tests;

pub use compiled::{
    CompiledResource, CompiledResourcePlan, FileResourcePlan, RestResourcePlan, SqlResourcePlan,
    compile_document, compile_document_with_project_root, validate_document,
};
pub use declarations::*;
