#![doc = "Declarative resource authoring boundary for firn."]

mod compiled;
mod declarations;
#[cfg(test)]
mod tests;

pub use compiled::{
    CompiledResource, CompiledResourcePlan, FileResourcePlan, RestResourcePlan, SqlResourcePlan,
    compile_document, validate_document,
};
pub use declarations::*;
