use arrow_schema::Schema;
use cdf_contract::{ContractPolicy, ObservedSchema, ValidationProgram, compile_validation_program};
use cdf_kernel::{Result, SchemaHash};

pub const SCHEMA_HASH_PREFIX: &str = "sha256:";

pub fn compile_observed_schema(
    policy: &ContractPolicy,
    observed_schema: &ObservedSchema,
) -> Result<ValidationProgram> {
    compile_validation_program(policy, observed_schema)
}

pub fn schema_hash(schema: &Schema) -> Result<SchemaHash> {
    cdf_kernel::canonical_arrow_schema_hash(schema)
}
