use arrow_schema::Schema;
use cdf_contract::{ContractPolicy, ObservedSchema, ValidationProgram, compile_validation_program};
use cdf_kernel::{Result, SchemaHash};
use sha2::{Digest, Sha256};

pub const SCHEMA_HASH_PREFIX: &str = "sha256:";

pub fn compile_observed_schema(
    policy: &ContractPolicy,
    observed_schema: &ObservedSchema,
) -> Result<ValidationProgram> {
    compile_validation_program(policy, observed_schema)
}

pub fn schema_hash(schema: &Schema) -> Result<SchemaHash> {
    let mut hasher = Sha256::new();
    hash_schema(&mut hasher, schema);
    SchemaHash::new(format!(
        "{SCHEMA_HASH_PREFIX}{}",
        hex::encode(hasher.finalize())
    ))
}

fn hash_schema(hasher: &mut Sha256, schema: &Schema) {
    hasher.update(b"schema");
    for field in schema.fields() {
        hash_field(hasher, field.as_ref());
    }
    hash_metadata(hasher, schema.metadata());
}

fn hash_field(hasher: &mut Sha256, field: &arrow_schema::Field) {
    hasher.update(b"field");
    hasher.update(field.name().as_bytes());
    hasher.update(field.data_type().to_string().as_bytes());
    hasher.update([u8::from(field.is_nullable())]);
    hash_metadata(hasher, field.metadata());
}

fn hash_metadata(hasher: &mut Sha256, metadata: &std::collections::HashMap<String, String>) {
    let mut entries = metadata.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| key.as_str());
    for (key, value) in entries {
        hasher.update(key.as_bytes());
        hasher.update(b"=");
        hasher.update(value.as_bytes());
        hasher.update(b"\n");
    }
}
