use serde::{Deserialize, Serialize};

use crate::ids::{ContractRef, SchemaHash, ValidationProgramHash};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationProgramRef {
    pub contract: ContractRef,
    pub program_hash: ValidationProgramHash,
    pub schema_hash: SchemaHash,
    pub policy: ContractPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractPolicy {
    Evolve,
    Freeze,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowDisposition {
    Accept,
    Quarantine,
    Reject,
    Fail,
}
