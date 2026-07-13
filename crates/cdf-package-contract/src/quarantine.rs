use cdf_kernel::SourcePosition;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuarantineRecord {
    pub source_row_ordinal: u64,
    pub rule_id: String,
    pub error_code: String,
    pub source_position: Option<SourcePosition>,
    pub observed_value_redacted: QuarantineObservedValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuarantineObservedValue {
    Null,
    Preserved { value: String },
    Hashed { algorithm: String, value: String },
    Omitted,
    Masked { value: String },
}
