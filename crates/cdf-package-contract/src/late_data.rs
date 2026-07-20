use cdf_kernel::{LateDataAction, PartitionId, SourcePosition, WatermarkClaim, WatermarkValue};
use serde::{Deserialize, Serialize};

pub const LATE_DATA_EVIDENCE_VERSION: u16 = 1;
pub const LATE_DATA_EVIDENCE_FILE: &str = "stats/late-data.json";

/// Identity-bearing evidence for one row observed behind the effective global watermark.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LateDataRecord {
    pub source_row_ordinal: u64,
    pub partition_id: PartitionId,
    pub source_position: Option<SourcePosition>,
    pub event_time: WatermarkValue,
    pub effective_watermark: WatermarkClaim,
    pub action: LateDataAction,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LateDataEvidence {
    pub version: u16,
    pub records: Vec<LateDataRecord>,
}

impl LateDataEvidence {
    pub fn new(records: Vec<LateDataRecord>) -> Self {
        Self {
            version: LATE_DATA_EVIDENCE_VERSION,
            records,
        }
    }
}
