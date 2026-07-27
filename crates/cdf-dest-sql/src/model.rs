use std::collections::{BTreeMap, BTreeSet};

use cdf_kernel::{
    CdfError, CheckpointId, IdempotencyToken, PackageHash, PipelineId, Receipt, ReceiptId,
    ResourceId, Result, SchemaHash, ScopeKey, SegmentId, SourcePosition, StateDelta, TargetName,
};
use cdf_package_contract::{QuarantineObservedValue, QuarantineRecord};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MirrorReadIntent {
    LoadForPackage,
    StateForScope,
    LoadsForTarget,
    StateHeads,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LoadMirrorKey {
    pub target: TargetName,
    pub package_hash: PackageHash,
    pub idempotency_token: IdempotencyToken,
}

impl LoadMirrorKey {
    pub fn from_receipt(receipt: &Receipt) -> Self {
        Self {
            target: receipt.target.clone(),
            package_hash: receipt.package_hash.clone(),
            idempotency_token: receipt.idempotency_token.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadMirrorMutation {
    pub receipt: Receipt,
    pub resource_id: Option<ResourceId>,
    pub duplicate: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadMirrorRow {
    pub receipt: Receipt,
}

impl From<&LoadMirrorMutation> for LoadMirrorRow {
    fn from(mutation: &LoadMirrorMutation) -> Self {
        Self {
            receipt: mutation.receipt.clone(),
        }
    }
}

impl LoadMirrorMutation {
    pub fn key(&self) -> LoadMirrorKey {
        LoadMirrorKey::from_receipt(&self.receipt)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MirrorInsertOutcome<T> {
    Inserted(T),
    Conflict,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateMirrorKey {
    pub pipeline_id: PipelineId,
    pub resource_id: ResourceId,
    pub scope: ScopeKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateMirrorMutation {
    pub key: StateMirrorKey,
    pub state_version: u16,
    pub checkpoint_id: CheckpointId,
    pub parent_checkpoint_id: Option<CheckpointId>,
    pub package_hash: PackageHash,
    pub schema_hash: SchemaHash,
    pub output_position: SourcePosition,
    pub receipt_id: ReceiptId,
    pub committed_at_ms: i64,
}

impl StateMirrorMutation {
    pub fn from_delta(delta: &StateDelta, receipt: &Receipt) -> Result<Self> {
        if delta.package_hash != receipt.package_hash || delta.schema_hash != receipt.schema_hash {
            return Err(CdfError::data(
                "SQL mirror state delta package/schema identity differs from receipt",
            ));
        }
        if !receipt.covers_state_delta(delta) {
            return Err(CdfError::data(
                "SQL mirror state delta contains a segment absent from the receipt",
            ));
        }
        Ok(Self {
            key: StateMirrorKey {
                pipeline_id: delta.pipeline_id.clone(),
                resource_id: delta.resource_id.clone(),
                scope: delta.scope.clone(),
            },
            state_version: delta.state_version,
            checkpoint_id: delta.checkpoint_id.clone(),
            parent_checkpoint_id: delta.parent_checkpoint_id.clone(),
            package_hash: receipt.package_hash.clone(),
            schema_hash: receipt.schema_hash.clone(),
            output_position: delta.output_position.clone(),
            receipt_id: receipt.receipt_id.clone(),
            committed_at_ms: receipt.committed_at_ms,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateMirrorRow {
    pub mutation: StateMirrorMutation,
}

impl From<&StateMirrorMutation> for StateMirrorRow {
    fn from(mutation: &StateMirrorMutation) -> Self {
        Self {
            mutation: mutation.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentRowRange {
    pub segment_id: SegmentId,
    pub row_key_start: u64,
    pub row_key_end: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentMirrorMutation {
    pub target: TargetName,
    pub package_hash: PackageHash,
    pub idempotency_token: IdempotencyToken,
    pub segment_id: SegmentId,
    pub scope: Option<ScopeKey>,
    pub output_position: Option<SourcePosition>,
    pub row_count: u64,
    pub byte_count: u64,
    pub committed_at_ms: i64,
    pub row_range: Option<SegmentRowRange>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentMirrorRow {
    pub mutation: SegmentMirrorMutation,
}

impl From<&SegmentMirrorMutation> for SegmentMirrorRow {
    fn from(mutation: &SegmentMirrorMutation) -> Self {
        Self {
            mutation: mutation.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct QuarantineMirrorKey {
    pub target: TargetName,
    pub package_hash: PackageHash,
    pub source_row_ordinal: u64,
    pub rule_id: String,
    pub error_code: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantineMirrorMutation {
    pub key: QuarantineMirrorKey,
    pub receipt_id: ReceiptId,
    pub source_position: Option<SourcePosition>,
    pub observed_value_redacted: QuarantineObservedValue,
    pub committed_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantineMirrorRow {
    pub mutation: QuarantineMirrorMutation,
}

impl From<&QuarantineMirrorMutation> for QuarantineMirrorRow {
    fn from(mutation: &QuarantineMirrorMutation) -> Self {
        Self {
            mutation: mutation.clone(),
        }
    }
}

impl QuarantineMirrorMutation {
    pub fn from_record(receipt: &Receipt, record: QuarantineRecord) -> Self {
        Self {
            key: QuarantineMirrorKey {
                target: receipt.target.clone(),
                package_hash: receipt.package_hash.clone(),
                source_row_ordinal: record.source_row_ordinal,
                rule_id: record.rule_id,
                error_code: record.error_code,
            },
            receipt_id: receipt.receipt_id.clone(),
            source_position: record.source_position,
            observed_value_redacted: record.observed_value_redacted,
            committed_at_ms: receipt.committed_at_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SegmentMirrorPolicy {
    Persist { require_row_ranges: bool },
    Exclude,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MirrorCommit {
    pub load: LoadMirrorMutation,
    pub state: Option<StateMirrorMutation>,
    pub segments: Vec<SegmentMirrorMutation>,
    pub segment_policy: SegmentMirrorPolicy,
}

impl MirrorCommit {
    pub fn new(
        receipt: Receipt,
        resource_id: Option<ResourceId>,
        state_delta: Option<&StateDelta>,
        segment_states: &[cdf_kernel::StateSegment],
        row_ranges: Vec<SegmentRowRange>,
        segment_policy: SegmentMirrorPolicy,
    ) -> Result<Self> {
        if matches!(segment_policy, SegmentMirrorPolicy::Exclude) && state_delta.is_some() {
            return Err(CdfError::data(
                "SQL mirror cannot exclude segment evidence for a checkpoint state mutation",
            ));
        }
        let state = state_delta
            .map(|delta| StateMirrorMutation::from_delta(delta, &receipt))
            .transpose()?;
        let state_by_id = segment_states
            .iter()
            .map(|segment| (segment.segment_id.clone(), segment))
            .collect::<BTreeMap<_, _>>();
        if state_by_id.len() != segment_states.len() {
            return Err(CdfError::data(
                "SQL mirror segment state contains duplicate segment identifiers",
            ));
        }
        if let Some(delta) = state_delta {
            let delta_by_id = delta
                .segments
                .iter()
                .map(|segment| (segment.segment_id.clone(), segment))
                .collect::<BTreeMap<_, _>>();
            if delta_by_id.len() != delta.segments.len() || delta_by_id != state_by_id {
                return Err(CdfError::data(
                    "SQL mirror state delta and segment evidence differ",
                ));
            }
        }
        let row_range_count = row_ranges.len();
        let mut ranges_by_start = row_ranges.iter().collect::<Vec<_>>();
        ranges_by_start.sort_by_key(|range| range.row_key_start);
        if ranges_by_start
            .windows(2)
            .any(|window| window[0].row_key_end > window[1].row_key_start)
        {
            return Err(CdfError::data("SQL mirror row ranges overlap"));
        }
        let range_by_id = row_ranges
            .into_iter()
            .map(|range| (range.segment_id.clone(), range))
            .collect::<BTreeMap<_, _>>();
        if range_by_id.len() != row_range_count {
            return Err(CdfError::data(
                "SQL mirror row ranges contain duplicate segment identifiers",
            ));
        }
        if matches!(
            segment_policy,
            SegmentMirrorPolicy::Persist {
                require_row_ranges: true
            }
        ) && range_by_id.len() != receipt.segment_acks.len()
        {
            return Err(CdfError::data(
                "SQL mirror requires one row range for every receipt segment",
            ));
        }
        let segments = if matches!(segment_policy, SegmentMirrorPolicy::Exclude) {
            Vec::new()
        } else {
            receipt
                .segment_acks
                .iter()
                .map(|ack| {
                let state = state_by_id.get(&ack.segment_id).copied();
                if state.is_some_and(|segment| {
                    segment.row_count != ack.row_count || segment.byte_count != ack.byte_count
                }) {
                    return Err(CdfError::data(format!(
                        "SQL mirror segment {} state counts differ from its acknowledgement",
                        ack.segment_id
                    )));
                }
                let row_range = range_by_id.get(&ack.segment_id).cloned();
                if let Some(range) = &row_range
                    && (range.row_key_end < range.row_key_start
                        || range.row_key_end - range.row_key_start != ack.row_count)
                {
                    return Err(CdfError::data(format!(
                        "SQL mirror segment {} row range does not match acknowledgement row count",
                        ack.segment_id
                    )));
                }
                Ok(SegmentMirrorMutation {
                    target: receipt.target.clone(),
                    package_hash: receipt.package_hash.clone(),
                    idempotency_token: receipt.idempotency_token.clone(),
                    segment_id: ack.segment_id.clone(),
                    scope: state.map(|segment| segment.scope.clone()),
                    output_position: state.map(|segment| segment.output_position.clone()),
                    row_count: ack.row_count,
                    byte_count: ack.byte_count,
                    committed_at_ms: receipt.committed_at_ms,
                    row_range,
                })
                })
                .collect::<Result<Vec<_>>>()?
        };
        let ack_ids = receipt
            .segment_acks
            .iter()
            .map(|ack| &ack.segment_id)
            .collect::<BTreeSet<_>>();
        if range_by_id
            .keys()
            .any(|segment_id| !ack_ids.contains(segment_id))
            || state_by_id
                .keys()
                .any(|segment_id| !ack_ids.contains(segment_id))
        {
            return Err(CdfError::data(
                "SQL mirror segment evidence identifies a segment absent from the receipt",
            ));
        }
        Ok(Self {
            load: LoadMirrorMutation {
                receipt,
                resource_id,
                duplicate: false,
            },
            state,
            segments,
            segment_policy,
        })
    }
}
