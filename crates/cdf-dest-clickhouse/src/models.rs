use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

use cdf_kernel::{
    CommitPlan, IdempotencyToken, PackageHash, ResourceId, SchemaHash, StateDelta, StateSegment,
    VerifyClause,
};
use serde::{Deserialize, Serialize};

use crate::{
    client::{AuthorizedClickHouseClient, ClickHouseConnectionOptions},
    identifier::ClickHouseIdentifier,
    mapping::ClickHouseColumn,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClickHouseMergeMode {
    #[default]
    ReplacingMergeTree,
    AtomicCopyOnWrite,
}

impl ClickHouseMergeMode {
    pub(crate) fn parse(value: Option<&str>) -> cdf_kernel::Result<Self> {
        match value.unwrap_or("replacing_merge_tree") {
            "replacing_merge_tree" => Ok(Self::ReplacingMergeTree),
            "atomic_copy_on_write" => Ok(Self::AtomicCopyOnWrite),
            value => Err(cdf_kernel::CdfError::contract(format!(
                "ClickHouse merge_mode `{value}` is unsupported; use replacing_merge_tree or atomic_copy_on_write"
            ))),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ReplacingMergeTree => "replacing_merge_tree",
            Self::AtomicCopyOnWrite => "atomic_copy_on_write",
        }
    }
}

#[derive(Clone)]
pub struct ClickHouseDestination {
    pub(crate) sheet: cdf_kernel::DestinationSheet,
    pub(crate) connection: Option<ClickHouseConnectionOptions>,
    pub(crate) target: Option<ClickHouseIdentifier>,
    pub(crate) execution: Option<cdf_runtime::ExecutionServices>,
    pub(crate) client: Arc<OnceLock<AuthorizedClickHouseClient>>,
    pub(crate) secret_redaction: Option<String>,
    pub(crate) merge_mode: ClickHouseMergeMode,
}

impl std::fmt::Debug for ClickHouseDestination {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClickHouseDestination")
            .field("connection", &self.connection)
            .field("target", &self.target)
            .field("execution_bound", &self.execution.is_some())
            .field("merge_mode", &self.merge_mode)
            .field(
                "secret_redaction",
                &self.secret_redaction.as_ref().map(|_| "<redacted>"),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClickHouseLoadPlanInput {
    pub(crate) package_hash: PackageHash,
    pub(crate) idempotency_token: IdempotencyToken,
    pub(crate) target: ClickHouseIdentifier,
    pub(crate) disposition: cdf_kernel::WriteDisposition,
    pub(crate) schema_hash: SchemaHash,
    pub(crate) segments: Vec<StateSegment>,
    pub(crate) columns: Vec<ClickHouseColumn>,
    pub(crate) merge_keys: Vec<ClickHouseIdentifier>,
    pub(crate) merge_mode: ClickHouseMergeMode,
    pub(crate) resource_id: Option<ResourceId>,
    pub(crate) state_delta: Option<StateDelta>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClickHouseLoadPlan {
    pub(crate) kernel: CommitPlan,
    pub(crate) package_hash: PackageHash,
    pub(crate) idempotency_token: IdempotencyToken,
    pub(crate) schema_hash: SchemaHash,
    pub(crate) segments: Vec<StateSegment>,
    pub(crate) target: ClickHouseIdentifier,
    pub(crate) columns: Vec<ClickHouseColumn>,
    pub(crate) merge_keys: Vec<ClickHouseIdentifier>,
    pub(crate) merge_mode: ClickHouseMergeMode,
    pub(crate) stage: ClickHouseIdentifier,
    pub(crate) incoming_stage: ClickHouseIdentifier,
    pub(crate) resource_id: Option<ResourceId>,
    pub(crate) state_delta: Option<StateDelta>,
    pub(crate) verify: VerifyClause,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClickHouseExpectedSegment {
    pub(crate) state: StateSegment,
    pub(crate) package_byte_count: u64,
    pub(crate) package_row_ord_start: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClickHouseSessionSegments {
    pub(crate) expected: BTreeMap<cdf_kernel::SegmentId, ClickHouseExpectedSegment>,
}

pub(crate) struct ClickHouseCommitRequest {
    pub(crate) plan: ClickHouseLoadPlan,
    pub(crate) segments: ClickHouseSessionSegments,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TargetCapabilities {
    pub(crate) database_engine: String,
    pub(crate) table_engine: String,
    pub(crate) create_table_query: String,
    pub(crate) engine_full: String,
    pub(crate) sorting_key: String,
    pub(crate) primary_key: String,
    pub(crate) partition_key: String,
    pub(crate) sampling_key: String,
    pub(crate) table_comment: String,
    pub(crate) dependencies: u64,
}
