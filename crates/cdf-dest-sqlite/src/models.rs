use std::{collections::BTreeMap, path::PathBuf};

use cdf_kernel::{
    CommitCounts, CommitPlan, IdempotencyToken, PackageHash, ResourceId, SchemaHash, StateDelta,
    StateSegment, TransactionMetadata, VerifyClause, WriteDisposition,
};
use serde::{Deserialize, Serialize};

use crate::{identifier::SqliteIdentifier, mapping::SqliteColumn};

#[derive(Clone)]
pub struct SqliteDestination {
    pub(crate) sheet: cdf_kernel::DestinationSheet,
    pub(crate) database_path: Option<PathBuf>,
    pub(crate) target: Option<SqliteIdentifier>,
    pub(crate) execution: Option<cdf_runtime::ExecutionServices>,
}

impl std::fmt::Debug for SqliteDestination {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteDestination")
            .field(
                "database_path",
                &self.database_path.as_ref().map(|_| "<redacted>"),
            )
            .field("target", &self.target)
            .field("execution_bound", &self.execution.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SqliteLoadPlanInput {
    pub(crate) package_hash: PackageHash,
    pub(crate) content: cdf_kernel::PackageContentAuthority,
    pub(crate) idempotency_token: IdempotencyToken,
    pub(crate) target: SqliteIdentifier,
    pub(crate) disposition: WriteDisposition,
    pub(crate) schema_hash: SchemaHash,
    pub(crate) segments: Vec<StateSegment>,
    pub(crate) columns: Vec<SqliteColumn>,
    pub(crate) merge_keys: Vec<SqliteIdentifier>,
    pub(crate) resource_id: Option<ResourceId>,
    pub(crate) state_delta: Option<StateDelta>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SqliteLoadPlan {
    pub(crate) kernel: CommitPlan,
    pub(crate) package_hash: PackageHash,
    pub(crate) content: cdf_kernel::PackageContentAuthority,
    pub(crate) idempotency_token: IdempotencyToken,
    pub(crate) schema_hash: SchemaHash,
    pub(crate) segments: Vec<StateSegment>,
    pub(crate) target: SqliteIdentifier,
    pub(crate) columns: Vec<SqliteColumn>,
    pub(crate) merge_keys: Vec<SqliteIdentifier>,
    pub(crate) resource_id: Option<ResourceId>,
    pub(crate) state_delta: Option<StateDelta>,
    pub(crate) verify: VerifyClause,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqliteExpectedSegment {
    pub(crate) state: StateSegment,
    pub(crate) package_byte_count: u64,
    pub(crate) package_row_ord_start: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqliteSessionSegments {
    pub(crate) expected: BTreeMap<cdf_kernel::SegmentId, SqliteExpectedSegment>,
}

pub(crate) struct SqliteCommitRequest {
    pub(crate) package: cdf_package_contract::SharedVerifiedPackageAccess,
    pub(crate) plan: SqliteLoadPlan,
    pub(crate) segments: SqliteSessionSegments,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SqliteReceiptInput {
    pub(crate) committed_at_ms: i64,
    pub(crate) counts: CommitCounts,
    pub(crate) transaction: TransactionMetadata,
}
