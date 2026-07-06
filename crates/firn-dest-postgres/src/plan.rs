use crate::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresLoadPlanInput {
    pub package_hash: PackageHash,
    pub idempotency_token: IdempotencyToken,
    pub target: PostgresTarget,
    pub disposition: WriteDisposition,
    pub schema_hash: SchemaHash,
    pub segments: Vec<StateSegment>,
    pub columns: Vec<PostgresColumn>,
    pub merge_keys: Vec<PostgresIdentifier>,
    pub dedup: MergeDedupPolicy,
    pub existing_table: Option<PostgresExistingTable>,
    pub resource_id: Option<ResourceId>,
    pub state_delta: Option<StateDelta>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresLoadPlan {
    pub kernel: CommitPlan,
    pub target: PostgresTarget,
    pub stage_table: PostgresIdentifier,
    pub columns: Vec<PostgresColumn>,
    pub merge_keys: Vec<PostgresIdentifier>,
    pub dedup: MergeDedupPolicy,
    pub resource_id: Option<ResourceId>,
    pub state_delta: Option<StateDelta>,
    pub system_ddl: Vec<PostgresStatement>,
    pub target_ddl: Vec<PostgresStatement>,
    pub idempotency_check: PostgresStatement,
    pub xid_probe: PostgresStatement,
    pub write_sql: Vec<PostgresStatement>,
    pub mirror_sql: Vec<PostgresStatement>,
    pub verify: VerifyClause,
    pub drift: PostgresDriftHooks,
}

impl PostgresLoadPlan {
    pub fn transactional_statements(&self) -> Vec<PostgresStatement> {
        let mut statements = vec![PostgresStatement::execute("begin", "BEGIN")];
        statements.extend(self.system_ddl.clone());
        statements.extend(self.target_ddl.clone());
        statements.push(self.idempotency_check.clone());
        statements.push(self.xid_probe.clone());
        statements.extend(self.write_sql.clone());
        statements.extend(self.mirror_sql.clone());
        statements.push(PostgresStatement::execute("commit", "COMMIT"));
        statements
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresStatement {
    pub name: String,
    pub sql: String,
    pub dry_run_safe: bool,
    pub expectation: StatementExpectation,
}

impl PostgresStatement {
    pub fn execute(name: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sql: sql.into(),
            dry_run_safe: true,
            expectation: StatementExpectation::Execute,
        }
    }

    pub fn query(
        name: impl Into<String>,
        sql: impl Into<String>,
        expectation: StatementExpectation,
    ) -> Self {
        Self {
            name: name.into(),
            sql: sql.into(),
            dry_run_safe: true,
            expectation,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatementExpectation {
    Execute,
    ReturnsXid,
    ReturnsDuplicateReceiptIfPresent,
    ReturnsZeroRows,
    ReturnsVerifyRow,
    ReturnsMirrorRows,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeDedupPolicy {
    First,
    Last,
    Fail,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresDriftHooks {
    pub load_for_package: PostgresStatement,
    pub state_for_scope: PostgresStatement,
    pub loads_for_target: PostgresStatement,
    pub state_heads: PostgresStatement,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresSourceExerciseHooks {
    pub snapshot_count: PostgresStatement,
    pub snapshot_page: PostgresStatement,
    pub incremental_page: Option<PostgresStatement>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresReceiptInput {
    pub receipt_id: ReceiptId,
    pub xid: String,
    pub committed_at_ms: i64,
    pub counts: CommitCounts,
    pub duplicate: bool,
}
