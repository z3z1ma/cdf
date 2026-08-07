use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use cdf_kernel::{
    Checkpoint, CheckpointId, CheckpointStatus, CheckpointStore, LeaseOwnerId, PipelineId,
    PromotionId, PromotionPublicationEvent, PromotionSettlementStore, Receipt, ResourceId, Result,
    RewindReport, RewindRequest, SchemaAuthorityCheck, SchemaAuthorityEstablishment,
    SchemaAuthorityEvent, SchemaAuthorityKey, SchemaAuthorityStore, SchemaHash, SchemaHead,
    SchemaPromotionFence, SchemaPromotionPlanState, SchemaPromotionState, SchemaPromotionTarget,
    SchemaVersion, ScopeKey, ScopeLease, ScopeLeaseClock, ScopeLeaseStore, StateDelta,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};

use crate::{
    SqliteCheckpointStore, SqliteRunLedger, SqliteSchemaAuthorityStore, SqliteScopeLeaseStore,
    sqlite::{SqliteCheckpointStore as CheckpointSql, commit_checkpoint_tx},
    support::{
        StateStorePathOwnership, database_open_path, database_path_exists,
        decode_private_promotion_publication_row, encode_json, managed_sqlite_open_flags,
        sqlite_error,
    },
};

/// One typed promotion settlement boundary over one SQLite consistency domain.
pub struct SqlitePromotionSettlementStore {
    path: PathBuf,
    path_ownership: StateStorePathOwnership,
    clock: Arc<dyn ScopeLeaseClock>,
}

impl SqlitePromotionSettlementStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_clock_and_path_ownership(
            path,
            Arc::new(crate::lease::SystemScopeLeaseClock),
            StateStorePathOwnership::CdfManaged,
        )
    }

    pub fn open_with_path_ownership(
        path: impl AsRef<Path>,
        ownership: StateStorePathOwnership,
    ) -> Result<Self> {
        Self::open_with_clock_and_path_ownership(
            path,
            Arc::new(crate::lease::SystemScopeLeaseClock),
            ownership,
        )
    }

    pub fn open_with_clock(
        path: impl AsRef<Path>,
        clock: Arc<dyn ScopeLeaseClock>,
    ) -> Result<Self> {
        Self::open_with_clock_and_path_ownership(path, clock, StateStorePathOwnership::CdfManaged)
    }

    pub fn open_with_clock_and_path_ownership(
        path: impl AsRef<Path>,
        clock: Arc<dyn ScopeLeaseClock>,
        ownership: StateStorePathOwnership,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let checkpoints = SqliteCheckpointStore::open_with_path_ownership(&path, ownership)?;
        drop(checkpoints);
        let leases = SqliteScopeLeaseStore::open_with_clock_and_path_ownership(
            &path,
            Arc::clone(&clock),
            ownership,
        )?;
        let lease_authority_domain_id = leases.authority_domain_id();
        drop(leases);
        let ledger = SqliteRunLedger::open_with_path_ownership(&path, ownership)?;
        drop(ledger);
        let schema = SqliteSchemaAuthorityStore::open_with_clock_and_path_ownership(
            &path,
            Arc::clone(&clock),
            ownership,
        )?;
        let authority_domain_id = schema.authority_domain_id();
        if lease_authority_domain_id != authority_domain_id {
            return Err(cdf_kernel::CdfError::internal(
                "promotion settlement components resolved different authority domains",
            ));
        }
        drop(schema);
        let path = database_open_path(&path, ownership)?;
        Ok(Self {
            path,
            path_ownership: ownership,
            clock,
        })
    }

    fn checkpoints(&self) -> Result<SqliteCheckpointStore> {
        SqliteCheckpointStore::open_existing_with_path_ownership(&self.path, self.path_ownership)
    }

    fn leases(&self) -> Result<SqliteScopeLeaseStore> {
        SqliteScopeLeaseStore::open_existing_with_clock_and_path_ownership(
            &self.path,
            Arc::clone(&self.clock),
            self.path_ownership,
        )
    }

    fn ledger(&self) -> Result<SqliteRunLedger> {
        SqliteRunLedger::open_existing_with_path_ownership(&self.path, self.path_ownership)
    }

    fn schema(&self) -> Result<SqliteSchemaAuthorityStore> {
        SqliteSchemaAuthorityStore::open_existing_with_clock_and_path_ownership(
            &self.path,
            Arc::clone(&self.clock),
            self.path_ownership,
        )
    }

    fn connection(&self) -> Result<Connection> {
        database_path_exists(&self.path, self.path_ownership)?;
        Connection::open_with_flags(&self.path, managed_sqlite_open_flags(false))
            .map_err(sqlite_error)
    }
}

impl SchemaAuthorityStore for SqlitePromotionSettlementStore {
    fn authority_domain_id(&self) -> cdf_kernel::LeaseAuthorityDomainId {
        self.schema()
            .expect("validated promotion settlement store can reopen schema authority")
            .authority_domain_id()
    }

    fn head(&self, key: &SchemaAuthorityKey) -> Result<Option<SchemaHead>> {
        self.schema()?.head(key)
    }

    fn version(
        &self,
        key: &SchemaAuthorityKey,
        schema_hash: &SchemaHash,
    ) -> Result<Option<SchemaVersion>> {
        self.schema()?.version(key, schema_hash)
    }

    fn establish_batch_if_absent(
        &self,
        establishments: Vec<SchemaAuthorityEstablishment>,
    ) -> Result<Vec<SchemaHead>> {
        self.schema()?.establish_batch_if_absent(establishments)
    }

    fn establish_batch_checked(
        &self,
        checks: Vec<SchemaAuthorityCheck>,
        establishments: Vec<SchemaAuthorityEstablishment>,
    ) -> Result<Vec<SchemaHead>> {
        self.schema()?
            .establish_batch_checked(checks, establishments)
    }

    fn begin_promotion(
        &self,
        expected: &SchemaHead,
        proposed: SchemaVersion,
        plan: SchemaPromotionPlanState,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaPromotionState> {
        self.schema()?
            .begin_promotion(expected, proposed, plan, fence)
    }

    fn resume_promotion(
        &self,
        expected_source: &SchemaHead,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaHead> {
        self.schema()?.resume_promotion(expected_source, fence)
    }

    fn promotion_state(
        &self,
        key: &SchemaAuthorityKey,
        promotion_id: &PromotionId,
    ) -> Result<Option<SchemaPromotionState>> {
        self.schema()?.promotion_state(key, promotion_id)
    }

    fn establish_promotion_cutoff(
        &self,
        expected_promoting: &SchemaHead,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaPromotionState> {
        self.schema()?
            .establish_promotion_cutoff(expected_promoting, fence)
    }

    fn commit_promotion_target(
        &self,
        expected_promoting: &SchemaHead,
        fence: &SchemaPromotionFence,
        target: &SchemaPromotionTarget,
        checkpoint_id: &CheckpointId,
        receipt: Receipt,
    ) -> Result<SchemaPromotionState> {
        self.schema()?.commit_promotion_target(
            expected_promoting,
            fence,
            target,
            checkpoint_id,
            receipt,
        )
    }

    fn publish_promotion(
        &self,
        expected_promoting: &SchemaHead,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaHead> {
        self.schema()?.publish_promotion(expected_promoting, fence)
    }

    fn history(&self, key: &SchemaAuthorityKey, limit: u32) -> Result<Vec<SchemaAuthorityEvent>> {
        self.schema()?.history(key, limit)
    }
}

impl CheckpointStore for SqlitePromotionSettlementStore {
    fn propose(&self, delta: StateDelta) -> Result<Checkpoint> {
        self.checkpoints()?.propose(delta)
    }

    fn commit(&self, checkpoint_id: &CheckpointId, receipt: Receipt) -> Result<Checkpoint> {
        self.checkpoints()?.commit(checkpoint_id, receipt)
    }

    fn abandon(&self, checkpoint_id: &CheckpointId) -> Result<Checkpoint> {
        self.checkpoints()?.abandon(checkpoint_id)
    }

    fn head(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Option<Checkpoint>> {
        self.checkpoints()?.head(pipeline_id, resource_id, scope)
    }

    fn history(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
    ) -> Result<Vec<Checkpoint>> {
        self.checkpoints()?.history(pipeline_id, resource_id, scope)
    }

    fn committed_schema_streak(
        &self,
        pipeline_id: &PipelineId,
        resource_id: &ResourceId,
        scope: &ScopeKey,
        schema_hash: &SchemaHash,
        limit: u32,
    ) -> Result<u32> {
        self.checkpoints()?.committed_schema_streak(
            pipeline_id,
            resource_id,
            scope,
            schema_hash,
            limit,
        )
    }

    fn rewind(&self, request: RewindRequest) -> Result<RewindReport> {
        self.checkpoints()?.rewind(request)
    }
}

impl ScopeLeaseStore for SqlitePromotionSettlementStore {
    fn authority_domain_id(&self) -> cdf_kernel::LeaseAuthorityDomainId {
        self.leases()
            .expect("validated promotion settlement store can reopen lease authority")
            .authority_domain_id()
    }

    fn acquire(
        &self,
        scope: ScopeKey,
        owner: LeaseOwnerId,
        lease_duration_ms: u64,
    ) -> Result<ScopeLease> {
        self.leases()?.acquire(scope, owner, lease_duration_ms)
    }

    fn renew(&self, lease: &ScopeLease, lease_duration_ms: u64) -> Result<ScopeLease> {
        self.leases()?.renew(lease, lease_duration_ms)
    }

    fn release(&self, lease: &ScopeLease) -> Result<()> {
        self.leases()?.release(lease)
    }

    fn assert_current(&self, lease: &ScopeLease) -> Result<()> {
        self.leases()?.assert_current(lease)
    }

    fn prove_expired(
        &self,
        lease: &ScopeLease,
        collector: LeaseOwnerId,
        cleanup_lease_duration_ms: u64,
    ) -> Result<Option<cdf_kernel::ExpiredScopeLeaseProof>> {
        self.leases()?
            .prove_expired(lease, collector, cleanup_lease_duration_ms)
    }
}

impl PromotionSettlementStore for SqlitePromotionSettlementStore {
    fn promotion_publication(
        &self,
        promotion_id: &PromotionId,
    ) -> Result<Option<PromotionPublicationEvent>> {
        self.ledger()?.promotion_publication(promotion_id)
    }

    fn commit_promotion_checkpoint(
        &self,
        lease: &ScopeLease,
        checkpoint_id: &CheckpointId,
        receipt: Receipt,
    ) -> Result<Checkpoint> {
        let mut conn = self.connection()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let checkpoint = CheckpointSql::fetch_by_id_tx(&tx, checkpoint_id)?.ok_or_else(|| {
            cdf_kernel::CdfError::data(format!("checkpoint {checkpoint_id} does not exist"))
        })?;
        if checkpoint.status == CheckpointStatus::Committed
            && checkpoint.receipt.as_ref() == Some(&receipt)
        {
            return Ok(checkpoint);
        }
        if checkpoint.delta.scope != lease.scope {
            return Err(cdf_kernel::CdfError::contract(
                "promotion checkpoint scope does not match settlement lease",
            ));
        }
        let head = CheckpointSql::head_tx(
            &tx,
            &checkpoint.delta.pipeline_id,
            &checkpoint.delta.resource_id,
            &checkpoint.delta.scope,
        )?;
        let expected_parent = head
            .as_ref()
            .map(|checkpoint| checkpoint.delta.checkpoint_id.clone());
        let expected_input = head
            .as_ref()
            .map(|checkpoint| checkpoint.delta.output_position.clone());
        if checkpoint.delta.parent_checkpoint_id != expected_parent
            || checkpoint.delta.input_position != expected_input
        {
            return Err(cdf_kernel::CdfError::contract(
                "promotion checkpoint parent/input does not match the exact committed head",
            ));
        }
        assert_current_lease_tx(&tx, lease, self.clock.now_ms()?)?;
        let committed = commit_checkpoint_tx(&tx, checkpoint_id, &receipt)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(committed)
    }

    fn publish_promotion(
        &self,
        lease: &ScopeLease,
        event: PromotionPublicationEvent,
    ) -> Result<PromotionPublicationEvent> {
        event.validate()?;
        let mut conn = self.connection()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let existing = publication_tx(&tx, &event.promotion_id)?;
        if let Some(existing) = existing {
            if existing.same_authority(&event) {
                return Ok(existing);
            }
            return Err(cdf_kernel::CdfError::contract(format!(
                "promotion publication {} conflicts with existing ledger authority",
                event.promotion_id
            )));
        }
        if !matches!(lease.scope, ScopeKey::SchemaContract { .. }) {
            return Err(cdf_kernel::CdfError::contract(
                "promotion publication requires a schema-contract scope lease",
            ));
        }
        assert_current_lease_tx(&tx, lease, self.clock.now_ms()?)?;
        tx.execute(
            "INSERT INTO cdf_promotion_publications (promotion_id, published_at_ms, event_json) VALUES (?, ?, ?)",
            params![event.promotion_id.as_str(), event.published_at_ms, encode_json(&event)?],
        )
        .map_err(sqlite_error)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(event)
    }
}

fn assert_current_lease_tx(tx: &Transaction<'_>, lease: &ScopeLease, now_ms: i64) -> Result<()> {
    let token = i64::try_from(lease.fencing_token.get())
        .map_err(|_| cdf_kernel::CdfError::contract("fencing token exceeds SQLite range"))?;
    let current = tx
        .query_row(
            "SELECT 1 FROM cdf_scope_leases WHERE scope_json = ? AND owner = ? AND fencing_token = ? AND released = 0 AND expires_at_ms > ?",
            params![encode_json(&lease.scope)?, lease.owner.as_str(), token, now_ms],
            |_| Ok(()),
        )
        .optional()
        .map_err(sqlite_error)?;
    current.ok_or_else(|| {
        cdf_kernel::CdfError::transient("scope lease is stale, released, or expired")
    })
}

fn publication_tx(
    tx: &Transaction<'_>,
    promotion_id: &PromotionId,
) -> Result<Option<PromotionPublicationEvent>> {
    tx.query_row(
        "SELECT promotion_id, published_at_ms, event_json FROM cdf_promotion_publications WHERE promotion_id = ?",
        params![promotion_id.as_str()],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        },
    )
    .optional()
    .map_err(sqlite_error)?
    .map(|(promotion_id, published_at_ms, json)| {
        decode_private_promotion_publication_row(promotion_id, published_at_ms, json)
    })
    .transpose()
}
