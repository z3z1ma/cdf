use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Mutex},
};

use cdf_kernel::{
    CdfError, Checkpoint, CheckpointId, CheckpointStatus, LeaseAuthorityDomainId,
    MAX_SCHEMA_AUTHORITY_HISTORY_LIMIT, Receipt, Result, RunId, SchemaAuthorityCheck,
    SchemaAuthorityEstablishment, SchemaAuthorityEvent, SchemaAuthorityEventKind,
    SchemaAuthorityKey, SchemaAuthorityStore, SchemaHash, SchemaHead, SchemaHeadStatus,
    SchemaPromotionCutoff, SchemaPromotionCutoffCheckpoint, SchemaPromotionFence,
    SchemaPromotionLifecyclePhase, SchemaPromotionPlanState, SchemaPromotionState,
    SchemaPromotionTarget, SchemaPromotionTargetSettlement, SchemaSettlementPermit,
    SchemaSettlementStore, SchemaVersion, SchemaVersionProvenance, ScopeLeaseClock,
};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};

use crate::{
    lease::{
        SystemScopeLeaseClock, assert_current_lease_at,
        initialize_schema as initialize_lease_schema,
        initialize_schema_with_domain as initialize_lease_schema_with_domain,
        read_authority_domain_id, validate_schema_version as validate_lease_schema_version,
    },
    sqlite::{
        SqliteCheckpointStore as CheckpointSql, commit_checkpoint_tx,
        initialize_schema as initialize_checkpoint_schema,
    },
    support::{
        SqliteConnectionGuard, SqliteErrorContext, StateStorePathOwnership, database_open_path,
        database_path_exists, encode_json, ensure_schema_version_table, lock_sqlite_connection,
        managed_sqlite_open_flags, prepare_managed_database_path, private_state_decode,
        read_component_schema_version, require_sqlite_tables, sqlite_error, sqlite_table_exists,
        with_sqlite_error_context, write_component_schema_version,
    },
};

pub(crate) const SCHEMA_AUTHORITY_COMPONENT: &str = "schema_authority_store";
pub(crate) const SCHEMA_AUTHORITY_SCHEMA_VERSION: i64 = 2;

const VERSION_SELECT: &str = "SELECT schema_hash, predecessor_schema_hash, created_at_ms, version_json FROM cdf_schema_versions";
const HEAD_SELECT: &str = "SELECT authority_domain_id, project_id, environment, resource_id, generation, schema_hash, status, promotion_id, promotion_from_schema_hash, promotion_to_schema_hash, promotion_lease_owner, promotion_fencing_token, head_json FROM cdf_schema_heads";
const EVENT_SELECT: &str = "SELECT authority_domain_id, project_id, environment, resource_id, ordinal, generation, schema_hash, recorded_at_ms, event_json FROM cdf_schema_authority_events";
const PERMIT_SELECT: &str = "SELECT authority_domain_id, project_id, environment, resource_id, run_id, generation, schema_hash, acquired_at_ms, expires_at_ms, released, permit_json FROM cdf_schema_settlement_permits";
const PROMOTION_SELECT: &str = "SELECT authority_domain_id, project_id, environment, resource_id, promotion_id, phase, from_generation, from_schema_hash, to_schema_hash, updated_at_ms, state_json FROM cdf_schema_promotions";

pub struct SqliteSchemaAuthorityStore {
    conn: Mutex<Connection>,
    clock: Arc<dyn ScopeLeaseClock>,
    authority_domain_id: LeaseAuthorityDomainId,
    error_context: SqliteErrorContext,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqliteSchemaAuthorityState {
    Missing,
    Uninitialized {
        authority_domain_id: Option<LeaseAuthorityDomainId>,
    },
    Ready {
        authority_domain_id: LeaseAuthorityDomainId,
    },
}

impl SqliteSchemaAuthorityStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_clock_and_path_ownership(
            path,
            Arc::new(SystemScopeLeaseClock),
            StateStorePathOwnership::CdfManaged,
        )
    }

    pub fn open_with_path_ownership(
        path: impl AsRef<Path>,
        ownership: StateStorePathOwnership,
    ) -> Result<Self> {
        Self::open_with_clock_and_path_ownership(path, Arc::new(SystemScopeLeaseClock), ownership)
    }

    pub fn open_with_authority_domain_and_path_ownership(
        path: impl AsRef<Path>,
        authority_domain_id: &LeaseAuthorityDomainId,
        ownership: StateStorePathOwnership,
    ) -> Result<Self> {
        let open_path = prepare_managed_database_path(path.as_ref(), ownership)?;
        let error_context = SqliteErrorContext::ManagedState;
        let (conn, observed_domain_id) = with_sqlite_error_context(error_context, || {
            let conn = Connection::open_with_flags(&open_path, managed_sqlite_open_flags(false))
                .map_err(sqlite_error)?;
            initialize_schema_with_domain(&conn, authority_domain_id)?;
            let observed_domain_id = read_authority_domain_id(&conn)?;
            Ok((conn, observed_domain_id))
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
            clock: Arc::new(SystemScopeLeaseClock),
            authority_domain_id: observed_domain_id,
            error_context,
        })
    }

    pub fn inspect_state(
        path: impl AsRef<Path>,
        ownership: StateStorePathOwnership,
    ) -> Result<SqliteSchemaAuthorityState> {
        let path = path.as_ref();
        if !database_path_exists(path, ownership)? {
            return Ok(SqliteSchemaAuthorityState::Missing);
        }
        let open_path = database_open_path(path, ownership)?;
        let error_context = SqliteErrorContext::ManagedReadOnly;
        with_sqlite_error_context(error_context, || {
            let conn = Connection::open_with_flags(&open_path, managed_sqlite_open_flags(true))
                .map_err(sqlite_error)?;
            validate_lease_schema_version(&conn)?;
            let lease_ready = sqlite_table_exists(&conn, "cdf_scope_lease_authority")?;
            let authority_domain_id = lease_ready
                .then(|| read_authority_domain_id(&conn))
                .transpose()?;
            match read_component_schema_version(&conn, SCHEMA_AUTHORITY_COMPONENT)? {
                Some(SCHEMA_AUTHORITY_SCHEMA_VERSION) => {
                    validate_schema_structure(&conn)?;
                    let authority_domain_id = authority_domain_id.ok_or_else(|| {
                        CdfError::internal(
                            "schema authority state is initialized without a lease authority domain",
                        )
                    })?;
                    Ok(SqliteSchemaAuthorityState::Ready {
                        authority_domain_id,
                    })
                }
                Some(version) => Err(unsupported_schema_version(version)),
                None if sqlite_table_exists(&conn, "cdf_schema_heads")?
                    || sqlite_table_exists(&conn, "cdf_schema_versions")?
                    || sqlite_table_exists(&conn, "cdf_schema_authority_events")?
                    || sqlite_table_exists(&conn, "cdf_schema_settlement_permits")?
                    || sqlite_table_exists(&conn, "cdf_schema_checkpoint_settlements")?
                    || sqlite_table_exists(&conn, "cdf_schema_promotions")? =>
                {
                    Err(CdfError::internal(format!(
                        "schema authority SQLite schema is unversioned; expected current version {SCHEMA_AUTHORITY_SCHEMA_VERSION}"
                    )))
                }
                None => Ok(SqliteSchemaAuthorityState::Uninitialized {
                    authority_domain_id,
                }),
            }
        })
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
        let open_path = prepare_managed_database_path(path.as_ref(), ownership)?;
        let error_context = SqliteErrorContext::ManagedState;
        let (conn, authority_domain_id) = with_sqlite_error_context(error_context, || {
            let conn = Connection::open_with_flags(&open_path, managed_sqlite_open_flags(false))
                .map_err(sqlite_error)?;
            initialize_schema(&conn)?;
            let authority_domain_id = read_authority_domain_id(&conn)?;
            Ok((conn, authority_domain_id))
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
            clock,
            authority_domain_id,
            error_context,
        })
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_read_only_with_path_ownership(path, StateStorePathOwnership::CdfManaged)
    }

    pub fn open_read_only_with_path_ownership(
        path: impl AsRef<Path>,
        ownership: StateStorePathOwnership,
    ) -> Result<Self> {
        let path = path.as_ref();
        if !database_path_exists(path, ownership)? {
            return Err(CdfError::data(format!(
                "schema authority state database {} is missing",
                path.display()
            )));
        }
        let open_path = database_open_path(path, ownership)?;
        let error_context = SqliteErrorContext::ManagedReadOnly;
        let (conn, authority_domain_id) = with_sqlite_error_context(error_context, || {
            let conn = Connection::open_with_flags(&open_path, managed_sqlite_open_flags(true))
                .map_err(sqlite_error)?;
            validate_lease_schema_version(&conn)?;
            validate_schema_version(&conn)?;
            let authority_domain_id = read_authority_domain_id(&conn)?;
            Ok((conn, authority_domain_id))
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
            clock: Arc::new(SystemScopeLeaseClock),
            authority_domain_id,
            error_context,
        })
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::open_in_memory_with_clock(Arc::new(SystemScopeLeaseClock))
    }

    pub fn open_in_memory_with_clock(clock: Arc<dyn ScopeLeaseClock>) -> Result<Self> {
        let error_context = SqliteErrorContext::EphemeralWorkspace;
        let (conn, authority_domain_id) = with_sqlite_error_context(error_context, || {
            let conn = Connection::open_in_memory().map_err(sqlite_error)?;
            initialize_schema(&conn)?;
            let authority_domain_id = read_authority_domain_id(&conn)?;
            Ok((conn, authority_domain_id))
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
            clock,
            authority_domain_id,
            error_context,
        })
    }

    fn lock(&self) -> Result<SqliteConnectionGuard<'_>> {
        lock_sqlite_connection(&self.conn, self.error_context)
    }

    fn validate_key_domain(&self, key: &SchemaAuthorityKey) -> Result<()> {
        key.validate()?;
        if key.authority_domain_id != self.authority_domain_id {
            return Err(CdfError::contract(format!(
                "schema authority key belongs to state domain {} but this store owns {}",
                key.authority_domain_id, self.authority_domain_id
            )));
        }
        Ok(())
    }

    fn validate_fence(&self, key: &SchemaAuthorityKey, fence: &SchemaPromotionFence) -> Result<()> {
        fence.validate()?;
        if fence.authority_domain_id != self.authority_domain_id {
            return Err(CdfError::contract(format!(
                "schema promotion fence belongs to state domain {} but this store owns {}",
                fence.authority_domain_id, self.authority_domain_id
            )));
        }
        if fence.lease.scope != key.promotion_scope()? {
            return Err(CdfError::contract(
                "schema promotion fence lease does not match the authority key",
            ));
        }
        Ok(())
    }

    fn establish_batch(
        &self,
        checks: Vec<SchemaAuthorityCheck>,
        establishments: Vec<SchemaAuthorityEstablishment>,
        fail_after_version_inserts: bool,
    ) -> Result<Vec<SchemaHead>> {
        let mut unique_keys = BTreeSet::new();
        for establishment in &establishments {
            establishment.validate()?;
            self.validate_key_domain(&establishment.key)?;
            if !unique_keys.insert(establishment.key.clone()) {
                return Err(CdfError::contract(format!(
                    "schema authority batch repeats resource {} in environment {}",
                    establishment.key.resource_id, establishment.key.environment
                )));
            }
        }
        if establishments.is_empty() && checks.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let mut results = Vec::with_capacity(establishments.len());
        let mut pending = Vec::new();

        let mut checked_keys = BTreeSet::new();
        for check in checks {
            check.validate()?;
            self.validate_key_domain(&check.key)?;
            if !checked_keys.insert(check.key.clone()) {
                return Err(CdfError::contract(format!(
                    "schema authority batch repeats precondition for resource {} in environment {}",
                    check.key.resource_id, check.key.environment
                )));
            }
            let current = fetch_head(&tx, &check.key)?;
            match (&check.precondition, current) {
                (cdf_kernel::SchemaAuthorityPrecondition::Absent, None) => {
                    if authority_has_any_version(&tx, &check.key)? {
                        return Err(CdfError::internal(format!(
                            "schema authority {} in environment {} has versions but no head",
                            check.key.resource_id, check.key.environment
                        )));
                    }
                }
                (
                    cdf_kernel::SchemaAuthorityPrecondition::Exact {
                        generation,
                        schema_hash,
                    },
                    Some(head),
                ) if head.generation == *generation
                    && head.schema_hash == *schema_hash
                    && matches!(head.status, SchemaHeadStatus::Active) =>
                {
                    require_head_versions(&tx, &head)?;
                }
                _ => {
                    return Err(CdfError::contract(format!(
                        "schema authority for {} in environment {} changed after preparation; prepare the selected resources again",
                        check.key.resource_id, check.key.environment
                    )));
                }
            }
        }

        for establishment in establishments {
            match fetch_head(&tx, &establishment.key)? {
                Some(existing) => {
                    require_head_versions(&tx, &existing)?;
                    if existing.generation != 1
                        || !matches!(existing.status, SchemaHeadStatus::Active)
                        || existing.schema_hash != establishment.version.schema_hash
                    {
                        return Err(first_use_conflict(&establishment.key, &existing));
                    }
                    let stored =
                        fetch_version(&tx, &establishment.key, &establishment.version.schema_hash)?
                            .ok_or_else(|| missing_head_version(&existing))?;
                    if stored != establishment.version {
                        return Err(first_use_conflict(&establishment.key, &existing));
                    }
                    results.push(existing);
                }
                None => {
                    if authority_has_any_version(&tx, &establishment.key)? {
                        return Err(CdfError::internal(format!(
                            "schema authority {} in environment {} has versions but no head",
                            establishment.key.resource_id, establishment.key.environment
                        )));
                    }
                    let head = SchemaHead::active(
                        establishment.key.clone(),
                        1,
                        establishment.version.schema_hash.clone(),
                    )?;
                    results.push(head.clone());
                    pending.push((establishment, head));
                }
            }
        }

        for (establishment, _) in &pending {
            insert_version(&tx, &establishment.key, &establishment.version)?;
        }
        if fail_after_version_inserts {
            return Err(CdfError::internal(
                "injected schema authority failure after version insertion",
            ));
        }
        let recorded_at_ms = self.clock.now_ms()?;
        for (_, head) in &pending {
            insert_head(&tx, head)?;
            insert_event(
                &tx,
                &SchemaAuthorityEvent {
                    key: head.key.clone(),
                    ordinal: 1,
                    generation: head.generation,
                    schema_hash: head.schema_hash.clone(),
                    recorded_at_ms,
                    kind: SchemaAuthorityEventKind::Established,
                },
            )?;
        }
        tx.commit().map_err(sqlite_error)?;
        Ok(results)
    }
}

fn initialize_schema(conn: &Connection) -> Result<()> {
    initialize_schema_with_domain(conn, &read_or_create_domain(conn)?)
}

fn read_or_create_domain(conn: &Connection) -> Result<LeaseAuthorityDomainId> {
    initialize_lease_schema(conn)?;
    read_authority_domain_id(conn)
}

fn initialize_schema_with_domain(
    conn: &Connection,
    authority_domain_id: &LeaseAuthorityDomainId,
) -> Result<()> {
    initialize_lease_schema_with_domain(conn, Some(authority_domain_id))?;
    match read_component_schema_version(conn, SCHEMA_AUTHORITY_COMPONENT)? {
        Some(SCHEMA_AUTHORITY_SCHEMA_VERSION) => validate_schema_structure(conn)?,
        Some(version) => return Err(unsupported_schema_version(version)),
        None if sqlite_table_exists(conn, "cdf_schema_heads")?
            || sqlite_table_exists(conn, "cdf_schema_versions")?
            || sqlite_table_exists(conn, "cdf_schema_authority_events")?
            || sqlite_table_exists(conn, "cdf_schema_settlement_permits")?
            || sqlite_table_exists(conn, "cdf_schema_checkpoint_settlements")?
            || sqlite_table_exists(conn, "cdf_schema_promotions")? =>
        {
            return Err(CdfError::internal(format!(
                "schema authority SQLite schema is unversioned; expected current version {SCHEMA_AUTHORITY_SCHEMA_VERSION}"
            )));
        }
        None => {}
    }
    initialize_checkpoint_schema(conn)?;
    ensure_schema_version_table(conn)?;
    conn.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = DELETE;
        PRAGMA synchronous = NORMAL;

        CREATE TABLE IF NOT EXISTS cdf_schema_versions (
            authority_domain_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            environment TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            schema_hash TEXT NOT NULL,
            predecessor_schema_hash TEXT,
            created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
            version_json TEXT NOT NULL,
            PRIMARY KEY (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            ),
            FOREIGN KEY (
                authority_domain_id, project_id, environment, resource_id,
                predecessor_schema_hash
            ) REFERENCES cdf_schema_versions (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            )
        );

        CREATE TABLE IF NOT EXISTS cdf_schema_heads (
            authority_domain_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            environment TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            generation INTEGER NOT NULL CHECK (generation > 0),
            schema_hash TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('active', 'promoting')),
            promotion_id TEXT,
            promotion_from_schema_hash TEXT,
            promotion_to_schema_hash TEXT,
            promotion_lease_owner TEXT,
            promotion_fencing_token INTEGER,
            head_json TEXT NOT NULL,
            PRIMARY KEY (authority_domain_id, project_id, environment, resource_id),
            FOREIGN KEY (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            ) REFERENCES cdf_schema_versions (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            ),
            FOREIGN KEY (
                authority_domain_id, project_id, environment, resource_id,
                promotion_to_schema_hash
            ) REFERENCES cdf_schema_versions (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            ),
            CHECK (
                (status = 'active'
                    AND promotion_id IS NULL
                    AND promotion_from_schema_hash IS NULL
                    AND promotion_to_schema_hash IS NULL
                    AND promotion_lease_owner IS NULL
                    AND promotion_fencing_token IS NULL)
                OR
                (status = 'promoting'
                    AND promotion_id IS NOT NULL
                    AND promotion_from_schema_hash = schema_hash
                    AND promotion_to_schema_hash IS NOT NULL
                    AND promotion_to_schema_hash != schema_hash
                    AND promotion_lease_owner IS NOT NULL
                    AND promotion_fencing_token > 0)
            )
        );

        CREATE TABLE IF NOT EXISTS cdf_schema_authority_events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            authority_domain_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            environment TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL CHECK (ordinal > 0),
            generation INTEGER NOT NULL CHECK (generation > 0),
            schema_hash TEXT NOT NULL,
            recorded_at_ms INTEGER NOT NULL CHECK (recorded_at_ms >= 0),
            event_json TEXT NOT NULL,
            UNIQUE (authority_domain_id, project_id, environment, resource_id, ordinal),
            FOREIGN KEY (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            ) REFERENCES cdf_schema_versions (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            )
        );

        CREATE INDEX IF NOT EXISTS cdf_schema_authority_events_history
            ON cdf_schema_authority_events (
                authority_domain_id, project_id, environment, resource_id, ordinal
            );

        CREATE TABLE IF NOT EXISTS cdf_schema_settlement_permits (
            authority_domain_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            environment TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            generation INTEGER NOT NULL CHECK (generation > 0),
            schema_hash TEXT NOT NULL,
            acquired_at_ms INTEGER NOT NULL CHECK (acquired_at_ms >= 0),
            expires_at_ms INTEGER NOT NULL CHECK (expires_at_ms > acquired_at_ms),
            released INTEGER NOT NULL CHECK (released IN (0, 1)),
            permit_json TEXT NOT NULL,
            PRIMARY KEY (
                authority_domain_id, project_id, environment, resource_id, run_id
            ),
            FOREIGN KEY (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            ) REFERENCES cdf_schema_versions (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            )
        );

        CREATE INDEX IF NOT EXISTS cdf_schema_settlement_permits_live
            ON cdf_schema_settlement_permits (
                authority_domain_id, project_id, environment, resource_id, released, expires_at_ms
            );

        CREATE TABLE IF NOT EXISTS cdf_schema_checkpoint_settlements (
            checkpoint_id TEXT PRIMARY KEY,
            authority_domain_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            environment TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            generation INTEGER NOT NULL CHECK (generation > 0),
            schema_hash TEXT NOT NULL,
            settled_at_ms INTEGER NOT NULL CHECK (settled_at_ms >= 0),
            FOREIGN KEY (checkpoint_id) REFERENCES cdf_checkpoints (checkpoint_id),
            FOREIGN KEY (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            ) REFERENCES cdf_schema_versions (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            ),
            FOREIGN KEY (
                authority_domain_id, project_id, environment, resource_id, run_id
            ) REFERENCES cdf_schema_settlement_permits (
                authority_domain_id, project_id, environment, resource_id, run_id
            )
        );

        CREATE INDEX IF NOT EXISTS cdf_schema_checkpoint_settlements_authority
            ON cdf_schema_checkpoint_settlements (
                authority_domain_id, project_id, environment, resource_id, generation
            );

        CREATE TABLE IF NOT EXISTS cdf_schema_promotions (
            authority_domain_id TEXT NOT NULL,
            project_id TEXT NOT NULL,
            environment TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            promotion_id TEXT NOT NULL,
            phase TEXT NOT NULL CHECK (phase IN ('fenced', 'cutoff_established', 'published')),
            from_generation INTEGER NOT NULL CHECK (from_generation > 0),
            from_schema_hash TEXT NOT NULL,
            to_schema_hash TEXT NOT NULL CHECK (to_schema_hash != from_schema_hash),
            updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= 0),
            state_json TEXT NOT NULL,
            PRIMARY KEY (
                authority_domain_id, project_id, environment, resource_id, promotion_id
            ),
            FOREIGN KEY (
                authority_domain_id, project_id, environment, resource_id, from_schema_hash
            ) REFERENCES cdf_schema_versions (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            ),
            FOREIGN KEY (
                authority_domain_id, project_id, environment, resource_id, to_schema_hash
            ) REFERENCES cdf_schema_versions (
                authority_domain_id, project_id, environment, resource_id, schema_hash
            )
        );

        CREATE UNIQUE INDEX IF NOT EXISTS cdf_schema_promotions_one_open
            ON cdf_schema_promotions (authority_domain_id, project_id, environment, resource_id)
            WHERE phase != 'published';

        CREATE TRIGGER IF NOT EXISTS cdf_schema_versions_no_update
        BEFORE UPDATE ON cdf_schema_versions
        BEGIN SELECT RAISE(ABORT, 'schema versions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS cdf_schema_versions_no_delete
        BEFORE DELETE ON cdf_schema_versions
        BEGIN SELECT RAISE(ABORT, 'schema versions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS cdf_schema_authority_events_no_update
        BEFORE UPDATE ON cdf_schema_authority_events
        BEGIN SELECT RAISE(ABORT, 'schema authority history is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS cdf_schema_authority_events_no_delete
        BEFORE DELETE ON cdf_schema_authority_events
        BEGIN SELECT RAISE(ABORT, 'schema authority history is append-only'); END;
        ",
    )
    .map_err(sqlite_error)?;
    write_component_schema_version(
        conn,
        SCHEMA_AUTHORITY_COMPONENT,
        SCHEMA_AUTHORITY_SCHEMA_VERSION,
    )
}

fn validate_schema_version(conn: &Connection) -> Result<()> {
    match read_component_schema_version(conn, SCHEMA_AUTHORITY_COMPONENT)? {
        Some(SCHEMA_AUTHORITY_SCHEMA_VERSION) => validate_schema_structure(conn),
        Some(version) => Err(unsupported_schema_version(version)),
        None => Err(CdfError::internal(format!(
            "schema authority SQLite schema version is missing; expected {SCHEMA_AUTHORITY_SCHEMA_VERSION}"
        ))),
    }
}

fn validate_schema_structure(conn: &Connection) -> Result<()> {
    require_sqlite_tables(
        conn,
        "schema authority store",
        &[
            "cdf_schema_versions",
            "cdf_schema_heads",
            "cdf_schema_authority_events",
            "cdf_schema_settlement_permits",
            "cdf_schema_checkpoint_settlements",
            "cdf_schema_promotions",
        ],
    )
}

fn unsupported_schema_version(version: i64) -> CdfError {
    CdfError::internal(format!(
        "unsupported schema authority SQLite schema version {version}; current schema version {SCHEMA_AUTHORITY_SCHEMA_VERSION} is required, so recreate this pre-production state store"
    ))
}

fn insert_version(
    tx: &Transaction<'_>,
    key: &SchemaAuthorityKey,
    version: &SchemaVersion,
) -> Result<()> {
    version.validate()?;
    tx.execute(
        "INSERT INTO cdf_schema_versions (
            authority_domain_id, project_id, environment, resource_id, schema_hash,
            predecessor_schema_hash, created_at_ms, version_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            key.authority_domain_id.as_str(),
            key.project_id.as_str(),
            key.environment.as_str(),
            key.resource_id.as_str(),
            version.schema_hash.as_str(),
            version.predecessor.as_ref().map(SchemaHash::as_str),
            version.created_at_ms,
            encode_json(version)?,
        ],
    )
    .map(|_| ())
    .map_err(sqlite_error)
}

fn insert_version_if_exact(
    tx: &Transaction<'_>,
    key: &SchemaAuthorityKey,
    version: &SchemaVersion,
) -> Result<()> {
    match fetch_version(tx, key, &version.schema_hash)? {
        Some(stored) if stored == *version => Ok(()),
        Some(_) => Err(CdfError::contract(format!(
            "schema version {} already exists with different immutable content",
            version.schema_hash
        ))),
        None => insert_version(tx, key, version),
    }
}

fn insert_head(tx: &Transaction<'_>, head: &SchemaHead) -> Result<()> {
    head.validate()?;
    let columns = head_status_columns(&head.status)?;
    tx.execute(
        "INSERT INTO cdf_schema_heads (
            authority_domain_id, project_id, environment, resource_id, generation, schema_hash,
            status, promotion_id, promotion_from_schema_hash, promotion_to_schema_hash,
            promotion_lease_owner, promotion_fencing_token, head_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            head.key.authority_domain_id.as_str(),
            head.key.project_id.as_str(),
            head.key.environment.as_str(),
            head.key.resource_id.as_str(),
            u64_to_i64("schema authority generation", head.generation)?,
            head.schema_hash.as_str(),
            columns.status,
            columns.promotion_id,
            columns.from_schema_hash,
            columns.to_schema_hash,
            columns.lease_owner,
            columns.fencing_token,
            encode_json(head)?,
        ],
    )
    .map(|_| ())
    .map_err(sqlite_error)
}

fn update_head(tx: &Transaction<'_>, head: &SchemaHead) -> Result<()> {
    head.validate()?;
    let columns = head_status_columns(&head.status)?;
    let changed = tx
        .execute(
            "UPDATE cdf_schema_heads SET
                generation = ?, schema_hash = ?, status = ?, promotion_id = ?,
                promotion_from_schema_hash = ?, promotion_to_schema_hash = ?,
                promotion_lease_owner = ?, promotion_fencing_token = ?, head_json = ?
             WHERE authority_domain_id = ? AND project_id = ? AND environment = ? AND resource_id = ?",
            params![
                u64_to_i64("schema authority generation", head.generation)?,
                head.schema_hash.as_str(),
                columns.status,
                columns.promotion_id,
                columns.from_schema_hash,
                columns.to_schema_hash,
                columns.lease_owner,
                columns.fencing_token,
                encode_json(head)?,
                head.key.authority_domain_id.as_str(),
                head.key.project_id.as_str(),
                head.key.environment.as_str(),
                head.key.resource_id.as_str(),
            ],
        )
        .map_err(sqlite_error)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(CdfError::internal(
            "schema authority head disappeared during fenced transaction",
        ))
    }
}

struct HeadStatusColumns<'a> {
    status: &'static str,
    promotion_id: Option<&'a str>,
    from_schema_hash: Option<&'a str>,
    to_schema_hash: Option<&'a str>,
    lease_owner: Option<&'a str>,
    fencing_token: Option<i64>,
}

fn head_status_columns(status: &SchemaHeadStatus) -> Result<HeadStatusColumns<'_>> {
    Ok(match status {
        SchemaHeadStatus::Active => HeadStatusColumns {
            status: "active",
            promotion_id: None,
            from_schema_hash: None,
            to_schema_hash: None,
            lease_owner: None,
            fencing_token: None,
        },
        SchemaHeadStatus::Promoting {
            promotion_id,
            from_schema_hash,
            to_schema_hash,
            lease_owner,
            fencing_token,
        } => HeadStatusColumns {
            status: "promoting",
            promotion_id: Some(promotion_id.as_str()),
            from_schema_hash: Some(from_schema_hash.as_str()),
            to_schema_hash: Some(to_schema_hash.as_str()),
            lease_owner: Some(lease_owner.as_str()),
            fencing_token: Some(u64_to_i64(
                "schema promotion fencing token",
                fencing_token.get(),
            )?),
        },
    })
}

fn insert_event(tx: &Transaction<'_>, event: &SchemaAuthorityEvent) -> Result<()> {
    event.validate()?;
    tx.execute(
        "INSERT INTO cdf_schema_authority_events (
            authority_domain_id, project_id, environment, resource_id, ordinal, generation,
            schema_hash, recorded_at_ms, event_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            event.key.authority_domain_id.as_str(),
            event.key.project_id.as_str(),
            event.key.environment.as_str(),
            event.key.resource_id.as_str(),
            u64_to_i64("schema authority event ordinal", event.ordinal)?,
            u64_to_i64("schema authority event generation", event.generation)?,
            event.schema_hash.as_str(),
            event.recorded_at_ms,
            encode_json(event)?,
        ],
    )
    .map(|_| ())
    .map_err(sqlite_error)
}

fn next_event_ordinal(tx: &Transaction<'_>, key: &SchemaAuthorityKey) -> Result<u64> {
    let current = tx
        .query_row(
            "SELECT COALESCE(MAX(ordinal), 0) FROM cdf_schema_authority_events
             WHERE authority_domain_id = ? AND project_id = ? AND environment = ? AND resource_id = ?",
            params![
                key.authority_domain_id.as_str(),
                key.project_id.as_str(),
                key.environment.as_str(),
                key.resource_id.as_str(),
            ],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sqlite_error)?;
    let current = u64::try_from(current)
        .map_err(|_| CdfError::internal("schema authority event ordinal is negative"))?;
    current
        .checked_add(1)
        .ok_or_else(|| CdfError::internal("schema authority event ordinal overflow"))
}

#[derive(Debug)]
struct StoredVersionRow {
    schema_hash: String,
    predecessor_schema_hash: Option<String>,
    created_at_ms: i64,
    version_json: String,
}

fn stored_version_row(row: &Row<'_>) -> rusqlite::Result<StoredVersionRow> {
    Ok(StoredVersionRow {
        schema_hash: row.get(0)?,
        predecessor_schema_hash: row.get(1)?,
        created_at_ms: row.get(2)?,
        version_json: row.get(3)?,
    })
}

fn fetch_version(
    conn: &Connection,
    key: &SchemaAuthorityKey,
    schema_hash: &SchemaHash,
) -> Result<Option<SchemaVersion>> {
    let sql = format!(
        "{VERSION_SELECT} WHERE authority_domain_id = ? AND project_id = ? AND environment = ? AND resource_id = ? AND schema_hash = ?"
    );
    conn.query_row(
        &sql,
        params![
            key.authority_domain_id.as_str(),
            key.project_id.as_str(),
            key.environment.as_str(),
            key.resource_id.as_str(),
            schema_hash.as_str(),
        ],
        stored_version_row,
    )
    .optional()
    .map_err(sqlite_error)?
    .map(decode_version)
    .transpose()
}

fn decode_version(row: StoredVersionRow) -> Result<SchemaVersion> {
    private_state_decode(
        "decode CDF-managed schema version",
        (|| {
            let version: SchemaVersion =
                serde_json::from_str(&row.version_json).map_err(|error| {
                    CdfError::internal(format!("decode schema version JSON: {error}"))
                })?;
            version.validate()?;
            if version.schema_hash.as_str() != row.schema_hash
                || version.predecessor.as_ref().map(SchemaHash::as_str)
                    != row.predecessor_schema_hash.as_deref()
                || version.created_at_ms != row.created_at_ms
            {
                return Err(CdfError::internal(
                    "schema version scalar columns do not match serialized version",
                ));
            }
            Ok(version)
        })(),
    )
}

fn authority_has_any_version(conn: &Connection, key: &SchemaAuthorityKey) -> Result<bool> {
    conn.query_row(
        "SELECT 1 FROM cdf_schema_versions
         WHERE authority_domain_id = ? AND project_id = ? AND environment = ? AND resource_id = ?
         LIMIT 1",
        params![
            key.authority_domain_id.as_str(),
            key.project_id.as_str(),
            key.environment.as_str(),
            key.resource_id.as_str(),
        ],
        |_| Ok(()),
    )
    .optional()
    .map(|row| row.is_some())
    .map_err(sqlite_error)
}

#[derive(Debug)]
struct StoredHeadRow {
    authority_domain_id: String,
    project_id: String,
    environment: String,
    resource_id: String,
    generation: i64,
    schema_hash: String,
    status: String,
    promotion_id: Option<String>,
    from_schema_hash: Option<String>,
    to_schema_hash: Option<String>,
    lease_owner: Option<String>,
    fencing_token: Option<i64>,
    head_json: String,
}

fn stored_head_row(row: &Row<'_>) -> rusqlite::Result<StoredHeadRow> {
    Ok(StoredHeadRow {
        authority_domain_id: row.get(0)?,
        project_id: row.get(1)?,
        environment: row.get(2)?,
        resource_id: row.get(3)?,
        generation: row.get(4)?,
        schema_hash: row.get(5)?,
        status: row.get(6)?,
        promotion_id: row.get(7)?,
        from_schema_hash: row.get(8)?,
        to_schema_hash: row.get(9)?,
        lease_owner: row.get(10)?,
        fencing_token: row.get(11)?,
        head_json: row.get(12)?,
    })
}

fn fetch_head(conn: &Connection, key: &SchemaAuthorityKey) -> Result<Option<SchemaHead>> {
    let sql = format!(
        "{HEAD_SELECT} WHERE authority_domain_id = ? AND project_id = ? AND environment = ? AND resource_id = ?"
    );
    conn.query_row(
        &sql,
        params![
            key.authority_domain_id.as_str(),
            key.project_id.as_str(),
            key.environment.as_str(),
            key.resource_id.as_str(),
        ],
        stored_head_row,
    )
    .optional()
    .map_err(sqlite_error)?
    .map(decode_head)
    .transpose()
}

fn decode_head(row: StoredHeadRow) -> Result<SchemaHead> {
    private_state_decode(
        "decode CDF-managed schema authority head",
        (|| {
            let head: SchemaHead = serde_json::from_str(&row.head_json)
                .map_err(|error| CdfError::internal(format!("decode schema head JSON: {error}")))?;
            head.validate()?;
            let columns = head_status_columns(&head.status)?;
            if head.key.authority_domain_id.as_str() != row.authority_domain_id
                || head.key.project_id.as_str() != row.project_id
                || head.key.environment.as_str() != row.environment
                || head.key.resource_id.as_str() != row.resource_id
                || u64_to_i64("schema authority generation", head.generation)? != row.generation
                || head.schema_hash.as_str() != row.schema_hash
                || columns.status != row.status
                || columns.promotion_id != row.promotion_id.as_deref()
                || columns.from_schema_hash != row.from_schema_hash.as_deref()
                || columns.to_schema_hash != row.to_schema_hash.as_deref()
                || columns.lease_owner != row.lease_owner.as_deref()
                || columns.fencing_token != row.fencing_token
            {
                return Err(CdfError::internal(
                    "schema authority head scalar columns do not match serialized head",
                ));
            }
            Ok(head)
        })(),
    )
}

fn require_head_versions(conn: &Connection, head: &SchemaHead) -> Result<()> {
    let current = fetch_version(conn, &head.key, &head.schema_hash)?
        .ok_or_else(|| missing_head_version(head))?;
    if current.schema_hash != head.schema_hash {
        return Err(missing_head_version(head));
    }
    if let SchemaHeadStatus::Promoting { to_schema_hash, .. } = &head.status {
        fetch_version(conn, &head.key, to_schema_hash)?.ok_or_else(|| {
            CdfError::internal(format!(
                "promoting schema authority {} references missing target version {}",
                head.key.resource_id, to_schema_hash
            ))
        })?;
    }
    Ok(())
}

fn require_event_versions(conn: &Connection, event: &SchemaAuthorityEvent) -> Result<()> {
    fetch_version(conn, &event.key, &event.schema_hash)?.ok_or_else(|| {
        CdfError::internal(format!(
            "schema authority event {} for {} references missing version {}",
            event.ordinal, event.key.resource_id, event.schema_hash
        ))
    })?;
    if let SchemaAuthorityEventKind::PromotionBegun { to_schema_hash, .. } = &event.kind {
        fetch_version(conn, &event.key, to_schema_hash)?.ok_or_else(|| {
            CdfError::internal(format!(
                "schema authority promotion event {} for {} references missing target version {}",
                event.ordinal, event.key.resource_id, to_schema_hash
            ))
        })?;
    }
    Ok(())
}

fn missing_head_version(head: &SchemaHead) -> CdfError {
    CdfError::internal(format!(
        "schema authority {} in environment {} references missing or corrupt version {}",
        head.key.resource_id, head.key.environment, head.schema_hash
    ))
}

fn first_use_conflict(key: &SchemaAuthorityKey, existing: &SchemaHead) -> CdfError {
    CdfError::contract(format!(
        "schema authority for {} in environment {} is already established at generation {} with schema {}",
        key.resource_id, key.environment, existing.generation, existing.schema_hash
    ))
}

fn stale_head(expected: &SchemaHead, observed: &SchemaHead) -> CdfError {
    CdfError::contract(format!(
        "schema authority for {} changed from generation {} schema {} to generation {} schema {}",
        expected.key.resource_id,
        expected.generation,
        expected.schema_hash,
        observed.generation,
        observed.schema_hash
    ))
}

#[derive(Debug)]
struct StoredEventRow {
    authority_domain_id: String,
    project_id: String,
    environment: String,
    resource_id: String,
    ordinal: i64,
    generation: i64,
    schema_hash: String,
    recorded_at_ms: i64,
    event_json: String,
}

fn stored_event_row(row: &Row<'_>) -> rusqlite::Result<StoredEventRow> {
    Ok(StoredEventRow {
        authority_domain_id: row.get(0)?,
        project_id: row.get(1)?,
        environment: row.get(2)?,
        resource_id: row.get(3)?,
        ordinal: row.get(4)?,
        generation: row.get(5)?,
        schema_hash: row.get(6)?,
        recorded_at_ms: row.get(7)?,
        event_json: row.get(8)?,
    })
}

fn decode_event(row: StoredEventRow) -> Result<SchemaAuthorityEvent> {
    private_state_decode(
        "decode CDF-managed schema authority event",
        (|| {
            let event: SchemaAuthorityEvent =
                serde_json::from_str(&row.event_json).map_err(|error| {
                    CdfError::internal(format!("decode schema authority event JSON: {error}"))
                })?;
            event.validate()?;
            if event.key.authority_domain_id.as_str() != row.authority_domain_id
                || event.key.project_id.as_str() != row.project_id
                || event.key.environment.as_str() != row.environment
                || event.key.resource_id.as_str() != row.resource_id
                || u64_to_i64("schema authority event ordinal", event.ordinal)? != row.ordinal
                || u64_to_i64("schema authority event generation", event.generation)?
                    != row.generation
                || event.schema_hash.as_str() != row.schema_hash
                || event.recorded_at_ms != row.recorded_at_ms
            {
                return Err(CdfError::internal(
                    "schema authority event scalar columns do not match serialized event",
                ));
            }
            Ok(event)
        })(),
    )
}

fn validate_history(key: &SchemaAuthorityKey, events: &[SchemaAuthorityEvent]) -> Result<()> {
    let mut previous = None::<&SchemaAuthorityEvent>;
    for event in events {
        if &event.key != key {
            return Err(CdfError::internal(
                "schema authority history contains an event for another key",
            ));
        }
        if let Some(previous) = previous
            && (event.ordinal <= previous.ordinal || event.generation < previous.generation)
        {
            return Err(CdfError::internal(
                "schema authority history is not monotonically ordered",
            ));
        }
        previous = Some(event);
    }
    Ok(())
}

fn u64_to_i64(name: &str, value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| CdfError::internal(format!("{name} exceeds SQLite i64")))
}

fn permit_expiry(now_ms: i64, duration_ms: u64) -> Result<i64> {
    if duration_ms == 0 {
        return Err(CdfError::contract(
            "schema settlement permit duration must be positive",
        ));
    }
    now_ms
        .checked_add(u64_to_i64(
            "schema settlement permit duration",
            duration_ms,
        )?)
        .ok_or_else(|| CdfError::contract("schema settlement permit expiry exceeds SQLite range"))
}

#[derive(Debug)]
struct StoredPermitRow {
    authority_domain_id: String,
    project_id: String,
    environment: String,
    resource_id: String,
    run_id: String,
    generation: i64,
    schema_hash: String,
    acquired_at_ms: i64,
    expires_at_ms: i64,
    released: i64,
    permit_json: String,
}

fn stored_permit_row(row: &Row<'_>) -> rusqlite::Result<StoredPermitRow> {
    Ok(StoredPermitRow {
        authority_domain_id: row.get(0)?,
        project_id: row.get(1)?,
        environment: row.get(2)?,
        resource_id: row.get(3)?,
        run_id: row.get(4)?,
        generation: row.get(5)?,
        schema_hash: row.get(6)?,
        acquired_at_ms: row.get(7)?,
        expires_at_ms: row.get(8)?,
        released: row.get(9)?,
        permit_json: row.get(10)?,
    })
}

fn fetch_permit(
    conn: &Connection,
    key: &SchemaAuthorityKey,
    run_id: &RunId,
) -> Result<Option<(SchemaSettlementPermit, bool)>> {
    let sql = format!(
        "{PERMIT_SELECT} WHERE authority_domain_id = ? AND project_id = ? AND environment = ? AND resource_id = ? AND run_id = ?"
    );
    conn.query_row(
        &sql,
        params![
            key.authority_domain_id.as_str(),
            key.project_id.as_str(),
            key.environment.as_str(),
            key.resource_id.as_str(),
            run_id.as_str(),
        ],
        stored_permit_row,
    )
    .optional()
    .map_err(sqlite_error)?
    .map(decode_permit)
    .transpose()
}

fn decode_permit(row: StoredPermitRow) -> Result<(SchemaSettlementPermit, bool)> {
    private_state_decode(
        "decode CDF-managed schema settlement permit",
        (|| {
            let permit: SchemaSettlementPermit =
                serde_json::from_str(&row.permit_json).map_err(|error| {
                    CdfError::internal(format!("decode settlement permit: {error}"))
                })?;
            permit.validate()?;
            if permit.key.authority_domain_id.as_str() != row.authority_domain_id
                || permit.key.project_id.as_str() != row.project_id
                || permit.key.environment.as_str() != row.environment
                || permit.key.resource_id.as_str() != row.resource_id
                || permit.run_id.as_str() != row.run_id
                || u64_to_i64("schema settlement generation", permit.generation)? != row.generation
                || permit.schema_hash.as_str() != row.schema_hash
                || permit.acquired_at_ms != row.acquired_at_ms
                || permit.expires_at_ms != row.expires_at_ms
                || !matches!(row.released, 0 | 1)
            {
                return Err(CdfError::internal(
                    "schema settlement permit columns do not match serialized authority",
                ));
            }
            Ok((permit, row.released == 1))
        })(),
    )
}

fn insert_permit(tx: &Transaction<'_>, permit: &SchemaSettlementPermit) -> Result<()> {
    permit.validate()?;
    tx.execute(
        "INSERT INTO cdf_schema_settlement_permits (
            authority_domain_id, project_id, environment, resource_id, run_id, generation,
            schema_hash, acquired_at_ms, expires_at_ms, released, permit_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
        params![
            permit.key.authority_domain_id.as_str(),
            permit.key.project_id.as_str(),
            permit.key.environment.as_str(),
            permit.key.resource_id.as_str(),
            permit.run_id.as_str(),
            u64_to_i64("schema settlement generation", permit.generation)?,
            permit.schema_hash.as_str(),
            permit.acquired_at_ms,
            permit.expires_at_ms,
            encode_json(permit)?,
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn head_allows_permit(head: &SchemaHead, permit: &SchemaSettlementPermit) -> bool {
    if head.key != permit.key
        || head.generation != permit.generation
        || head.schema_hash != permit.schema_hash
    {
        return false;
    }
    match &head.status {
        SchemaHeadStatus::Active => true,
        SchemaHeadStatus::Promoting {
            from_schema_hash, ..
        } => from_schema_hash == &permit.schema_hash,
    }
}

fn require_live_permit(
    conn: &Connection,
    permit: &SchemaSettlementPermit,
    now_ms: i64,
) -> Result<()> {
    let Some((current, released)) = fetch_permit(conn, &permit.key, &permit.run_id)? else {
        return Err(CdfError::transient(
            "schema settlement permit does not exist",
        ));
    };
    if current != *permit || released || current.expires_at_ms <= now_ms {
        return Err(CdfError::transient(
            "schema settlement permit is stale, released, or expired",
        ));
    }
    let head = fetch_head(conn, &permit.key)?
        .ok_or_else(|| CdfError::contract("schema settlement authority head is missing"))?;
    if !head_allows_permit(&head, permit) {
        return Err(CdfError::contract(format!(
            "schema settlement permit for generation {} and schema {} is not current authority",
            permit.generation, permit.schema_hash
        )));
    }
    Ok(())
}

fn promotion_phase(phase: SchemaPromotionLifecyclePhase) -> &'static str {
    match phase {
        SchemaPromotionLifecyclePhase::Fenced => "fenced",
        SchemaPromotionLifecyclePhase::CutoffEstablished => "cutoff_established",
        SchemaPromotionLifecyclePhase::Published => "published",
    }
}

#[derive(Debug)]
struct StoredPromotionRow {
    authority_domain_id: String,
    project_id: String,
    environment: String,
    resource_id: String,
    promotion_id: String,
    phase: String,
    from_generation: i64,
    from_schema_hash: String,
    to_schema_hash: String,
    updated_at_ms: i64,
    state_json: String,
}

fn stored_promotion_row(row: &Row<'_>) -> rusqlite::Result<StoredPromotionRow> {
    Ok(StoredPromotionRow {
        authority_domain_id: row.get(0)?,
        project_id: row.get(1)?,
        environment: row.get(2)?,
        resource_id: row.get(3)?,
        promotion_id: row.get(4)?,
        phase: row.get(5)?,
        from_generation: row.get(6)?,
        from_schema_hash: row.get(7)?,
        to_schema_hash: row.get(8)?,
        updated_at_ms: row.get(9)?,
        state_json: row.get(10)?,
    })
}

fn fetch_promotion(
    conn: &Connection,
    key: &SchemaAuthorityKey,
    promotion_id: &cdf_kernel::PromotionId,
) -> Result<Option<SchemaPromotionState>> {
    let sql = format!(
        "{PROMOTION_SELECT} WHERE authority_domain_id = ? AND project_id = ? AND environment = ? AND resource_id = ? AND promotion_id = ?"
    );
    conn.query_row(
        &sql,
        params![
            key.authority_domain_id.as_str(),
            key.project_id.as_str(),
            key.environment.as_str(),
            key.resource_id.as_str(),
            promotion_id.as_str(),
        ],
        stored_promotion_row,
    )
    .optional()
    .map_err(sqlite_error)?
    .map(decode_promotion)
    .transpose()
}

fn decode_promotion(row: StoredPromotionRow) -> Result<SchemaPromotionState> {
    private_state_decode(
        "decode CDF-managed schema promotion state",
        (|| {
            let state: SchemaPromotionState =
                serde_json::from_str(&row.state_json).map_err(|error| {
                    CdfError::internal(format!("decode schema promotion state: {error}"))
                })?;
            state.validate()?;
            if state.key.authority_domain_id.as_str() != row.authority_domain_id
                || state.key.project_id.as_str() != row.project_id
                || state.key.environment.as_str() != row.environment
                || state.key.resource_id.as_str() != row.resource_id
                || state.plan.promotion_id.as_str() != row.promotion_id
                || promotion_phase(state.phase) != row.phase
                || u64_to_i64("schema promotion generation", state.from_generation)?
                    != row.from_generation
                || state.from_schema_hash.as_str() != row.from_schema_hash
                || state.to_schema_hash.as_str() != row.to_schema_hash
                || state.updated_at_ms != row.updated_at_ms
            {
                return Err(CdfError::internal(
                    "schema promotion scalar columns do not match serialized authority",
                ));
            }
            Ok(state)
        })(),
    )
}

fn insert_promotion(tx: &Transaction<'_>, state: &SchemaPromotionState) -> Result<()> {
    state.validate()?;
    tx.execute(
        "INSERT INTO cdf_schema_promotions (
            authority_domain_id, project_id, environment, resource_id, promotion_id, phase,
            from_generation, from_schema_hash, to_schema_hash, updated_at_ms, state_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            state.key.authority_domain_id.as_str(),
            state.key.project_id.as_str(),
            state.key.environment.as_str(),
            state.key.resource_id.as_str(),
            state.plan.promotion_id.as_str(),
            promotion_phase(state.phase),
            u64_to_i64("schema promotion generation", state.from_generation)?,
            state.from_schema_hash.as_str(),
            state.to_schema_hash.as_str(),
            state.updated_at_ms,
            encode_json(state)?,
        ],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn update_promotion(tx: &Transaction<'_>, state: &SchemaPromotionState) -> Result<()> {
    state.validate()?;
    let updated = tx
        .execute(
            "UPDATE cdf_schema_promotions SET phase = ?, updated_at_ms = ?, state_json = ?
             WHERE authority_domain_id = ? AND project_id = ? AND environment = ?
               AND resource_id = ? AND promotion_id = ? AND from_generation = ?
               AND from_schema_hash = ? AND to_schema_hash = ?",
            params![
                promotion_phase(state.phase),
                state.updated_at_ms,
                encode_json(state)?,
                state.key.authority_domain_id.as_str(),
                state.key.project_id.as_str(),
                state.key.environment.as_str(),
                state.key.resource_id.as_str(),
                state.plan.promotion_id.as_str(),
                u64_to_i64("schema promotion generation", state.from_generation)?,
                state.from_schema_hash.as_str(),
                state.to_schema_hash.as_str(),
            ],
        )
        .map_err(sqlite_error)?;
    if updated != 1 {
        return Err(CdfError::internal(
            "schema promotion state update did not match one current record",
        ));
    }
    Ok(())
}

fn live_permit_count(conn: &Connection, key: &SchemaAuthorityKey, now_ms: i64) -> Result<u64> {
    let count = conn
        .query_row(
            "SELECT COUNT(*) FROM cdf_schema_settlement_permits
             WHERE authority_domain_id = ? AND project_id = ? AND environment = ?
               AND resource_id = ? AND released = 0 AND expires_at_ms > ?",
            params![
                key.authority_domain_id.as_str(),
                key.project_id.as_str(),
                key.environment.as_str(),
                key.resource_id.as_str(),
                now_ms,
            ],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sqlite_error)?;
    u64::try_from(count).map_err(|_| CdfError::internal("live permit count is negative"))
}

fn promotion_cutoff(
    conn: &Connection,
    state: &SchemaPromotionState,
    established_at_ms: i64,
) -> Result<SchemaPromotionCutoff> {
    let mut statement = conn
        .prepare(
            "SELECT settlement.checkpoint_id, checkpoint.package_hash, settlement.run_id
             FROM cdf_schema_checkpoint_settlements AS settlement
             JOIN cdf_checkpoints AS checkpoint ON checkpoint.checkpoint_id = settlement.checkpoint_id
             WHERE settlement.authority_domain_id = ? AND settlement.project_id = ?
               AND settlement.environment = ? AND settlement.resource_id = ?
               AND settlement.generation = ? AND settlement.schema_hash = ?
               AND checkpoint.status = 'committed'
             ORDER BY settlement.checkpoint_id",
        )
        .map_err(sqlite_error)?;
    let checkpoints = statement
        .query_map(
            params![
                state.key.authority_domain_id.as_str(),
                state.key.project_id.as_str(),
                state.key.environment.as_str(),
                state.key.resource_id.as_str(),
                u64_to_i64("schema promotion generation", state.from_generation)?,
                state.from_schema_hash.as_str(),
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .map_err(sqlite_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sqlite_error)?
        .into_iter()
        .map(|(checkpoint_id, package_hash, run_id)| {
            Ok(SchemaPromotionCutoffCheckpoint {
                checkpoint_id: CheckpointId::new(checkpoint_id)?,
                package_hash: cdf_kernel::PackageHash::new(package_hash)?,
                run_id: RunId::new(run_id)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let cutoff = SchemaPromotionCutoff {
        generation: state.from_generation,
        schema_hash: state.from_schema_hash.clone(),
        established_at_ms,
        checkpoints,
    };
    cutoff.validate()?;
    Ok(cutoff)
}

impl SchemaAuthorityStore for SqliteSchemaAuthorityStore {
    fn authority_domain_id(&self) -> LeaseAuthorityDomainId {
        self.authority_domain_id.clone()
    }

    fn head(&self, key: &SchemaAuthorityKey) -> Result<Option<SchemaHead>> {
        self.validate_key_domain(key)?;
        let conn = self.lock()?;
        let head = fetch_head(&conn, key)?;
        if let Some(head) = &head {
            require_head_versions(&conn, head)?;
        } else if authority_has_any_version(&conn, key)? {
            return Err(CdfError::internal(format!(
                "schema authority {} in environment {} has versions but no head",
                key.resource_id, key.environment
            )));
        }
        Ok(head)
    }

    fn version(
        &self,
        key: &SchemaAuthorityKey,
        schema_hash: &SchemaHash,
    ) -> Result<Option<SchemaVersion>> {
        self.validate_key_domain(key)?;
        let conn = self.lock()?;
        fetch_version(&conn, key, schema_hash)
    }

    fn establish_batch_if_absent(
        &self,
        establishments: Vec<SchemaAuthorityEstablishment>,
    ) -> Result<Vec<SchemaHead>> {
        self.establish_batch(Vec::new(), establishments, false)
    }

    fn establish_batch_checked(
        &self,
        checks: Vec<SchemaAuthorityCheck>,
        establishments: Vec<SchemaAuthorityEstablishment>,
    ) -> Result<Vec<SchemaHead>> {
        self.establish_batch(checks, establishments, false)
    }

    fn begin_promotion(
        &self,
        expected: &SchemaHead,
        proposed: SchemaVersion,
        plan: SchemaPromotionPlanState,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaPromotionState> {
        expected.validate()?;
        proposed.validate()?;
        plan.validate()?;
        self.validate_key_domain(&expected.key)?;
        self.validate_fence(&expected.key, fence)?;
        if !matches!(expected.status, SchemaHeadStatus::Active) {
            return Err(CdfError::contract(
                "schema promotion must begin from an active head",
            ));
        }
        if proposed.predecessor.as_ref() != Some(&expected.schema_hash) {
            return Err(CdfError::contract(
                "proposed schema version predecessor does not match the active head",
            ));
        }
        if !matches!(
            &proposed.provenance,
            SchemaVersionProvenance::Promotion { promotion_id }
                if promotion_id == &fence.promotion_id
        ) {
            return Err(CdfError::contract(
                "proposed schema version provenance does not match the promotion fence",
            ));
        }
        if plan.promotion_id != fence.promotion_id {
            return Err(CdfError::contract(
                "persisted schema promotion plan does not match the promotion fence",
            ));
        }

        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let now_ms = self.clock.now_ms()?;
        assert_current_lease_at(&tx, &fence.lease, now_ms)?;
        let current = fetch_head(&tx, &expected.key)?.ok_or_else(|| {
            CdfError::contract("schema promotion expected an active authority head")
        })?;
        if current != *expected {
            return Err(stale_head(expected, &current));
        }
        require_head_versions(&tx, &current)?;
        insert_version_if_exact(&tx, &expected.key, &proposed)?;
        let promoting = SchemaHead {
            key: expected.key.clone(),
            generation: expected.generation,
            schema_hash: expected.schema_hash.clone(),
            status: SchemaHeadStatus::Promoting {
                promotion_id: fence.promotion_id.clone(),
                from_schema_hash: expected.schema_hash.clone(),
                to_schema_hash: proposed.schema_hash.clone(),
                lease_owner: fence.lease.owner.clone(),
                fencing_token: fence.lease.fencing_token,
            },
        };
        promoting.validate()?;
        update_head(&tx, &promoting)?;
        let state = SchemaPromotionState {
            key: expected.key.clone(),
            plan,
            from_generation: expected.generation,
            from_schema_hash: expected.schema_hash.clone(),
            to_schema_hash: proposed.schema_hash.clone(),
            phase: SchemaPromotionLifecyclePhase::Fenced,
            cutoff: None,
            target_settlements: Vec::new(),
            published_generation: None,
            updated_at_ms: now_ms,
        };
        insert_promotion(&tx, &state)?;
        let event = SchemaAuthorityEvent {
            key: expected.key.clone(),
            ordinal: next_event_ordinal(&tx, &expected.key)?,
            generation: expected.generation,
            schema_hash: expected.schema_hash.clone(),
            recorded_at_ms: now_ms,
            kind: SchemaAuthorityEventKind::PromotionBegun {
                promotion_id: fence.promotion_id.clone(),
                from_schema_hash: expected.schema_hash.clone(),
                to_schema_hash: proposed.schema_hash,
                lease_owner: fence.lease.owner.clone(),
                fencing_token: fence.lease.fencing_token,
            },
        };
        insert_event(&tx, &event)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(state)
    }

    fn promotion_state(
        &self,
        key: &SchemaAuthorityKey,
        promotion_id: &cdf_kernel::PromotionId,
    ) -> Result<Option<SchemaPromotionState>> {
        self.validate_key_domain(key)?;
        cdf_kernel::PromotionId::new(promotion_id.as_str()).map(drop)?;
        let conn = self.lock()?;
        fetch_promotion(&conn, key, promotion_id)
    }

    fn establish_promotion_cutoff(
        &self,
        expected_promoting: &SchemaHead,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaPromotionState> {
        expected_promoting.validate()?;
        self.validate_key_domain(&expected_promoting.key)?;
        self.validate_fence(&expected_promoting.key, fence)?;
        let SchemaHeadStatus::Promoting { promotion_id, .. } = &expected_promoting.status else {
            return Err(CdfError::contract(
                "schema promotion cutoff requires a promoting head",
            ));
        };
        if promotion_id != &fence.promotion_id {
            return Err(CdfError::contract(
                "schema promotion cutoff does not match the supplied fence",
            ));
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let now_ms = self.clock.now_ms()?;
        assert_current_lease_at(&tx, &fence.lease, now_ms)?;
        let current = fetch_head(&tx, &expected_promoting.key)?
            .ok_or_else(|| CdfError::contract("schema promotion authority head is missing"))?;
        if current != *expected_promoting {
            return Err(stale_head(expected_promoting, &current));
        }
        let mut state = fetch_promotion(&tx, &expected_promoting.key, promotion_id)?
            .ok_or_else(|| CdfError::internal("promoting head has no persisted promotion state"))?;
        if state.phase != SchemaPromotionLifecyclePhase::Fenced {
            return Ok(state);
        }
        let live_permits = live_permit_count(&tx, &expected_promoting.key, now_ms)?;
        if live_permits != 0 {
            return Err(CdfError::transient(format!(
                "schema promotion is waiting for {live_permits} earlier run settlement permit(s) to commit, release, or expire"
            )));
        }
        let cutoff = promotion_cutoff(&tx, &state, now_ms)?;
        let checkpoint_count = u64::try_from(cutoff.checkpoints.len())
            .map_err(|_| CdfError::internal("promotion cutoff checkpoint count overflow"))?;
        state.phase = SchemaPromotionLifecyclePhase::CutoffEstablished;
        state.cutoff = Some(cutoff);
        state.updated_at_ms = now_ms;
        update_promotion(&tx, &state)?;
        insert_event(
            &tx,
            &SchemaAuthorityEvent {
                key: state.key.clone(),
                ordinal: next_event_ordinal(&tx, &state.key)?,
                generation: state.from_generation,
                schema_hash: state.from_schema_hash.clone(),
                recorded_at_ms: now_ms,
                kind: SchemaAuthorityEventKind::PromotionCutoffEstablished {
                    promotion_id: promotion_id.clone(),
                    checkpoint_count,
                },
            },
        )?;
        tx.commit().map_err(sqlite_error)?;
        Ok(state)
    }

    fn commit_promotion_target(
        &self,
        expected_promoting: &SchemaHead,
        fence: &SchemaPromotionFence,
        target: &SchemaPromotionTarget,
        checkpoint_id: &CheckpointId,
        receipt: Receipt,
    ) -> Result<SchemaPromotionState> {
        expected_promoting.validate()?;
        target.validate()?;
        self.validate_key_domain(&expected_promoting.key)?;
        self.validate_fence(&expected_promoting.key, fence)?;
        let SchemaHeadStatus::Promoting { promotion_id, .. } = &expected_promoting.status else {
            return Err(CdfError::contract(
                "schema promotion target settlement requires a promoting head",
            ));
        };
        if promotion_id != &fence.promotion_id
            || receipt.destination != target.destination_id
            || receipt.target != target.target
        {
            return Err(CdfError::contract(
                "schema promotion target receipt does not match the head, fence, and target",
            ));
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let now_ms = self.clock.now_ms()?;
        assert_current_lease_at(&tx, &fence.lease, now_ms)?;
        let current = fetch_head(&tx, &expected_promoting.key)?
            .ok_or_else(|| CdfError::contract("schema promotion authority head is missing"))?;
        if current != *expected_promoting {
            return Err(stale_head(expected_promoting, &current));
        }
        let mut state = fetch_promotion(&tx, &expected_promoting.key, promotion_id)?
            .ok_or_else(|| CdfError::internal("promoting head has no persisted promotion state"))?;
        if state.phase != SchemaPromotionLifecyclePhase::CutoffEstablished {
            return Err(CdfError::contract(
                "schema promotion target cannot settle before the correction cutoff",
            ));
        }
        if !state.plan.required_targets.contains(target) {
            return Err(CdfError::contract(format!(
                "destination {} target {} is not required by the persisted promotion plan",
                target.destination_id, target.target
            )));
        }
        if let Some(existing) = state
            .target_settlements
            .iter()
            .find(|settlement| settlement.target == *target)
        {
            if existing.checkpoint_id == *checkpoint_id
                && existing.receipt_id == receipt.receipt_id
                && existing.correction_package_hash == receipt.package_hash
            {
                return Ok(state);
            }
            return Err(CdfError::contract(format!(
                "destination {} target {} already has conflicting promotion settlement authority",
                target.destination_id, target.target
            )));
        }
        let checkpoint = CheckpointSql::fetch_by_id_tx(&tx, checkpoint_id)?.ok_or_else(|| {
            CdfError::data(format!(
                "promotion checkpoint {checkpoint_id} does not exist"
            ))
        })?;
        if checkpoint.delta.resource_id != expected_promoting.key.resource_id
            || checkpoint.delta.scope != fence.lease.scope
        {
            return Err(CdfError::contract(
                "promotion checkpoint resource/scope does not match schema promotion authority",
            ));
        }
        commit_checkpoint_tx(&tx, checkpoint_id, &receipt)?;
        state
            .target_settlements
            .push(SchemaPromotionTargetSettlement {
                target: target.clone(),
                correction_package_hash: receipt.package_hash.clone(),
                receipt_id: receipt.receipt_id.clone(),
                checkpoint_id: checkpoint_id.clone(),
                settled_at_ms: receipt.committed_at_ms,
            });
        state
            .target_settlements
            .sort_by(|left, right| left.target.cmp(&right.target));
        state.updated_at_ms = now_ms;
        update_promotion(&tx, &state)?;
        insert_event(
            &tx,
            &SchemaAuthorityEvent {
                key: state.key.clone(),
                ordinal: next_event_ordinal(&tx, &state.key)?,
                generation: state.from_generation,
                schema_hash: state.from_schema_hash.clone(),
                recorded_at_ms: now_ms,
                kind: SchemaAuthorityEventKind::PromotionTargetSettled {
                    promotion_id: promotion_id.clone(),
                    destination_id: target.destination_id.clone(),
                    target: target.target.clone(),
                    checkpoint_id: checkpoint_id.clone(),
                    receipt_id: receipt.receipt_id,
                },
            },
        )?;
        tx.commit().map_err(sqlite_error)?;
        Ok(state)
    }

    fn publish_promotion(
        &self,
        expected_promoting: &SchemaHead,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaHead> {
        expected_promoting.validate()?;
        self.validate_key_domain(&expected_promoting.key)?;
        self.validate_fence(&expected_promoting.key, fence)?;
        let SchemaHeadStatus::Promoting {
            promotion_id,
            from_schema_hash,
            to_schema_hash,
            lease_owner,
            fencing_token,
        } = &expected_promoting.status
        else {
            return Err(CdfError::contract(
                "schema promotion publication requires a promoting head",
            ));
        };
        if promotion_id != &fence.promotion_id
            || lease_owner != &fence.lease.owner
            || fencing_token != &fence.lease.fencing_token
        {
            return Err(CdfError::contract(
                "schema promotion head does not match the supplied fence",
            ));
        }

        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let now_ms = self.clock.now_ms()?;
        assert_current_lease_at(&tx, &fence.lease, now_ms)?;
        let current = fetch_head(&tx, &expected_promoting.key)?
            .ok_or_else(|| CdfError::contract("schema promotion expected an authority head"))?;
        if current != *expected_promoting {
            return Err(stale_head(expected_promoting, &current));
        }
        require_head_versions(&tx, &current)?;
        let mut state = fetch_promotion(&tx, &current.key, promotion_id)?
            .ok_or_else(|| CdfError::internal("promoting head has no persisted promotion state"))?;
        if state.phase != SchemaPromotionLifecyclePhase::CutoffEstablished || state.cutoff.is_none()
        {
            return Err(CdfError::contract(
                "schema promotion cannot publish before its committed-run cutoff is established",
            ));
        }
        if live_permit_count(&tx, &current.key, now_ms)? != 0 {
            return Err(CdfError::transient(
                "schema promotion cannot publish while an earlier run settlement permit is live",
            ));
        }
        let settled_targets = state
            .target_settlements
            .iter()
            .map(|settlement| settlement.target.clone())
            .collect::<Vec<_>>();
        if settled_targets != state.plan.required_targets {
            return Err(CdfError::contract(
                "schema promotion cannot publish until every planned target is settled",
            ));
        }
        for settlement in &state.target_settlements {
            let checkpoint = CheckpointSql::fetch_by_id_tx(&tx, &settlement.checkpoint_id)?
                .ok_or_else(|| {
                    CdfError::internal(format!(
                        "promotion target checkpoint {} is missing",
                        settlement.checkpoint_id
                    ))
                })?;
            if checkpoint.status != CheckpointStatus::Committed
                || checkpoint.receipt.as_ref().is_none_or(|receipt| {
                    receipt.receipt_id != settlement.receipt_id
                        || receipt.package_hash != settlement.correction_package_hash
                })
            {
                return Err(CdfError::contract(format!(
                    "promotion target checkpoint {} is not committed with its persisted receipt and package authority",
                    settlement.checkpoint_id
                )));
            }
        }
        let next_generation = current
            .generation
            .checked_add(1)
            .ok_or_else(|| CdfError::internal("schema authority generation overflow"))?;
        let active =
            SchemaHead::active(current.key.clone(), next_generation, to_schema_hash.clone())?;
        update_head(&tx, &active)?;
        state.phase = SchemaPromotionLifecyclePhase::Published;
        state.published_generation = Some(next_generation);
        state.updated_at_ms = now_ms;
        update_promotion(&tx, &state)?;
        insert_event(
            &tx,
            &SchemaAuthorityEvent {
                key: current.key,
                ordinal: next_event_ordinal(&tx, &active.key)?,
                generation: active.generation,
                schema_hash: active.schema_hash.clone(),
                recorded_at_ms: now_ms,
                kind: SchemaAuthorityEventKind::PromotionPublished {
                    promotion_id: promotion_id.clone(),
                    from_schema_hash: from_schema_hash.clone(),
                    to_schema_hash: to_schema_hash.clone(),
                    lease_owner: lease_owner.clone(),
                    fencing_token: *fencing_token,
                },
            },
        )?;
        tx.commit().map_err(sqlite_error)?;
        Ok(active)
    }

    fn history(&self, key: &SchemaAuthorityKey, limit: u32) -> Result<Vec<SchemaAuthorityEvent>> {
        self.validate_key_domain(key)?;
        if limit == 0 || limit > MAX_SCHEMA_AUTHORITY_HISTORY_LIMIT {
            return Err(CdfError::contract(format!(
                "schema authority history limit must be between 1 and {MAX_SCHEMA_AUTHORITY_HISTORY_LIMIT}"
            )));
        }
        let conn = self.lock()?;
        let sql = format!(
            "SELECT * FROM ({EVENT_SELECT} WHERE authority_domain_id = ? AND project_id = ? AND environment = ? AND resource_id = ? ORDER BY ordinal DESC LIMIT ?) ORDER BY ordinal"
        );
        let mut statement = conn.prepare(&sql).map_err(sqlite_error)?;
        let rows = statement
            .query_map(
                params![
                    key.authority_domain_id.as_str(),
                    key.project_id.as_str(),
                    key.environment.as_str(),
                    key.resource_id.as_str(),
                    i64::from(limit),
                ],
                stored_event_row,
            )
            .map_err(sqlite_error)?;
        let events = rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sqlite_error)?
            .into_iter()
            .map(decode_event)
            .collect::<Result<Vec<_>>>()?;
        validate_history(key, &events)?;
        for event in &events {
            require_event_versions(&conn, event)?;
        }
        Ok(events)
    }
}

impl SchemaSettlementStore for SqliteSchemaAuthorityStore {
    fn acquire_run_permit(
        &self,
        expected_active: &SchemaHead,
        run_id: RunId,
        permit_duration_ms: u64,
    ) -> Result<SchemaSettlementPermit> {
        expected_active.validate()?;
        self.validate_key_domain(&expected_active.key)?;
        RunId::new(run_id.as_str()).map(drop)?;
        if !matches!(expected_active.status, SchemaHeadStatus::Active) {
            return Err(CdfError::contract(
                "schema settlement permit requires an active authority head",
            ));
        }

        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let now_ms = self.clock.now_ms()?;
        let current = fetch_head(&tx, &expected_active.key)?
            .ok_or_else(|| CdfError::contract("schema settlement authority head is missing"))?;
        if current != *expected_active {
            return Err(stale_head(expected_active, &current));
        }
        if let Some((existing, released)) = fetch_permit(&tx, &expected_active.key, &run_id)? {
            if !released && existing.expires_at_ms > now_ms {
                return Ok(existing);
            }
            return Err(CdfError::contract(format!(
                "run {run_id} already consumed or expired its schema settlement permit"
            )));
        }
        let permit = SchemaSettlementPermit {
            key: expected_active.key.clone(),
            run_id,
            generation: expected_active.generation,
            schema_hash: expected_active.schema_hash.clone(),
            acquired_at_ms: now_ms,
            expires_at_ms: permit_expiry(now_ms, permit_duration_ms)?,
        };
        insert_permit(&tx, &permit)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(permit)
    }

    fn renew_run_permit(
        &self,
        permit: &SchemaSettlementPermit,
        permit_duration_ms: u64,
    ) -> Result<SchemaSettlementPermit> {
        permit.validate()?;
        self.validate_key_domain(&permit.key)?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let now_ms = self.clock.now_ms()?;
        require_live_permit(&tx, permit, now_ms)?;
        let renewed = SchemaSettlementPermit {
            expires_at_ms: permit_expiry(now_ms, permit_duration_ms)?,
            ..permit.clone()
        };
        tx.execute(
            "UPDATE cdf_schema_settlement_permits
             SET expires_at_ms = ?, permit_json = ?
             WHERE authority_domain_id = ? AND project_id = ? AND environment = ?
               AND resource_id = ? AND run_id = ? AND released = 0",
            params![
                renewed.expires_at_ms,
                encode_json(&renewed)?,
                renewed.key.authority_domain_id.as_str(),
                renewed.key.project_id.as_str(),
                renewed.key.environment.as_str(),
                renewed.key.resource_id.as_str(),
                renewed.run_id.as_str(),
            ],
        )
        .map_err(sqlite_error)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(renewed)
    }

    fn assert_run_permit(&self, permit: &SchemaSettlementPermit) -> Result<()> {
        permit.validate()?;
        self.validate_key_domain(&permit.key)?;
        let conn = self.lock()?;
        require_live_permit(&conn, permit, self.clock.now_ms()?)
    }

    fn release_run_permit(&self, permit: &SchemaSettlementPermit) -> Result<()> {
        permit.validate()?;
        self.validate_key_domain(&permit.key)?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let Some((current, released)) = fetch_permit(&tx, &permit.key, &permit.run_id)? else {
            return Err(CdfError::transient(
                "schema settlement permit does not exist",
            ));
        };
        if current != *permit {
            return Err(CdfError::transient("schema settlement permit is stale"));
        }
        if !released {
            tx.execute(
                "UPDATE cdf_schema_settlement_permits SET released = 1
                 WHERE authority_domain_id = ? AND project_id = ? AND environment = ?
                   AND resource_id = ? AND run_id = ? AND released = 0",
                params![
                    permit.key.authority_domain_id.as_str(),
                    permit.key.project_id.as_str(),
                    permit.key.environment.as_str(),
                    permit.key.resource_id.as_str(),
                    permit.run_id.as_str(),
                ],
            )
            .map_err(sqlite_error)?;
        }
        tx.commit().map_err(sqlite_error)
    }

    fn commit_run_checkpoint(
        &self,
        permit: &SchemaSettlementPermit,
        checkpoint_id: &CheckpointId,
        receipt: Receipt,
    ) -> Result<Checkpoint> {
        permit.validate()?;
        self.validate_key_domain(&permit.key)?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let checkpoint = CheckpointSql::fetch_by_id_tx(&tx, checkpoint_id)?
            .ok_or_else(|| CdfError::data(format!("checkpoint {checkpoint_id} does not exist")))?;
        if checkpoint.delta.resource_id != permit.key.resource_id {
            return Err(CdfError::contract(format!(
                "checkpoint {checkpoint_id} does not match schema settlement authority for {} generation {}",
                permit.key.resource_id, permit.generation
            )));
        }
        if checkpoint.status == CheckpointStatus::Committed
            && checkpoint.receipt.as_ref() == Some(&receipt)
        {
            return Ok(checkpoint);
        }
        require_live_permit(&tx, permit, self.clock.now_ms()?)?;
        let committed = commit_checkpoint_tx(&tx, checkpoint_id, &receipt)?;
        tx.execute(
            "INSERT INTO cdf_schema_checkpoint_settlements (
                checkpoint_id, authority_domain_id, project_id, environment, resource_id,
                run_id, generation, schema_hash, settled_at_ms
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                checkpoint_id.as_str(),
                permit.key.authority_domain_id.as_str(),
                permit.key.project_id.as_str(),
                permit.key.environment.as_str(),
                permit.key.resource_id.as_str(),
                permit.run_id.as_str(),
                u64_to_i64("schema settlement generation", permit.generation)?,
                permit.schema_hash.as_str(),
                receipt.committed_at_ms,
            ],
        )
        .map_err(sqlite_error)?;
        tx.execute(
            "UPDATE cdf_schema_settlement_permits SET released = 1
             WHERE authority_domain_id = ? AND project_id = ? AND environment = ?
               AND resource_id = ? AND run_id = ? AND released = 0",
            params![
                permit.key.authority_domain_id.as_str(),
                permit.key.project_id.as_str(),
                permit.key.environment.as_str(),
                permit.key.resource_id.as_str(),
                permit.run_id.as_str(),
            ],
        )
        .map_err(sqlite_error)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(committed)
    }
}

#[cfg(test)]
impl SqliteSchemaAuthorityStore {
    pub(crate) fn establish_batch_with_failure_for_test(
        &self,
        establishments: Vec<SchemaAuthorityEstablishment>,
    ) -> Result<Vec<SchemaHead>> {
        self.establish_batch(Vec::new(), establishments, true)
    }

    pub(crate) fn execute_for_test<P>(&self, sql: &str, params: P) -> rusqlite::Result<usize>
    where
        P: rusqlite::Params,
    {
        self.conn.lock().unwrap().execute(sql, params)
    }

    pub(crate) fn query_row_for_test<T, P, F>(
        &self,
        sql: &str,
        params: P,
        f: F,
    ) -> rusqlite::Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.conn.lock().unwrap().query_row(sql, params, f)
    }
}
