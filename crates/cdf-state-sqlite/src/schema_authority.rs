use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Mutex},
};

use cdf_kernel::{
    CdfError, LeaseAuthorityDomainId, MAX_SCHEMA_AUTHORITY_HISTORY_LIMIT, Result,
    SchemaAuthorityCheck, SchemaAuthorityEstablishment, SchemaAuthorityEvent,
    SchemaAuthorityEventKind, SchemaAuthorityKey, SchemaAuthorityStore, SchemaHash, SchemaHead,
    SchemaHeadStatus, SchemaPromotionFence, SchemaVersion, SchemaVersionProvenance,
    ScopeLeaseClock,
};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};

use crate::{
    lease::{
        SystemScopeLeaseClock, assert_current_lease_at,
        initialize_schema as initialize_lease_schema,
        initialize_schema_with_domain as initialize_lease_schema_with_domain,
        read_authority_domain_id, validate_schema_version as validate_lease_schema_version,
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
pub(crate) const SCHEMA_AUTHORITY_SCHEMA_VERSION: i64 = 1;

const VERSION_SELECT: &str = "SELECT schema_hash, predecessor_schema_hash, created_at_ms, version_json FROM cdf_schema_versions";
const HEAD_SELECT: &str = "SELECT authority_domain_id, project_id, environment, resource_id, generation, schema_hash, status, promotion_id, promotion_from_schema_hash, promotion_to_schema_hash, promotion_lease_owner, promotion_fencing_token, head_json FROM cdf_schema_heads";
const EVENT_SELECT: &str = "SELECT authority_domain_id, project_id, environment, resource_id, ordinal, generation, schema_hash, recorded_at_ms, event_json FROM cdf_schema_authority_events";

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
                    || sqlite_table_exists(&conn, "cdf_schema_authority_events")? =>
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
            || sqlite_table_exists(conn, "cdf_schema_authority_events")? =>
        {
            return Err(CdfError::internal(format!(
                "schema authority SQLite schema is unversioned; expected current version {SCHEMA_AUTHORITY_SCHEMA_VERSION}"
            )));
        }
        None => {}
    }
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
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaHead> {
        expected.validate()?;
        proposed.validate()?;
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
        Ok(promoting)
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
        let next_generation = current
            .generation
            .checked_add(1)
            .ok_or_else(|| CdfError::internal("schema authority generation overflow"))?;
        let active =
            SchemaHead::active(current.key.clone(), next_generation, to_schema_hash.clone())?;
        update_head(&tx, &active)?;
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
