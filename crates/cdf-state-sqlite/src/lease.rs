use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_kernel::{
    CdfError, FencingToken, LeaseOwnerId, Result, ScopeKey, ScopeLease, ScopeLeaseClock,
    ScopeLeaseStore,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, params};

use crate::support::{
    decode_json, encode_json, ensure_migration_table, lock_error, read_component_schema_version,
    sqlite_error, write_component_schema_version,
};

pub(crate) const SCOPE_LEASE_COMPONENT: &str = "scope_lease_store";
pub(crate) const SCOPE_LEASE_SCHEMA_VERSION: i64 = 1;

pub struct InMemoryScopeLeaseStore {
    leases: Mutex<BTreeMap<String, LeaseRecord>>,
    clock: Arc<dyn ScopeLeaseClock>,
}

#[derive(Clone)]
struct LeaseRecord {
    lease: ScopeLease,
    released: bool,
}

impl InMemoryScopeLeaseStore {
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemScopeLeaseClock))
    }

    pub fn with_clock(clock: Arc<dyn ScopeLeaseClock>) -> Self {
        Self {
            leases: Mutex::new(BTreeMap::new()),
            clock,
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, BTreeMap<String, LeaseRecord>>> {
        self.leases.lock().map_err(lock_error)
    }
}

impl Default for InMemoryScopeLeaseStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ScopeLeaseStore for InMemoryScopeLeaseStore {
    fn acquire(
        &self,
        scope: ScopeKey,
        owner: LeaseOwnerId,
        lease_duration_ms: u64,
    ) -> Result<ScopeLease> {
        let now_ms = self.clock.now_ms()?;
        let expires_at_ms = expiry(now_ms, lease_duration_ms)?;
        let key = encode_json(&scope)?;
        let mut leases = self.lock()?;
        let next_token = match leases.get(&key) {
            Some(record) if !record.released && !record.lease.is_expired_at(now_ms) => {
                return Err(contention_error(&scope));
            }
            Some(record) => record
                .lease
                .fencing_token
                .get()
                .checked_add(1)
                .ok_or_else(|| CdfError::internal("scope lease fencing token overflow"))?,
            None => 1,
        };
        let lease = ScopeLease {
            scope,
            owner,
            fencing_token: FencingToken::new(next_token)?,
            acquired_at_ms: now_ms,
            expires_at_ms,
        };
        leases.insert(
            key,
            LeaseRecord {
                lease: lease.clone(),
                released: false,
            },
        );
        Ok(lease)
    }

    fn renew(&self, lease: &ScopeLease, lease_duration_ms: u64) -> Result<ScopeLease> {
        let now_ms = self.clock.now_ms()?;
        let expires_at_ms = expiry(now_ms, lease_duration_ms)?;
        let key = encode_json(&lease.scope)?;
        let mut leases = self.lock()?;
        let record = leases
            .get_mut(&key)
            .ok_or_else(|| stale_error(&lease.scope))?;
        ensure_current(record, lease, now_ms)?;
        record.lease.expires_at_ms = expires_at_ms;
        Ok(record.lease.clone())
    }

    fn release(&self, lease: &ScopeLease) -> Result<()> {
        let now_ms = self.clock.now_ms()?;
        let key = encode_json(&lease.scope)?;
        let mut leases = self.lock()?;
        let record = leases
            .get_mut(&key)
            .ok_or_else(|| stale_error(&lease.scope))?;
        ensure_current(record, lease, now_ms)?;
        record.released = true;
        Ok(())
    }

    fn assert_current(&self, lease: &ScopeLease) -> Result<()> {
        let now_ms = self.clock.now_ms()?;
        let key = encode_json(&lease.scope)?;
        let leases = self.lock()?;
        let record = leases.get(&key).ok_or_else(|| stale_error(&lease.scope))?;
        ensure_current(record, lease, now_ms)
    }
}

pub struct SqliteScopeLeaseStore {
    conn: Mutex<Connection>,
    clock: Arc<dyn ScopeLeaseClock>,
}

impl SqliteScopeLeaseStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_clock(path, Arc::new(SystemScopeLeaseClock))
    }

    pub fn open_with_clock(
        path: impl AsRef<Path>,
        clock: Arc<dyn ScopeLeaseClock>,
    ) -> Result<Self> {
        let conn = Connection::open(path.as_ref()).map_err(sqlite_error)?;
        initialize_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            clock,
        })
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open_with_flags(path.as_ref(), OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(sqlite_error)?;
        validate_schema_version(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            clock: Arc::new(SystemScopeLeaseClock),
        })
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::open_in_memory_with_clock(Arc::new(SystemScopeLeaseClock))
    }

    pub fn open_in_memory_with_clock(clock: Arc<dyn ScopeLeaseClock>) -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(sqlite_error)?;
        initialize_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            clock,
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(lock_error)
    }
}

impl ScopeLeaseStore for SqliteScopeLeaseStore {
    fn acquire(
        &self,
        scope: ScopeKey,
        owner: LeaseOwnerId,
        lease_duration_ms: u64,
    ) -> Result<ScopeLease> {
        let now_ms = self.clock.now_ms()?;
        let expires_at_ms = expiry(now_ms, lease_duration_ms)?;
        let scope_json = encode_json(&scope)?;
        let conn = self.lock()?;
        let lease = conn
            .query_row(
                "INSERT INTO cdf_scope_leases (scope_json, owner, fencing_token, acquired_at_ms, expires_at_ms, released) \
                 VALUES (?, ?, 1, ?, ?, 0) \
                 ON CONFLICT(scope_json) DO UPDATE SET \
                    owner = excluded.owner, \
                    fencing_token = cdf_scope_leases.fencing_token + 1, \
                    acquired_at_ms = excluded.acquired_at_ms, \
                    expires_at_ms = excluded.expires_at_ms, \
                    released = 0 \
                 WHERE cdf_scope_leases.released = 1 OR cdf_scope_leases.expires_at_ms <= excluded.acquired_at_ms \
                 RETURNING scope_json, owner, fencing_token, acquired_at_ms, expires_at_ms",
                params![scope_json, owner.as_str(), now_ms, expires_at_ms],
                row_to_lease,
            )
            .optional()
            .map_err(sqlite_error)?;
        lease.ok_or_else(|| contention_error(&scope))
    }

    fn renew(&self, lease: &ScopeLease, lease_duration_ms: u64) -> Result<ScopeLease> {
        let now_ms = self.clock.now_ms()?;
        let expires_at_ms = expiry(now_ms, lease_duration_ms)?;
        let conn = self.lock()?;
        conn.query_row(
            "UPDATE cdf_scope_leases SET expires_at_ms = ? \
             WHERE scope_json = ? AND owner = ? AND fencing_token = ? AND released = 0 AND expires_at_ms > ? \
             RETURNING scope_json, owner, fencing_token, acquired_at_ms, expires_at_ms",
            params![
                expires_at_ms,
                encode_json(&lease.scope)?,
                lease.owner.as_str(),
                token_i64(lease.fencing_token)?,
                now_ms,
            ],
            row_to_lease,
        )
        .optional()
        .map_err(sqlite_error)?
        .ok_or_else(|| stale_error(&lease.scope))
    }

    fn release(&self, lease: &ScopeLease) -> Result<()> {
        let now_ms = self.clock.now_ms()?;
        let changed = self
            .lock()?
            .execute(
                "UPDATE cdf_scope_leases SET released = 1 \
                 WHERE scope_json = ? AND owner = ? AND fencing_token = ? AND released = 0 AND expires_at_ms > ?",
                params![
                    encode_json(&lease.scope)?,
                    lease.owner.as_str(),
                    token_i64(lease.fencing_token)?,
                    now_ms,
                ],
            )
            .map_err(sqlite_error)?;
        if changed == 1 {
            Ok(())
        } else {
            Err(stale_error(&lease.scope))
        }
    }

    fn assert_current(&self, lease: &ScopeLease) -> Result<()> {
        let now_ms = self.clock.now_ms()?;
        let current = self
            .lock()?
            .query_row(
                "SELECT 1 FROM cdf_scope_leases \
                 WHERE scope_json = ? AND owner = ? AND fencing_token = ? AND released = 0 AND expires_at_ms > ?",
                params![
                    encode_json(&lease.scope)?,
                    lease.owner.as_str(),
                    token_i64(lease.fencing_token)?,
                    now_ms,
                ],
                |_| Ok(()),
            )
            .optional()
            .map_err(sqlite_error)?;
        current.ok_or_else(|| stale_error(&lease.scope))
    }
}

pub(crate) fn initialize_schema(conn: &Connection) -> Result<()> {
    ensure_migration_table(conn)?;
    validate_schema_version(conn)?;
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS cdf_scope_leases (
            scope_json TEXT PRIMARY KEY,
            owner TEXT NOT NULL,
            fencing_token INTEGER NOT NULL CHECK (fencing_token > 0),
            acquired_at_ms INTEGER NOT NULL,
            expires_at_ms INTEGER NOT NULL,
            released INTEGER NOT NULL CHECK (released IN (0, 1)),
            CHECK (expires_at_ms > acquired_at_ms)
        );
        ",
    )
    .map_err(sqlite_error)?;
    write_component_schema_version(conn, SCOPE_LEASE_COMPONENT, SCOPE_LEASE_SCHEMA_VERSION)
}

fn validate_schema_version(conn: &Connection) -> Result<()> {
    match read_component_schema_version(conn, SCOPE_LEASE_COMPONENT)? {
        Some(SCOPE_LEASE_SCHEMA_VERSION) | None => Ok(()),
        Some(version) => Err(CdfError::internal(format!(
            "unsupported scope lease store SQLite schema version {version}"
        ))),
    }
}

fn row_to_lease(row: &Row<'_>) -> rusqlite::Result<ScopeLease> {
    row_to_lease_result(row).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn row_to_lease_result(row: &Row<'_>) -> Result<ScopeLease> {
    let token = row.get::<_, i64>(2).map_err(sqlite_error)?;
    let token = u64::try_from(token)
        .map_err(|_| CdfError::data("scope lease fencing token is not positive"))?;
    Ok(ScopeLease {
        scope: decode_json(&row.get::<_, String>(0).map_err(sqlite_error)?, 1)?,
        owner: LeaseOwnerId::new(row.get::<_, String>(1).map_err(sqlite_error)?)?,
        fencing_token: FencingToken::new(token)?,
        acquired_at_ms: row.get(3).map_err(sqlite_error)?,
        expires_at_ms: row.get(4).map_err(sqlite_error)?,
    })
}

fn ensure_current(record: &LeaseRecord, lease: &ScopeLease, now_ms: i64) -> Result<()> {
    if !record.released
        && record.lease.owner == lease.owner
        && record.lease.fencing_token == lease.fencing_token
        && !record.lease.is_expired_at(now_ms)
    {
        Ok(())
    } else {
        Err(stale_error(&lease.scope))
    }
}

fn expiry(now_ms: i64, duration_ms: u64) -> Result<i64> {
    if duration_ms == 0 {
        return Err(CdfError::contract("scope lease duration must be positive"));
    }
    let duration_ms = i64::try_from(duration_ms)
        .map_err(|_| CdfError::contract("scope lease duration exceeds i64 milliseconds"))?;
    now_ms
        .checked_add(duration_ms)
        .ok_or_else(|| CdfError::contract("scope lease expiry overflows epoch milliseconds"))
}

fn token_i64(token: FencingToken) -> Result<i64> {
    i64::try_from(token.get())
        .map_err(|_| CdfError::internal("scope lease fencing token exceeds SQLite integer range"))
}

fn contention_error(scope: &ScopeKey) -> CdfError {
    CdfError::contract(format!("scope lease is already held for {scope:?}"))
}

fn stale_error(scope: &ScopeKey) -> CdfError {
    CdfError::contract(format!(
        "scope lease is expired, released, or superseded for {scope:?}"
    ))
}

pub(crate) struct SystemScopeLeaseClock;

impl ScopeLeaseClock for SystemScopeLeaseClock {
    fn now_ms(&self) -> Result<i64> {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                CdfError::internal(format!("system clock precedes Unix epoch: {error}"))
            })?;
        i64::try_from(elapsed.as_millis())
            .map_err(|_| CdfError::internal("system epoch milliseconds exceed i64"))
    }
}
