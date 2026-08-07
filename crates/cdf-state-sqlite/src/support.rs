use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
    error::Error as _,
    fs::OpenOptions,
    ops::{Deref, DerefMut},
    path::Path,
    sync::{Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, Checkpoint, CheckpointId, CheckpointStatus, ErrorKind,
    PackageHash, PipelineId, Receipt, ResourceId, Result, RewindRequest, ScopeKey, StateDelta,
    StateSegment,
};
use rusqlite::{Connection, ErrorCode, OptionalExtension, params};
use serde::de::DeserializeOwned;

pub(crate) fn rewind_marker(
    request: &RewindRequest,
    current_head: &Checkpoint,
    target: &Checkpoint,
    created_at_ms: i64,
) -> Checkpoint {
    let mut delta = target.delta.clone();
    delta.checkpoint_id = request.marker_checkpoint_id.clone();
    delta.parent_checkpoint_id = Some(current_head.delta.checkpoint_id.clone());
    delta.input_position = Some(current_head.delta.output_position.clone());

    Checkpoint {
        delta,
        status: CheckpointStatus::Rewound,
        receipt: None,
        is_head: false,
        created_at_ms,
        committed_at_ms: None,
        rewind_target_checkpoint_id: Some(target.delta.checkpoint_id.clone()),
    }
}

pub(crate) fn packages_ahead_of_state(
    history: &[Checkpoint],
    current_head_id: &CheckpointId,
    target_id: &CheckpointId,
) -> Vec<PackageHash> {
    let by_id: BTreeMap<CheckpointId, &Checkpoint> = history
        .iter()
        .map(|checkpoint| (checkpoint.delta.checkpoint_id.clone(), checkpoint))
        .collect();
    let target_lineage = lineage_ids(&by_id, target_id);
    let mut packages = Vec::new();
    let mut cursor = Some(current_head_id.clone());

    while let Some(checkpoint_id) = cursor {
        if target_lineage.contains(&checkpoint_id) {
            break;
        }
        let Some(checkpoint) = by_id.get(&checkpoint_id) else {
            break;
        };
        if checkpoint.status == CheckpointStatus::Committed {
            packages.push(checkpoint.delta.package_hash.clone());
        }
        cursor = checkpoint.delta.parent_checkpoint_id.clone();
    }

    packages
}

fn lineage_ids(
    by_id: &BTreeMap<CheckpointId, &Checkpoint>,
    start_id: &CheckpointId,
) -> BTreeSet<CheckpointId> {
    let mut lineage = BTreeSet::new();
    let mut cursor = Some(start_id.clone());
    while let Some(checkpoint_id) = cursor {
        if !lineage.insert(checkpoint_id.clone()) {
            break;
        }
        cursor = by_id
            .get(&checkpoint_id)
            .and_then(|checkpoint| checkpoint.delta.parent_checkpoint_id.clone());
    }
    lineage
}

pub(crate) fn verify_receipt(receipt: &Receipt, delta: &StateDelta) -> Result<()> {
    if !receipt.covers_state_delta(delta)
        || !receipt_matches_segment_counts(receipt, &delta.segments)
    {
        return Err(CdfError::contract(format!(
            "receipt {} does not cover checkpoint {}",
            receipt.receipt_id, delta.checkpoint_id
        )));
    }
    Ok(())
}

fn receipt_matches_segment_counts(receipt: &Receipt, segments: &[StateSegment]) -> bool {
    let acks: BTreeMap<_, _> = receipt
        .segment_acks
        .iter()
        .map(|ack| (&ack.segment_id, ack))
        .collect();
    segments.iter().all(|segment| {
        acks.get(&segment.segment_id).is_some_and(|ack| {
            ack.row_count == segment.row_count && ack.byte_count == segment.byte_count
        })
    })
}

pub(crate) fn same_tuple(
    delta: &StateDelta,
    pipeline_id: &PipelineId,
    resource_id: &ResourceId,
    scope: &ScopeKey,
) -> bool {
    delta.pipeline_id == *pipeline_id && delta.resource_id == *resource_id && delta.scope == *scope
}

pub(crate) fn validate_state_version(state_version: u16) -> Result<()> {
    if state_version == CHECKPOINT_STATE_VERSION {
        Ok(())
    } else {
        Err(CdfError::contract(format!(
            "unsupported checkpoint state version {state_version}"
        )))
    }
}

pub(crate) fn encode_json<T: serde::Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value)
        .map_err(|error| CdfError::internal(format!("encode CDF-managed state JSON: {error}")))
}

pub(crate) fn decode_json<T: DeserializeOwned>(json: &str, state_version: u16) -> Result<T> {
    validate_state_version(state_version)?;
    serde_json::from_str(json)
        .map_err(|error| CdfError::internal(format!("decode CDF-managed state JSON: {error}")))
}

pub(crate) fn missing_checkpoint(checkpoint_id: &CheckpointId) -> CdfError {
    CdfError::contract(format!("checkpoint {checkpoint_id} does not exist"))
}

pub(crate) fn now_ms() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            CdfError::environment(format!(
                "read the host clock for the SQLite state store: {error}; correct the system clock and retry"
            ))
        })?;
    i64::try_from(elapsed.as_millis()).map_err(|error| {
        CdfError::environment(format!(
            "represent the host clock for the SQLite state store: {error}; correct the system clock and retry"
        ))
    })
}

pub(crate) fn ensure_schema_version_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS cdf_sqlite_schema_versions (
            component TEXT PRIMARY KEY,
            version INTEGER NOT NULL,
            recorded_at_ms INTEGER NOT NULL
        );
        ",
    )
    .map_err(sqlite_error)
}

fn schema_version_table_exists(conn: &Connection) -> Result<bool> {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'cdf_sqlite_schema_versions'",
        [],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(sqlite_error)
}

pub(crate) fn read_component_schema_version(
    conn: &Connection,
    component: &str,
) -> Result<Option<i64>> {
    if !schema_version_table_exists(conn)? {
        return Ok(None);
    }
    conn.query_row(
        "SELECT version FROM cdf_sqlite_schema_versions WHERE component = ?",
        params![component],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map_err(sqlite_error)
}

pub(crate) fn write_component_schema_version(
    conn: &Connection,
    component: &str,
    version: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO cdf_sqlite_schema_versions (component, version, recorded_at_ms)
         VALUES (?, ?, ?)
         ON CONFLICT(component) DO NOTHING",
        params![component, version, now_ms()?],
    )
    .map(|_| ())
    .map_err(sqlite_error)
}

pub(crate) fn sqlite_table_exists(conn: &Connection, table: &str) -> Result<bool> {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        params![table],
        |row| row.get::<_, i64>(0),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(sqlite_error)
}

pub(crate) fn require_sqlite_tables(
    conn: &Connection,
    component: &str,
    tables: &[&str],
) -> Result<()> {
    for table in tables {
        if !sqlite_table_exists(conn, table)? {
            return Err(CdfError::internal(format!(
                "{component} SQLite schema is incomplete; required table {table} is missing"
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqliteErrorContext {
    ManagedState,
    ManagedReadOnly,
    EphemeralWorkspace,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateStorePathOwnership {
    CdfManaged,
    Configured,
}

thread_local! {
    static ACTIVE_SQLITE_ERROR_CONTEXT: Cell<SqliteErrorContext> =
        const { Cell::new(SqliteErrorContext::ManagedState) };
}

struct SqliteErrorContextGuard {
    previous: SqliteErrorContext,
}

impl SqliteErrorContextGuard {
    fn enter(context: SqliteErrorContext) -> Self {
        let previous = ACTIVE_SQLITE_ERROR_CONTEXT.replace(context);
        Self { previous }
    }
}

impl Drop for SqliteErrorContextGuard {
    fn drop(&mut self) {
        ACTIVE_SQLITE_ERROR_CONTEXT.set(self.previous);
    }
}

pub(crate) fn with_sqlite_error_context<T>(
    context: SqliteErrorContext,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let _guard = SqliteErrorContextGuard::enter(context);
    operation()
}

pub(crate) struct SqliteConnectionGuard<'a> {
    connection: MutexGuard<'a, Connection>,
    _context: SqliteErrorContextGuard,
}

impl Deref for SqliteConnectionGuard<'_> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl DerefMut for SqliteConnectionGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

pub(crate) fn lock_sqlite_connection(
    connection: &Mutex<Connection>,
    context: SqliteErrorContext,
) -> Result<SqliteConnectionGuard<'_>> {
    let connection = connection.lock().map_err(lock_error)?;
    Ok(SqliteConnectionGuard {
        connection,
        _context: SqliteErrorContextGuard::enter(context),
    })
}

pub(crate) fn private_state_decode<T>(action: &str, result: Result<T>) -> Result<T> {
    result.map_err(|error| match error.kind {
        ErrorKind::Environment | ErrorKind::Transient | ErrorKind::RateLimited => error,
        _ => CdfError::internal(format!("{action}: {}", error.message)),
    })
}

pub fn classify_sqlite_error(
    context: SqliteErrorContext,
    action: &str,
    error: rusqlite::Error,
) -> CdfError {
    if let Some(mut error) = embedded_cdf_error(&error) {
        error.message = format!("{action}: {}", error.message);
        error
    } else if sqlite_retryable_contention(context, &error) {
        CdfError::transient(format!(
            "{action}: {error}; another process is using the CDF-managed SQLite state store, so retry after the competing transaction completes"
        ))
    } else if sqlite_host_error(context, &error) {
        let remediation = match context {
            SqliteErrorContext::ManagedState => {
                "check the state path, permissions, free space, device availability, memory, and process file limits before retrying"
            }
            SqliteErrorContext::ManagedReadOnly => {
                "check the state path, permissions, device availability, memory, and process file limits before retrying"
            }
            SqliteErrorContext::EphemeralWorkspace => {
                "free memory and restore the SQLite runtime or process resource limits before retrying"
            }
        };
        CdfError::environment(format!("{action}: {error}; {remediation}"))
    } else {
        CdfError::internal(format!("{action}: {error}"))
    }
}

fn embedded_cdf_error(error: &rusqlite::Error) -> Option<CdfError> {
    let mut source = error.source();
    while let Some(candidate) = source {
        if let Some(error) = candidate.downcast_ref::<CdfError>() {
            return Some(error.clone());
        }
        if let Some(error) = candidate
            .downcast_ref::<std::io::Error>()
            .and_then(cdf_kernel::embedded_cdf_error)
        {
            return Some(error);
        }
        source = candidate.source();
    }
    None
}

pub(crate) fn sqlite_error(error: rusqlite::Error) -> CdfError {
    ACTIVE_SQLITE_ERROR_CONTEXT.with(|context| {
        let context = context.get();
        let action = match context {
            SqliteErrorContext::ManagedState => "access the CDF-managed SQLite state store",
            SqliteErrorContext::ManagedReadOnly => {
                "access the read-only CDF-managed SQLite state store"
            }
            SqliteErrorContext::EphemeralWorkspace => {
                "access the ephemeral in-memory SQLite workspace"
            }
        };
        classify_sqlite_error(context, action, error)
    })
}

pub fn managed_database_path_exists(path: &Path) -> Result<bool> {
    database_path_exists(path, StateStorePathOwnership::CdfManaged)
}

pub fn database_path_exists(path: &Path, ownership: StateStorePathOwnership) -> Result<bool> {
    validate_managed_database_ancestor_components(path, ownership)?;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            let state = match std::fs::metadata(path) {
                Ok(_) => "live",
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => "dangling",
                Err(_) => "unresolvable",
            };
            Err(database_shape_error(
                path,
                ownership,
                format!("state-store path is a {state} symlink"),
            ))
        }
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(database_shape_error(
            path,
            ownership,
            "state-store path is not a regular file",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            validate_missing_managed_database_ancestors(path, ownership)?;
            Ok(false)
        }
        Err(error)
            if error.kind() == std::io::ErrorKind::NotADirectory
                || cdf_kernel::is_filesystem_loop(&error) =>
        {
            Err(database_shape_error(
                path,
                ownership,
                format!("state-store path has an invalid filesystem shape: {error}"),
            ))
        }
        Err(error) => Err(CdfError::environment(format!(
            "inspect CDF-managed SQLite state store {}: {error}; check the state path, parent-directory permissions, device availability, and process file limits before retrying",
            path.display()
        ))),
    }
}

pub(crate) fn prepare_managed_database_path(
    path: &Path,
    ownership: StateStorePathOwnership,
) -> Result<std::path::PathBuf> {
    if database_path_exists(path, ownership)? {
        return database_open_path(path, ownership);
    }
    let open_path = database_open_path(path, ownership)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(&open_path) {
        Ok(_) => Ok(open_path),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if database_path_exists(&open_path, ownership)? {
                Ok(open_path)
            } else {
                Err(database_shape_error(
                    path,
                    ownership,
                    format!(
                        "state-store path {} disappeared during creation",
                        open_path.display()
                    ),
                ))
            }
        }
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::NotADirectory
                    | std::io::ErrorKind::IsADirectory
                    | std::io::ErrorKind::InvalidInput
                    | std::io::ErrorKind::InvalidData
            ) || cdf_kernel::is_filesystem_loop(&error) =>
        {
            Err(database_shape_error(
                path,
                ownership,
                format!(
                    "create state store {} with an invalid filesystem shape: {error}",
                    open_path.display()
                ),
            ))
        }
        Err(error) => Err(CdfError::environment(format!(
            "create CDF-managed SQLite state store {}: {error}; check the state path, parent-directory permissions, free space, device availability, and process file limits before retrying",
            open_path.display()
        ))),
    }
}

pub fn managed_database_open_path(path: &Path) -> Result<std::path::PathBuf> {
    database_open_path(path, StateStorePathOwnership::CdfManaged)
}

pub fn database_open_path(
    path: &Path,
    ownership: StateStorePathOwnership,
) -> Result<std::path::PathBuf> {
    let file_name = path.file_name().ok_or_else(|| {
        database_shape_error(
            path,
            ownership,
            "state-store path has no database file name",
        )
    })?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let canonical_parent = std::fs::canonicalize(parent).map_err(|error| {
        if matches!(
            error.kind(),
            std::io::ErrorKind::NotFound
                | std::io::ErrorKind::NotADirectory
                | std::io::ErrorKind::IsADirectory
                | std::io::ErrorKind::InvalidInput
                | std::io::ErrorKind::InvalidData
        ) || cdf_kernel::is_filesystem_loop(&error)
        {
            database_shape_error(
                path,
                ownership,
                format!(
                    "resolve state-store parent {} with an invalid filesystem shape: {error}",
                    parent.display()
                ),
            )
        } else {
            CdfError::environment(format!(
                "resolve CDF-managed SQLite state-store parent {}: {error}; check the state path, parent-directory permissions, device availability, and process file limits before retrying",
                parent.display()
            ))
        }
    })?;
    Ok(canonical_parent.join(file_name))
}

fn database_shape_error(
    path: &Path,
    ownership: StateStorePathOwnership,
    detail: impl std::fmt::Display,
) -> CdfError {
    match ownership {
        StateStorePathOwnership::CdfManaged => CdfError::internal(format!(
            "CDF-managed SQLite state store {} is invalid: {detail}",
            path.display()
        )),
        StateStorePathOwnership::Configured => CdfError::contract(format!(
            "configured SQLite state path {} is invalid: {detail}; choose a regular database file beneath a valid directory",
            path.display()
        )),
    }
}

pub(crate) fn managed_sqlite_open_flags(read_only: bool) -> rusqlite::OpenFlags {
    let access = if read_only {
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
    } else {
        rusqlite::OpenFlags::default()
    };
    access | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW
}

fn validate_managed_database_ancestor_components(
    path: &Path,
    ownership: StateStorePathOwnership,
) -> Result<()> {
    if ownership == StateStorePathOwnership::Configured {
        return Ok(());
    }
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let mut trusted_base = std::path::PathBuf::new();
    let mut found_anchor = false;
    for component in parent.components() {
        if component.as_os_str() == ".cdf" {
            found_anchor = true;
            break;
        }
        trusted_base.push(component.as_os_str());
    }
    if !found_anchor {
        return Ok(());
    }
    let relative = parent.strip_prefix(&trusted_base).map_err(|error| {
        CdfError::internal(format!(
            "derive CDF-managed SQLite state-store ancestry for {}: {error}",
            path.display()
        ))
    })?;
    let mut current = trusted_base;
    for component in relative.components() {
        current.push(component.as_os_str());
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(metadata) if metadata.file_type().is_symlink() => {
                let state = match std::fs::metadata(&current) {
                    Ok(_) => "live",
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => "dangling",
                    Err(_) => "unresolvable",
                };
                return Err(CdfError::internal(format!(
                    "CDF-managed SQLite state-store ancestor {} is a {state} symlink",
                    current.display()
                )));
            }
            Ok(_) => {
                return Err(CdfError::internal(format!(
                    "CDF-managed SQLite state-store ancestor {} is not a real directory",
                    current.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error)
                if error.kind() == std::io::ErrorKind::NotADirectory
                    || cdf_kernel::is_filesystem_loop(&error) =>
            {
                return Err(CdfError::internal(format!(
                    "CDF-managed SQLite state-store ancestor {} has an invalid filesystem shape: {error}",
                    current.display()
                )));
            }
            Err(error) => {
                return Err(CdfError::environment(format!(
                    "inspect CDF-managed SQLite state-store ancestor {}: {error}; check the state path, parent-directory permissions, device availability, and process file limits before retrying",
                    current.display()
                )));
            }
        }
    }
    Ok(())
}

fn validate_missing_managed_database_ancestors(
    path: &Path,
    ownership: StateStorePathOwnership,
) -> Result<()> {
    let mut cursor = path.parent();
    while let Some(parent) = cursor {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        match std::fs::metadata(parent) {
            Ok(metadata) if metadata.is_dir() => return Ok(()),
            Ok(_) => {
                return Err(database_shape_error(
                    path,
                    ownership,
                    format!(
                        "state-store ancestor {} is not a directory",
                        parent.display()
                    ),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match std::fs::symlink_metadata(parent) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(database_shape_error(
                            path,
                            ownership,
                            format!(
                                "state-store ancestor {} is a dangling symlink",
                                parent.display()
                            ),
                        ));
                    }
                    Ok(_) => {
                        return Err(database_shape_error(
                            path,
                            ownership,
                            format!(
                                "state-store ancestor {} changed filesystem shape during inspection",
                                parent.display()
                            ),
                        ));
                    }
                    Err(link_error) if link_error.kind() == std::io::ErrorKind::NotFound => {
                        cursor = parent.parent();
                    }
                    Err(link_error)
                        if link_error.kind() == std::io::ErrorKind::NotADirectory
                            || cdf_kernel::is_filesystem_loop(&link_error) =>
                    {
                        return Err(database_shape_error(
                            path,
                            ownership,
                            format!(
                                "state-store ancestor {} has an invalid filesystem shape: {link_error}",
                                parent.display()
                            ),
                        ));
                    }
                    Err(link_error) => {
                        return Err(CdfError::environment(format!(
                            "inspect CDF-managed SQLite state-store ancestor {}: {link_error}; check the state path, parent-directory permissions, device availability, and process file limits before retrying",
                            parent.display()
                        )));
                    }
                }
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::NotADirectory
                    || cdf_kernel::is_filesystem_loop(&error) =>
            {
                return Err(database_shape_error(
                    path,
                    ownership,
                    format!(
                        "state-store ancestor {} has an invalid filesystem shape: {error}",
                        parent.display()
                    ),
                ));
            }
            Err(error) => {
                return Err(CdfError::environment(format!(
                    "inspect CDF-managed SQLite state-store ancestor {}: {error}; check the state path, parent-directory permissions, device availability, and process file limits before retrying",
                    parent.display()
                )));
            }
        }
    }
    Ok(())
}

fn sqlite_retryable_contention(context: SqliteErrorContext, error: &rusqlite::Error) -> bool {
    matches!(
        context,
        SqliteErrorContext::ManagedState | SqliteErrorContext::ManagedReadOnly
    ) && matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(
                failure.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
            )
    )
}

fn sqlite_host_error(context: SqliteErrorContext, error: &rusqlite::Error) -> bool {
    match error {
        rusqlite::Error::SqliteSingleThreadedMode => true,
        rusqlite::Error::SqliteFailure(failure, _) => match failure.code {
            ErrorCode::OutOfMemory | ErrorCode::DiskFull | ErrorCode::NoLargeFileSupport => true,
            ErrorCode::ReadOnly => context == SqliteErrorContext::ManagedState,
            ErrorCode::FileLockingProtocolFailed => matches!(
                context,
                SqliteErrorContext::ManagedState | SqliteErrorContext::ManagedReadOnly
            ),
            ErrorCode::CannotOpen => !matches!(
                failure.extended_code,
                rusqlite::ffi::SQLITE_CANTOPEN_ISDIR
                    | rusqlite::ffi::SQLITE_CANTOPEN_SYMLINK
                    | rusqlite::ffi::SQLITE_CANTOPEN_DIRTYWAL
            ),
            ErrorCode::SystemIoFailure => !matches!(
                failure.extended_code,
                rusqlite::ffi::SQLITE_IOERR_DATA
                    | rusqlite::ffi::SQLITE_IOERR_CORRUPTFS
                    | rusqlite::ffi::SQLITE_IOERR_SHORT_READ
            ),
            ErrorCode::PermissionDenied => true,
            _ => false,
        },
        _ => false,
    }
}

pub(crate) fn lock_error<T>(error: std::sync::PoisonError<T>) -> CdfError {
    CdfError::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cdf_kernel::{ErrorKind, ErrorKind::RateLimited};
    use rusqlite::types::Type;

    use super::*;

    #[test]
    fn sqlite_failures_separate_host_ownership_from_private_state_invariants() {
        let host = sqlite_error(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
            None,
        ));
        assert_eq!(host.kind, ErrorKind::Environment);
        assert!(host.message.contains("state path"));

        let invariant = sqlite_error(rusqlite::Error::InvalidQuery);
        assert_eq!(invariant.kind, ErrorKind::Internal);

        for extended_code in [
            rusqlite::ffi::SQLITE_CANTOPEN_ISDIR,
            rusqlite::ffi::SQLITE_CANTOPEN_SYMLINK,
            rusqlite::ffi::SQLITE_CANTOPEN_DIRTYWAL,
            rusqlite::ffi::SQLITE_IOERR_DATA,
            rusqlite::ffi::SQLITE_IOERR_CORRUPTFS,
            rusqlite::ffi::SQLITE_IOERR_SHORT_READ,
        ] {
            let shape_or_corruption = sqlite_error(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(extended_code),
                None,
            ));
            assert_eq!(shape_or_corruption.kind, ErrorKind::Internal);
        }

        for primary_code in [rusqlite::ffi::SQLITE_NOTFOUND, rusqlite::ffi::SQLITE_AUTH] {
            let sqlite_semantic = sqlite_error(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(primary_code),
                None,
            ));
            assert_eq!(sqlite_semantic.kind, ErrorKind::Internal);
        }

        let permission = sqlite_error(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_PERM),
            None,
        ));
        assert_eq!(permission.kind, ErrorKind::Environment);

        let ephemeral_permission = classify_sqlite_error(
            SqliteErrorContext::EphemeralWorkspace,
            "operate an in-memory workspace",
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_PERM),
                None,
            ),
        );
        assert_eq!(ephemeral_permission.kind, ErrorKind::Environment);
    }

    #[test]
    fn directory_at_managed_database_path_is_a_private_state_invariant() {
        let root = tempfile::tempdir().unwrap();
        let managed = root.path().join(".cdf");
        std::fs::create_dir(&managed).unwrap();
        let database = managed.join("state.db");
        std::fs::create_dir(&database).unwrap();
        let classified = crate::SqliteCheckpointStore::open(&database)
            .err()
            .expect("directory database path must fail");

        assert_eq!(classified.kind, ErrorKind::Internal);
    }

    #[cfg(unix)]
    #[test]
    fn dangling_symlink_at_managed_database_path_is_a_private_state_invariant() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let managed = root.path().join(".cdf");
        std::fs::create_dir(&managed).unwrap();
        let database = managed.join("state.db");
        symlink(root.path().join("missing.db"), &database).unwrap();

        let classified = managed_database_path_exists(&database).unwrap_err();

        assert_eq!(classified.kind, ErrorKind::Internal);
        assert!(classified.message.contains("dangling symlink"));
    }

    #[cfg(unix)]
    #[test]
    fn dangling_symlink_ancestor_of_managed_database_is_a_private_state_invariant() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let managed = root.path().join(".cdf");
        std::fs::create_dir(&managed).unwrap();
        let state_root = managed.join("state");
        symlink(root.path().join("missing"), &state_root).unwrap();

        let classified = managed_database_path_exists(&state_root.join("state.db")).unwrap_err();

        assert_eq!(classified.kind, ErrorKind::Internal);
        assert!(classified.message.contains("ancestor"));
        assert!(classified.message.contains("dangling symlink"));
    }

    #[test]
    fn directory_at_configured_database_path_is_contract_owned() {
        let root = tempfile::tempdir().unwrap();
        let configured_parent = root.path().join("custom/.cdf");
        std::fs::create_dir_all(&configured_parent).unwrap();
        let database = configured_parent.join("state.db");
        std::fs::create_dir(&database).unwrap();

        let classified = crate::SqliteCheckpointStore::open_with_path_ownership(
            &database,
            StateStorePathOwnership::Configured,
        )
        .err()
        .expect("configured directory database path must fail");

        assert_eq!(classified.kind, ErrorKind::Contract);
        assert!(classified.message.contains("configured SQLite state path"));
    }

    #[cfg(unix)]
    #[test]
    fn live_symlink_ancestor_with_existing_state_subtree_is_a_private_state_invariant() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(outside.path().join("state")).unwrap();
        let managed = root.path().join(".cdf");
        symlink(outside.path(), &managed).unwrap();

        let classified = managed_database_path_exists(&managed.join("state/state.db")).unwrap_err();

        assert_eq!(classified.kind, ErrorKind::Internal);
        assert!(classified.message.contains("ancestor"));
    }

    #[cfg(unix)]
    #[test]
    fn configured_live_symlink_ancestor_opens_database_at_canonical_target() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let existing = outside.path().join("existing");
        std::fs::create_dir(&existing).unwrap();
        let configured_parent = root.path().join("custom");
        std::fs::create_dir(&configured_parent).unwrap();
        let link = configured_parent.join(".cdf");
        symlink(outside.path(), &link).unwrap();
        let configured = link.join("existing/state.db");

        let store = crate::SqliteCheckpointStore::open_with_path_ownership(
            &configured,
            StateStorePathOwnership::Configured,
        )
        .unwrap();
        drop(store);

        assert!(existing.join("state.db").is_file());
        crate::SqliteCheckpointStore::open_read_only_with_path_ownership(
            &configured,
            StateStorePathOwnership::Configured,
        )
        .unwrap();
    }

    #[test]
    fn competing_managed_database_transaction_is_transient() {
        let root = tempfile::tempdir().unwrap();
        let database = root.path().join("state.db");
        let first = Connection::open(&database).unwrap();
        first
            .execute_batch("CREATE TABLE owner (value INTEGER); BEGIN EXCLUSIVE;")
            .unwrap();
        let second = Connection::open(&database).unwrap();
        second.busy_timeout(Duration::ZERO).unwrap();
        let error = second
            .execute_batch("INSERT INTO owner VALUES (1);")
            .unwrap_err();

        let classified = sqlite_error(error);

        assert_eq!(classified.kind, ErrorKind::Transient);
        assert!(classified.message.contains("competing transaction"));
    }

    #[test]
    fn wrapped_cdf_errors_preserve_kind_message_and_retry_metadata() {
        let embedded = CdfError::rate_limited("embedded ownership", Some(375));
        let nested = std::io::Error::other(embedded.clone());
        let wrapped = rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(nested));

        let classified = sqlite_error(wrapped);

        assert_eq!(classified.kind, embedded.kind);
        assert_eq!(classified.retry_after_ms, embedded.retry_after_ms);
        assert!(classified.message.contains("CDF-managed SQLite"));
        assert!(classified.message.contains("embedded ownership"));
        assert_eq!(classified.kind, RateLimited);
        assert_eq!(classified.retry_after_ms, Some(375));

        let contract = CdfError::contract("embedded contract");
        let directly_wrapped =
            rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(contract.clone()));
        let classified = sqlite_error(directly_wrapped);
        assert_eq!(classified.kind, contract.kind);
        assert!(classified.message.contains("embedded contract"));
    }

    #[test]
    fn ephemeral_workspace_readonly_is_an_internal_invariant() {
        let error = classify_sqlite_error(
            SqliteErrorContext::EphemeralWorkspace,
            "operate an in-memory workspace",
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_READONLY),
                None,
            ),
        );

        assert_eq!(error.kind, ErrorKind::Internal);
    }

    #[test]
    fn malformed_private_state_json_is_internal() {
        let error = decode_json::<serde_json::Value>("{", CHECKPOINT_STATE_VERSION).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Internal);
        assert!(error.message.contains("CDF-managed state JSON"));
    }

    #[test]
    fn private_state_domain_validation_is_remapped_to_internal() {
        let error = private_state_decode::<()>(
            "decode private row",
            Err(CdfError::contract("stored identifier is invalid")),
        )
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Internal);
        assert!(error.message.contains("stored identifier is invalid"));
    }
}
