use std::{
    fs,
    path::{Path, PathBuf},
};

use cdf_kernel::CdfError;
use cdf_package::PackageReader;
use cdf_package_contract::MANIFEST_FILE;
use cdf_state_sqlite::{SqliteCheckpointStore, SqliteErrorContext, classify_sqlite_error};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, Statement, params, types::ValueRef};
use serde::Serialize;
use serde_json::{Number, Value};

use crate::{context::ProjectManifestContext, error_catalog, output::CliError};

const TABLES: &[&str] = &[
    "manifest_project",
    "manifest_inputs",
    "manifest_resources",
    "manifest_fields",
    "manifest_semantics",
    "manifest_lineage",
    "manifest_diagnostics",
    "checkpoints",
    "packages",
    "package_files",
    "package_segments",
    "package_receipts",
    "package_receipt_segments",
];

const CHECKPOINT_HISTORY_SELECT: &str = "
    SELECT
        sequence,
        checkpoint_id,
        pipeline_id,
        resource_id,
        scope_json,
        state_version,
        parent_checkpoint_id,
        input_position_json,
        output_position_json,
        package_hash,
        schema_hash,
        receipt_id,
        status,
        is_head,
        created_at_ms,
        committed_at_ms,
        delta_json,
        receipt_json,
        rewind_target_checkpoint_id
    FROM cdf_checkpoints
    ORDER BY sequence
";

const MUTATING_KEYWORDS: &[&str] = &[
    "insert", "update", "delete", "create", "drop", "alter", "pragma", "attach", "detach",
    "vacuum", "reindex", "replace",
];

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct SystemSqlReport {
    pub tables: Vec<&'static str>,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl SystemSqlReport {
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

pub(crate) fn run(
    context: &ProjectManifestContext,
    query: &str,
) -> Result<SystemSqlReport, CliError> {
    let query = read_only_query(query)?;
    let conn = Connection::open_in_memory().map_err(workspace_sqlite_error)?;
    create_schema(&conn)?;
    mount_manifest(&conn, &context.manifest)?;
    mount_checkpoints(
        &conn,
        context.state_store_path()?,
        context.state_store_path_ownership(),
    )?;
    mount_packages(&conn, context.package_root())?;
    query_rows(&conn, query)
}

fn create_schema(conn: &Connection) -> Result<(), CliError> {
    conn.execute_batch(
        "
        CREATE TABLE checkpoints (
            sequence INTEGER NOT NULL,
            checkpoint_id TEXT NOT NULL,
            pipeline_id TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            scope_json TEXT NOT NULL,
            state_version INTEGER NOT NULL,
            parent_checkpoint_id TEXT,
            input_position_json TEXT,
            output_position_json TEXT NOT NULL,
            package_hash TEXT NOT NULL,
            schema_hash TEXT NOT NULL,
            receipt_id TEXT,
            status TEXT NOT NULL,
            is_head INTEGER NOT NULL,
            created_at_ms INTEGER NOT NULL,
            committed_at_ms INTEGER,
            delta_json TEXT NOT NULL,
            receipt_json TEXT,
            rewind_target_checkpoint_id TEXT
        );

        CREATE TABLE packages (
            package_path TEXT NOT NULL,
            package_id TEXT NOT NULL,
            package_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            signing_input TEXT NOT NULL,
            signature TEXT,
            identity_file_count INTEGER NOT NULL,
            segment_count INTEGER NOT NULL,
            receipt_count INTEGER NOT NULL
        );

        CREATE TABLE package_files (
            package_hash TEXT NOT NULL,
            package_id TEXT NOT NULL,
            path TEXT NOT NULL,
            byte_count INTEGER NOT NULL,
            sha256 TEXT NOT NULL
        );

        CREATE TABLE package_segments (
            package_hash TEXT NOT NULL,
            package_id TEXT NOT NULL,
            segment_id TEXT NOT NULL,
            path TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            byte_count INTEGER NOT NULL,
            sha256 TEXT NOT NULL
        );

        CREATE TABLE package_receipts (
            package_hash TEXT NOT NULL,
            package_id TEXT NOT NULL,
            receipt_id TEXT NOT NULL,
            destination TEXT NOT NULL,
            target TEXT NOT NULL,
            disposition TEXT NOT NULL,
            idempotency_token TEXT NOT NULL,
            rows_written INTEGER NOT NULL,
            rows_inserted INTEGER,
            rows_updated INTEGER,
            rows_deleted INTEGER,
            schema_hash TEXT NOT NULL,
            committed_at_ms INTEGER NOT NULL,
            receipt_json TEXT NOT NULL
        );

        CREATE TABLE package_receipt_segments (
            package_hash TEXT NOT NULL,
            package_id TEXT NOT NULL,
            receipt_id TEXT NOT NULL,
            segment_id TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            byte_count INTEGER NOT NULL
        );

        CREATE TABLE manifest_project (
            version INTEGER NOT NULL,
            manifest_hash TEXT NOT NULL,
            project_name TEXT NOT NULL,
            environment TEXT NOT NULL,
            environment_binding_hash TEXT NOT NULL,
            compiler_version TEXT NOT NULL,
            dependency_tuple_json TEXT NOT NULL,
            dependency_tuple_hash TEXT NOT NULL,
            normalizer TEXT NOT NULL,
            lock_content_hash TEXT NOT NULL,
            lock_semantic_hash TEXT NOT NULL,
            compilation_mode TEXT NOT NULL,
            compiler_policies_json TEXT NOT NULL,
            features_json TEXT NOT NULL,
            authored_inputs_hash TEXT NOT NULL,
            lock_binding_hash TEXT NOT NULL,
            semantics_hash TEXT NOT NULL,
            lineage_hash TEXT NOT NULL,
            generated_at_unix_ms INTEGER
        );

        CREATE TABLE manifest_inputs (
            input_id TEXT NOT NULL,
            input_kind TEXT NOT NULL,
            location_json TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            parser TEXT NOT NULL,
            parser_version INTEGER NOT NULL,
            generation_json TEXT NOT NULL
        );

        CREATE TABLE manifest_resources (
            resource_id TEXT NOT NULL,
            compilation_hash TEXT NOT NULL,
            namespace TEXT NOT NULL,
            resource_name TEXT NOT NULL,
            resource_file TEXT NOT NULL,
            default_target TEXT NOT NULL,
            authored_form TEXT NOT NULL,
            authored_sql TEXT NOT NULL,
            authored_content_hash TEXT NOT NULL,
            authored_ast_hash TEXT NOT NULL,
            authored_input_ids_json TEXT NOT NULL,
            configured_source_json TEXT NOT NULL,
            canonical_arguments_hash TEXT NOT NULL,
            source_node_id TEXT NOT NULL,
            effective_json TEXT NOT NULL,
            relational_plan_json TEXT NOT NULL,
            descriptor_json TEXT NOT NULL,
            capabilities_json TEXT NOT NULL,
            execution_extent_json TEXT NOT NULL,
            compiled_stream_policy_json TEXT,
            source_plan_json TEXT NOT NULL,
            source_binding_json TEXT NOT NULL,
            output_schema_json TEXT NOT NULL,
            output_schema_hash TEXT NOT NULL,
            contract_json TEXT,
            destination_json TEXT NOT NULL
        );

        CREATE TABLE manifest_fields (
            resource_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            path TEXT NOT NULL,
            field_json TEXT NOT NULL,
            semantic_reference TEXT,
            semantic_definition_hash TEXT
        );

        CREATE TABLE manifest_semantics (
            definition_id TEXT NOT NULL,
            definition_hash TEXT NOT NULL,
            source_json TEXT NOT NULL,
            definition_json TEXT NOT NULL,
            compatibility_profile_hash TEXT NOT NULL,
            privacy_profile_hash TEXT NOT NULL,
            destination_mapping_profile_hash TEXT NOT NULL,
            references_json TEXT NOT NULL
        );

        CREATE TABLE manifest_lineage (
            edge_id TEXT NOT NULL,
            from_json TEXT NOT NULL,
            to_json TEXT NOT NULL,
            relation TEXT NOT NULL
        );

        CREATE TABLE manifest_diagnostics (
            ordinal INTEGER NOT NULL,
            severity TEXT NOT NULL,
            code TEXT NOT NULL,
            resource_id TEXT,
            input_id TEXT,
            message TEXT NOT NULL,
            remediation TEXT,
            authority TEXT NOT NULL,
            blocks_execution INTEGER NOT NULL
        );
        ",
    )
    .map_err(workspace_sqlite_error)
}

fn mount_manifest(
    conn: &Connection,
    manifest: &cdf_project::ProjectManifest,
) -> Result<(), CliError> {
    conn.execute(
        "
        INSERT INTO manifest_project (
            version,
            manifest_hash,
            project_name,
            environment,
            environment_binding_hash,
            compiler_version,
            dependency_tuple_json,
            dependency_tuple_hash,
            normalizer,
            lock_content_hash,
            lock_semantic_hash,
            compilation_mode,
            compiler_policies_json,
            features_json,
            authored_inputs_hash,
            lock_binding_hash,
            semantics_hash,
            lineage_hash,
            generated_at_unix_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ",
        params![
            i64::from(manifest.version),
            manifest.manifest_hash.as_str(),
            &manifest.header.project_name,
            &manifest.header.environment,
            manifest.header.environment_binding_hash.as_str(),
            &manifest.header.compiler_version,
            json_string(&manifest.header.dependency_tuple)?,
            manifest.header.dependency_tuple_hash.as_str(),
            &manifest.header.normalizer,
            manifest.header.lock_content_hash.as_str(),
            manifest.header.lock_semantic_hash.as_str(),
            json_scalar_string(&manifest.header.compilation_mode)?,
            json_string(&manifest.header.compiler_policies)?,
            json_string(&manifest.header.features)?,
            manifest.hashes.authored_inputs.as_str(),
            manifest.hashes.lock_binding.as_str(),
            manifest.hashes.semantics.as_str(),
            manifest.hashes.lineage.as_str(),
            manifest.generated_at_unix_ms,
        ],
    )
    .map_err(workspace_sqlite_error)?;

    let mut insert_input = conn
        .prepare(
            "
            INSERT INTO manifest_inputs (
                input_id,
                input_kind,
                location_json,
                content_hash,
                parser,
                parser_version,
                generation_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(workspace_sqlite_error)?;
    for input in &manifest.inputs {
        insert_input
            .execute(params![
                &input.input_id,
                json_scalar_string(&input.input_kind)?,
                json_string(&input.location)?,
                input.content_hash.as_str(),
                &input.parser,
                i64::from(input.parser_version),
                json_string(&input.generation)?,
            ])
            .map_err(workspace_sqlite_error)?;
    }

    let mut insert_resource = conn
        .prepare(
            "
            INSERT INTO manifest_resources (
                resource_id,
                compilation_hash,
                namespace,
                resource_name,
                resource_file,
                default_target,
                authored_form,
                authored_sql,
                authored_content_hash,
                authored_ast_hash,
                authored_input_ids_json,
                configured_source_json,
                canonical_arguments_hash,
                source_node_id,
                effective_json,
                relational_plan_json,
                descriptor_json,
                capabilities_json,
                execution_extent_json,
                compiled_stream_policy_json,
                source_plan_json,
                source_binding_json,
                output_schema_json,
                output_schema_hash,
                contract_json,
                destination_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(workspace_sqlite_error)?;
    let mut insert_field = conn
        .prepare(
            "
            INSERT INTO manifest_fields (
                resource_id,
                ordinal,
                path,
                field_json,
                semantic_reference,
                semantic_definition_hash
            ) VALUES (?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(workspace_sqlite_error)?;
    for resource in &manifest.resources {
        insert_resource
            .execute(params![
                &resource.resource_id,
                resource.compilation_hash.as_str(),
                &resource.origin.namespace,
                &resource.origin.resource_name,
                &resource.origin.relative_path,
                &resource.origin.default_target,
                json_scalar_string(&resource.origin.authored_form)?,
                &resource.origin.authored_sql,
                &resource.origin.authored_content_hash,
                &resource.origin.authored_ast_hash,
                json_string(&resource.origin.authored_input_ids)?,
                json_string(&resource.configured_source)?,
                &resource.canonical_arguments_hash,
                &resource.source_node_id,
                json_string(&resource.effective)?,
                json_string(&resource.relational_plan)?,
                json_string(&resource.descriptor)?,
                json_string(&resource.capabilities)?,
                json_string(&resource.execution_extent)?,
                optional_json_string(resource.compiled_stream_policy.as_ref())?,
                json_string(&resource.source_plan)?,
                json_string(&resource.source_binding)?,
                json_string(&resource.output_schema)?,
                resource.output_schema_hash.as_str(),
                optional_json_string(resource.contract.as_ref())?,
                json_string(&resource.destination)?,
            ])
            .map_err(workspace_sqlite_error)?;
        for field in &resource.fields {
            insert_field
                .execute(params![
                    &resource.resource_id,
                    i64::from(field.ordinal),
                    &field.path,
                    json_string(&field.field)?,
                    field.semantic_reference.as_deref(),
                    field.semantic_definition_hash.as_deref(),
                ])
                .map_err(workspace_sqlite_error)?;
        }
    }

    let mut insert_semantic = conn
        .prepare(
            "
            INSERT INTO manifest_semantics (
                definition_id,
                definition_hash,
                source_json,
                definition_json,
                compatibility_profile_hash,
                privacy_profile_hash,
                destination_mapping_profile_hash,
                references_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(workspace_sqlite_error)?;
    for semantic in &manifest.semantics {
        insert_semantic
            .execute(params![
                &semantic.definition_id,
                &semantic.definition_hash,
                json_string(&semantic.source)?,
                json_string(&semantic.definition)?,
                semantic.compatibility_profile_hash.as_str(),
                semantic.privacy_profile_hash.as_str(),
                semantic.destination_mapping_profile_hash.as_str(),
                json_string(&semantic.references)?,
            ])
            .map_err(workspace_sqlite_error)?;
    }

    let mut insert_lineage = conn
        .prepare(
            "
            INSERT INTO manifest_lineage (edge_id, from_json, to_json, relation)
            VALUES (?, ?, ?, ?)
            ",
        )
        .map_err(workspace_sqlite_error)?;
    for edge in &manifest.lineage {
        insert_lineage
            .execute(params![
                &edge.edge_id,
                json_string(&edge.from)?,
                json_string(&edge.to)?,
                json_scalar_string(&edge.relation)?,
            ])
            .map_err(workspace_sqlite_error)?;
    }

    let mut insert_diagnostic = conn
        .prepare(
            "
            INSERT INTO manifest_diagnostics (
                ordinal,
                severity,
                code,
                resource_id,
                input_id,
                message,
                remediation,
                authority,
                blocks_execution
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(workspace_sqlite_error)?;
    for (ordinal, diagnostic) in manifest.diagnostics.iter().enumerate() {
        insert_diagnostic
            .execute(params![
                manifest_ordinal_i64(ordinal)?,
                json_scalar_string(&diagnostic.severity)?,
                &diagnostic.code,
                diagnostic.resource_id.as_deref(),
                diagnostic.input_id.as_deref(),
                &diagnostic.message,
                diagnostic.remediation.as_deref(),
                &diagnostic.authority,
                diagnostic.blocks_execution,
            ])
            .map_err(workspace_sqlite_error)?;
    }
    Ok(())
}

fn mount_checkpoints(
    conn: &Connection,
    path: PathBuf,
    ownership: cdf_state_sqlite::StateStorePathOwnership,
) -> Result<(), CliError> {
    if !managed_state_database_exists(&path, ownership)? {
        return Ok(());
    }
    let open_path = cdf_state_sqlite::database_open_path(&path, ownership)?;
    let ledger = Connection::open_with_flags(
        open_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(managed_state_sqlite_error)?;
    let has_checkpoints = ledger
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'cdf_checkpoints'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(managed_state_sqlite_error)?
        .is_some();
    let checkpoint_footprint =
        has_checkpoints || managed_component_marker_exists(&ledger, "checkpoint_store")?;
    if !checkpoint_footprint {
        return Ok(());
    }
    SqliteCheckpointStore::open_read_only_with_path_ownership(&path, ownership)
        .and_then(|store| store.validate_integrity())
        .map_err(system_sql_store_error)?;
    if !has_checkpoints {
        return Ok(());
    }
    let mut insert = conn
        .prepare(
            "
            INSERT INTO checkpoints (
                sequence,
                checkpoint_id,
                pipeline_id,
                resource_id,
                scope_json,
                state_version,
                parent_checkpoint_id,
                input_position_json,
                output_position_json,
                package_hash,
                schema_hash,
                receipt_id,
                status,
                is_head,
                created_at_ms,
                committed_at_ms,
                delta_json,
                receipt_json,
                rewind_target_checkpoint_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(workspace_sqlite_error)?;
    let mut select = ledger
        .prepare(CHECKPOINT_HISTORY_SELECT)
        .map_err(managed_state_sqlite_error)?;
    let rows = select
        .query_map([], raw_checkpoint_row)
        .map_err(managed_state_sqlite_error)?;
    for row in rows {
        let row = row.map_err(managed_state_sqlite_error)?;
        insert
            .execute(params![
                row.sequence,
                row.checkpoint_id,
                row.pipeline_id,
                row.resource_id,
                row.scope_json,
                row.state_version,
                row.parent_checkpoint_id,
                row.input_position_json,
                row.output_position_json,
                row.package_hash,
                row.schema_hash,
                row.receipt_id,
                row.status,
                row.is_head,
                row.created_at_ms,
                row.committed_at_ms,
                row.delta_json,
                row.receipt_json,
                row.rewind_target_checkpoint_id,
            ])
            .map_err(workspace_sqlite_error)?;
    }
    Ok(())
}

fn managed_state_database_exists(
    path: &Path,
    ownership: cdf_state_sqlite::StateStorePathOwnership,
) -> Result<bool, CliError> {
    match cdf_state_sqlite::database_path_exists(path, ownership) {
        Ok(exists) => Ok(exists),
        Err(error) if error.kind == cdf_kernel::ErrorKind::Internal => {
            Err(CliError::mapped(error, error_catalog::SQL_INTERNAL))
        }
        Err(error) => Err(error.into()),
    }
}

fn mount_packages(conn: &Connection, root: PathBuf) -> Result<(), CliError> {
    if !system_sql_directory_exists(&root)? {
        return Ok(());
    }
    let mut entries = fs::read_dir(&root)
        .map_err(|error| system_sql_artifact_io_error("read package directory", &root, error))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| system_sql_artifact_io_error("read package entry", &root, error))?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| system_sql_artifact_io_error("inspect package entry", &path, error))?;
        if file_type.is_symlink() {
            return Err(CdfError::data(format!(
                "CDF system-SQL package entry {} must not be a symlink",
                path.display()
            ))
            .into());
        }
        if !file_type.is_dir() {
            continue;
        }
        if system_sql_regular_file_exists(&path.join(MANIFEST_FILE))? {
            mount_package(conn, &path)?;
        }
    }
    Ok(())
}

fn system_sql_directory_exists(path: &Path) -> Result<bool, CliError> {
    system_sql_path_exists(path, true)
}

fn system_sql_regular_file_exists(path: &Path) -> Result<bool, CliError> {
    system_sql_path_exists(path, false)
}

fn system_sql_path_exists(path: &Path, directory: bool) -> Result<bool, CliError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if (directory && metadata.is_dir()) || (!directory && metadata.is_file()) => {
            Ok(true)
        }
        Ok(_) => Err(CdfError::data(format!(
            "CDF system-SQL artifact {} has the wrong filesystem shape",
            path.display()
        ))
        .into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            validate_missing_system_sql_ancestors(path)?;
            Ok(false)
        }
        Err(error) => Err(system_sql_artifact_io_error("inspect", path, error)),
    }
}

fn validate_missing_system_sql_ancestors(path: &Path) -> Result<(), CliError> {
    let mut cursor = path.parent();
    while let Some(parent) = cursor {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        match fs::metadata(parent) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(CdfError::data(format!(
                    "CDF system-SQL artifact ancestor {} is not a real directory",
                    parent.display()
                ))
                .into());
            }
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::symlink_metadata(parent) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(CdfError::data(format!(
                            "CDF system-SQL artifact ancestor {} is a dangling symlink",
                            parent.display()
                        ))
                        .into());
                    }
                    Ok(_) => {
                        return Err(CdfError::data(format!(
                            "CDF system-SQL artifact ancestor {} changed filesystem shape during inspection",
                            parent.display()
                        ))
                        .into());
                    }
                    Err(link_error) if link_error.kind() == std::io::ErrorKind::NotFound => {
                        cursor = parent.parent();
                    }
                    Err(link_error) => {
                        return Err(system_sql_artifact_io_error(
                            "inspect ancestor",
                            parent,
                            link_error,
                        ));
                    }
                }
            }
            Err(error) => {
                return Err(system_sql_artifact_io_error(
                    "inspect ancestor",
                    parent,
                    error,
                ));
            }
        }
    }
    Ok(())
}

fn system_sql_artifact_io_error(action: &str, path: &Path, error: std::io::Error) -> CliError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::InvalidData
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::data(format!(
            "{action} CDF system-SQL artifact {}: {error}",
            path.display()
        ))
        .into()
    } else {
        CdfError::environment(format!(
            "{action} CDF system-SQL artifact {}: {error}; check path permissions, device availability, memory, and process file limits before retrying",
            path.display()
        ))
        .into()
    }
}

fn mount_package(conn: &Connection, path: &Path) -> Result<(), CliError> {
    let manifest = cdf_package::read_manifest_header(path)?;
    let package_id = manifest.identity.package_id.as_str();
    let package_hash = manifest.package_hash.as_str();
    let reader = PackageReader::open(path)?;
    let receipt_count = reader.receipt_count()?;
    let mut identity_file_count = 0_u64;
    let mut segment_count = 0_u64;
    cdf_package::visit_manifest_entries(
        path,
        &mut |_| {
            identity_file_count = identity_file_count
                .checked_add(1)
                .ok_or_else(|| CdfError::data("package identity file count overflowed u64"))?;
            Ok(())
        },
        &mut |_| {
            segment_count = segment_count
                .checked_add(1)
                .ok_or_else(|| CdfError::data("package segment count overflowed u64"))?;
            Ok(())
        },
    )?;
    conn.execute(
        "
        INSERT INTO packages (
            package_path,
            package_id,
            package_hash,
            status,
            signing_input,
            signature,
            identity_file_count,
            segment_count,
            receipt_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ",
        params![
            path.display().to_string(),
            package_id,
            package_hash,
            manifest.lifecycle.status.as_str(),
            &manifest.signature.signing_input,
            manifest.signature.value.as_deref(),
            to_i64(identity_file_count)?,
            to_i64(segment_count)?,
            to_i64(receipt_count)?,
        ],
    )
    .map_err(workspace_sqlite_error)?;

    let mut insert_file = conn
        .prepare(
            "
            INSERT INTO package_files (
                package_hash,
                package_id,
                path,
                byte_count,
                sha256
            ) VALUES (?, ?, ?, ?, ?)
            ",
        )
        .map_err(workspace_sqlite_error)?;
    let mut insert_segment = conn
        .prepare(
            "
            INSERT INTO package_segments (
                package_hash,
                package_id,
                segment_id,
                path,
                row_count,
                byte_count,
                sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(workspace_sqlite_error)?;
    cdf_package::visit_manifest_entries(
        path,
        &mut |file| {
            insert_file
                .execute(params![
                    package_hash,
                    package_id,
                    &file.path,
                    to_i64(file.byte_count)?,
                    &file.sha256,
                ])
                .map_err(workspace_sqlite_error)?;
            Ok(())
        },
        &mut |segment| {
            insert_segment
                .execute(params![
                    package_hash,
                    package_id,
                    segment.segment_id.as_str(),
                    &segment.path,
                    to_i64(segment.row_count)?,
                    to_i64(segment.byte_count)?,
                    &segment.sha256,
                ])
                .map_err(workspace_sqlite_error)?;
            Ok(())
        },
    )?;

    let mut insert_receipt = conn
        .prepare(
            "
            INSERT INTO package_receipts (
                package_hash,
                package_id,
                receipt_id,
                destination,
                target,
                disposition,
                idempotency_token,
                rows_written,
                rows_inserted,
                rows_updated,
                rows_deleted,
                schema_hash,
                committed_at_ms,
                receipt_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(workspace_sqlite_error)?;
    let mut insert_ack = conn
        .prepare(
            "
            INSERT INTO package_receipt_segments (
                package_hash,
                package_id,
                receipt_id,
                segment_id,
                row_count,
                byte_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            ",
        )
        .map_err(workspace_sqlite_error)?;
    let mut mount_error = None;
    let visit_result = reader.for_each_receipt(&mut |receipt| {
        let result = (|| -> Result<(), CliError> {
            let receipt_json = json_string(&receipt)?;
            insert_receipt
                .execute(params![
                    package_hash,
                    package_id,
                    receipt.receipt_id.as_str(),
                    receipt.destination.as_str(),
                    receipt.target.as_str(),
                    json_scalar_string(&receipt.disposition)?,
                    receipt.idempotency_token.as_str(),
                    to_i64(receipt.counts.rows_written)?,
                    optional_to_i64(receipt.counts.rows_inserted)?,
                    optional_to_i64(receipt.counts.rows_updated)?,
                    optional_to_i64(receipt.counts.rows_deleted)?,
                    receipt.schema_hash.as_str(),
                    receipt.committed_at_ms,
                    receipt_json,
                ])
                .map_err(workspace_sqlite_error)?;
            for ack in &receipt.segment_acks {
                insert_ack
                    .execute(params![
                        package_hash,
                        package_id,
                        receipt.receipt_id.as_str(),
                        ack.segment_id.as_str(),
                        to_i64(ack.row_count)?,
                        to_i64(ack.byte_count)?,
                    ])
                    .map_err(workspace_sqlite_error)?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                mount_error = Some(error);
                Err(CdfError::internal("mount package receipt failed"))
            }
        }
    });
    if let Some(error) = mount_error {
        return Err(error);
    }
    visit_result?;
    Ok(())
}

#[derive(Debug)]
struct RawCheckpointRow {
    sequence: i64,
    checkpoint_id: String,
    pipeline_id: String,
    resource_id: String,
    scope_json: String,
    state_version: i64,
    parent_checkpoint_id: Option<String>,
    input_position_json: Option<String>,
    output_position_json: String,
    package_hash: String,
    schema_hash: String,
    receipt_id: Option<String>,
    status: String,
    is_head: i64,
    created_at_ms: i64,
    committed_at_ms: Option<i64>,
    delta_json: String,
    receipt_json: Option<String>,
    rewind_target_checkpoint_id: Option<String>,
}

fn raw_checkpoint_row(row: &Row<'_>) -> rusqlite::Result<RawCheckpointRow> {
    Ok(RawCheckpointRow {
        sequence: row.get("sequence")?,
        checkpoint_id: row.get("checkpoint_id")?,
        pipeline_id: row.get("pipeline_id")?,
        resource_id: row.get("resource_id")?,
        scope_json: row.get("scope_json")?,
        state_version: row.get("state_version")?,
        parent_checkpoint_id: row.get("parent_checkpoint_id")?,
        input_position_json: row.get("input_position_json")?,
        output_position_json: row.get("output_position_json")?,
        package_hash: row.get("package_hash")?,
        schema_hash: row.get("schema_hash")?,
        receipt_id: row.get("receipt_id")?,
        status: row.get("status")?,
        is_head: row.get("is_head")?,
        created_at_ms: row.get("created_at_ms")?,
        committed_at_ms: row.get("committed_at_ms")?,
        delta_json: row.get("delta_json")?,
        receipt_json: row.get("receipt_json")?,
        rewind_target_checkpoint_id: row.get("rewind_target_checkpoint_id")?,
    })
}

fn query_rows(conn: &Connection, query: &str) -> Result<SystemSqlReport, CliError> {
    let mut stmt = conn.prepare(query).map_err(query_cli_error)?;
    reject_non_readonly_statement(&stmt)?;
    let columns = stmt
        .column_names()
        .into_iter()
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Err(CliError::usage_with(
            "sql requires a read-only query that returns columns",
            error_catalog::SQL_QUERY,
        ));
    }

    let mut rows = stmt.query([]).map_err(query_cli_error)?;
    let mut values = Vec::new();
    while let Some(row) = rows.next().map_err(query_cli_error)? {
        values.push(row_values(row, columns.len())?);
    }
    Ok(SystemSqlReport {
        tables: TABLES.to_vec(),
        columns,
        rows: values,
    })
}

fn reject_non_readonly_statement(stmt: &Statement<'_>) -> Result<(), CliError> {
    if stmt.readonly() {
        Ok(())
    } else {
        Err(CliError::usage_with(
            "sql accepts one read-only SELECT or WITH query",
            error_catalog::SQL_QUERY,
        ))
    }
}

fn row_values(row: &Row<'_>, column_count: usize) -> Result<Vec<Value>, CliError> {
    (0..column_count)
        .map(|index| {
            row.get_ref(index)
                .map(sql_json_value)
                .map_err(query_cli_error)
        })
        .collect()
}

pub(crate) fn read_only_query(query: &str) -> Result<&str, CliError> {
    let query = query.trim();
    if query.is_empty() {
        return Err(CliError::usage_with(
            "sql requires a query string",
            error_catalog::SQL_QUERY,
        ));
    }
    let query = strip_trailing_semicolon(query)?;
    match leading_keyword(query).as_deref() {
        Some("select" | "with") => {
            reject_mutating_keywords(query)?;
            Ok(query)
        }
        _ => Err(CliError::usage_with(
            "sql accepts one read-only SELECT or WITH query",
            error_catalog::SQL_QUERY,
        )),
    }
}

fn strip_trailing_semicolon(query: &str) -> Result<&str, CliError> {
    let mut semicolon = None;
    let mut scanner = Scanner::new(query);
    while let Some((index, ch)) = scanner.next_code_char()? {
        if ch == ';' {
            semicolon = Some(index);
            break;
        }
    }
    let Some(index) = semicolon else {
        return Ok(query);
    };
    let rest = &query[index + 1..];
    if has_code(rest)? {
        return Err(CliError::usage_with(
            "sql accepts one query statement",
            error_catalog::SQL_QUERY,
        ));
    }
    Ok(query[..index].trim_end())
}

fn leading_keyword(query: &str) -> Option<String> {
    let mut scanner = Scanner::new(query);
    while let Some((_, ch)) = scanner.next_leading_char().ok()? {
        if ch.is_whitespace() {
            continue;
        }
        if ch == '_' || ch.is_ascii_alphabetic() {
            let mut keyword = String::new();
            keyword.push(ch.to_ascii_lowercase());
            while let Some(ch) = scanner.peek_char() {
                if ch == '_' || ch.is_ascii_alphanumeric() {
                    keyword.push(ch.to_ascii_lowercase());
                    scanner.next_char();
                } else {
                    break;
                }
            }
            return Some(keyword);
        }
        return None;
    }
    None
}

fn reject_mutating_keywords(query: &str) -> Result<(), CliError> {
    let mut scanner = Scanner::new(query);
    while let Some(keyword) = scanner.next_keyword()? {
        if MUTATING_KEYWORDS.contains(&keyword.as_str()) {
            return Err(CliError::usage_with(
                "sql accepts one read-only SELECT or WITH query",
                error_catalog::SQL_QUERY,
            ));
        }
    }
    Ok(())
}

fn has_code(sql: &str) -> Result<bool, CliError> {
    let mut scanner = Scanner::new(sql);
    while let Some((_, ch)) = scanner.next_code_char()? {
        if !ch.is_whitespace() {
            return Ok(true);
        }
    }
    Ok(false)
}

struct Scanner<'a> {
    sql: &'a str,
    cursor: usize,
}

impl<'a> Scanner<'a> {
    fn new(sql: &'a str) -> Self {
        Self { sql, cursor: 0 }
    }

    fn next_keyword(&mut self) -> Result<Option<String>, CliError> {
        while let Some((_, ch)) = self.next_code_char()? {
            if ch == '_' || ch.is_ascii_alphabetic() {
                let mut keyword = String::new();
                keyword.push(ch.to_ascii_lowercase());
                while let Some(ch) = self.peek_char() {
                    if ch == '_' || ch.is_ascii_alphanumeric() {
                        keyword.push(ch.to_ascii_lowercase());
                        self.next_char();
                    } else {
                        break;
                    }
                }
                return Ok(Some(keyword));
            }
        }
        Ok(None)
    }

    fn next_leading_char(&mut self) -> Result<Option<(usize, char)>, CliError> {
        loop {
            let Some((index, ch)) = self.next_char() else {
                return Ok(None);
            };
            match ch {
                '-' if self.peek_char() == Some('-') => {
                    self.next_char();
                    self.skip_line_comment();
                }
                '/' if self.peek_char() == Some('*') => {
                    self.next_char();
                    self.skip_block_comment()?;
                }
                _ => return Ok(Some((index, ch))),
            }
        }
    }

    fn next_code_char(&mut self) -> Result<Option<(usize, char)>, CliError> {
        loop {
            let Some((index, ch)) = self.next_char() else {
                return Ok(None);
            };
            match ch {
                '\'' => self.skip_quoted('\'')?,
                '"' => self.skip_quoted('"')?,
                '-' if self.peek_char() == Some('-') => {
                    self.next_char();
                    self.skip_line_comment();
                }
                '/' if self.peek_char() == Some('*') => {
                    self.next_char();
                    self.skip_block_comment()?;
                }
                _ => return Ok(Some((index, ch))),
            }
        }
    }

    fn next_char(&mut self) -> Option<(usize, char)> {
        let rest = self.sql.get(self.cursor..)?;
        let (offset, ch) = rest.char_indices().next()?;
        let index = self.cursor + offset;
        self.cursor = index + ch.len_utf8();
        Some((index, ch))
    }

    fn peek_char(&self) -> Option<char> {
        self.sql.get(self.cursor..)?.chars().next()
    }

    fn skip_quoted(&mut self, quote: char) -> Result<(), CliError> {
        while let Some((_, ch)) = self.next_char() {
            if ch == quote {
                if self.peek_char() == Some(quote) {
                    self.next_char();
                } else {
                    return Ok(());
                }
            }
        }
        Err(CliError::usage_with(
            "sql query contains an unterminated string",
            error_catalog::SQL_QUERY,
        ))
    }

    fn skip_line_comment(&mut self) {
        while let Some((_, ch)) = self.next_char() {
            if ch == '\n' {
                return;
            }
        }
    }

    fn skip_block_comment(&mut self) -> Result<(), CliError> {
        while let Some((_, ch)) = self.next_char() {
            if ch == '*' && self.peek_char() == Some('/') {
                self.next_char();
                return Ok(());
            }
        }
        Err(CliError::usage_with(
            "sql query contains an unterminated comment",
            error_catalog::SQL_QUERY,
        ))
    }
}

fn sql_json_value(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::Number(value.into()),
        ValueRef::Real(value) => Number::from_f64(value).map_or(Value::Null, Value::Number),
        ValueRef::Text(value) => Value::String(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::String(format!("0x{}", hex_bytes(value))),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn json_string<T: Serialize>(value: &T) -> Result<String, CliError> {
    serde_json::to_string(value).map_err(json_cli_error)
}

fn optional_json_string<T: Serialize>(value: Option<&T>) -> Result<Option<String>, CliError> {
    value.map(json_string).transpose()
}

fn json_scalar_string<T: Serialize>(value: &T) -> Result<String, CliError> {
    let value = serde_json::to_value(value).map_err(json_cli_error)?;
    value.as_str().map(ToOwned::to_owned).ok_or_else(|| {
        CliError::mapped(
            CdfError::data("expected JSON string scalar"),
            error_catalog::SQL_RESULT,
        )
    })
}

fn to_i64(value: impl TryInto<i64>) -> Result<i64, CliError> {
    value.try_into().map_err(|_| {
        CliError::mapped(
            CdfError::data("package value does not fit in the SQLite i64 representation"),
            error_catalog::PACKAGE_ARTIFACT,
        )
    })
}

fn optional_to_i64<T>(value: Option<T>) -> Result<Option<i64>, CliError>
where
    T: TryInto<i64>,
{
    value.map(to_i64).transpose()
}

fn manifest_ordinal_i64(value: usize) -> Result<i64, CliError> {
    i64::try_from(value).map_err(|_| {
        CliError::mapped(
            CdfError::data("manifest ordinal does not fit in the SQLite i64 representation"),
            error_catalog::SQL_RESULT,
        )
    })
}

fn workspace_sqlite_error(error: rusqlite::Error) -> CliError {
    map_sqlite_error(
        SqliteErrorContext::EphemeralWorkspace,
        "operate the CDF system-SQL in-memory workspace",
        error,
    )
}

fn managed_state_sqlite_error(error: rusqlite::Error) -> CliError {
    map_sqlite_error(
        SqliteErrorContext::ManagedState,
        "read the CDF system-SQL state store",
        error,
    )
}

fn system_sql_store_error(error: CdfError) -> CliError {
    if error.kind == cdf_kernel::ErrorKind::Internal {
        CliError::mapped(error, error_catalog::SQL_INTERNAL)
    } else {
        error.into()
    }
}

fn managed_component_marker_exists(conn: &Connection, component: &str) -> Result<bool, CliError> {
    let version_table = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'cdf_sqlite_schema_versions'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(managed_state_sqlite_error)?
        .is_some();
    if !version_table {
        return Ok(false);
    }
    conn.query_row(
        "SELECT 1 FROM cdf_sqlite_schema_versions WHERE component = ?",
        params![component],
        |_| Ok(()),
    )
    .optional()
    .map(|value| value.is_some())
    .map_err(managed_state_sqlite_error)
}

fn map_sqlite_error(context: SqliteErrorContext, action: &str, error: rusqlite::Error) -> CliError {
    let error = classify_sqlite_error(context, action, error);
    if error.kind == cdf_kernel::ErrorKind::Internal {
        CliError::mapped(error, error_catalog::SQL_INTERNAL)
    } else {
        error.into()
    }
}

fn query_cli_error(error: rusqlite::Error) -> CliError {
    let is_user_query_error = matches!(
        error,
        rusqlite::Error::InvalidQuery
            | rusqlite::Error::MultipleStatement
            | rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ErrorCode::Unknown,
                    ..
                },
                _
            )
    );
    let message = error.to_string();
    let classified = classify_sqlite_error(
        SqliteErrorContext::EphemeralWorkspace,
        "execute the CDF system-SQL query",
        error,
    );
    if classified.kind != cdf_kernel::ErrorKind::Internal {
        classified.into()
    } else if is_user_query_error {
        CliError::usage_with(
            format!("sql query failed: {message}"),
            error_catalog::SQL_QUERY,
        )
    } else {
        CliError::mapped(classified, error_catalog::SQL_INTERNAL)
    }
}

fn json_cli_error(error: serde_json::Error) -> CliError {
    CliError::mapped(CdfError::data(error.to_string()), error_catalog::SQL_RESULT)
}

#[cfg(test)]
mod tests {
    use cdf_kernel::ErrorKind;

    use super::*;

    #[test]
    fn system_sql_managed_state_host_failures_keep_stable_environment_mapping() {
        let error = managed_state_sqlite_error(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CANTOPEN),
            None,
        ));

        assert_eq!(error.kind, ErrorKind::Environment);
        assert_eq!(error.code, "CDF-ENV-HOST");
        assert!(error.message.contains("state path"));
        assert!(error.remediation.is_some());
    }

    #[test]
    fn system_sql_configured_directory_at_state_database_is_contract_owned() {
        let root = tempfile::tempdir().unwrap();
        let state_path = root.path().join("state.db");
        fs::create_dir(&state_path).unwrap();
        let workspace = Connection::open_in_memory().unwrap();
        create_schema(&workspace).unwrap();

        let error = mount_checkpoints(
            &workspace,
            state_path,
            cdf_state_sqlite::StateStorePathOwnership::Configured,
        )
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Contract);
    }

    #[test]
    fn system_sql_rejects_checkpoint_marker_without_owned_table() {
        let root = tempfile::tempdir().unwrap();
        let state_path = root.path().join(".cdf").join("state.db");
        fs::create_dir_all(state_path.parent().unwrap()).unwrap();
        let ledger = Connection::open(&state_path).unwrap();
        ledger
            .execute_batch(
                "
                CREATE TABLE cdf_sqlite_schema_versions (
                    component TEXT PRIMARY KEY,
                    version INTEGER NOT NULL,
                    recorded_at_ms INTEGER NOT NULL
                );
                INSERT INTO cdf_sqlite_schema_versions (component, version, recorded_at_ms)
                VALUES ('checkpoint_store', 1, 1);
                ",
            )
            .unwrap();
        drop(ledger);
        let workspace = Connection::open_in_memory().unwrap();
        create_schema(&workspace).unwrap();

        let error = mount_checkpoints(
            &workspace,
            state_path,
            cdf_state_sqlite::StateStorePathOwnership::CdfManaged,
        )
        .unwrap_err();

        assert_eq!(error.kind, ErrorKind::Internal);
        assert_eq!(error.code, error_catalog::SQL_INTERNAL.code);
    }

    #[test]
    fn system_sql_ephemeral_workspace_resource_failure_is_environment_owned() {
        let error = workspace_sqlite_error(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_NOMEM),
            None,
        ));

        assert_eq!(error.kind, ErrorKind::Environment);
        assert_eq!(error.code, "CDF-ENV-HOST");
        assert!(error.message.contains("free memory"));
        assert!(!error.message.contains("state path"));
    }

    #[test]
    fn system_sql_package_manifest_parent_file_is_data_owned() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("package");
        fs::write(&parent, b"not a directory").unwrap();

        let error = system_sql_regular_file_exists(&parent.join(MANIFEST_FILE)).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Data);
    }

    #[cfg(unix)]
    #[test]
    fn system_sql_rejects_symlinked_package_directories() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), root.path().join("escaped-package")).unwrap();
        let workspace = Connection::open_in_memory().unwrap();

        let error = mount_packages(&workspace, root.path().to_path_buf()).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Data);
        assert!(error.message.contains("must not be a symlink"));
    }

    #[test]
    fn system_sql_query_errors_preserve_host_and_invariant_ownership() {
        let host = query_cli_error(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_NOMEM),
            None,
        ));
        assert_eq!(host.kind, ErrorKind::Environment);
        assert_eq!(host.code, "CDF-ENV-HOST");

        let usage = query_cli_error(rusqlite::Error::InvalidQuery);
        assert_eq!(usage.kind, ErrorKind::Contract);
        assert_eq!(usage.code, error_catalog::SQL_QUERY.code);

        let invariant = query_cli_error(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            None,
        ));
        assert_eq!(invariant.kind, ErrorKind::Internal);
        assert_eq!(invariant.code, error_catalog::SQL_INTERNAL.code);
    }
}
