use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use cdf_kernel::{CdfError, QueryableResource, Result};
use cdf_runtime::{
    CompiledSourcePlan, SourceAddCursor, SourceAddCursorOrdering, SourceAddPlanner,
    SourceAddProposal, SourceAddRequest, SourceAttestationStrength, SourceCompileRequest,
    SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest, SourceDiscoverySession,
    SourceDriver, SourceDriverDescriptor, SourceDriverId, SourceEvidenceLocation,
    SourceExecutionCapabilities, SourceExecutorClass, SourceHealthRequest, SourceHealthResult,
    SourceHealthStatus, SourceResolutionContext, SourceRetryGranularity, SourceSchemaObservation,
    artifact_hash,
};
use serde::{Deserialize, Serialize};

use crate::{
    catalog::discover_sqlite_table,
    identifier::SqliteIdentifier,
    source::{
        SQLITE_MAXIMUM_BATCH_BYTES, SQLITE_SOURCE_BLOCKING_LANE_ID, SqliteTableResource,
        SqliteTemporalEncoding, sqlite_source_blocking_lane, sqlite_table_capabilities,
        validate_sqlite_table_resource_shape,
    },
};

#[derive(Clone, Debug)]
pub struct SqliteSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
}

impl SqliteSourceDriver {
    pub fn new() -> Result<Self> {
        let temporal_encoding = serde_json::json!({
            "enum": ["iso8601_text", "unix_seconds", "unix_milliseconds", "unix_microseconds", "unix_nanoseconds"]
        });
        let option_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "source": {
                "type": "object",
                "additionalProperties": false,
                "required": ["location"],
                "properties": {
                    "location": {"type": "string", "pattern": "^sqlite://"},
                    "dialect": {"const": "sqlite", "default": "sqlite"}
                }
            },
            "resource": {
                "type": "object",
                "additionalProperties": false,
                "required": ["table"],
                "properties": {
                    "table": {"type": "string", "minLength": 1},
                    "stable_key": {"type": "string", "minLength": 1},
                    "temporal_encodings": {
                        "type": "object",
                        "additionalProperties": temporal_encoding
                    }
                }
            }
        });
        Ok(Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new("sqlite")?,
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema)?,
                kinds: vec!["sqlite".to_owned()],
                schemes: vec!["sqlite".to_owned()],
            },
            option_schema,
        })
    }
}

impl SourceDriver for SqliteSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }
    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn validate_portable_plan(&self, plan: &CompiledSourcePlan) -> Result<()> {
        plan.validate()?;
        let physical = decode_physical_plan(plan)?;
        physical.validate()?;
        if Path::new(&physical.database_path).is_absolute() {
            return Err(CdfError::contract(
                "portable SQLite source plans require a project-relative database location; move the database below the project root and recompile",
            ));
        }
        validate_sqlite_compile_shape(
            &plan.descriptor,
            &Arc::new(plan.schema.clone()),
            &physical.table,
            physical.stable_key.as_ref(),
            &physical.temporal_encodings,
        )
    }

    fn add_planner(&self) -> Option<&dyn SourceAddPlanner> {
        Some(self)
    }

    fn health(
        &self,
        request: SourceHealthRequest,
        context: &SourceResolutionContext<'_>,
        output: &mut dyn cdf_runtime::SourceHealthSink,
    ) -> Result<()> {
        if request.compiled_plans.is_empty() {
            return output.emit(SourceHealthResult {
                probe_id: "catalog".to_owned(),
                status: SourceHealthStatus::Skipped,
                message: "no SQLite resources are compiled".to_owned(),
                details: serde_json::json!({"resources": 0}),
            });
        }
        let probe_request =
            SourceDiscoveryRequest::new(1, 1)?.with_cancellation(request.budget.cancellation());
        for plan in &request.compiled_plans {
            request.budget.consume_work(1)?;
            request.budget.consume_list_entries(1)?;
            let resource_id = plan.descriptor.resource_id.as_str();
            let probe = self.discovery_session(plan, context).and_then(|session| {
                let candidate = session.candidates()?.into_iter().next().ok_or_else(|| {
                    CdfError::data("SQLite health probe produced no catalog candidate")
                })?;
                session.observe(&candidate, &probe_request)
            });
            let result = match probe {
                Ok(observation) => SourceHealthResult {
                    probe_id: resource_id.to_owned(),
                    status: SourceHealthStatus::Passed,
                    message: "SQLite read-only catalog probe passed".to_owned(),
                    details: serde_json::json!({
                        "resource_id": resource_id,
                        "columns": observation.schema.fields().len(),
                    }),
                },
                Err(error) => SourceHealthResult::failed(
                    resource_id,
                    "SQLite read-only catalog probe failed",
                    &plan.descriptor.resource_id,
                    &error,
                ),
            };
            output.emit(result)?;
        }
        Ok(())
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        let source: SqliteSourceOptions = decode_options("SQLite source", request.source_options)?;
        let resource: SqliteResourceOptions =
            decode_options("SQLite resource", request.resource_options)?;
        if !source
            .dialect
            .as_deref()
            .is_none_or(|dialect| dialect.eq_ignore_ascii_case("sqlite"))
        {
            return Err(CdfError::contract(
                "SQLite source dialect must be `sqlite` when declared",
            ));
        }
        let database_path = normalize_sqlite_location(&source.location)?;
        let table = SqliteIdentifier::new(resource.table)?;
        let stable_key = resource.stable_key.map(SqliteIdentifier::new).transpose()?;
        let mut temporal_encodings = resource.temporal_encodings;
        for field in request.schema.fields() {
            if matches!(
                field.data_type(),
                arrow_schema::DataType::Date32 | arrow_schema::DataType::Timestamp(..)
            ) && !temporal_encodings.contains_key(field.name())
                && let Some(value) = field
                    .metadata()
                    .get(crate::catalog::SQLITE_TEMPORAL_ENCODING_METADATA_KEY)
            {
                let encoding = serde_json::from_value(serde_json::Value::String(value.clone()))
                    .map_err(|error| {
                        CdfError::contract(format!(
                            "SQLite field `{}` has invalid catalog temporal encoding: {error}",
                            field.name()
                        ))
                    })?;
                temporal_encodings.insert(field.name().to_owned(), encoding);
            }
        }
        validate_sqlite_compile_shape(
            &request.descriptor,
            &Arc::new(request.schema.clone()),
            &table,
            stable_key.as_ref(),
            &temporal_encodings,
        )?;
        let physical = SqlitePhysicalPlan {
            database_path: database_path.clone(),
            table: table.clone(),
            stable_key: stable_key.clone(),
            temporal_encodings: temporal_encodings.clone(),
        };
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            sqlite_table_capabilities(&request.descriptor),
            execution_capabilities(&request.descriptor),
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog: request.baseline_observation_schema_catalog,
                redacted_options: serde_json::json!({
                    "location": if Path::new(&database_path).is_absolute() { "sqlite://[local-database]" } else { source.location.as_str() },
                    "dialect": "sqlite",
                    "table": table.as_str(),
                    "stable_key": stable_key.as_ref().map(SqliteIdentifier::as_str),
                    "temporal_encodings": temporal_encodings,
                }),
                physical_plan: serde_json::to_value(physical).map_err(|error| {
                    CdfError::internal(format!("serialize SQLite source plan: {error}"))
                })?,
            },
        )
    }

    fn discovery_session(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Box<dyn SourceDiscoverySession>> {
        plan.validate()?;
        let physical = decode_physical_plan(plan)?;
        physical.validate()?;
        Ok(Box::new(SqliteDiscoverySession {
            database_path: resolve_database_path(context.project_root(), &physical.database_path),
            resource_id: plan.descriptor.resource_id.clone(),
            table: physical.table,
            execution: context.execution().clone(),
        }))
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        let physical = decode_physical_plan(plan)?;
        physical.validate()?;
        let resource = SqliteTableResource::from_compiled_plan(
            plan,
            resolve_database_path(context.project_root(), &physical.database_path),
            physical.table,
            physical.stable_key,
            physical.temporal_encodings,
            context.execution().clone(),
        )?;
        Ok(Arc::new(resource))
    }
}

impl SourceAddPlanner for SqliteSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        request.validate()?;
        if !request.location.starts_with("sqlite://") {
            return Ok(None);
        }
        const KEYS: [&str; 4] = ["table", "cursor", "stable_key", "cursor_encoding"];
        let unknown = request
            .options
            .keys()
            .filter(|key| !KEYS.contains(&key.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Err(CdfError::contract(format!(
                "SQLite cdf add received unknown options: {}",
                unknown.join(", ")
            )));
        }
        let table = request.options.get("table").ok_or_else(|| {
            CdfError::contract("SQLite cdf add requires `--option table=<table>`")
        })?;
        SqliteIdentifier::new(table.clone())?;
        let cursor = request.options.get("cursor");
        let stable_key = request.options.get("stable_key");
        let cursor_encoding = request.options.get("cursor_encoding");
        if cursor.is_some() != stable_key.is_some() {
            return Err(CdfError::contract(
                "SQLite cdf add cursor and stable_key options must be supplied together",
            ));
        }
        if cursor_encoding.is_some() && cursor.is_none() {
            return Err(CdfError::contract(
                "SQLite cdf add cursor_encoding requires a cursor",
            ));
        }
        let encoding = cursor_encoding
            .map(|value| serde_json::from_value::<SqliteTemporalEncoding>(serde_json::Value::String(value.clone()))
                .map_err(|_| CdfError::contract("SQLite cursor_encoding must be iso8601_text, unix_seconds, unix_milliseconds, unix_microseconds, or unix_nanoseconds")))
            .transpose()?;
        let relative = portable_add_path(request)?;
        let location = format!("sqlite://{}", portable_path(&relative)?);
        let mut resource_options =
            BTreeMap::from([("table".to_owned(), serde_json::Value::String(table.clone()))]);
        if let Some(stable_key) = stable_key {
            SqliteIdentifier::new(stable_key.clone())?;
            resource_options.insert(
                "stable_key".to_owned(),
                serde_json::Value::String(stable_key.clone()),
            );
        }
        if let (Some(cursor), Some(encoding)) = (cursor, encoding) {
            resource_options.insert(
                "temporal_encodings".to_owned(),
                serde_json::json!({cursor: encoding}),
            );
        }
        Ok(Some(SourceAddProposal {
            source_kind: "sqlite".to_owned(),
            source_options: BTreeMap::from([
                (
                    "location".to_owned(),
                    serde_json::Value::String(location.clone()),
                ),
                (
                    "dialect".to_owned(),
                    serde_json::Value::String("sqlite".to_owned()),
                ),
            ]),
            resource_options,
            cursor: cursor.map(|field| SourceAddCursor {
                field: field.clone(),
                parameter: None,
                ordering: SourceAddCursorOrdering::Exact,
                lag_tolerance_ms: 0,
            }),
            display_location: SourceEvidenceLocation::from_operational(&location)?,
            display_selection: table.clone(),
            private_files: Vec::new(),
        }))
    }
}

struct SqliteDiscoverySession {
    database_path: PathBuf,
    resource_id: cdf_kernel::ResourceId,
    table: SqliteIdentifier,
    execution: cdf_runtime::ExecutionServices,
}

impl SourceDiscoverySession for SqliteDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        SourceDiscoveryKind::SchemaMetadata
    }
    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(vec![SourceDiscoveryCandidate::new(
            self.table.as_str(),
            None,
            None,
            BTreeMap::from([
                ("source_kind".to_owned(), "sqlite".to_owned()),
                ("dialect".to_owned(), "sqlite".to_owned()),
            ]),
        )?])
    }
    fn observe(
        &self,
        candidate: &SourceDiscoveryCandidate,
        request: &SourceDiscoveryRequest,
    ) -> Result<SourceSchemaObservation> {
        request.validate()?;
        if candidate.canonical_location != self.table.as_str() {
            return Err(CdfError::contract(
                "SQLite discovery candidate does not match the compiled table",
            ));
        }
        let path = self.database_path.clone();
        let resource_id = self.resource_id.clone();
        let table = self.table.clone();
        let discovery = self
            .execution
            .run_blocking(SQLITE_SOURCE_BLOCKING_LANE_ID, move || {
                discover_sqlite_table(&path, &resource_id, &table)
            })?;
        let mut identity = discovery.source_identity;
        identity.insert(
            "catalog_column_count".to_owned(),
            discovery.schema.fields().len().to_string(),
        );
        SourceSchemaObservation::new(candidate, discovery.schema, identity, 0, 0)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SqliteSourceOptions {
    location: String,
    #[serde(default)]
    dialect: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SqliteResourceOptions {
    table: String,
    #[serde(default)]
    stable_key: Option<String>,
    #[serde(default)]
    temporal_encodings: BTreeMap<String, SqliteTemporalEncoding>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SqlitePhysicalPlan {
    database_path: String,
    table: SqliteIdentifier,
    stable_key: Option<SqliteIdentifier>,
    temporal_encodings: BTreeMap<String, SqliteTemporalEncoding>,
}
impl SqlitePhysicalPlan {
    fn validate(&self) -> Result<()> {
        validate_normalized_path(&self.database_path)?;
        for field in self.temporal_encodings.keys() {
            SqliteIdentifier::new(field.clone())?;
        }
        Ok(())
    }
}

fn decode_physical_plan(plan: &CompiledSourcePlan) -> Result<SqlitePhysicalPlan> {
    serde_json::from_value(plan.physical_plan.clone())
        .map_err(|error| CdfError::contract(format!("invalid SQLite source plan: {error}")))
}

fn decode_options<T: for<'de> Deserialize<'de>>(
    label: &str,
    options: BTreeMap<String, serde_json::Value>,
) -> Result<T> {
    serde_json::from_value(serde_json::Value::Object(options.into_iter().collect()))
        .map_err(|error| CdfError::contract(format!("{label} options are invalid: {error}")))
}

fn execution_capabilities(
    descriptor: &cdf_kernel::ResourceDescriptor,
) -> SourceExecutionCapabilities {
    let resumable = descriptor.cursor.is_some();
    SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: SQLITE_MAXIMUM_BATCH_BYTES,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: 64 * 1024 * 1024,
        maximum_emitted_batch_bytes: SQLITE_MAXIMUM_BATCH_BYTES,
        maximum_concurrency: 1,
        useful_concurrency: 1,
        executor_class: SourceExecutorClass::BlockingLane,
        blocking_lane: Some(sqlite_source_blocking_lane()),
        pausable: true,
        spillable: false,
        idempotent_reads: true,
        reopenable: true,
        resumable,
        speculative_safe: false,
        retry_granularity: SourceRetryGranularity::None,
        retryable_errors: Vec::new(),
        retry_policy: None,
        attestation: SourceAttestationStrength::None,
        rate_limit: None,
        quota_authority: None,
        canonical_order: resumable,
        bounded: true,
        batch_memory: cdf_runtime::SourceBatchMemoryContract::Preaccounted,
        telemetry_version: "v1".to_owned(),
    }
}

fn validate_sqlite_compile_shape(
    descriptor: &cdf_kernel::ResourceDescriptor,
    schema: &arrow_schema::SchemaRef,
    table: &SqliteIdentifier,
    stable_key: Option<&SqliteIdentifier>,
    temporal_encodings: &BTreeMap<String, SqliteTemporalEncoding>,
) -> Result<()> {
    if !schema.fields().is_empty() {
        return validate_sqlite_table_resource_shape(
            descriptor,
            schema,
            table,
            stable_key,
            temporal_encodings,
        );
    }
    if !matches!(descriptor.schema_source, cdf_kernel::SchemaSource::Discover) {
        return Err(CdfError::data(
            "SQLite source compilation requires a nonempty fixed schema or discover mode",
        ));
    }
    match (&descriptor.cursor, stable_key) {
        (None, Some(_)) => {
            return Err(CdfError::contract(
                "SQLite stable_key is valid only for a cursor resource",
            ));
        }
        (Some(_), None) => {
            return Err(CdfError::contract(
                "SQLite cursor resources require a stable_key tie-breaker",
            ));
        }
        (Some(cursor), Some(stable_key)) if cursor.field == stable_key.as_str() => {
            return Err(CdfError::contract(
                "SQLite cursor stable_key must differ from the cursor field",
            ));
        }
        _ => {}
    }
    for field in temporal_encodings.keys() {
        SqliteIdentifier::new(field.clone())?;
    }
    Ok(())
}

fn normalize_sqlite_location(location: &str) -> Result<String> {
    let raw = location
        .strip_prefix("sqlite://")
        .ok_or_else(|| CdfError::contract("SQLite source location must begin with `sqlite://`"))?;
    if raw.is_empty() || raw.contains(['?', '#', '%']) || raw.chars().any(char::is_control) {
        return Err(CdfError::contract(
            "SQLite source location must be a nonempty literal local path without query, fragment, percent escapes, or control characters",
        ));
    }
    let path = Path::new(raw);
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::RootDir => normalized.push(Path::new("/")),
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::ParentDir => {
                return Err(CdfError::contract(
                    "SQLite source location must not contain parent traversal",
                ));
            }
        }
    }
    let normalized = normalized
        .to_str()
        .ok_or_else(|| CdfError::contract("SQLite source location must be valid UTF-8"))?
        .replace(std::path::MAIN_SEPARATOR, "/");
    validate_normalized_path(&normalized)?;
    Ok(normalized)
}

fn validate_normalized_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.chars().any(char::is_control)
        || Path::new(path)
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        return Err(CdfError::contract(
            "SQLite compiled database path is not normalized",
        ));
    }
    Ok(())
}

fn resolve_database_path(project_root: &Path, path: &str) -> PathBuf {
    let path = Path::new(path);
    if path.is_absolute() {
        path.to_owned()
    } else {
        project_root.join(path)
    }
}

fn portable_add_path(request: &SourceAddRequest) -> Result<PathBuf> {
    let normalized = normalize_sqlite_location(&request.location)?;
    let candidate = resolve_database_path(&request.current_dir, &normalized);
    crate::error::validate_source_file(&candidate)?;
    let database = std::fs::canonicalize(&candidate).map_err(|error| {
        crate::error::classify_source_io("resolve SQLite source database", &error)
    })?;
    let project = std::fs::canonicalize(&request.project_root)
        .map_err(|error| crate::error::classify_source_io("resolve SQLite project root", &error))?;
    database.strip_prefix(&project).map(Path::to_path_buf).map_err(|_| CdfError::contract(
        "cdf add SQLite databases must be below the project root so the compiled source remains portable",
    ))
}

fn portable_path(path: &Path) -> Result<String> {
    path.to_str()
        .map(|value| value.replace(std::path::MAIN_SEPARATOR, "/"))
        .ok_or_else(|| CdfError::contract("SQLite project-relative path must be valid UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{
        CursorOrderingClaim, CursorSpec, ResourceDescriptor, ResourceId, SchemaHash, SchemaSource,
        ScopeKey, TrustLevel, WriteDisposition,
    };

    fn descriptor() -> ResourceDescriptor {
        ResourceDescriptor {
            resource_id: ResourceId::new("local.events").unwrap(),
            schema_source: SchemaSource::Declared {
                schema_hash: SchemaHash::new("sha256:sqlite-driver-test").unwrap(),
                source: "sqlite://fixtures/events.sqlite".to_owned(),
            },
            primary_key: Vec::new(),
            merge_key: Vec::new(),
            cursor: None,
            write_disposition: WriteDisposition::Append,
            deduplication: None,
            contract: None,
            state_scope: ScopeKey::Resource,
            freshness: None,
            trust_level: TrustLevel::Governed,
        }
    }

    #[test]
    fn compiles_contact_free_redacted_plan_and_single_blocking_lane() {
        let plan = SqliteSourceDriver::new()
            .unwrap()
            .compile(SourceCompileRequest {
                source_kind: "sqlite".to_owned(),
                context: cdf_runtime::SourceCompileContext {
                    source_name: "local".to_owned(),
                    project_root: None,
                    cursor_pushdown: None,
                },
                source_options: BTreeMap::from([(
                    "location".to_owned(),
                    serde_json::json!("sqlite://fixtures/events.sqlite"),
                )]),
                resource_options: BTreeMap::from([(
                    "table".to_owned(),
                    serde_json::json!("events"),
                )]),
                descriptor: descriptor(),
                schema: Schema::new(vec![Field::new("id", DataType::Int64, false)]),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap();
        assert_eq!(plan.driver.driver_id.as_str(), "sqlite");
        assert_eq!(plan.execution_capabilities.maximum_concurrency, 1);
        assert_eq!(
            plan.execution_capabilities
                .blocking_lane
                .as_ref()
                .unwrap()
                .lane_id,
            SQLITE_SOURCE_BLOCKING_LANE_ID
        );
        assert!(
            SqliteSourceDriver::new()
                .unwrap()
                .validate_portable_plan(&plan)
                .is_ok()
        );
    }

    #[test]
    fn absolute_paths_compile_for_local_use_but_fail_portable_admission_and_redact() {
        let driver = SqliteSourceDriver::new().unwrap();
        let plan = driver
            .compile(SourceCompileRequest {
                source_kind: "sqlite".to_owned(),
                context: cdf_runtime::SourceCompileContext {
                    source_name: "local".to_owned(),
                    project_root: None,
                    cursor_pushdown: None,
                },
                source_options: BTreeMap::from([(
                    "location".to_owned(),
                    serde_json::json!("sqlite:///private/operator/events.sqlite"),
                )]),
                resource_options: BTreeMap::from([(
                    "table".to_owned(),
                    serde_json::json!("events"),
                )]),
                descriptor: descriptor(),
                schema: Schema::new(vec![Field::new("id", DataType::Int64, false)]),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap();
        assert_eq!(
            plan.redacted_options["location"],
            "sqlite://[local-database]"
        );
        assert!(
            driver
                .validate_portable_plan(&plan)
                .unwrap_err()
                .message
                .contains("project-relative")
        );
    }

    #[test]
    fn execution_capabilities_only_claim_resume_and_order_for_cursor_resources() {
        let snapshot = descriptor();
        let snapshot_capabilities = execution_capabilities(&snapshot);
        assert!(!snapshot_capabilities.resumable);
        assert!(!snapshot_capabilities.canonical_order);

        let mut cursor = descriptor();
        cursor.cursor = Some(CursorSpec {
            field: "updated_at".to_owned(),
            ordering: CursorOrderingClaim::Exact,
            lag_tolerance_ms: 0,
        });
        let cursor_capabilities = execution_capabilities(&cursor);
        assert!(cursor_capabilities.resumable);
        assert!(cursor_capabilities.canonical_order);
    }
}
