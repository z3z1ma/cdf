use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use cdf_kernel::{CdfError, QueryableResource, Result};
use cdf_runtime::{
    CompiledSourcePlan, SourceAddCursor, SourceAddCursorOrdering, SourceAddPlanner,
    SourceAddProposal, SourceAddRequest, SourceAttestationStrength, SourceCatalogCandidate,
    SourceCatalogDiscoverer, SourceCatalogDiscovery, SourceCatalogRequest, SourceCompileRequest,
    SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest, SourceDiscoverySession,
    SourceDriver, SourceDriverDescriptor, SourceDriverId, SourceEvidenceLocation,
    SourceExecutionCapabilities, SourceExecutorClass, SourceHealthRequest, SourceHealthResult,
    SourceHealthStatus, SourceResolutionContext, SourceRetryGranularity, SourceSchemaObservation,
    artifact_hash,
};
use serde::{Deserialize, Serialize};

use crate::{
    catalog::{discover_sqlite_table, discover_sqlite_tables},
    identifier::SqliteIdentifier,
    native::{SqliteNativeOptions, SqliteSourceInput, discover_sqlite_query},
    source::{
        SQLITE_MAXIMUM_BATCH_BYTES, SQLITE_SOURCE_BLOCKING_LANE_ID, SqliteSourceResource,
        SqliteTemporalEncoding, sqlite_source_blocking_lane, sqlite_source_capabilities,
        validate_sqlite_source_resource_shape,
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
                "oneOf": [
                    {"type": "object", "required": ["table"], "properties": {"table": {"type": "string"}}},
                    {"type": "object", "required": ["query"], "properties": {"query": {"type": "string"}}}
                ],
                "properties": {
                    "table": {"type": "string", "minLength": 1},
                    "query": {"type": "string", "minLength": 1},
                    "stable_key": {"type": "string", "minLength": 1},
                    "discovery_records": {"type": "integer", "minimum": 1, "maximum": 100000, "default": 1000},
                    "discovery_bytes": {"type": "integer", "minimum": 1024, "maximum": 67108864, "default": 16777216},
                    "output_batch_rows": {"type": "integer", "minimum": 1, "maximum": 100000, "default": 32768},
                    "busy_timeout_ms": {"type": "integer", "minimum": 1, "maximum": 3600000},
                    "cache_kib": {"type": "integer", "minimum": 64, "maximum": 1048576},
                    "mmap_bytes": {"type": "integer", "minimum": 0, "maximum": 1073741824},
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
                driver_version: "3.0.0".to_owned(),
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
            &physical.input,
            physical.stable_key.as_ref(),
            &physical.temporal_encodings,
        )
    }

    fn add_planner(&self) -> Option<&dyn SourceAddPlanner> {
        Some(self)
    }

    fn catalog_discoverer(&self) -> Option<&dyn SourceCatalogDiscoverer> {
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
        let probe_request = SourceDiscoveryRequest::new(
            crate::native::SQLITE_DEFAULT_DISCOVERY_BYTES,
            crate::native::SQLITE_DEFAULT_DISCOVERY_RECORDS,
        )?
        .with_cancellation(request.budget.cancellation());
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
        let input = SqliteSourceInput::from_authored(resource.table, resource.query)?;
        let options = SqliteNativeOptions::from_authored(
            resource.discovery_records,
            resource.discovery_bytes,
            resource.output_batch_rows,
            resource.busy_timeout_ms,
            resource.cache_kib,
            resource.mmap_bytes,
        )?;
        let stable_key = resource.stable_key.map(SqliteIdentifier::new).transpose()?;
        let mut temporal_encodings = resource.temporal_encodings;
        let schema = request.schema;
        for field in schema.fields() {
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
            &Arc::new(schema.clone()),
            &input,
            stable_key.as_ref(),
            &temporal_encodings,
        )?;
        let physical = SqlitePhysicalPlan {
            database_path: database_path.clone(),
            input: input.clone(),
            stable_key: stable_key.clone(),
            temporal_encodings: temporal_encodings.clone(),
            options: options.clone(),
        };
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            sqlite_source_capabilities(&request.descriptor),
            execution_capabilities(&request.descriptor),
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema,
                type_policy_allowances: request.type_policy_allowances,
                source_materializations: Vec::new(),
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog: request.baseline_observation_schema_catalog,
                redacted_options: serde_json::json!({
                    "location": if Path::new(&database_path).is_absolute() { "sqlite://[local-database]" } else { source.location.as_str() },
                    "dialect": "sqlite",
                    "input": input.redacted_evidence(),
                    "stable_key": stable_key.as_ref().map(SqliteIdentifier::as_str),
                    "temporal_encodings": temporal_encodings,
                    "discovery_records": options.discovery_records,
                    "discovery_bytes": options.discovery_bytes,
                    "output_batch_rows": options.output_batch_rows,
                    "busy_timeout_ms": options.busy_timeout_ms,
                    "cache_kib": options.cache_kib,
                    "mmap_bytes": options.mmap_bytes,
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
            input: physical.input,
            options: physical.options,
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
        let resource = SqliteSourceResource::from_compiled_plan(
            plan,
            resolve_database_path(context.project_root(), &physical.database_path),
            physical.input,
            physical.stable_key,
            physical.temporal_encodings,
            physical.options,
            context.execution().clone(),
        )?;
        Ok(Arc::new(resource))
    }
}

impl SourceCatalogDiscoverer for SqliteSourceDriver {
    fn discover_catalog(
        &self,
        request: &SourceCatalogRequest,
        context: &SourceResolutionContext<'_>,
    ) -> Result<SourceCatalogDiscovery> {
        request.validate()?;
        context
            .execution()
            .ensure_blocking_lanes(&[sqlite_source_blocking_lane()])?;
        let source: SqliteSourceOptions =
            decode_options("SQLite source", request.source_options.clone())?;
        let database_path = resolve_database_path(
            context.project_root(),
            &normalize_sqlite_location(&source.location)?,
        );
        let maximum = request.maximum_candidates;
        let discovery_resource = cdf_kernel::ResourceId::new("discovery.catalog")?;
        let path = database_path.clone();
        let tables = context
            .execution()
            .run_blocking(SQLITE_SOURCE_BLOCKING_LANE_ID, move || {
                discover_sqlite_tables(&path, maximum)
            })?;
        let complete = tables.len() <= maximum;
        let selected = tables.into_iter().take(maximum).collect::<Vec<_>>();
        let mut candidates = Vec::with_capacity(selected.len());
        for table_name in selected {
            let table = SqliteIdentifier::new(table_name.clone())?;
            let path = database_path.clone();
            let resource_id = discovery_resource.clone();
            let schema = context
                .execution()
                .run_blocking(SQLITE_SOURCE_BLOCKING_LANE_ID, move || {
                    discover_sqlite_table(&path, &resource_id, &table)
                })?;
            candidates.push(SourceCatalogCandidate {
                relation_id: table_name.clone(),
                display_label: table_name.clone(),
                relation_kind: "table".to_owned(),
                resource_token: catalog_resource_token(&table_name),
                resource_options: BTreeMap::from([(
                    "table".to_owned(),
                    serde_json::Value::String(table_name),
                )]),
                schema: Some(cdf_kernel::CanonicalArrowSchema::from_arrow(
                    &schema.schema,
                )?),
            });
        }
        SourceCatalogDiscovery::new(
            request,
            "sqlite_table",
            candidates,
            complete,
            (!complete).then(|| "narrow relation selectors to a complete catalog set".to_owned()),
        )
    }
}

fn catalog_resource_token(value: &str) -> Option<String> {
    let mut bytes = value.bytes();
    (value.len() <= 128
        && bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'))
    .then(|| value.to_owned())
}

impl SourceAddPlanner for SqliteSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        request.validate()?;
        if !request.location.starts_with("sqlite://") {
            return Ok(None);
        }
        const KEYS: [&str; 11] = [
            "table",
            "query",
            "cursor",
            "stable_key",
            "cursor_encoding",
            "discovery_records",
            "discovery_bytes",
            "output_batch_rows",
            "busy_timeout_ms",
            "cache_kib",
            "mmap_bytes",
        ];
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
        let table = request.options.get("table").cloned();
        let query = request.options.get("query").cloned();
        let input = SqliteSourceInput::from_authored(table.clone(), query.clone())?;
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
        let discovery_records = parse_add_u64(&request.options, "discovery_records")?;
        let discovery_bytes = parse_add_u64(&request.options, "discovery_bytes")?;
        let output_batch_rows = parse_add_u64(&request.options, "output_batch_rows")?;
        let busy_timeout_ms = parse_add_u64(&request.options, "busy_timeout_ms")?;
        let cache_kib = parse_add_u64(&request.options, "cache_kib")?;
        let mmap_bytes = parse_add_u64(&request.options, "mmap_bytes")?;
        SqliteNativeOptions::from_authored(
            discovery_records,
            discovery_bytes,
            output_batch_rows,
            busy_timeout_ms,
            cache_kib,
            mmap_bytes,
        )?;
        let relative = portable_add_path(request)?;
        let location = format!("sqlite://{}", portable_path(&relative)?);
        let mut resource_options = match (table, query) {
            (Some(table), None) => {
                BTreeMap::from([("table".to_owned(), serde_json::Value::String(table))])
            }
            (None, Some(query)) => {
                BTreeMap::from([("query".to_owned(), serde_json::Value::String(query))])
            }
            _ => unreachable!("input validation proved exactly one SQLite source input"),
        };
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
        for (name, value) in [
            ("discovery_records", discovery_records),
            ("discovery_bytes", discovery_bytes),
            ("output_batch_rows", output_batch_rows),
            ("busy_timeout_ms", busy_timeout_ms),
            ("cache_kib", cache_kib),
            ("mmap_bytes", mmap_bytes),
        ] {
            if let Some(value) = value {
                resource_options.insert(name.to_owned(), serde_json::json!(value));
            }
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
            display_selection: input.location_summary(),
            private_files: Vec::new(),
        }))
    }
}

struct SqliteDiscoverySession {
    database_path: PathBuf,
    resource_id: cdf_kernel::ResourceId,
    input: SqliteSourceInput,
    options: SqliteNativeOptions,
    execution: cdf_runtime::ExecutionServices,
}

impl SourceDiscoverySession for SqliteDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        match &self.input {
            SqliteSourceInput::Table { .. } => SourceDiscoveryKind::SchemaMetadata,
            SqliteSourceInput::Query { .. } => SourceDiscoveryKind::BoundedContent,
        }
    }
    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(vec![SourceDiscoveryCandidate::new(
            self.input.location_summary(),
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
        if candidate.canonical_location != self.input.location_summary() {
            return Err(CdfError::contract(
                "SQLite discovery candidate does not match the compiled input",
            ));
        }
        let path = self.database_path.clone();
        let resource_id = self.resource_id.clone();
        let input = self.input.clone();
        let options = self.options.clone();
        let maximum_records = request.maximum_records;
        let maximum_bytes = request.maximum_bytes;
        let discovery = self
            .execution
            .run_blocking(SQLITE_SOURCE_BLOCKING_LANE_ID, move || match &input {
                SqliteSourceInput::Table { table } => {
                    let discovery = discover_sqlite_table(&path, &resource_id, table)?;
                    Ok((discovery.schema, discovery.source_identity, 0, 0))
                }
                SqliteSourceInput::Query { .. } => {
                    let discovery = discover_sqlite_query(
                        &path,
                        &resource_id,
                        &input,
                        &options,
                        maximum_records,
                        maximum_bytes,
                    )?;
                    let identity = BTreeMap::from([
                        ("source_kind".to_owned(), "sqlite".to_owned()),
                        ("dialect".to_owned(), "sqlite".to_owned()),
                        ("sample_complete".to_owned(), discovery.complete.to_string()),
                        (
                            "sample_record_limit".to_owned(),
                            maximum_records.min(options.discovery_records).to_string(),
                        ),
                        (
                            "sample_byte_limit".to_owned(),
                            maximum_bytes.min(options.discovery_bytes).to_string(),
                        ),
                    ]);
                    Ok((
                        discovery.schema,
                        identity,
                        discovery.records_read,
                        discovery.bytes_read,
                    ))
                }
            })?;
        let (schema, mut identity, records_read, bytes_read) = discovery;
        identity.insert(
            "output_column_count".to_owned(),
            schema.fields().len().to_string(),
        );
        SourceSchemaObservation::new(candidate, schema, identity, bytes_read, records_read)
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
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    stable_key: Option<String>,
    #[serde(default)]
    temporal_encodings: BTreeMap<String, SqliteTemporalEncoding>,
    #[serde(default)]
    discovery_records: Option<u64>,
    #[serde(default)]
    discovery_bytes: Option<u64>,
    #[serde(default)]
    output_batch_rows: Option<u64>,
    #[serde(default)]
    busy_timeout_ms: Option<u64>,
    #[serde(default)]
    cache_kib: Option<u64>,
    #[serde(default)]
    mmap_bytes: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SqlitePhysicalPlan {
    database_path: String,
    input: SqliteSourceInput,
    stable_key: Option<SqliteIdentifier>,
    temporal_encodings: BTreeMap<String, SqliteTemporalEncoding>,
    options: SqliteNativeOptions,
}
impl SqlitePhysicalPlan {
    fn validate(&self) -> Result<()> {
        validate_normalized_path(&self.database_path)?;
        self.input.validate()?;
        self.options.validate()?;
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

fn parse_add_u64(options: &BTreeMap<String, String>, name: &str) -> Result<Option<u64>> {
    options
        .get(name)
        .map(|value| {
            value.parse::<u64>().map_err(|_| {
                CdfError::contract(format!(
                    "SQLite cdf add option `{name}` must be an unsigned integer"
                ))
            })
        })
        .transpose()
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
        attestation: SourceAttestationStrength::Metadata,
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
    input: &SqliteSourceInput,
    stable_key: Option<&SqliteIdentifier>,
    temporal_encodings: &BTreeMap<String, SqliteTemporalEncoding>,
) -> Result<()> {
    input.validate()?;
    if !schema.fields().is_empty() {
        return validate_sqlite_source_resource_shape(
            descriptor,
            schema,
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
    fn registry_compile_preserves_explicit_compiler_schema() {
        let mut registry = cdf_runtime::SourceRegistry::new();
        registry
            .register(SqliteSourceDriver::new().unwrap())
            .unwrap();
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let plan = registry
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
                    "query".to_owned(),
                    serde_json::json!("SELECT id FROM events"),
                )]),
                descriptor: descriptor(),
                schema: schema.clone(),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap();
        assert_eq!(plan.schema, schema);
    }

    #[test]
    fn compiles_native_query_and_identity_bearing_controls_without_literal_evidence() {
        let query = "SELECT id FROM private_events WHERE tenant = 'private-value'";
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
                resource_options: BTreeMap::from([
                    ("query".to_owned(), serde_json::json!(query)),
                    ("discovery_records".to_owned(), serde_json::json!(250)),
                    ("discovery_bytes".to_owned(), serde_json::json!(1_048_576)),
                    ("output_batch_rows".to_owned(), serde_json::json!(8_192)),
                    ("busy_timeout_ms".to_owned(), serde_json::json!(5_000)),
                    ("cache_kib".to_owned(), serde_json::json!(65_536)),
                    ("mmap_bytes".to_owned(), serde_json::json!(268_435_456)),
                ]),
                descriptor: descriptor(),
                schema: Schema::new(vec![Field::new("id", DataType::Int64, true)]),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap();
        let evidence = plan.redacted_options.to_string();
        assert!(evidence.contains("query_sha256"));
        assert!(!evidence.contains("private-value"));
        assert_eq!(plan.redacted_options["output_batch_rows"], 8_192);
        let physical = decode_physical_plan(&plan).unwrap();
        assert_eq!(physical.options.discovery_records, 250);
        assert_eq!(physical.options.output_batch_rows, 8_192);
        let SqliteSourceInput::Query { sql, .. } = physical.input else {
            panic!("compiled input must remain a native query");
        };
        assert_eq!(sql, query);
    }

    #[test]
    fn add_planner_preserves_native_query_and_resource_controls() {
        let project = tempfile::tempdir().unwrap();
        let database = project.path().join("events.sqlite");
        drop(rusqlite::Connection::open(&database).unwrap());
        let query = "SELECT id FROM private_events WHERE tenant = 'private-value'";
        let proposal = SqliteSourceDriver::new()
            .unwrap()
            .add_planner()
            .unwrap()
            .propose_add(&SourceAddRequest {
                source_name: "local".to_owned(),
                resource_name: "events".to_owned(),
                location: "sqlite://events.sqlite".to_owned(),
                project_root: project.path().to_owned(),
                current_dir: project.path().to_owned(),
                options: BTreeMap::from([
                    ("query".to_owned(), query.to_owned()),
                    ("discovery_records".to_owned(), "250".to_owned()),
                    ("discovery_bytes".to_owned(), "1048576".to_owned()),
                    ("output_batch_rows".to_owned(), "8192".to_owned()),
                    ("busy_timeout_ms".to_owned(), "5000".to_owned()),
                    ("cache_kib".to_owned(), "65536".to_owned()),
                    ("mmap_bytes".to_owned(), "268435456".to_owned()),
                ]),
                project_options: None,
            })
            .unwrap()
            .unwrap();

        assert_eq!(proposal.source_kind, "sqlite");
        assert_eq!(
            proposal.source_options["location"],
            "sqlite://events.sqlite"
        );
        assert_eq!(proposal.resource_options["query"], query);
        assert_eq!(proposal.resource_options["discovery_records"], 250);
        assert_eq!(proposal.resource_options["discovery_bytes"], 1_048_576);
        assert_eq!(proposal.resource_options["output_batch_rows"], 8_192);
        assert_eq!(proposal.resource_options["busy_timeout_ms"], 5_000);
        assert_eq!(proposal.resource_options["cache_kib"], 65_536);
        assert_eq!(proposal.resource_options["mmap_bytes"], 268_435_456);
        assert!(proposal.display_selection.starts_with("query:sha256:"));
        assert!(!proposal.display_selection.contains("private-value"));
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
