use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use cdf_http::{SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{CdfError, QueryableResource, Result, SchemaSource};
use cdf_runtime::{
    CompiledSourcePlan, SourceAddCursor, SourceAddCursorOrdering, SourceAddPlanner,
    SourceAddPrivateFile, SourceAddProposal, SourceAddRequest, SourceAttestationStrength,
    SourceCompileRequest, SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest,
    SourceDiscoverySession, SourceDriver, SourceDriverDescriptor, SourceDriverId,
    SourceEvidenceLocation, SourceExecutionCapabilities, SourceExecutorClass, SourceHealthRequest,
    SourceHealthResult, SourceHealthStatus, SourceResolutionContext, SourceRetryGranularity,
    SourceSchemaObservation, artifact_hash,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    catalog::discover_clickhouse_table,
    client::ClickHouseConnection,
    execution::{CLICKHOUSE_MAXIMUM_BATCH_BYTES, CLICKHOUSE_MAXIMUM_POLL_BYTES},
    identifier::ClickHouseIdentifier,
    resource::{
        ClickHouseTableResource, clickhouse_table_capabilities, validate_compiled_schema_evidence,
    },
    types::validate_resource_shape,
};

const DEFAULT_MAX_THREADS: u64 = 4;
const DEFAULT_MAX_BLOCK_ROWS: u64 = 65_536;
const DEFAULT_STREAM_BUFFER_BATCHES: usize = 1;

#[derive(Clone, Debug)]
pub struct ClickHouseSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
}

impl ClickHouseSourceDriver {
    pub fn new() -> Result<Self> {
        let option_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "source": {
                "type": "object",
                "additionalProperties": false,
                "required": ["endpoint", "database"],
                "properties": {
                    "endpoint": {
                        "oneOf": [
                            {"type": "string", "pattern": "^clickhouse://"},
                            {"type": "string", "pattern": "^clickhouses://"}
                        ]
                    },
                    "database": {"type": "string", "minLength": 1},
                    "username": {"type": "string", "pattern": "^secret://"},
                    "password": {"type": "string", "pattern": "^secret://"},
                    "dialect": {"const": "clickhouse", "default": "clickhouse"},
                    "max_threads": {"type": "integer", "minimum": 1, "maximum": 256, "default": DEFAULT_MAX_THREADS},
                    "max_block_rows": {"type": "integer", "minimum": 1, "maximum": 1000000, "default": DEFAULT_MAX_BLOCK_ROWS},
                    "stream_buffer_batches": {"type": "integer", "minimum": 1, "maximum": 64, "default": DEFAULT_STREAM_BUFFER_BATCHES}
                }
            },
            "resource": {
                "type": "object",
                "additionalProperties": false,
                "required": ["table"],
                "properties": {
                    "table": {"type": "string", "minLength": 1},
                    "stable_key": {"type": "string", "minLength": 1},
                    "max_threads": {"type": "integer", "minimum": 1, "maximum": 256},
                    "max_block_rows": {"type": "integer", "minimum": 1, "maximum": 1000000},
                    "stream_buffer_batches": {"type": "integer", "minimum": 1, "maximum": 64}
                }
            }
        });
        Ok(Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new("clickhouse")?,
                driver_version: "2.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema)?,
                kinds: vec!["clickhouse".to_owned()],
                schemes: vec!["clickhouse".to_owned(), "clickhouses".to_owned()],
            },
            option_schema,
        })
    }
}

impl SourceDriver for ClickHouseSourceDriver {
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
        validate_compile_shape(
            &plan.descriptor,
            &Arc::new(plan.schema.clone()),
            &physical.table,
            physical.stable_key.as_ref(),
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
                message: "no ClickHouse resources are compiled".to_owned(),
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
                    CdfError::data("ClickHouse health probe produced no catalog candidate")
                })?;
                session.observe(&candidate, &probe_request)
            });
            output.emit(match probe {
                Ok(observation) => SourceHealthResult {
                    probe_id: resource_id.to_owned(),
                    status: SourceHealthStatus::Passed,
                    message: "ClickHouse Arrow catalog probe passed".to_owned(),
                    details: serde_json::json!({
                        "resource_id": resource_id,
                        "columns": observation.schema.fields().len(),
                    }),
                },
                Err(error) => SourceHealthResult::failed(
                    resource_id,
                    "ClickHouse Arrow catalog probe failed",
                    &plan.descriptor.resource_id,
                    &error,
                ),
            })?;
        }
        Ok(())
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        if request.source_kind != "clickhouse" {
            return Err(CdfError::contract(
                "ClickHouse driver can compile only source kind `clickhouse`",
            ));
        }
        let source: ClickHouseSourceOptions =
            decode_options("ClickHouse source", request.source_options)?;
        let resource: ClickHouseResourceOptions =
            decode_options("ClickHouse resource", request.resource_options)?;
        if !source
            .dialect
            .as_deref()
            .is_none_or(|dialect| dialect.eq_ignore_ascii_case("clickhouse"))
        {
            return Err(CdfError::contract(
                "ClickHouse source dialect must be `clickhouse` when declared",
            ));
        }
        source.validate_operational_defaults()?;
        let endpoint = normalize_endpoint(&source.endpoint)?;
        let database = ClickHouseIdentifier::new(source.database)?;
        let table = ClickHouseIdentifier::new(resource.table)?;
        let stable_key = resource
            .stable_key
            .map(ClickHouseIdentifier::new)
            .transpose()?;
        let username = source.username.map(SecretUri::new).transpose()?;
        let password = source.password.map(SecretUri::new).transpose()?;
        let physical = ClickHousePhysicalPlan {
            endpoint: endpoint.clone(),
            database: database.clone(),
            table: table.clone(),
            stable_key: stable_key.clone(),
            username: username.map(|value| value.as_str().to_owned()),
            password: password.map(|value| value.as_str().to_owned()),
            max_threads: resource.max_threads.unwrap_or(source.max_threads),
            max_block_rows: resource.max_block_rows.unwrap_or(source.max_block_rows),
            stream_buffer_batches: resource
                .stream_buffer_batches
                .unwrap_or(source.stream_buffer_batches),
        };
        physical.validate()?;
        validate_compile_shape(
            &request.descriptor,
            &Arc::new(request.schema.clone()),
            &table,
            stable_key.as_ref(),
        )?;
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            clickhouse_table_capabilities(&request.descriptor),
            execution_capabilities(&request.descriptor),
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                source_materializations: Vec::new(),
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog: request.baseline_observation_schema_catalog,
                redacted_options: serde_json::json!({
                    "endpoint": display_endpoint(&endpoint)?,
                    "database": database.as_str(),
                    "table": table.as_str(),
                    "stable_key": stable_key.as_ref().map(ClickHouseIdentifier::as_str),
                    "username": physical.username.as_deref(),
                    "password": physical.password.as_deref(),
                    "dialect": "clickhouse",
                    "max_threads": physical.max_threads,
                    "max_block_rows": physical.max_block_rows,
                    "stream_buffer_batches": physical.stream_buffer_batches,
                }),
                physical_plan: serde_json::to_value(&physical).map_err(|error| {
                    CdfError::internal(format!("serialize ClickHouse source plan: {error}"))
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
        let connection = physical.resolve(context.secret_provider().as_ref())?;
        Ok(Box::new(ClickHouseDiscoverySession {
            connection,
            resource_id: plan.descriptor.resource_id.clone(),
            table: physical.table,
            cursor_field: plan
                .descriptor
                .cursor
                .as_ref()
                .map(|cursor| cursor.field.clone()),
            execution: context.execution().clone(),
            egress: context.egress_scope(&plan.driver.driver_id),
        }))
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        let physical = decode_physical_plan(plan)?;
        physical.validate()?;
        validate_compiled_schema_evidence(plan)?;
        let connection = physical.resolve(context.secret_provider().as_ref())?;
        Ok(Arc::new(ClickHouseTableResource::from_compiled_plan(
            plan,
            physical.endpoint,
            physical.database,
            physical.table,
            physical.stable_key,
            physical.stream_buffer_batches,
            context.egress_scope(&plan.driver.driver_id),
            context.execution().clone(),
            move |cancellation| {
                cancellation.check()?;
                Ok(connection.clone())
            },
        )?))
    }
}

impl SourceAddPlanner for ClickHouseSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        request.validate()?;
        let Some((scheme, _)) = request.location.split_once("://") else {
            return Ok(None);
        };
        if !matches!(scheme, "clickhouse" | "clickhouses") {
            return Ok(None);
        }
        const KEYS: [&str; 5] = [
            "cursor",
            "stable_key",
            "max_threads",
            "max_block_rows",
            "stream_buffer_batches",
        ];
        let unknown = request
            .options
            .keys()
            .filter(|key| !KEYS.contains(&key.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Err(CdfError::contract(format!(
                "ClickHouse cdf add received unknown options: {}",
                unknown.join(", ")
            )));
        }
        let mut parsed = Url::parse(&request.location).map_err(|error| {
            CdfError::contract(format!("cdf add could not parse ClickHouse URL: {error}"))
        })?;
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(CdfError::contract(
                "ClickHouse cdf add URL must not contain query or fragment text",
            ));
        }
        let segments = parsed
            .path_segments()
            .map(|segments| {
                segments
                    .filter(|segment| !segment.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if segments.len() != 2 {
            return Err(CdfError::contract(
                "ClickHouse cdf add URL must end with exactly `/database/table`",
            ));
        }
        let database = ClickHouseIdentifier::new(segments[0].clone())?;
        let table = ClickHouseIdentifier::new(segments[1].clone())?;
        let username = (!parsed.username().is_empty()).then(|| parsed.username().to_owned());
        let password = parsed.password().map(str::to_owned);
        parsed
            .set_username("")
            .map_err(|()| CdfError::contract("clear ClickHouse URL username"))?;
        parsed
            .set_password(None)
            .map_err(|()| CdfError::contract("clear ClickHouse URL password"))?;
        parsed.set_path("");
        let endpoint = parsed.to_string().trim_end_matches('/').to_owned();
        normalize_endpoint(&endpoint)?;
        let mut source_options = BTreeMap::from([
            ("endpoint".to_owned(), serde_json::json!(endpoint)),
            ("database".to_owned(), serde_json::json!(database.as_str())),
            ("dialect".to_owned(), serde_json::json!("clickhouse")),
        ]);
        let mut private_files = Vec::new();
        if let Some(username) = username {
            let (reference, file) = private_file(&request.source_name, "username", username)?;
            source_options.insert("username".to_owned(), serde_json::json!(reference.as_str()));
            private_files.push(file);
        }
        if let Some(password) = password {
            let (reference, file) = private_file(&request.source_name, "password", password)?;
            source_options.insert("password".to_owned(), serde_json::json!(reference.as_str()));
            private_files.push(file);
        }
        for key in ["max_threads", "max_block_rows", "stream_buffer_batches"] {
            if let Some(value) = request.options.get(key) {
                let value = value.parse::<u64>().map_err(|_| {
                    CdfError::contract(format!("ClickHouse cdf add {key} must be an integer"))
                })?;
                source_options.insert(key.to_owned(), serde_json::json!(value));
            }
        }
        let cursor = request.options.get("cursor");
        let stable_key = request.options.get("stable_key");
        if cursor.is_some() != stable_key.is_some() {
            return Err(CdfError::contract(
                "ClickHouse cdf add cursor and stable_key must be supplied together",
            ));
        }
        let mut resource_options =
            BTreeMap::from([("table".to_owned(), serde_json::json!(table.as_str()))]);
        if let Some(stable_key) = stable_key {
            ClickHouseIdentifier::new(stable_key.clone())?;
            resource_options.insert("stable_key".to_owned(), serde_json::json!(stable_key));
        }
        Ok(Some(SourceAddProposal {
            source_kind: "clickhouse".to_owned(),
            source_options,
            resource_options,
            cursor: cursor.map(|field| SourceAddCursor {
                field: field.clone(),
                parameter: None,
                ordering: SourceAddCursorOrdering::Exact,
                lag_tolerance_ms: 0,
            }),
            display_location: SourceEvidenceLocation::from_operational(&endpoint)?,
            display_selection: format!("{}.{}", database.as_str(), table.as_str()),
            private_files,
        }))
    }
}

struct ClickHouseDiscoverySession {
    connection: ClickHouseConnection,
    resource_id: cdf_kernel::ResourceId,
    table: ClickHouseIdentifier,
    cursor_field: Option<String>,
    execution: cdf_runtime::ExecutionServices,
    egress: cdf_runtime::SourceEgressScope,
}

impl SourceDiscoverySession for ClickHouseDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        SourceDiscoveryKind::SchemaMetadata
    }

    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(vec![SourceDiscoveryCandidate::new(
            format!(
                "{}.{}",
                self.connection.database.as_str(),
                self.table.as_str()
            ),
            None,
            None,
            BTreeMap::from([
                ("source_kind".to_owned(), "clickhouse".to_owned()),
                ("dialect".to_owned(), "clickhouse".to_owned()),
            ]),
        )?])
    }

    fn observe(
        &self,
        candidate: &SourceDiscoveryCandidate,
        request: &SourceDiscoveryRequest,
    ) -> Result<SourceSchemaObservation> {
        request.validate()?;
        let expected = format!(
            "{}.{}",
            self.connection.database.as_str(),
            self.table.as_str()
        );
        if candidate.canonical_location != expected {
            return Err(CdfError::contract(
                "ClickHouse discovery candidate changed after compilation",
            ));
        }
        let discovery = self.execution.run_io(discover_clickhouse_table(
            self.connection.clone(),
            self.resource_id.clone(),
            self.table.clone(),
            self.cursor_field.clone(),
            self.execution.memory(),
            self.egress.clone(),
            request.cancellation.clone(),
        ))?;
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
struct ClickHouseSourceOptions {
    endpoint: String,
    database: String,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    dialect: Option<String>,
    #[serde(default = "default_max_threads")]
    max_threads: u64,
    #[serde(default = "default_max_block_rows")]
    max_block_rows: u64,
    #[serde(default = "default_stream_buffer_batches")]
    stream_buffer_batches: usize,
}

impl ClickHouseSourceOptions {
    fn validate_operational_defaults(&self) -> Result<()> {
        validate_source_default("max_threads", self.max_threads, 1, 256)?;
        validate_source_default("max_block_rows", self.max_block_rows, 1, 1_000_000)?;
        validate_source_default(
            "stream_buffer_batches",
            u64::try_from(self.stream_buffer_batches).map_err(|_| {
                CdfError::contract(
                    "ClickHouse source stream_buffer_batches exceeds platform bounds",
                )
            })?,
            1,
            64,
        )?;
        Ok(())
    }
}

fn validate_source_default(name: &str, value: u64, minimum: u64, maximum: u64) -> Result<()> {
    if !(minimum..=maximum).contains(&value) {
        return Err(CdfError::contract(format!(
            "ClickHouse source {name} must be in {minimum}..={maximum}",
        )));
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClickHouseResourceOptions {
    table: String,
    #[serde(default)]
    stable_key: Option<String>,
    #[serde(default)]
    max_threads: Option<u64>,
    #[serde(default)]
    max_block_rows: Option<u64>,
    #[serde(default)]
    stream_buffer_batches: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClickHousePhysicalPlan {
    endpoint: String,
    database: ClickHouseIdentifier,
    table: ClickHouseIdentifier,
    stable_key: Option<ClickHouseIdentifier>,
    username: Option<String>,
    password: Option<String>,
    max_threads: u64,
    max_block_rows: u64,
    stream_buffer_batches: usize,
}

impl ClickHousePhysicalPlan {
    fn validate(&self) -> Result<()> {
        let normalized = normalize_endpoint(&display_endpoint(&self.endpoint)?)?;
        if normalized != self.endpoint {
            return Err(CdfError::contract(
                "ClickHouse physical endpoint is not canonically normalized",
            ));
        }
        ClickHouseConnection::new(
            self.endpoint.clone(),
            self.database.clone(),
            None,
            None,
            self.max_threads,
            self.max_block_rows,
        )
        .validate()?;
        if !(1..=64).contains(&self.stream_buffer_batches) {
            return Err(CdfError::contract(
                "ClickHouse stream_buffer_batches must be between 1 and 64",
            ));
        }
        self.username
            .as_ref()
            .map(|value| SecretUri::new(value.clone()))
            .transpose()?;
        self.password
            .as_ref()
            .map(|value| SecretUri::new(value.clone()))
            .transpose()?;
        Ok(())
    }

    fn resolve(&self, provider: &dyn SecretProvider) -> Result<ClickHouseConnection> {
        let username = self
            .username
            .as_ref()
            .map(|uri| {
                provider
                    .resolve(&SecretUri::new(uri.clone())?)?
                    .as_str()
                    .map(str::to_owned)
            })
            .transpose()?;
        let password = self
            .password
            .as_ref()
            .map(|uri| {
                provider
                    .resolve(&SecretUri::new(uri.clone())?)?
                    .as_str()
                    .map(str::to_owned)
            })
            .transpose()?;
        let connection = ClickHouseConnection::new(
            self.endpoint.clone(),
            self.database.clone(),
            username,
            password,
            self.max_threads,
            self.max_block_rows,
        );
        connection.validate()?;
        Ok(connection)
    }
}

fn decode_physical_plan(plan: &CompiledSourcePlan) -> Result<ClickHousePhysicalPlan> {
    serde_json::from_value(plan.physical_plan.clone())
        .map_err(|error| CdfError::contract(format!("invalid ClickHouse source plan: {error}")))
}

fn decode_options<T: for<'de> Deserialize<'de>>(
    label: &str,
    options: BTreeMap<String, serde_json::Value>,
) -> Result<T> {
    serde_json::from_value(serde_json::Value::Object(options.into_iter().collect()))
        .map_err(|error| CdfError::contract(format!("{label} options are invalid: {error}")))
}

fn normalize_endpoint(value: &str) -> Result<String> {
    let operational = value
        .strip_prefix("clickhouse://")
        .map(|authority| format!("http://{authority}"))
        .or_else(|| {
            value
                .strip_prefix("clickhouses://")
                .map(|authority| format!("https://{authority}"))
        })
        .ok_or_else(|| {
            CdfError::contract("ClickHouse endpoint must use clickhouse:// or clickhouses://")
        })?;
    let mut parsed = Url::parse(&operational).map_err(|error| {
        CdfError::contract(format!("ClickHouse endpoint is not a valid URL: {error}"))
    })?;
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !matches!(parsed.path(), "" | "/")
    {
        return Err(CdfError::contract(
            "ClickHouse endpoint must be a credential-free clickhouse:// or clickhouses:// authority without path, query, or fragment",
        ));
    }
    parsed.set_path("");
    Ok(parsed.to_string().trim_end_matches('/').to_owned())
}

fn display_endpoint(operational: &str) -> Result<String> {
    operational
        .strip_prefix("http://")
        .map(|authority| format!("clickhouse://{authority}"))
        .or_else(|| {
            operational
                .strip_prefix("https://")
                .map(|authority| format!("clickhouses://{authority}"))
        })
        .ok_or_else(|| CdfError::contract("ClickHouse operational endpoint must use HTTP or HTTPS"))
}

fn private_file(
    source_name: &str,
    suffix: &str,
    value: String,
) -> Result<(SecretUri, SourceAddPrivateFile)> {
    let relative_path = PathBuf::from(format!(
        ".cdf/secrets/sources/{source_name}.clickhouse-{suffix}"
    ));
    let reference = SecretUri::new(format!(
        "secret://file/.cdf/secrets/sources/{source_name}.clickhouse-{suffix}"
    ))?;
    Ok((
        reference.clone(),
        SourceAddPrivateFile {
            reference,
            relative_path,
            value: SecretValue::new(value),
        },
    ))
}

fn validate_compile_shape(
    descriptor: &cdf_kernel::ResourceDescriptor,
    schema: &arrow_schema::SchemaRef,
    table: &ClickHouseIdentifier,
    stable_key: Option<&ClickHouseIdentifier>,
) -> Result<()> {
    if !schema.fields().is_empty() {
        return validate_resource_shape(descriptor, schema, table, stable_key);
    }
    if !matches!(&descriptor.schema_source, SchemaSource::Discover) {
        return Err(CdfError::data(
            "ClickHouse compilation requires a nonempty fixed schema or discover mode",
        ));
    }
    match (&descriptor.cursor, stable_key) {
        (None, Some(_)) => Err(CdfError::contract(
            "ClickHouse stable_key is valid only for a cursor resource",
        )),
        (Some(_), None) => Err(CdfError::contract(
            "ClickHouse cursor resources require a stable_key tie-breaker",
        )),
        (Some(cursor), Some(stable_key)) if cursor.field == stable_key.as_str() => Err(
            CdfError::contract("ClickHouse stable_key must differ from the cursor field"),
        ),
        _ => Ok(()),
    }
}

fn execution_capabilities(
    descriptor: &cdf_kernel::ResourceDescriptor,
) -> SourceExecutionCapabilities {
    let resumable = descriptor.cursor.is_some();
    SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: CLICKHOUSE_MAXIMUM_POLL_BYTES,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: CLICKHOUSE_MAXIMUM_BATCH_BYTES,
        maximum_emitted_batch_bytes: CLICKHOUSE_MAXIMUM_BATCH_BYTES,
        maximum_concurrency: 1,
        useful_concurrency: 1,
        executor_class: SourceExecutorClass::Io,
        blocking_lane: None,
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

const fn default_max_threads() -> u64 {
    DEFAULT_MAX_THREADS
}

const fn default_max_block_rows() -> u64 {
    DEFAULT_MAX_BLOCK_ROWS
}

const fn default_stream_buffer_batches() -> usize {
    DEFAULT_STREAM_BUFFER_BATCHES
}
