use std::{collections::BTreeMap, sync::Arc};

use cdf_http::SecretUri;
use cdf_kernel::{CdfError, QueryableResource, Result};
use cdf_runtime::{
    CompiledSourcePlan, SourceAddPlanner, SourceAttestationStrength, SourceCatalogDiscoverer,
    SourceCompileRequest, SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest,
    SourceDiscoverySession, SourceDriver, SourceDriverDescriptor, SourceDriverId,
    SourceExecutionCapabilities, SourceExecutorClass, SourceHealthRequest, SourceHealthResult,
    SourceHealthStatus, SourceResolutionContext, SourceRetryGranularity, SourceSchemaObservation,
    artifact_hash,
};
use mysql_async::{Conn, Opts, prelude::Queryable};
use serde::{Deserialize, Serialize};

use crate::{
    error::classify_mysql_error,
    native::{MySqlIsolation, MySqlNativeOptions, MySqlSourceInput},
    resource::{
        MYSQL_MAXIMUM_BATCH_BYTES, MySqlSourceResource, apply_session_options,
        mysql_source_capabilities,
    },
    schema::schema_from_columns,
};

#[derive(Clone, Debug)]
pub struct MySqlSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
}

impl MySqlSourceDriver {
    pub fn new() -> Result<Self> {
        let controls = serde_json::json!({
            "isolation": {
                "type": "string",
                "enum": ["read_committed", "repeatable_read", "serializable"]
            },
            "fetch_rows": {"type": "integer", "minimum": 1, "maximum": 100000},
            "output_batch_rows": {"type": "integer", "minimum": 1, "maximum": 100000},
            "max_execution_time_ms": {"type": "integer", "minimum": 1, "maximum": 3600000},
            "lock_wait_timeout_ms": {"type": "integer", "minimum": 1, "maximum": 3600000},
            "use_invisible_indexes": {"type": "boolean"}
        });
        let mut source_properties = controls
            .as_object()
            .cloned()
            .ok_or_else(|| CdfError::internal("construct MySQL source option schema"))?;
        source_properties.insert(
            "connection".to_owned(),
            serde_json::json!({"type": "string", "pattern": "^secret://"}),
        );
        source_properties.insert(
            "dialect".to_owned(),
            serde_json::json!({"const": "mysql", "default": "mysql"}),
        );
        let mut resource_properties = controls
            .as_object()
            .cloned()
            .ok_or_else(|| CdfError::internal("construct MySQL resource option schema"))?;
        resource_properties.insert(
            "table".to_owned(),
            serde_json::json!({"type": "string", "minLength": 1}),
        );
        resource_properties.insert(
            "query".to_owned(),
            serde_json::json!({"type": "string", "minLength": 1}),
        );
        let option_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "source": {
                "type": "object",
                "additionalProperties": false,
                "required": ["connection"],
                "properties": source_properties
            },
            "resource": {
                "type": "object",
                "additionalProperties": false,
                "oneOf": [
                    {"type": "object", "required": ["table"], "properties": {"table": {"type": "string"}}},
                    {"type": "object", "required": ["query"], "properties": {"query": {"type": "string"}}}
                ],
                "properties": resource_properties
            }
        });
        Ok(Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new("mysql")?,
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema)?,
                kinds: vec!["mysql".to_owned()],
                schemes: vec!["mysql".to_owned()],
            },
            option_schema,
        })
    }
}

impl SourceDriver for MySqlSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn validate_portable_plan(&self, plan: &CompiledSourcePlan) -> Result<()> {
        plan.validate()?;
        let physical: MySqlPhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid MySQL source plan: {error}")))?;
        SecretUri::new(physical.connection)?;
        physical.input.validate()?;
        physical.options.validate()
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
                message: "no MySQL resources are compiled".to_owned(),
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
                let candidates = session.candidates()?;
                let candidate = candidates.first().ok_or_else(|| {
                    CdfError::data("MySQL health probe produced no discovery candidate")
                })?;
                session.observe(candidate, &probe_request)
            });
            output.emit(match probe {
                Ok(observation) => SourceHealthResult {
                    probe_id: resource_id.to_owned(),
                    status: SourceHealthStatus::Passed,
                    message: "MySQL prepared-schema probe passed".to_owned(),
                    details: serde_json::json!({
                        "resource_id": resource_id,
                        "columns": observation.schema.fields().len(),
                    }),
                },
                Err(error) => SourceHealthResult::failed(
                    resource_id,
                    "MySQL prepared-schema probe failed",
                    &plan.descriptor.resource_id,
                    &error,
                ),
            })?;
        }
        Ok(())
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        let source: MySqlSourceOptions = decode_options("MySQL source", request.source_options)?;
        let resource: MySqlResourceOptions =
            decode_options("MySQL resource", request.resource_options)?;
        if !source
            .dialect
            .as_deref()
            .is_none_or(|dialect| dialect.eq_ignore_ascii_case("mysql"))
        {
            return Err(CdfError::contract(
                "MySQL source dialect must be `mysql` when declared",
            ));
        }
        let connection = SecretUri::new(source.connection.clone())?;
        let input = MySqlSourceInput::from_authored(resource.table, resource.query)?;
        MySqlNativeOptions::from_authored(
            source.isolation.unwrap_or_default(),
            source.fetch_rows,
            source.output_batch_rows,
            source.max_execution_time_ms,
            source.lock_wait_timeout_ms,
            source.use_invisible_indexes.unwrap_or(false),
        )?;
        let options = MySqlNativeOptions::from_authored(
            resource.isolation.or(source.isolation).unwrap_or_default(),
            resource.fetch_rows.or(source.fetch_rows),
            resource.output_batch_rows.or(source.output_batch_rows),
            resource
                .max_execution_time_ms
                .or(source.max_execution_time_ms),
            resource
                .lock_wait_timeout_ms
                .or(source.lock_wait_timeout_ms),
            resource
                .use_invisible_indexes
                .or(source.use_invisible_indexes)
                .unwrap_or(false),
        )?;
        let physical_plan = MySqlPhysicalPlan {
            connection: connection.as_str().to_owned(),
            input: input.clone(),
            options: options.clone(),
        };
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            mysql_source_capabilities(&request.descriptor),
            execution_capabilities(matches!(input, MySqlSourceInput::Query { .. })),
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                source_materializations: Vec::new(),
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog: request.baseline_observation_schema_catalog,
                redacted_options: serde_json::json!({
                    "connection": connection.as_str(),
                    "dialect": "mysql",
                    "input": input.redacted_evidence(),
                    "isolation": options.isolation,
                    "fetch_rows": options.fetch_rows,
                    "output_batch_rows": options.output_batch_rows,
                    "max_execution_time_ms": options.max_execution_time_ms,
                    "lock_wait_timeout_ms": options.lock_wait_timeout_ms,
                    "use_invisible_indexes": options.use_invisible_indexes,
                }),
                physical_plan: serde_json::to_value(physical_plan).map_err(|error| {
                    CdfError::internal(format!("serialize MySQL source plan: {error}"))
                })?,
            },
        )
    }

    fn discovery_session(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Box<dyn SourceDiscoverySession>> {
        self.validate_portable_plan(plan)?;
        let physical: MySqlPhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid MySQL source plan: {error}")))?;
        let reference = SecretUri::new(physical.connection)?;
        let connection = context.secret_provider().resolve(&reference)?;
        Ok(Box::new(MySqlDiscoverySession {
            connection: connection.as_str()?.to_owned(),
            resource_id: plan.descriptor.resource_id.clone(),
            input: physical.input,
            options: physical.options,
            execution: context.execution().clone(),
            egress: context.egress_scope(&plan.driver.driver_id),
        }))
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        self.validate_portable_plan(plan)?;
        let physical: MySqlPhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid MySQL source plan: {error}")))?;
        let reference = SecretUri::new(physical.connection)?;
        let secrets = Arc::clone(context.secret_provider());
        Ok(Arc::new(MySqlSourceResource::from_compiled_plan(
            plan,
            physical.input,
            physical.options,
            context.execution().clone(),
            context.egress_scope(&plan.driver.driver_id),
            move |cancellation| {
                cancellation.check()?;
                let value = secrets.resolve(&reference)?;
                let connection = value.as_str()?.to_owned();
                cancellation.check()?;
                Ok(connection)
            },
        )?))
    }
}

struct MySqlDiscoverySession {
    connection: String,
    resource_id: cdf_kernel::ResourceId,
    input: MySqlSourceInput,
    options: MySqlNativeOptions,
    execution: cdf_runtime::ExecutionServices,
    egress: cdf_runtime::SourceEgressScope,
}

impl SourceDiscoverySession for MySqlDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        SourceDiscoveryKind::SchemaMetadata
    }

    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(vec![SourceDiscoveryCandidate::new(
            self.input.location_summary(),
            None,
            None,
            BTreeMap::from([
                ("source_kind".to_owned(), "mysql".to_owned()),
                ("dialect".to_owned(), "mysql".to_owned()),
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
                "MySQL discovery candidate differs from the compiled input",
            ));
        }
        let connection = self.connection.clone();
        let resource_id = self.resource_id.clone();
        let input = self.input.clone();
        let options = self.options.clone();
        let egress = self.egress.clone();
        let schema = self.execution.run_io(async move {
            egress.authorize(&connection)?;
            let opts = Opts::from_url(&connection)
                .map_err(|_| CdfError::auth("MySQL source connection URI is invalid"))?;
            let mut connection = Conn::new(opts).await.map_err(|error| {
                classify_mysql_error("connect to MySQL prepared-schema discovery", error)
            })?;
            let cancellation = cdf_runtime::RunCancellation::default();
            apply_session_options(&mut connection, &options, &cancellation).await?;
            let mut transaction = connection
                .start_transaction(options.transaction_options())
                .await
                .map_err(|error| classify_mysql_error("begin MySQL discovery snapshot", error))?;
            let sql = match &input {
                MySqlSourceInput::Table { target } => {
                    format!("SELECT * FROM {} LIMIT 0", target.sql())
                }
                MySqlSourceInput::Query { sql, .. } => sql.clone(),
            };
            let statement = transaction.prep(sql).await.map_err(|error| {
                classify_mysql_error("prepare MySQL source for discovery", error)
            })?;
            let schema = schema_from_columns(&resource_id, statement.columns().as_ref())?;
            transaction
                .rollback()
                .await
                .map_err(|error| classify_mysql_error("close MySQL discovery snapshot", error))?;
            Ok(schema)
        })?;
        let column_count = u64::try_from(schema.fields().len())
            .map_err(|_| CdfError::data("MySQL discovery column count exceeds u64"))?;
        SourceSchemaObservation::new(
            candidate,
            schema,
            BTreeMap::from([
                ("source_kind".to_owned(), "mysql".to_owned()),
                ("dialect".to_owned(), "mysql".to_owned()),
                ("output_column_count".to_owned(), column_count.to_string()),
            ]),
            0,
            0,
        )
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MySqlSourceOptions {
    pub(crate) connection: String,
    #[serde(default)]
    pub(crate) dialect: Option<String>,
    #[serde(default)]
    pub(crate) isolation: Option<MySqlIsolation>,
    #[serde(default)]
    pub(crate) fetch_rows: Option<u64>,
    #[serde(default)]
    pub(crate) output_batch_rows: Option<u64>,
    #[serde(default)]
    pub(crate) max_execution_time_ms: Option<u64>,
    #[serde(default)]
    pub(crate) lock_wait_timeout_ms: Option<u64>,
    #[serde(default)]
    pub(crate) use_invisible_indexes: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MySqlResourceOptions {
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    isolation: Option<MySqlIsolation>,
    #[serde(default)]
    fetch_rows: Option<u64>,
    #[serde(default)]
    output_batch_rows: Option<u64>,
    #[serde(default)]
    max_execution_time_ms: Option<u64>,
    #[serde(default)]
    lock_wait_timeout_ms: Option<u64>,
    #[serde(default)]
    use_invisible_indexes: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MySqlPhysicalPlan {
    connection: String,
    input: MySqlSourceInput,
    options: MySqlNativeOptions,
}

pub(crate) fn decode_options<T: for<'de> Deserialize<'de>>(
    label: &str,
    options: BTreeMap<String, serde_json::Value>,
) -> Result<T> {
    serde_json::from_value(serde_json::Value::Object(options.into_iter().collect()))
        .map_err(|error| CdfError::contract(format!("{label} options are invalid: {error}")))
}

fn execution_capabilities(query_input: bool) -> SourceExecutionCapabilities {
    SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: MYSQL_MAXIMUM_BATCH_BYTES,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: MYSQL_MAXIMUM_BATCH_BYTES,
        maximum_emitted_batch_bytes: MYSQL_MAXIMUM_BATCH_BYTES,
        maximum_concurrency: 1,
        useful_concurrency: 1,
        executor_class: SourceExecutorClass::Io,
        blocking_lane: None,
        pausable: true,
        spillable: false,
        idempotent_reads: true,
        reopenable: true,
        resumable: false,
        speculative_safe: false,
        retry_granularity: SourceRetryGranularity::None,
        retryable_errors: Vec::new(),
        retry_policy: None,
        attestation: if query_input {
            SourceAttestationStrength::Metadata
        } else {
            SourceAttestationStrength::None
        },
        rate_limit: None,
        quota_authority: None,
        canonical_order: false,
        bounded: true,
        batch_memory: cdf_runtime::SourceBatchMemoryContract::Preaccounted,
        telemetry_version: "v1".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{
        ResourceDescriptor, ResourceId, SchemaHash, SchemaSource, ScopeKey, TrustLevel,
        WriteDisposition,
    };

    use super::*;

    fn descriptor() -> ResourceDescriptor {
        ResourceDescriptor {
            resource_id: ResourceId::new("warehouse.orders").unwrap(),
            schema_source: SchemaSource::Declared {
                schema_hash: SchemaHash::new("schema-mysql-driver").unwrap(),
                source: "mysql://warehouse/orders".to_owned(),
            },
            primary_key: vec!["id".to_owned()],
            merge_key: Vec::new(),
            cursor: None,
            write_disposition: WriteDisposition::Replace,
            deduplication: None,
            contract: None,
            state_scope: ScopeKey::Resource,
            freshness: None,
            trust_level: TrustLevel::Governed,
        }
    }

    fn schema() -> Schema {
        let mut schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        schema.metadata.insert(
            crate::schema::MYSQL_GENERATION_SCHEMA_KEY.to_owned(),
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        );
        schema
    }

    #[test]
    fn compile_is_redacted_and_cursor_optional_for_replace() {
        let driver = MySqlSourceDriver::new().unwrap();
        let plan = driver
            .compile(SourceCompileRequest {
                source_kind: "mysql".to_owned(),
                context: cdf_runtime::SourceCompileContext {
                    source_name: "warehouse".to_owned(),
                    project_root: None,
                    cursor_pushdown: None,
                },
                source_options: BTreeMap::from([
                    (
                        "connection".to_owned(),
                        serde_json::json!("secret://env/MYSQL_URL"),
                    ),
                    ("fetch_rows".to_owned(), serde_json::json!(4096)),
                ]),
                resource_options: BTreeMap::from([(
                    "table".to_owned(),
                    serde_json::json!("warehouse.orders"),
                )]),
                descriptor: descriptor(),
                schema: schema(),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap();
        assert_eq!(plan.driver.driver_id.as_str(), "mysql");
        assert_eq!(plan.redacted_options["fetch_rows"], 4096);
        assert_eq!(
            plan.resource_capabilities.incremental,
            cdf_kernel::IncrementalShape::Full
        );
        assert_eq!(plan.execution_capabilities.maximum_concurrency, 1);
        assert_eq!(plan.execution_capabilities.useful_concurrency, 1);
        let encoded = serde_json::to_string(&plan).unwrap();
        assert!(encoded.contains("secret://env/MYSQL_URL"));
        assert!(!encoded.contains("password"));
    }

    #[test]
    fn resource_controls_override_source_defaults() {
        let driver = MySqlSourceDriver::new().unwrap();
        let plan = driver
            .compile(SourceCompileRequest {
                source_kind: "mysql".to_owned(),
                context: cdf_runtime::SourceCompileContext {
                    source_name: "warehouse".to_owned(),
                    project_root: None,
                    cursor_pushdown: None,
                },
                source_options: BTreeMap::from([
                    (
                        "connection".to_owned(),
                        serde_json::json!("secret://env/MYSQL_URL"),
                    ),
                    ("fetch_rows".to_owned(), serde_json::json!(4096)),
                    ("isolation".to_owned(), serde_json::json!("read_committed")),
                ]),
                resource_options: BTreeMap::from([
                    ("query".to_owned(), serde_json::json!("SELECT 1 AS id")),
                    ("fetch_rows".to_owned(), serde_json::json!(8192)),
                    ("isolation".to_owned(), serde_json::json!("serializable")),
                ]),
                descriptor: descriptor(),
                schema: schema(),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap();
        assert_eq!(plan.redacted_options["fetch_rows"], 8192);
        assert_eq!(plan.redacted_options["isolation"], "serializable");
        assert!(!plan.redacted_options.to_string().contains("SELECT 1"));
    }
}
