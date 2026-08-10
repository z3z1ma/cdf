use std::{collections::BTreeMap, sync::Arc};

use cdf_http::{SecretUri, SecretValue};
use cdf_kernel::{CdfError, QueryableResource, Result};
use cdf_runtime::{
    CompiledSourcePlan, SourceAddPlanner, SourceAddPrivateFile, SourceAddProposal,
    SourceAddRequest, SourceAttestationStrength, SourceCompileRequest, SourceDiscoveryCandidate,
    SourceDiscoveryKind, SourceDiscoveryRequest, SourceDiscoverySession, SourceDriver,
    SourceDriverDescriptor, SourceDriverId, SourceEvidenceLocation, SourceExecutionCapabilities,
    SourceExecutorClass, SourceHealthRequest, SourceHealthResult, SourceHealthStatus,
    SourceResolutionContext, SourceRetryGranularity, SourceSchemaObservation, artifact_hash,
};
use serde::{Deserialize, Serialize};

use crate::native::{
    PostgresIsolation, PostgresNativeOptions, PostgresSourceInput, describe_postgres_query,
};
use crate::{
    POSTGRES_MAXIMUM_BATCH_BYTES, PostgresSourceResource, discover_postgres_table_catalog_schema,
    postgres_source_blocking_lane, postgres_source_capabilities,
};

#[derive(Clone, Debug)]
pub struct PostgresSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
}

impl PostgresSourceDriver {
    pub fn new() -> Result<Self> {
        let option_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "source": {
                "type": "object",
                "additionalProperties": false,
                "required": ["connection"],
                "properties": {
                    "connection": {"type": "string", "pattern": "^secret://"},
                    "dialect": {"const": "postgres", "default": "postgres"},
                    "isolation": {
                        "type": "string",
                        "enum": ["read_committed", "repeatable_read", "serializable"],
                        "default": "repeatable_read"
                    },
                    "statement_timeout_ms": {"type": "integer", "minimum": 1, "maximum": 3600000},
                    "lock_timeout_ms": {"type": "integer", "minimum": 1, "maximum": 3600000},
                    "output_batch_rows": {"type": "integer", "minimum": 1, "maximum": 100000, "default": 65536},
                    "search_path": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string", "minLength": 1}
                    }
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
                    "isolation": {
                        "type": "string",
                        "enum": ["read_committed", "repeatable_read", "serializable"]
                    },
                    "statement_timeout_ms": {"type": "integer", "minimum": 1, "maximum": 3600000},
                    "lock_timeout_ms": {"type": "integer", "minimum": 1, "maximum": 3600000},
                    "output_batch_rows": {"type": "integer", "minimum": 1, "maximum": 100000},
                    "search_path": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string", "minLength": 1}
                    }
                }
            }
        });
        Ok(Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new("postgres")?,
                driver_version: "3.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema)?,
                kinds: vec!["postgres".to_owned()],
                schemes: vec!["postgres".to_owned(), "postgresql".to_owned()],
            },
            option_schema,
        })
    }
}

impl SourceDriver for PostgresSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn validate_portable_plan(&self, plan: &CompiledSourcePlan) -> Result<()> {
        plan.validate()?;
        let physical: PostgresPhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| {
                CdfError::contract(format!("invalid Postgres source plan: {error}"))
            })?;
        SecretUri::new(physical.connection)?;
        physical.input.validate()?;
        physical.options.validate()?;
        Ok(())
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
                message: "no Postgres resources are compiled".to_owned(),
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
                    CdfError::data("Postgres health probe produced no catalog candidate")
                })?;
                session.observe(candidate, &probe_request)
            });
            let result = match probe {
                Ok(observation) => SourceHealthResult {
                    probe_id: resource_id.to_owned(),
                    status: SourceHealthStatus::Passed,
                    message: "Postgres source schema probe passed".to_owned(),
                    details: serde_json::json!({
                        "resource_id": resource_id,
                        "columns": observation.schema.fields().len(),
                    }),
                },
                Err(error) => SourceHealthResult::failed(
                    resource_id,
                    "Postgres source schema probe failed",
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
        let source: PostgresSourceOptions =
            decode_options("Postgres source", request.source_options)?;
        let resource: PostgresResourceOptions =
            decode_options("Postgres resource", request.resource_options)?;
        if !source
            .dialect
            .as_deref()
            .is_none_or(|dialect| dialect.eq_ignore_ascii_case("postgres"))
        {
            return Err(CdfError::contract(
                "Postgres source dialect must be `postgres` when declared",
            ));
        }
        let connection = SecretUri::new(source.connection.clone())?;
        let input = PostgresSourceInput::from_authored(resource.table, resource.query)?;
        if source.search_path.as_ref().is_some_and(Vec::is_empty) {
            return Err(CdfError::contract(
                "Postgres source search_path must contain at least one identifier when declared",
            ));
        }
        if resource.search_path.as_ref().is_some_and(Vec::is_empty) {
            return Err(CdfError::contract(
                "Postgres resource search_path must contain at least one identifier when declared",
            ));
        }
        PostgresNativeOptions::from_authored(
            source.isolation.unwrap_or_default(),
            source.statement_timeout_ms,
            source.lock_timeout_ms,
            source.output_batch_rows,
            source.search_path.clone().unwrap_or_default(),
        )?;
        let options = PostgresNativeOptions::from_authored(
            resource.isolation.or(source.isolation).unwrap_or_default(),
            resource
                .statement_timeout_ms
                .or(source.statement_timeout_ms),
            resource.lock_timeout_ms.or(source.lock_timeout_ms),
            resource.output_batch_rows.or(source.output_batch_rows),
            resource
                .search_path
                .or(source.search_path)
                .unwrap_or_default(),
        )?;
        let physical_plan = PostgresPhysicalPlan {
            connection: connection.as_str().to_owned(),
            input: input.clone(),
            options: options.clone(),
        };
        let capabilities = postgres_source_capabilities(&request.descriptor);
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            capabilities,
            execution_capabilities(matches!(input, PostgresSourceInput::Query { .. })),
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                source_materializations: Vec::new(),
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog: request.baseline_observation_schema_catalog,
                redacted_options: serde_json::json!({
                    "connection": connection.as_str(),
                    "dialect": "postgres",
                    "input": input.redacted_evidence(),
                    "isolation": options.isolation,
                    "statement_timeout_ms": options.statement_timeout_ms,
                    "lock_timeout_ms": options.lock_timeout_ms,
                    "output_batch_rows": options.output_batch_rows,
                    "search_path": options.search_path,
                }),
                physical_plan: serde_json::to_value(physical_plan).map_err(|error| {
                    CdfError::internal(format!("serialize Postgres source plan: {error}"))
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
        let physical: PostgresPhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| {
                CdfError::contract(format!("invalid Postgres source plan: {error}"))
            })?;
        let connection = SecretUri::new(physical.connection)?;
        let database_url = context.secret_provider().resolve(&connection)?;
        Ok(Box::new(PostgresDiscoverySession {
            database_url: database_url.as_str()?.to_owned(),
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
        let physical: PostgresPhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| {
                CdfError::contract(format!("invalid Postgres source plan: {error}"))
            })?;
        let connection = SecretUri::new(physical.connection)?;
        let secret_provider = Arc::clone(context.secret_provider());
        let resource = PostgresSourceResource::from_compiled_plan_with_connection_resolver(
            plan,
            physical.input,
            physical.options,
            context.egress_scope(&plan.driver.driver_id),
            move |cancellation| {
                cancellation.check()?;
                let secret = secret_provider.resolve(&connection)?;
                let database_url = secret.as_str()?.to_owned();
                cancellation.check()?;
                Ok(database_url)
            },
        )?
        .with_type_policy(plan.type_policy_allowances)
        .with_execution(context.execution().clone())?;
        Ok(Arc::new(resource))
    }
}

impl SourceAddPlanner for PostgresSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        request.validate()?;
        let Some((scheme, _)) = request.location.split_once("://") else {
            return Ok(None);
        };
        if !matches!(scheme, "postgres" | "postgresql") {
            return Ok(None);
        }
        const ALLOWED_OPTIONS: &[&str] = &[
            "query",
            "isolation",
            "statement_timeout_ms",
            "lock_timeout_ms",
            "output_batch_rows",
            "search_path",
            "cursor",
        ];
        if let Some(key) = request
            .options
            .keys()
            .find(|key| !ALLOWED_OPTIONS.contains(&key.as_str()))
        {
            return Err(CdfError::contract(format!(
                "Postgres cdf add option `{key}` is not supported"
            )));
        }
        let mut parsed = url::Url::parse(&request.location).map_err(|error| {
            CdfError::contract(format!("cdf add could not parse Postgres DSN: {error}"))
        })?;
        let mut segments = parsed
            .path_segments()
            .map(|segments| {
                segments
                    .filter(|segment| !segment.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let query = request.options.get("query").cloned();
        let (input, display_selection) = if let Some(query) = query {
            if segments.len() != 1 {
                return Err(CdfError::contract(
                    "cdf add Postgres query DSN must end with exactly `/database`",
                ));
            }
            let input = PostgresSourceInput::from_authored(None, Some(query))?;
            let display_selection = input.location_summary();
            (input, display_selection)
        } else {
            if segments.len() != 2 {
                return Err(CdfError::contract(
                    "cdf add Postgres table DSN must end with exactly `/database/table`",
                ));
            }
            let table = segments.pop().expect("length checked");
            parsed.set_path(&format!("/{}", segments.join("/")));
            (
                PostgresSourceInput::from_authored(Some(table.clone()), None)?,
                table,
            )
        };
        let dsn = parsed.to_string();
        let relative_path =
            std::path::PathBuf::from(format!(".cdf/secrets/sources/{}.dsn", request.source_name));
        let reference = SecretUri::new(format!(
            "secret://file/.cdf/secrets/sources/{}.dsn",
            request.source_name
        ))?;
        let mut resource_options = match &input {
            PostgresSourceInput::Table { target } => {
                BTreeMap::from([("table".to_owned(), serde_json::json!(target.display_name()))])
            }
            PostgresSourceInput::Query { sql, .. } => {
                BTreeMap::from([("query".to_owned(), serde_json::json!(sql))])
            }
        };
        let isolation = request
            .options
            .get("isolation")
            .map(|value| {
                serde_json::from_value::<PostgresIsolation>(serde_json::json!(value)).map_err(
                    |_| {
                        CdfError::contract(
                            "Postgres cdf add isolation must be read_committed, repeatable_read, or serializable",
                        )
                    },
                )
            })
            .transpose()?
            .unwrap_or_default();
        if let Some(value) = request.options.get("isolation") {
            resource_options.insert("isolation".to_owned(), serde_json::json!(value));
        }
        let statement_timeout_ms = parse_add_u64(&request.options, "statement_timeout_ms")?;
        let lock_timeout_ms = parse_add_u64(&request.options, "lock_timeout_ms")?;
        let output_batch_rows = parse_add_u64(&request.options, "output_batch_rows")?;
        for (key, value) in [
            ("statement_timeout_ms", statement_timeout_ms),
            ("lock_timeout_ms", lock_timeout_ms),
            ("output_batch_rows", output_batch_rows),
        ] {
            if let Some(value) = value {
                resource_options.insert(key.to_owned(), serde_json::json!(value));
            }
        }
        let search_path = request
            .options
            .get("search_path")
            .map(|value| parse_add_search_path(value))
            .transpose()?
            .unwrap_or_default();
        if !search_path.is_empty() {
            resource_options.insert("search_path".to_owned(), serde_json::json!(search_path));
        }
        PostgresNativeOptions::from_authored(
            isolation,
            statement_timeout_ms,
            lock_timeout_ms,
            output_batch_rows,
            search_path,
        )?;
        Ok(Some(SourceAddProposal {
            source_kind: "postgres".to_owned(),
            source_options: BTreeMap::from([(
                "connection".to_owned(),
                serde_json::Value::String(reference.as_str().to_owned()),
            )]),
            resource_options,
            cursor: request
                .options
                .get("cursor")
                .map(|field| cdf_runtime::SourceAddCursor {
                    field: field.clone(),
                    parameter: None,
                    ordering: cdf_runtime::SourceAddCursorOrdering::Exact,
                    lag_tolerance_ms: 0,
                }),
            display_location: SourceEvidenceLocation::from_operational(&dsn)?,
            display_selection,
            private_files: vec![SourceAddPrivateFile {
                reference,
                relative_path,
                value: SecretValue::new(dsn),
            }],
        }))
    }
}

fn parse_add_u64(options: &BTreeMap<String, String>, key: &str) -> Result<Option<u64>> {
    options
        .get(key)
        .map(|value| {
            value.parse::<u64>().map_err(|_| {
                CdfError::contract(format!("Postgres cdf add {key} must be an integer"))
            })
        })
        .transpose()
}

fn parse_add_search_path(value: &str) -> Result<Vec<String>> {
    let values = if value.trim_start().starts_with('[') {
        serde_json::from_str::<Vec<String>>(value).map_err(|_| {
            CdfError::contract(
                "Postgres cdf add search_path must be a JSON string array or comma-separated identifiers",
            )
        })?
    } else {
        value.split(',').map(str::trim).map(str::to_owned).collect()
    };
    if values.is_empty() || values.iter().any(String::is_empty) {
        return Err(CdfError::contract(
            "Postgres cdf add search_path requires at least one nonempty identifier",
        ));
    }
    for value in &values {
        cdf_postgres::PostgresIdentifier::user(value)?;
    }
    Ok(values)
}

struct PostgresDiscoverySession {
    database_url: String,
    resource_id: cdf_kernel::ResourceId,
    input: PostgresSourceInput,
    options: PostgresNativeOptions,
    execution: cdf_runtime::ExecutionServices,
    egress: cdf_runtime::SourceEgressScope,
}

impl SourceDiscoverySession for PostgresDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        SourceDiscoveryKind::SchemaMetadata
    }

    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(vec![SourceDiscoveryCandidate::new(
            self.input.location_summary(),
            None,
            None,
            BTreeMap::from([
                ("source_kind".to_owned(), "postgres".to_owned()),
                ("dialect".to_owned(), "postgres".to_owned()),
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
            return Err(CdfError::contract(format!(
                "Postgres discovery candidate `{}` does not match compiled input `{}`",
                candidate.canonical_location,
                self.input.location_summary()
            )));
        }
        let database_url = self.database_url.clone();
        let resource_id = self.resource_id.clone();
        let input = self.input.clone();
        let options = self.options.clone();
        let egress = self.egress.clone();
        let discovery =
            self.execution
                .run_blocking("postgres-source.sync", move || match &input {
                    PostgresSourceInput::Table { target } => {
                        discover_postgres_table_catalog_schema(
                            &database_url,
                            &resource_id,
                            target,
                            &egress,
                        )
                    }
                    PostgresSourceInput::Query { .. } => {
                        egress.authorize(&database_url)?;
                        let mut client = postgres::Client::connect(&database_url, postgres::NoTls)
                            .map_err(|error| {
                                crate::error::classify_postgres_error(
                                    "connect to Postgres native query for schema discovery",
                                    error,
                                )
                            })?;
                        let mut transaction = options.begin_transaction(
                            &mut client,
                            "begin Postgres native-query discovery transaction",
                        )?;
                        let schema = describe_postgres_query(
                            &mut transaction,
                            &resource_id,
                            &input,
                            &options,
                        )?;
                        let source_identity = BTreeMap::from([
                            ("source_kind".to_owned(), "postgres".to_owned()),
                            ("dialect".to_owned(), "postgres".to_owned()),
                            (
                                "query_generation".to_owned(),
                                crate::native::query_generation_from_schema(&schema)?.to_owned(),
                            ),
                        ]);
                        Ok(crate::catalog::PostgresCatalogDiscovery {
                            schema,
                            source_identity,
                        })
                    }
                })?;
        let column_count = u64::try_from(discovery.schema.fields().len())
            .map_err(|_| CdfError::data("Postgres discovery column count exceeds u64"))?;
        let mut source_identity = discovery.source_identity;
        source_identity.insert("output_column_count".to_owned(), column_count.to_string());
        SourceSchemaObservation::new(candidate, discovery.schema, source_identity, 0, 0)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostgresSourceOptions {
    connection: String,
    #[serde(default)]
    dialect: Option<String>,
    #[serde(default)]
    isolation: Option<PostgresIsolation>,
    #[serde(default)]
    statement_timeout_ms: Option<u64>,
    #[serde(default)]
    lock_timeout_ms: Option<u64>,
    #[serde(default)]
    output_batch_rows: Option<u64>,
    #[serde(default)]
    search_path: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostgresResourceOptions {
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    isolation: Option<PostgresIsolation>,
    #[serde(default)]
    statement_timeout_ms: Option<u64>,
    #[serde(default)]
    lock_timeout_ms: Option<u64>,
    #[serde(default)]
    output_batch_rows: Option<u64>,
    #[serde(default)]
    search_path: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostgresPhysicalPlan {
    connection: String,
    input: PostgresSourceInput,
    options: PostgresNativeOptions,
}

fn decode_options<T: for<'de> Deserialize<'de>>(
    label: &str,
    options: std::collections::BTreeMap<String, serde_json::Value>,
) -> Result<T> {
    serde_json::from_value(serde_json::Value::Object(options.into_iter().collect()))
        .map_err(|error| CdfError::contract(format!("{label} options are invalid: {error}")))
}

fn execution_capabilities(query_input: bool) -> SourceExecutionCapabilities {
    SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: POSTGRES_MAXIMUM_BATCH_BYTES,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: POSTGRES_MAXIMUM_BATCH_BYTES,
        maximum_emitted_batch_bytes: POSTGRES_MAXIMUM_BATCH_BYTES,
        maximum_concurrency: 1,
        useful_concurrency: 1,
        executor_class: SourceExecutorClass::BlockingLane,
        blocking_lane: Some(postgres_source_blocking_lane()),
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
    use std::collections::BTreeMap;

    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{
        ResourceDescriptor, ResourceId, SchemaHash, SchemaSource, ScopeKey, TrustLevel,
        WriteDisposition,
    };
    use cdf_runtime::{SourceDriver, SourceExecutorClass};

    use super::*;

    fn descriptor() -> ResourceDescriptor {
        ResourceDescriptor {
            resource_id: ResourceId::new("warehouse.orders").unwrap(),
            schema_source: SchemaSource::Declared {
                schema_hash: SchemaHash::new("schema-postgres-driver").unwrap(),
                source: "postgres://warehouse/orders".to_owned(),
            },
            primary_key: vec!["id".to_owned()],
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
    fn compiles_strict_redacted_plan_and_declares_managed_lane() {
        let driver = PostgresSourceDriver::new().unwrap();
        let plan = driver
            .compile(SourceCompileRequest {
                source_kind: "postgres".to_owned(),
                context: cdf_runtime::SourceCompileContext {
                    source_name: "warehouse".to_owned(),
                    project_root: None,
                    cursor_pushdown: None,
                },
                source_options: BTreeMap::from([
                    (
                        "connection".to_owned(),
                        serde_json::json!("secret://env/WAREHOUSE_URL"),
                    ),
                    ("dialect".to_owned(), serde_json::json!("postgres")),
                ]),
                resource_options: BTreeMap::from([(
                    "table".to_owned(),
                    serde_json::json!("public.orders"),
                )]),
                descriptor: descriptor(),
                schema: Schema::new(vec![Field::new("id", DataType::Int64, false)]),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap();

        assert_eq!(plan.driver.driver_id.as_str(), "postgres");
        assert_eq!(
            plan.execution_capabilities.executor_class,
            SourceExecutorClass::BlockingLane
        );
        assert_eq!(plan.execution_capabilities.maximum_concurrency, 1);
        assert_eq!(plan.execution_capabilities.useful_concurrency, 1);
        assert_eq!(
            plan.execution_capabilities
                .blocking_lane
                .as_ref()
                .unwrap()
                .lane_id,
            "postgres-source.sync"
        );
        let encoded = serde_json::to_string(&plan).unwrap();
        assert!(encoded.contains("secret://env/WAREHOUSE_URL"));
        assert!(!encoded.contains("postgres://user:password"));

        let error = driver
            .compile(SourceCompileRequest {
                source_kind: "postgres".to_owned(),
                context: cdf_runtime::SourceCompileContext {
                    source_name: "warehouse".to_owned(),
                    project_root: None,
                    cursor_pushdown: None,
                },
                source_options: BTreeMap::from([
                    (
                        "connection".to_owned(),
                        serde_json::json!("postgres://inline"),
                    ),
                    ("unexpected".to_owned(), serde_json::json!(true)),
                ]),
                resource_options: BTreeMap::from([(
                    "table".to_owned(),
                    serde_json::json!("orders"),
                )]),
                descriptor: descriptor(),
                schema: Schema::new(vec![Field::new("id", DataType::Int64, false)]),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("unknown field `unexpected`"));
    }

    #[test]
    fn compiles_native_query_controls_without_literal_evidence() {
        let driver = PostgresSourceDriver::new().unwrap();
        let query = "SELECT id, amount FROM private_ledger WHERE tenant = 'private-value'";
        let source_options = BTreeMap::from([
            (
                "connection".to_owned(),
                serde_json::json!("secret://env/WAREHOUSE_URL"),
            ),
            ("isolation".to_owned(), serde_json::json!("read_committed")),
            ("statement_timeout_ms".to_owned(), serde_json::json!(15_000)),
            ("lock_timeout_ms".to_owned(), serde_json::json!(2_500)),
            ("output_batch_rows".to_owned(), serde_json::json!(4_096)),
            (
                "search_path".to_owned(),
                serde_json::json!(["source_default"]),
            ),
        ]);
        let source_default_plan = driver
            .compile(SourceCompileRequest {
                source_kind: "postgres".to_owned(),
                context: cdf_runtime::SourceCompileContext {
                    source_name: "warehouse".to_owned(),
                    project_root: None,
                    cursor_pushdown: None,
                },
                source_options: source_options.clone(),
                resource_options: BTreeMap::from([("query".to_owned(), serde_json::json!(query))]),
                descriptor: descriptor(),
                schema: Schema::new(vec![
                    Field::new("id", DataType::Int64, true),
                    Field::new("amount", DataType::Utf8, true),
                ]),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap();
        assert_eq!(source_default_plan.driver.driver_version, "3.0.0");
        assert_eq!(
            source_default_plan.redacted_options["isolation"],
            "read_committed"
        );
        assert_eq!(
            source_default_plan.redacted_options["statement_timeout_ms"],
            15_000
        );
        assert_eq!(
            source_default_plan.redacted_options["output_batch_rows"],
            4_096
        );
        assert_eq!(
            source_default_plan.redacted_options["search_path"],
            serde_json::json!(["source_default"])
        );
        let plan = driver
            .compile(SourceCompileRequest {
                source_kind: "postgres".to_owned(),
                context: cdf_runtime::SourceCompileContext {
                    source_name: "warehouse".to_owned(),
                    project_root: None,
                    cursor_pushdown: None,
                },
                source_options,
                resource_options: BTreeMap::from([
                    ("query".to_owned(), serde_json::json!(query)),
                    ("isolation".to_owned(), serde_json::json!("serializable")),
                    ("statement_timeout_ms".to_owned(), serde_json::json!(30_000)),
                    ("lock_timeout_ms".to_owned(), serde_json::json!(5_000)),
                    ("output_batch_rows".to_owned(), serde_json::json!(8_192)),
                    (
                        "search_path".to_owned(),
                        serde_json::json!(["analytics", "public"]),
                    ),
                ]),
                descriptor: descriptor(),
                schema: Schema::new(vec![
                    Field::new("id", DataType::Int64, true),
                    Field::new("amount", DataType::Utf8, true),
                ]),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap();

        assert_eq!(
            plan.execution_capabilities.attestation,
            SourceAttestationStrength::Metadata
        );
        let evidence = plan.redacted_options.to_string();
        assert!(evidence.contains("query_sha256"));
        assert!(evidence.contains("8192"));
        assert!(!evidence.contains("private-value"));
        let physical: PostgresPhysicalPlan =
            serde_json::from_value(plan.physical_plan.clone()).unwrap();
        let PostgresSourceInput::Query { sql, .. } = physical.input else {
            panic!("compiled input must remain a native query");
        };
        assert_eq!(sql, query);
        assert_eq!(physical.options.output_batch_rows, 8_192);
        assert_eq!(physical.options.isolation, PostgresIsolation::Serializable);
        assert_eq!(physical.options.statement_timeout_ms, Some(30_000));
        assert_eq!(physical.options.lock_timeout_ms, Some(5_000));
        assert_eq!(physical.options.search_path[0].as_str(), "analytics");
        driver.validate_portable_plan(&plan).unwrap();

        let error = driver
            .compile(SourceCompileRequest {
                source_kind: "postgres".to_owned(),
                context: cdf_runtime::SourceCompileContext {
                    source_name: "warehouse".to_owned(),
                    project_root: None,
                    cursor_pushdown: None,
                },
                source_options: BTreeMap::from([(
                    "connection".to_owned(),
                    serde_json::json!("secret://env/WAREHOUSE_URL"),
                )]),
                resource_options: BTreeMap::from([
                    ("table".to_owned(), serde_json::json!("public.orders")),
                    ("query".to_owned(), serde_json::json!("SELECT 1")),
                ]),
                descriptor: descriptor(),
                schema: Schema::new(vec![Field::new("id", DataType::Int64, true)]),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("exactly one"));
    }

    #[test]
    fn add_planner_preserves_native_query_and_places_credentials_in_private_file() {
        let driver = PostgresSourceDriver::new().unwrap();
        let query = "SELECT id FROM private_ledger WHERE tenant = 'private-value'";
        let proposal = driver
            .add_planner()
            .unwrap()
            .propose_add(&SourceAddRequest {
                source_name: "warehouse".to_owned(),
                resource_name: "ledger".to_owned(),
                location: "postgresql://reader:private-password@postgres.example:5432/analytics"
                    .to_owned(),
                project_root: "/project".into(),
                current_dir: "/project".into(),
                options: BTreeMap::from([
                    ("query".to_owned(), query.to_owned()),
                    ("isolation".to_owned(), "serializable".to_owned()),
                    ("statement_timeout_ms".to_owned(), "30000".to_owned()),
                    ("output_batch_rows".to_owned(), "8192".to_owned()),
                    ("search_path".to_owned(), "analytics, public".to_owned()),
                ]),
                project_options: None,
            })
            .unwrap()
            .unwrap();

        assert_eq!(
            proposal.source_options["connection"],
            "secret://file/.cdf/secrets/sources/warehouse.dsn"
        );
        assert_eq!(proposal.resource_options["query"], query);
        assert_eq!(proposal.resource_options["output_batch_rows"], 8_192);
        assert_eq!(
            proposal.resource_options["search_path"],
            serde_json::json!(["analytics", "public"])
        );
        assert!(proposal.display_selection.starts_with("query:sha256:"));
        assert_eq!(proposal.private_files.len(), 1);
        assert_eq!(
            proposal.private_files[0].value.as_str().unwrap(),
            "postgresql://reader:private-password@postgres.example:5432/analytics"
        );
        let rendered = format!("{proposal:?}");
        assert!(!rendered.contains("private-password"));
    }
}
