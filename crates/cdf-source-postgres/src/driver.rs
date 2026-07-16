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

use crate::{
    POSTGRES_MAXIMUM_BATCH_BYTES, PostgresTableResource, PostgresTarget,
    discover_postgres_table_catalog_schema, postgres_source_blocking_lane,
    postgres_table_capabilities,
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
                    "dialect": {"const": "postgres", "default": "postgres"}
                }
            },
            "resource": {
                "type": "object",
                "additionalProperties": false,
                "required": ["table"],
                "properties": {
                    "table": {"type": "string", "minLength": 1}
                }
            }
        });
        Ok(Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new("postgres")?,
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema)?,
                kinds: vec!["sql".to_owned()],
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

    fn add_planner(&self) -> Option<&dyn SourceAddPlanner> {
        Some(self)
    }

    fn health(
        &self,
        request: SourceHealthRequest,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Vec<SourceHealthResult>> {
        if request.compiled_plans.is_empty() {
            return Ok(vec![SourceHealthResult {
                probe_id: "catalog".to_owned(),
                status: SourceHealthStatus::Skipped,
                message: "no Postgres resources are compiled".to_owned(),
                details: serde_json::json!({"resources": 0}),
            }]);
        }
        let probe_request = SourceDiscoveryRequest::new(1, 1)?;
        Ok(request
            .compiled_plans
            .iter()
            .map(|plan| {
                let resource_id = plan.descriptor.resource_id.as_str();
                let probe = self.discovery_session(plan, context).and_then(|session| {
                    let candidates = session.candidates()?;
                    let candidate = candidates.first().ok_or_else(|| {
                        CdfError::data("Postgres health probe produced no catalog candidate")
                    })?;
                    session.observe(candidate, &probe_request)
                });
                match probe {
                    Ok(observation) => SourceHealthResult {
                        probe_id: resource_id.to_owned(),
                        status: SourceHealthStatus::Passed,
                        message: "Postgres catalog probe passed".to_owned(),
                        details: serde_json::json!({
                            "resource_id": resource_id,
                            "columns": observation.schema.fields().len(),
                        }),
                    },
                    Err(error) => SourceHealthResult::failed(
                        resource_id,
                        "Postgres catalog probe failed",
                        &plan.descriptor.resource_id,
                        &error,
                    ),
                }
            })
            .collect())
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
        let target = PostgresTarget::parse(&resource.table)?;
        let physical_plan = PostgresPhysicalPlan {
            connection: connection.as_str().to_owned(),
            target: target.display_name(),
        };
        let capabilities = postgres_table_capabilities(&request.descriptor);
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            capabilities,
            execution_capabilities(),
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog: request.baseline_observation_schema_catalog,
                redacted_options: serde_json::json!({
                    "connection": connection.as_str(),
                    "dialect": "postgres",
                    "table": target.display_name(),
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
            target: PostgresTarget::parse(&physical.target)?,
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
        let target = PostgresTarget::parse(&physical.target)?;
        let secret_provider = Arc::clone(context.secret_provider());
        let resource = PostgresTableResource::new_with_connection_resolver(
            plan.descriptor.clone(),
            Arc::new(plan.schema.clone()),
            target,
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
        .with_compiled_source_plan_hash(cdf_runtime::artifact_hash(plan)?)
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
        if !request.options.is_empty() {
            return Err(CdfError::contract(
                "Postgres cdf add does not accept source options; encode connection parameters in the DSN and edit generated resource configuration for table semantics",
            ));
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
        if segments.len() < 2 {
            return Err(CdfError::contract(
                "cdf add Postgres DSN must end with `/database/table`",
            ));
        }
        let table = segments.pop().expect("length checked");
        parsed.set_path(&format!("/{}", segments.join("/")));
        let dsn = parsed.to_string();
        let relative_path =
            std::path::PathBuf::from(format!(".cdf/secrets/sources/{}.dsn", request.source_name));
        let reference = SecretUri::new(format!(
            "secret://file/.cdf/secrets/sources/{}.dsn",
            request.source_name
        ))?;
        Ok(Some(SourceAddProposal {
            source_kind: "sql".to_owned(),
            source_options: BTreeMap::from([(
                "connection".to_owned(),
                serde_json::Value::String(reference.as_str().to_owned()),
            )]),
            resource_options: BTreeMap::from([(
                "table".to_owned(),
                serde_json::Value::String(table.clone()),
            )]),
            cursor: None,
            display_location: SourceEvidenceLocation::from_operational(&dsn)?,
            display_selection: table,
            private_files: vec![SourceAddPrivateFile {
                reference,
                relative_path,
                value: SecretValue::new(dsn),
            }],
        }))
    }
}

struct PostgresDiscoverySession {
    database_url: String,
    resource_id: cdf_kernel::ResourceId,
    target: PostgresTarget,
    execution: cdf_runtime::ExecutionServices,
    egress: cdf_runtime::SourceEgressScope,
}

impl SourceDiscoverySession for PostgresDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        SourceDiscoveryKind::SchemaMetadata
    }

    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(vec![SourceDiscoveryCandidate::new(
            self.target.display_name(),
            None,
            None,
            BTreeMap::from([
                ("source_kind".to_owned(), "sql".to_owned()),
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
        if candidate.canonical_location != self.target.display_name() {
            return Err(CdfError::contract(format!(
                "Postgres discovery candidate `{}` does not match compiled target `{}`",
                candidate.canonical_location,
                self.target.display_name()
            )));
        }
        let database_url = self.database_url.clone();
        let resource_id = self.resource_id.clone();
        let target = self.target.clone();
        let egress = self.egress.clone();
        let discovery = self
            .execution
            .run_blocking("postgres-source.sync", move || {
                discover_postgres_table_catalog_schema(
                    &database_url,
                    &resource_id,
                    &target,
                    &egress,
                )
            })?;
        let column_count = u64::try_from(discovery.schema.fields().len())
            .map_err(|_| CdfError::data("Postgres discovery column count exceeds u64"))?;
        let mut source_identity = discovery.source_identity;
        source_identity.insert("catalog_column_count".to_owned(), column_count.to_string());
        SourceSchemaObservation::new(candidate, discovery.schema, source_identity, 0, 0)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostgresSourceOptions {
    connection: String,
    #[serde(default)]
    dialect: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostgresResourceOptions {
    table: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostgresPhysicalPlan {
    connection: String,
    target: String,
}

fn decode_options<T: for<'de> Deserialize<'de>>(
    label: &str,
    options: std::collections::BTreeMap<String, serde_json::Value>,
) -> Result<T> {
    serde_json::from_value(serde_json::Value::Object(options.into_iter().collect()))
        .map_err(|error| CdfError::contract(format!("{label} options are invalid: {error}")))
}

fn execution_capabilities() -> SourceExecutionCapabilities {
    SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: POSTGRES_MAXIMUM_BATCH_BYTES,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: POSTGRES_MAXIMUM_BATCH_BYTES,
        maximum_concurrency: 4,
        useful_concurrency: 4,
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
        attestation: SourceAttestationStrength::None,
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
                source_kind: "sql".to_owned(),
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
                source_kind: "sql".to_owned(),
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
}
