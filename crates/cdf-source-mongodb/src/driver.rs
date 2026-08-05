use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use cdf_http::{SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{CdfError, QueryableResource, Result, SchemaSource};
use cdf_memory::{ConsumerKey, MemoryClass, ReservationRequest, reserve};
use cdf_runtime::{
    CompiledSourcePlan, SourceAddCursor, SourceAddCursorOrdering, SourceAddPlanner,
    SourceAddPrivateFile, SourceAddProposal, SourceAddRequest, SourceAttestationStrength,
    SourceCompileRequest, SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest,
    SourceDiscoverySession, SourceDriver, SourceDriverDescriptor, SourceDriverId,
    SourceEgressScope, SourceEvidenceLocation, SourceExecutionCapabilities, SourceExecutorClass,
    SourceHealthRequest, SourceHealthResult, SourceHealthStatus, SourceResolutionContext,
    SourceRetryGranularity, SourceSchemaObservation, artifact_hash,
};
use futures::StreamExt;
use mongodb::bson::{Bson, Document, doc};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    execution::{
        MONGODB_MAXIMUM_DECODE_BYTES, MONGODB_MAXIMUM_OUTPUT_BATCH_BYTES,
        MONGODB_MAXIMUM_WIRE_BATCH_BYTES, connect_mongodb,
    },
    identifier::MongoDbIdentifier,
    resource::{
        MongoDbCollectionResource, mongodb_collection_capabilities,
        validate_compiled_schema_evidence, validate_resource_shape,
    },
    schema::SchemaInference,
};

const DEFAULT_BATCH_ROWS: u32 = 65_536;
const DEFAULT_MAX_POOL_SIZE: u32 = 1;
const DEFAULT_STREAM_BUFFER_BATCHES: usize = 1;
const DEFAULT_DISCOVERY_RECORDS: u64 = 1_000;
const DEFAULT_DISCOVERY_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct MongoDbSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
}

impl MongoDbSourceDriver {
    pub fn new() -> Result<Self> {
        let option_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "source": {
                "type": "object",
                "additionalProperties": false,
                "required": ["endpoint", "database"],
                "properties": {
                    "endpoint": {"type": "string", "minLength": 1},
                    "database": {"type": "string", "minLength": 1},
                    "username": {"type": "string", "pattern": "^secret://"},
                    "password": {"type": "string", "pattern": "^secret://"},
                    "auth_source": {"type": "string", "minLength": 1},
                    "batch_rows": {"type": "integer", "minimum": 1, "maximum": 100000, "default": DEFAULT_BATCH_ROWS},
                    "max_pool_size": {"type": "integer", "minimum": 1, "maximum": 8, "default": DEFAULT_MAX_POOL_SIZE},
                    "stream_buffer_batches": {"type": "integer", "minimum": 1, "maximum": 16, "default": DEFAULT_STREAM_BUFFER_BATCHES},
                    "discovery_records": {"type": "integer", "minimum": 1, "maximum": 100000, "default": DEFAULT_DISCOVERY_RECORDS},
                    "discovery_bytes": {"type": "integer", "minimum": 1024, "maximum": 67108864, "default": DEFAULT_DISCOVERY_BYTES}
                }
            },
            "resource": {
                "type": "object",
                "additionalProperties": false,
                "required": ["collection"],
                "properties": {
                    "collection": {"type": "string", "minLength": 1}
                }
            }
        });
        Ok(Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new("mongodb")?,
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema)?,
                kinds: vec!["mongodb".to_owned()],
                schemes: vec!["mongodb".to_owned(), "mongodb+srv".to_owned()],
            },
            option_schema,
        })
    }
}

impl SourceDriver for MongoDbSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn add_planner(&self) -> Option<&dyn SourceAddPlanner> {
        Some(self)
    }

    fn validate_portable_plan(&self, plan: &CompiledSourcePlan) -> Result<()> {
        plan.validate()?;
        let physical = decode_physical_plan(plan)?;
        physical.validate()?;
        validate_compile_shape(
            &plan.descriptor,
            &Arc::new(plan.schema.clone()),
            &physical.collection,
        )
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
                message: "no MongoDB resources are compiled".to_owned(),
                details: serde_json::json!({"resources": 0}),
            });
        }
        let probe_request = SourceDiscoveryRequest::new(16 * 1024 * 1024, 1)?
            .with_cancellation(request.budget.cancellation());
        for plan in &request.compiled_plans {
            request.budget.consume_work(1)?;
            request.budget.consume_list_entries(1)?;
            let resource_id = plan.descriptor.resource_id.as_str();
            let probe = self.discovery_session(plan, context).and_then(|session| {
                let candidate = session.candidates()?.into_iter().next().ok_or_else(|| {
                    CdfError::data("MongoDB health probe produced no collection candidate")
                })?;
                session.observe(&candidate, &probe_request)
            });
            output.emit(match probe {
                Ok(observation) => SourceHealthResult {
                    probe_id: resource_id.to_owned(),
                    status: SourceHealthStatus::Passed,
                    message: "MongoDB raw BSON collection probe passed".to_owned(),
                    details: serde_json::json!({
                        "resource_id": resource_id,
                        "sampled_records": observation.records_read,
                        "sampled_bytes": observation.bytes_read,
                    }),
                },
                Err(error) => SourceHealthResult::failed(
                    resource_id,
                    "MongoDB collection probe failed",
                    &plan.descriptor.resource_id,
                    &error,
                ),
            })?;
        }
        Ok(())
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        if request.source_kind != "mongodb" {
            return Err(CdfError::contract(
                "MongoDB driver can compile only source kind `mongodb`",
            ));
        }
        let source: MongoDbSourceOptions =
            decode_options("MongoDB source", request.source_options)?;
        let resource: MongoDbResourceOptions =
            decode_options("MongoDB resource", request.resource_options)?;
        let endpoint = normalize_endpoint(&source.endpoint)?;
        let database = MongoDbIdentifier::new(source.database)?;
        let collection = MongoDbIdentifier::new(resource.collection)?;
        let username = source.username.map(SecretUri::new).transpose()?;
        let password = source.password.map(SecretUri::new).transpose()?;
        let auth_source = source.auth_source.map(MongoDbIdentifier::new).transpose()?;
        let physical = MongoDbPhysicalPlan {
            endpoint: endpoint.clone(),
            database: database.clone(),
            collection: collection.clone(),
            username: username.map(|value| value.as_str().to_owned()),
            password: password.map(|value| value.as_str().to_owned()),
            auth_source,
            batch_rows: source.batch_rows,
            max_pool_size: source.max_pool_size,
            stream_buffer_batches: source.stream_buffer_batches,
            discovery_records: source.discovery_records,
            discovery_bytes: source.discovery_bytes,
        };
        physical.validate()?;
        validate_compile_shape(
            &request.descriptor,
            &Arc::new(request.schema.clone()),
            &collection,
        )?;
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            mongodb_collection_capabilities(&request.descriptor),
            execution_capabilities(&request.descriptor),
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog: request.baseline_observation_schema_catalog,
                redacted_options: serde_json::json!({
                    "endpoint": cdf_runtime::SourceEvidenceLocation::from_operational(&endpoint)?.as_str(),
                    "database": database.as_str(),
                    "collection": collection.as_str(),
                    "username": physical.username.as_deref(),
                    "password": physical.password.as_deref(),
                    "auth_source": physical.auth_source.as_ref().map(MongoDbIdentifier::as_str),
                    "batch_rows": physical.batch_rows,
                    "max_pool_size": physical.max_pool_size,
                    "stream_buffer_batches": physical.stream_buffer_batches,
                    "discovery_records": physical.discovery_records,
                    "discovery_bytes": physical.discovery_bytes,
                }),
                physical_plan: serde_json::to_value(&physical).map_err(|error| {
                    CdfError::internal(format!("serialize MongoDB source plan: {error}"))
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
        let runtime = physical.resolve(context.secret_provider().as_ref())?;
        Ok(Box::new(MongoDbDiscoverySession {
            database: physical.database,
            collection: physical.collection,
            discovery_records: physical.discovery_records,
            discovery_bytes: physical.discovery_bytes,
            runtime,
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
        let runtime = physical.resolve(context.secret_provider().as_ref())?;
        Ok(Arc::new(MongoDbCollectionResource::from_compiled_plan(
            plan,
            physical.endpoint,
            physical.database,
            physical.collection,
            physical.batch_rows,
            physical.stream_buffer_batches,
            runtime,
            context.egress_scope(&plan.driver.driver_id),
            context.execution().clone(),
        )?))
    }
}

impl SourceAddPlanner for MongoDbSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        request.validate()?;
        let Some((scheme, _)) = request.location.split_once("://") else {
            return Ok(None);
        };
        if !matches!(scheme, "mongodb" | "mongodb+srv") {
            return Ok(None);
        }
        const KEYS: [&str; 7] = [
            "auth_source",
            "batch_rows",
            "max_pool_size",
            "stream_buffer_batches",
            "discovery_records",
            "discovery_bytes",
            "cursor",
        ];
        let unknown = request
            .options
            .keys()
            .filter(|key| !KEYS.contains(&key.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Err(CdfError::contract(format!(
                "MongoDB cdf add received unknown options: {}",
                unknown.join(", ")
            )));
        }
        let mut parsed = Url::parse(&request.location).map_err(|error| {
            CdfError::contract(format!("cdf add could not parse MongoDB URL: {error}"))
        })?;
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(CdfError::contract(
                "MongoDB cdf add URL must not contain query or fragment text",
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
                "MongoDB cdf add URL must end with exactly `/database/collection`",
            ));
        }
        let database = MongoDbIdentifier::new(segments[0].clone())?;
        let collection = MongoDbIdentifier::new(segments[1].clone())?;
        let username = (!parsed.username().is_empty()).then(|| parsed.username().to_owned());
        let password = parsed.password().map(str::to_owned);
        parsed
            .set_username("")
            .map_err(|()| CdfError::contract("clear MongoDB URL username"))?;
        parsed
            .set_password(None)
            .map_err(|()| CdfError::contract("clear MongoDB URL password"))?;
        parsed.set_path("");
        let endpoint = normalize_endpoint(parsed.as_str())?;
        let mut source_options = BTreeMap::from([
            ("endpoint".to_owned(), serde_json::json!(endpoint)),
            ("database".to_owned(), serde_json::json!(database.as_str())),
        ]);
        let mut private_files = Vec::new();
        if let Some(username) = username {
            let (reference, file) = add_private_file(&request.source_name, "username", username)?;
            source_options.insert("username".to_owned(), serde_json::json!(reference.as_str()));
            private_files.push(file);
        }
        if let Some(password) = password {
            let (reference, file) = add_private_file(&request.source_name, "password", password)?;
            source_options.insert("password".to_owned(), serde_json::json!(reference.as_str()));
            private_files.push(file);
        }
        if let Some(auth_source) = request.options.get("auth_source") {
            source_options.insert(
                "auth_source".to_owned(),
                serde_json::json!(MongoDbIdentifier::new(auth_source.clone())?.as_str()),
            );
        }
        for key in [
            "batch_rows",
            "max_pool_size",
            "stream_buffer_batches",
            "discovery_records",
            "discovery_bytes",
        ] {
            if let Some(value) = request.options.get(key) {
                let value = value.parse::<u64>().map_err(|_| {
                    CdfError::contract(format!("MongoDB cdf add {key} must be an integer"))
                })?;
                source_options.insert(key.to_owned(), serde_json::json!(value));
            }
        }
        Ok(Some(SourceAddProposal {
            source_kind: "mongodb".to_owned(),
            source_options,
            resource_options: BTreeMap::from([(
                "collection".to_owned(),
                serde_json::json!(collection.as_str()),
            )]),
            cursor: request.options.get("cursor").map(|field| SourceAddCursor {
                field: field.clone(),
                parameter: None,
                ordering: SourceAddCursorOrdering::Exact,
                lag_tolerance_ms: 0,
            }),
            display_location: SourceEvidenceLocation::from_operational(&endpoint)?,
            display_selection: format!("{}.{}", database.as_str(), collection.as_str()),
            private_files,
        }))
    }
}

fn add_private_file(
    source_name: &str,
    field: &str,
    value: String,
) -> Result<(SecretUri, SourceAddPrivateFile)> {
    let relative_path = PathBuf::from(format!(".cdf/secrets/sources/{source_name}.{field}"));
    let reference = SecretUri::new(format!(
        "secret://file/.cdf/secrets/sources/{source_name}.{field}"
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

struct MongoDbDiscoverySession {
    database: MongoDbIdentifier,
    collection: MongoDbIdentifier,
    discovery_records: u64,
    discovery_bytes: u64,
    runtime: MongoDbRuntimeConfig,
    execution: cdf_runtime::ExecutionServices,
    egress: SourceEgressScope,
}

impl SourceDiscoverySession for MongoDbDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        SourceDiscoveryKind::BoundedContent
    }

    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(vec![SourceDiscoveryCandidate::new(
            format!("{}.{}", self.database, self.collection),
            None,
            None,
            BTreeMap::from([
                ("source_kind".to_owned(), "mongodb".to_owned()),
                ("database".to_owned(), self.database.as_str().to_owned()),
                ("collection".to_owned(), self.collection.as_str().to_owned()),
            ]),
        )?])
    }

    fn observe(
        &self,
        candidate: &SourceDiscoveryCandidate,
        request: &SourceDiscoveryRequest,
    ) -> Result<SourceSchemaObservation> {
        request.validate()?;
        let expected = format!("{}.{}", self.database, self.collection);
        if candidate.canonical_location != expected {
            return Err(CdfError::contract(
                "MongoDB discovery candidate changed after compilation",
            ));
        }
        let maximum_records = request.maximum_records.min(self.discovery_records);
        let maximum_bytes = request.maximum_bytes.min(self.discovery_bytes);
        let runtime = self.runtime.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        let execution = self.execution.clone();
        let egress = self.egress.clone();
        let cancellation = request.cancellation.clone();
        let (schema, records, bytes, server_version, collection_metadata) =
            self.execution.run_io(async move {
                discover_mongodb_collection(MongoDbDiscoveryInput {
                    runtime,
                    database,
                    collection,
                    maximum_records,
                    maximum_bytes,
                    memory: execution.memory(),
                    egress,
                    cancellation,
                })
                .await
            })?;
        let mut source_identity = BTreeMap::from([
            ("server_version".to_owned(), server_version),
            ("sample_records".to_owned(), records.to_string()),
            ("sample_bytes".to_owned(), bytes.to_string()),
        ]);
        source_identity.extend(collection_metadata.identity());
        SourceSchemaObservation::new(candidate, schema, source_identity, bytes, records)
    }
}

struct MongoDbDiscoveryInput {
    runtime: MongoDbRuntimeConfig,
    database: MongoDbIdentifier,
    collection: MongoDbIdentifier,
    maximum_records: u64,
    maximum_bytes: u64,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    egress: SourceEgressScope,
    cancellation: cdf_runtime::RunCancellation,
}

async fn discover_mongodb_collection(
    input: MongoDbDiscoveryInput,
) -> Result<(
    arrow_schema::Schema,
    u64,
    u64,
    String,
    MongoDbCollectionMetadata,
)> {
    let MongoDbDiscoveryInput {
        runtime,
        database,
        collection,
        maximum_records,
        maximum_bytes,
        memory,
        egress,
        cancellation,
    } = input;
    let handle = connect_mongodb(&runtime, Arc::clone(&memory), &egress, &cancellation).await?;
    let database_handle = handle.client.database(database.as_str());
    let build_info = cancellation
        .await_or_cancel(async {
            database_handle
                .run_command(doc! {"buildInfo": 1_i32})
                .await
                .map_err(|error| {
                    crate::error::classify_mongodb_error("read MongoDB server version", error)
                })
        })
        .await?;
    let version = validate_server_version(&build_info)?;
    let collection_metadata =
        read_collection_metadata(&database_handle, &collection, &cancellation).await?;
    let _raw_lease = cancellation
        .await_or_cancel(reserve(
            memory,
            ReservationRequest::new(
                ConsumerKey::new("mongodb-discovery-raw", MemoryClass::Discovery)?,
                MONGODB_MAXIMUM_WIRE_BATCH_BYTES + MONGODB_MAXIMUM_OUTPUT_BATCH_BYTES,
            )?,
        ))
        .await?;
    let limit = i64::try_from(maximum_records)
        .map_err(|_| CdfError::contract("MongoDB discovery record bound exceeds i64"))?;
    let batch_size = u32::try_from(maximum_records.min(u64::from(u32::MAX)))
        .map_err(|_| CdfError::contract("MongoDB discovery batch size exceeds u32"))?;
    let mut cursor = cancellation
        .await_or_cancel(async {
            database_handle
                .collection::<Document>(collection.as_str())
                .find(Document::new())
                .limit(limit)
                .batch_size(batch_size)
                .batch()
                .await
                .map_err(|error| {
                    crate::error::classify_mongodb_error("open MongoDB discovery cursor", error)
                })
        })
        .await?;
    let mut inference = SchemaInference::default();
    let mut records = 0_u64;
    let mut bytes = 0_u64;
    'batches: while let Some(batch) = cancellation
        .await_or_cancel(async {
            cursor.next().await.transpose().map_err(|error| {
                crate::error::classify_mongodb_error("read MongoDB discovery cursor", error)
            })
        })
        .await?
    {
        for value in batch.doc_slices().map_err(|error| {
            crate::error::classify_mongodb_error("decode MongoDB discovery batch", error)
        })? {
            let value = value.map_err(|error| {
                CdfError::data(format!(
                    "MongoDB discovery returned malformed BSON: {error}"
                ))
            })?;
            let document = value.as_document().ok_or_else(|| {
                CdfError::data("MongoDB discovery cursor returned a non-document item")
            })?;
            let document_bytes = u64::try_from(document.as_bytes().len())
                .map_err(|_| CdfError::internal("MongoDB discovery document exceeds u64"))?;
            if records >= maximum_records || bytes.saturating_add(document_bytes) > maximum_bytes {
                break 'batches;
            }
            inference.observe(document)?;
            records += 1;
            bytes += document_bytes;
        }
    }
    let (schema, inferred_records, inferred_bytes) = inference.finish()?;
    if inferred_records as u64 != records || inferred_bytes != bytes {
        return Err(CdfError::internal(
            "MongoDB discovery counters diverged from inference authority",
        ));
    }
    let mut metadata = schema.metadata().clone();
    metadata.extend(collection_metadata.schema_metadata());
    let schema = arrow_schema::Schema::new_with_metadata(schema.fields().clone(), metadata);
    Ok((schema, records, bytes, version, collection_metadata))
}

#[derive(Clone, Debug)]
struct MongoDbCollectionMetadata {
    collection_type: String,
    default_collation: String,
    validator_sha256: Option<String>,
    validation_level: Option<String>,
    validation_action: Option<String>,
}

impl MongoDbCollectionMetadata {
    fn identity(&self) -> BTreeMap<String, String> {
        let mut identity = BTreeMap::from([
            ("collection_type".to_owned(), self.collection_type.clone()),
            (
                "default_collation".to_owned(),
                self.default_collation.clone(),
            ),
            (
                "validator_present".to_owned(),
                self.validator_sha256.is_some().to_string(),
            ),
        ]);
        if let Some(hash) = &self.validator_sha256 {
            identity.insert("validator_sha256".to_owned(), hash.clone());
        }
        if let Some(level) = &self.validation_level {
            identity.insert("validation_level".to_owned(), level.clone());
        }
        if let Some(action) = &self.validation_action {
            identity.insert("validation_action".to_owned(), action.clone());
        }
        identity
    }

    fn schema_metadata(&self) -> std::collections::HashMap<String, String> {
        self.identity()
            .into_iter()
            .map(|(key, value)| (format!("cdf:mongodb_{key}"), value))
            .collect()
    }
}

async fn read_collection_metadata(
    database: &mongodb::Database,
    collection: &MongoDbIdentifier,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<MongoDbCollectionMetadata> {
    let response = cancellation
        .await_or_cancel(async {
            database
                .run_command(doc! {
                    "listCollections": 1_i32,
                    "filter": {"name": collection.as_str()},
                    "nameOnly": false,
                    "authorizedCollections": true,
                })
                .await
                .map_err(|error| {
                    crate::error::classify_mongodb_error("read MongoDB collection metadata", error)
                })
        })
        .await?;
    let entries = response
        .get_document("cursor")
        .ok()
        .and_then(|cursor| cursor.get_array("firstBatch").ok())
        .ok_or_else(|| CdfError::data("MongoDB listCollections omitted cursor.firstBatch"))?;
    let entry = entries
        .iter()
        .filter_map(Bson::as_document)
        .find(|entry| entry.get_str("name").ok() == Some(collection.as_str()))
        .ok_or_else(|| {
            CdfError::data(format!(
                "MongoDB collection `{collection}` was not returned by bounded metadata discovery"
            ))
        })?;
    let options = entry.get_document("options").cloned().unwrap_or_default();
    let default_collation = options
        .get_document("collation")
        .ok()
        .and_then(|collation| collation.get_str("locale").ok())
        .map_or_else(|| "simple".to_owned(), |locale| locale.to_owned());
    let validator_sha256 = options
        .get_document("validator")
        .ok()
        .filter(|validator| !validator.is_empty())
        .map(artifact_hash)
        .transpose()?;
    Ok(MongoDbCollectionMetadata {
        collection_type: entry.get_str("type").unwrap_or("collection").to_owned(),
        default_collation,
        validator_sha256,
        validation_level: options.get_str("validationLevel").ok().map(str::to_owned),
        validation_action: options.get_str("validationAction").ok().map(str::to_owned),
    })
}

fn validate_server_version(build_info: &Document) -> Result<String> {
    let version = build_info
        .get_str("version")
        .map_err(|_| CdfError::data("MongoDB buildInfo omitted its version string"))?;
    let major = build_info
        .get_array("versionArray")
        .ok()
        .and_then(|values| values.first())
        .and_then(|value| match value {
            Bson::Int32(value) => Some(i64::from(*value)),
            Bson::Int64(value) => Some(*value),
            _ => None,
        })
        .ok_or_else(|| CdfError::data("MongoDB buildInfo omitted its major version"))?;
    if major < 8 {
        return Err(CdfError::contract(format!(
            "MongoDB source requires server 8.0 or later; observed major version {major}"
        )));
    }
    Ok(version.to_owned())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MongoDbSourceOptions {
    endpoint: String,
    database: String,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    auth_source: Option<String>,
    #[serde(default = "default_batch_rows")]
    batch_rows: u32,
    #[serde(default = "default_max_pool_size")]
    max_pool_size: u32,
    #[serde(default = "default_stream_buffer_batches")]
    stream_buffer_batches: usize,
    #[serde(default = "default_discovery_records")]
    discovery_records: u64,
    #[serde(default = "default_discovery_bytes")]
    discovery_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MongoDbResourceOptions {
    collection: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MongoDbPhysicalPlan {
    endpoint: String,
    database: MongoDbIdentifier,
    collection: MongoDbIdentifier,
    username: Option<String>,
    password: Option<String>,
    auth_source: Option<MongoDbIdentifier>,
    batch_rows: u32,
    max_pool_size: u32,
    stream_buffer_batches: usize,
    discovery_records: u64,
    discovery_bytes: u64,
}

impl MongoDbPhysicalPlan {
    fn validate(&self) -> Result<()> {
        if normalize_endpoint(&self.endpoint)? != self.endpoint {
            return Err(CdfError::contract(
                "MongoDB physical endpoint is not canonically normalized",
            ));
        }
        if !(1..=100_000).contains(&self.batch_rows)
            || !(1..=8).contains(&self.max_pool_size)
            || !(1..=16).contains(&self.stream_buffer_batches)
            || !(1..=100_000).contains(&self.discovery_records)
            || !(1_024..=67_108_864).contains(&self.discovery_bytes)
        {
            return Err(CdfError::contract(
                "MongoDB compiled batch, pool, buffer, or discovery bounds are invalid",
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

    fn resolve(&self, provider: &dyn SecretProvider) -> Result<MongoDbRuntimeConfig> {
        let username = resolve_secret(self.username.as_deref(), provider)?;
        let password = resolve_secret(self.password.as_deref(), provider)?;
        Ok(MongoDbRuntimeConfig {
            endpoint: self.endpoint.clone(),
            username,
            password,
            auth_source: self
                .auth_source
                .as_ref()
                .map(|value| value.as_str().to_owned()),
            max_pool_size: self.max_pool_size,
        })
    }
}

#[derive(Clone)]
pub(crate) struct MongoDbRuntimeConfig {
    pub(crate) endpoint: String,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) auth_source: Option<String>,
    pub(crate) max_pool_size: u32,
}

fn resolve_secret(value: Option<&str>, provider: &dyn SecretProvider) -> Result<Option<String>> {
    value
        .map(|value| {
            provider
                .resolve(&SecretUri::new(value.to_owned())?)?
                .as_str()
                .map(str::to_owned)
        })
        .transpose()
}

fn decode_physical_plan(plan: &CompiledSourcePlan) -> Result<MongoDbPhysicalPlan> {
    serde_json::from_value(plan.physical_plan.clone())
        .map_err(|error| CdfError::contract(format!("invalid MongoDB source plan: {error}")))
}

fn decode_options<T: for<'de> Deserialize<'de>>(
    label: &str,
    options: BTreeMap<String, serde_json::Value>,
) -> Result<T> {
    serde_json::from_value(serde_json::Value::Object(options.into_iter().collect()))
        .map_err(|error| CdfError::contract(format!("{label} options are invalid: {error}")))
}

fn normalize_endpoint(value: &str) -> Result<String> {
    let mut parsed = Url::parse(value).map_err(|error| {
        CdfError::contract(format!("MongoDB endpoint is not a valid URL: {error}"))
    })?;
    if !matches!(parsed.scheme(), "mongodb" | "mongodb+srv")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !matches!(parsed.path(), "" | "/")
    {
        return Err(CdfError::contract(
            "MongoDB endpoint must be a credential-free mongodb:// or mongodb+srv:// authority without database path, query, or fragment",
        ));
    }
    parsed.set_path("");
    Ok(parsed.to_string().trim_end_matches('/').to_owned())
}

fn validate_compile_shape(
    descriptor: &cdf_kernel::ResourceDescriptor,
    schema: &arrow_schema::SchemaRef,
    collection: &MongoDbIdentifier,
) -> Result<()> {
    if !schema.fields().is_empty() {
        return validate_resource_shape(descriptor, schema, collection);
    }
    if !matches!(&descriptor.schema_source, SchemaSource::Discover) {
        return Err(CdfError::data(
            "MongoDB compilation requires a nonempty fixed schema or discover mode",
        ));
    }
    Ok(())
}

fn execution_capabilities(
    descriptor: &cdf_kernel::ResourceDescriptor,
) -> SourceExecutionCapabilities {
    let resumable = descriptor.cursor.is_some();
    SourceExecutionCapabilities {
        minimum_poll_bytes: 16 * 1024,
        maximum_poll_bytes: MONGODB_MAXIMUM_WIRE_BATCH_BYTES,
        minimum_decode_bytes: 16 * 1024,
        maximum_decode_bytes: MONGODB_MAXIMUM_DECODE_BYTES,
        maximum_emitted_batch_bytes: MONGODB_MAXIMUM_OUTPUT_BATCH_BYTES,
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

const fn default_batch_rows() -> u32 {
    DEFAULT_BATCH_ROWS
}
const fn default_max_pool_size() -> u32 {
    DEFAULT_MAX_POOL_SIZE
}
const fn default_stream_buffer_batches() -> usize {
    DEFAULT_STREAM_BUFFER_BATCHES
}
const fn default_discovery_records() -> u64 {
    DEFAULT_DISCOVERY_RECORDS
}
const fn default_discovery_bytes() -> u64 {
    DEFAULT_DISCOVERY_BYTES
}
