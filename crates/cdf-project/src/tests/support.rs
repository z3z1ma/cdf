use super::{
    Arc, CapabilitySupport, CdfError, ConcurrencyLimit, DestinationId, DestinationSheet,
    FileRuntimeDependencies, FileSourceDriver, FileTransportFacade, HttpRequest, HttpResponse,
    HttpTransport, IdempotencySupport, IdentifierRules, Mutex, NORMALIZER_NAMECASE_V1,
    QueryableResource, Result, TransactionSupport, TypeMapping, TypeMappingFidelity, VecDeque,
    WriteDisposition,
};

pub(super) fn test_execution_services() -> cdf_runtime::ExecutionServices {
    cdf_engine::StandaloneExecutionHost::default_services(64 * 1024 * 1024)
        .unwrap()
        .1
}

pub(super) fn test_format_registry() -> Arc<cdf_runtime::FormatRegistry> {
    let mut registry = cdf_builtin_drivers::new_builtin_format_registry().unwrap();
    registry
        .register(Arc::new(
            cdf_format_avro::AvroOcfFormatDriver::new().unwrap(),
        ))
        .unwrap();
    registry
        .register(Arc::new(
            cdf_format_avro::AvroSingleObjectFormatDriver::new().unwrap(),
        ))
        .unwrap();
    Arc::new(registry)
}

pub(crate) fn test_source_registry() -> cdf_runtime::SourceRegistry {
    let formats = test_format_registry();
    let runtime_formats = Arc::clone(&formats);
    let mut registry = cdf_runtime::SourceRegistry::new();
    registry
        .register(
            FileSourceDriver::new(formats, move |secrets, execution, egress| {
                Ok(FileRuntimeDependencies::new(
                    FileTransportFacade::new()
                        .with_shared_secret_provider(secrets)
                        .with_execution_services(execution.clone()),
                    execution,
                    Arc::clone(&runtime_formats),
                    Arc::new(cdf_runtime::ByteTransformRegistry::default()),
                    egress,
                ))
            })
            .unwrap(),
        )
        .unwrap();
    registry
        .register(
            cdf_source_rest::RestSourceDriver::new(|| Ok(Box::new(RecordingTransport::default())))
                .unwrap(),
        )
        .unwrap();
    registry
        .register(cdf_source_postgres::PostgresSourceDriver::new().unwrap())
        .unwrap();
    registry
        .register(ProjectReferenceTestDriver::new())
        .unwrap();
    registry
}

#[derive(Debug)]
pub(super) struct ProjectReferenceTestDriver {
    pub(super) descriptor: cdf_runtime::SourceDriverDescriptor,
    pub(super) option_schema: serde_json::Value,
}

impl ProjectReferenceTestDriver {
    pub(super) fn new() -> Self {
        let option_schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "source": {
                "type": "object",
                "additionalProperties": false,
                "properties": {"uri": {"type": "string"}}
            },
            "resource": {
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            },
        });
        Self {
            descriptor: cdf_runtime::SourceDriverDescriptor {
                driver_id: cdf_runtime::SourceDriverId::new("python").unwrap(),
                driver_version: "test-v1".to_owned(),
                option_schema_hash: cdf_runtime::artifact_hash(&option_schema).unwrap(),
                kinds: vec!["python".to_owned()],
                schemes: vec!["python".to_owned()],
            },
            option_schema,
        }
    }
}

impl cdf_runtime::SourceDriver for ProjectReferenceTestDriver {
    fn descriptor(&self) -> &cdf_runtime::SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn validate_project_options(&self, options: &serde_json::Value) -> Result<()> {
        let options = options
            .as_object()
            .ok_or_else(|| CdfError::contract("test reference source options must be an object"))?;
        if !options
            .get("interpreter")
            .is_some_and(serde_json::Value::is_string)
            || options
                .get("require_free_threaded")
                .is_some_and(|value| !value.is_boolean())
            || options
                .keys()
                .any(|key| !matches!(key.as_str(), "interpreter" | "require_free_threaded"))
        {
            return Err(CdfError::contract(
                "test reference source options require interpreter and optional require_free_threaded",
            ));
        }
        Ok(())
    }

    fn compile(
        &self,
        _request: cdf_runtime::SourceCompileRequest,
    ) -> Result<cdf_runtime::CompiledSourcePlan> {
        Err(CdfError::internal(
            "project validation fixture does not compile reference sources",
        ))
    }

    fn discovery_session(
        &self,
        _plan: &cdf_runtime::CompiledSourcePlan,
        _context: &cdf_runtime::SourceResolutionContext<'_>,
    ) -> Result<Box<dyn cdf_runtime::SourceDiscoverySession>> {
        Err(CdfError::internal(
            "project validation fixture does not discover reference sources",
        ))
    }

    fn health(
        &self,
        request: cdf_runtime::SourceHealthRequest,
        _context: &cdf_runtime::SourceResolutionContext<'_>,
        output: &mut dyn cdf_runtime::SourceHealthSink,
    ) -> Result<()> {
        for plan in request.compiled_plans {
            output.emit(cdf_runtime::SourceHealthResult {
                probe_id: plan.descriptor.resource_id.as_str().replace('.', "_"),
                status: cdf_runtime::SourceHealthStatus::Unsupported,
                message: "project reference fixture has no health operation".to_owned(),
                details: serde_json::json!({}),
            })?;
        }
        Ok(())
    }

    fn resolve(
        &self,
        _plan: &cdf_runtime::CompiledSourcePlan,
        _context: &cdf_runtime::SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        Err(CdfError::internal(
            "project validation fixture does not resolve reference sources",
        ))
    }
}

pub(crate) const BOOK_PROJECT: &str = r#"
[project]
name = "acme_data"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"
retention = { default = "5 runs" }

[environments.prod]
destination = "postgres://secret://env/PROD_DWH"
retention = { default = "90d", financial = "400d" }

[python]
interpreter = ".venv/bin/python"

[defaults]
contract = "governed"

[sources.github]
type = "rest"
base_url = "https://api.github.com"
auth = { kind = "bearer", token = "secret://env/GITHUB_TOKEN" }
"#;

pub(crate) const GITHUB_RESOURCE: &str = r#"
[source.github]
kind = "rest"
base_url = "https://api.github.com"
auth = { kind = "bearer", token = "secret://env/GITHUB_TOKEN" }

[resource.issues]
path = "/repos/{owner}/{repo}/issues"
records = "$"
primary_key = ["id"]
merge_key = ["id"]
cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "5m" }
write_disposition = "merge"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "timestamp_micros", nullable = false, timezone = "UTC" },
] }
"#;

pub(crate) fn compile_declarative_fixture(
    registry: &cdf_runtime::SourceRegistry,
    input: &str,
) -> cdf_kernel::Result<Vec<cdf_declarative::CompiledResource>> {
    let document = cdf_declarative::parse_toml(input)?;
    cdf_declarative::compile_document(registry, &document)
}

pub(super) fn compile_declarative_fixture_with_root(
    registry: &cdf_runtime::SourceRegistry,
    input: &str,
    project_root: &std::path::Path,
) -> cdf_kernel::Result<Vec<cdf_declarative::CompiledResource>> {
    let document = cdf_declarative::parse_toml(input)?;
    cdf_declarative::compile_document_with_project_root(registry, &document, project_root)
}

pub(super) struct RecordingResponse {
    pub(super) response: HttpResponse,
    pub(super) body: Vec<u8>,
}

#[derive(Clone, Default)]
pub(super) struct RecordingTransport {
    pub(super) state: Arc<Mutex<RecordingTransportState>>,
}

#[derive(Default)]
pub(super) struct RecordingTransportState {
    pub(super) requests: Vec<HttpRequest>,
    pub(super) responses: VecDeque<RecordingResponse>,
}

impl RecordingTransport {
    pub(super) fn new<I>(responses: I) -> Self
    where
        I: IntoIterator<Item = RecordingResponse>,
    {
        Self {
            state: Arc::new(Mutex::new(RecordingTransportState {
                requests: Vec::new(),
                responses: responses.into_iter().collect(),
            })),
        }
    }

    pub(super) fn requests(&self) -> Vec<HttpRequest> {
        self.state.lock().unwrap().requests.clone()
    }
}

impl HttpTransport for RecordingTransport {
    fn send(
        &self,
        request: HttpRequest,
        budget: cdf_http::HttpResponseBudget,
    ) -> cdf_kernel::BoxFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let template = {
                let mut state = self.state.lock().unwrap();
                state.requests.push(request);
                state
                    .responses
                    .pop_front()
                    .ok_or_else(|| CdfError::internal("test transport exhausted responses"))?
            };
            Ok(template
                .response
                .with_body(budget.account_body(template.body).await?))
        })
    }
}

pub(super) fn destination_sheet(name: &str, fidelity: TypeMappingFidelity) -> DestinationSheet {
    DestinationSheet {
        destination: DestinationId::new(name).unwrap(),
        supported_dispositions: vec![WriteDisposition::Append, WriteDisposition::Merge],
        transactions: TransactionSupport::AtomicPackage,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: vec![TypeMapping {
            arrow_type: "utf8".to_owned(),
            destination_type: "text".to_owned(),
            fidelity,
        }],
        identifier_rules: IdentifierRules {
            normalizer: NORMALIZER_NAMECASE_V1.to_owned(),
            max_length: Some(63),
            allowed_pattern: Some("[a-z_][a-z0-9_]*".to_owned()),
        },
        migration_support: CapabilitySupport::Supported,
        quarantine_tables: CapabilitySupport::Supported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    }
}
