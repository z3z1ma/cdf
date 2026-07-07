use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_http::{
    AuthScheme, EgressAllowlist, PaginationConfig, QuotaHeaderPolicy, RateLimitPolicy,
    ResetHeaderSemantics, SecretUri,
};
use cdf_kernel::{
    BackpressureSupport, BatchStream, BoxFuture, CapabilitySupport, CdfError, ContractRef,
    CursorOrderingClaim, CursorSpec, DeliveryGuarantee, EstimateSupport, FilterCapabilities,
    FreshnessSpec, IncrementalShape, PartitionId, PartitionPlan, PartitioningCapabilities, PlanId,
    PushdownFidelity, PushedPredicate, QueryableResource, ReplaySupport, ResourceCapabilities,
    ResourceDescriptor, ResourceId, ResourceStream, Result, ScanPlan, ScanRequest, SchemaHash,
    SchemaSource, ScopeKey, ScopeKind, TrustLevel, WriteDisposition, with_cdf_metadata,
};
use sha2::{Digest, Sha256};

use crate::declarations::*;
use crate::file_runtime::open_file_resource;

#[derive(Clone, Debug)]
pub struct CompiledResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    capabilities: ResourceCapabilities,
    plan: CompiledResourcePlan,
}

impl CompiledResource {
    pub fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    pub fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    pub fn plan(&self) -> &CompiledResourcePlan {
        &self.plan
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompiledResourcePlan {
    Rest(Box<RestResourcePlan>),
    Sql(SqlResourcePlan),
    Files(FileResourcePlan),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestResourcePlan {
    pub source: String,
    pub base_url: String,
    pub path: String,
    pub params: BTreeMap<String, String>,
    pub record_selector: String,
    pub pagination: Option<PaginationConfig>,
    pub auth: Option<AuthScheme>,
    pub rate_limit: RateLimitPolicy,
    pub respect_headers: Vec<String>,
    pub allowlist: EgressAllowlist,
    pub cursor_param: Option<String>,
    pub cursor_filter_fidelity: PushdownFidelity,
    pub records_transform: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlResourcePlan {
    pub source: String,
    pub dialect: Option<String>,
    pub connection: SecretUri,
    pub query: Option<String>,
    pub table: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileResourcePlan {
    pub source: String,
    pub root: String,
    pub glob: String,
    pub format: FileFormatDeclaration,
}

pub fn compile_document(document: &DeclarativeDocument) -> Result<Vec<CompiledResource>> {
    compile_document_inner(document, None)
}

pub fn compile_document_with_project_root(
    document: &DeclarativeDocument,
    project_root: impl AsRef<Path>,
) -> Result<Vec<CompiledResource>> {
    compile_document_inner(document, Some(project_root.as_ref()))
}

fn compile_document_inner(
    document: &DeclarativeDocument,
    project_root: Option<&Path>,
) -> Result<Vec<CompiledResource>> {
    if document.source.is_empty() {
        return Err(CdfError::contract(
            "declarative document must contain at least one source",
        ));
    }
    if document.resource.is_empty() {
        return Err(CdfError::contract(
            "declarative document must contain at least one resource",
        ));
    }

    document
        .resource
        .iter()
        .map(|(name, resource)| {
            let source_name = resolve_source_name(document, resource)?;
            let source = document.source.get(&source_name).ok_or_else(|| {
                CdfError::contract(format!(
                    "resource `{name}` references unknown source `{source_name}`"
                ))
            })?;
            compile_resource(name, &source_name, source, resource, project_root)
        })
        .collect()
}

pub fn validate_document(document: &DeclarativeDocument) -> Result<()> {
    compile_document(document).map(drop)
}

impl ResourceStream for CompiledResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        Ok(vec![partition_for_plan(&self.descriptor, &self.plan)?])
    }

    fn open(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>> {
        match &self.plan {
            CompiledResourcePlan::Files(plan) => {
                open_file_resource(&self.descriptor, plan, partition)
            }
            CompiledResourcePlan::Rest(_) | CompiledResourcePlan::Sql(_) => Box::pin(async {
                Err(CdfError::internal(
                    "declarative resource execution is outside the MVP compiler crate",
                ))
            }),
        }
    }
}

impl QueryableResource for CompiledResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        if request.resource_id != self.descriptor.resource_id {
            return Err(CdfError::contract(format!(
                "scan request resource `{}` does not match compiled resource `{}`",
                request.resource_id, self.descriptor.resource_id
            )));
        }

        let mut pushed_predicates = Vec::new();
        let mut unsupported_predicates = Vec::new();
        for predicate in &request.filters {
            match self.predicate_fidelity(&predicate.expression) {
                PushdownFidelity::Unsupported => unsupported_predicates.push(predicate.clone()),
                fidelity => pushed_predicates.push(PushedPredicate {
                    predicate: predicate.clone(),
                    fidelity,
                }),
            }
        }

        Ok(ScanPlan {
            plan_id: PlanId::new(format!("plan-{}", self.descriptor.resource_id))?,
            request: request.clone(),
            partitions: self.plan_partitions(request)?,
            pushed_predicates,
            unsupported_predicates,
            estimated_rows: None,
            estimated_bytes: None,
            delivery_guarantee: delivery_guarantee(&self.descriptor),
        })
    }
}

impl CompiledResource {
    fn predicate_fidelity(&self, expression: &str) -> PushdownFidelity {
        match &self.plan {
            CompiledResourcePlan::Rest(plan) => {
                let Some(cursor) = &self.descriptor.cursor else {
                    return PushdownFidelity::Unsupported;
                };
                if expression.contains(&cursor.field)
                    || plan
                        .cursor_param
                        .as_ref()
                        .is_some_and(|param| expression.contains(param))
                {
                    plan.cursor_filter_fidelity.clone()
                } else {
                    PushdownFidelity::Unsupported
                }
            }
            CompiledResourcePlan::Sql(_) => PushdownFidelity::Exact,
            CompiledResourcePlan::Files(_) => PushdownFidelity::Unsupported,
        }
    }
}

fn resolve_source_name(
    document: &DeclarativeDocument,
    resource: &ResourceDeclaration,
) -> Result<String> {
    if let Some(source) = &resource.source {
        return Ok(source.clone());
    }
    if document.source.len() == 1 {
        return Ok(document
            .source
            .keys()
            .next()
            .expect("length was checked")
            .clone());
    }
    Err(CdfError::contract(
        "resource source must be declared when a document has multiple sources",
    ))
}

fn compile_resource(
    name: &str,
    source_name: &str,
    source: &SourceDeclaration,
    resource: &ResourceDeclaration,
    project_root: Option<&Path>,
) -> Result<CompiledResource> {
    validate_escape_hatch(resource)?;
    validate_fields(name, resource)?;

    let resource_id = resource
        .id
        .clone()
        .unwrap_or_else(|| format!("{source_name}.{name}"));
    let descriptor_resource_id = ResourceId::new(resource_id.clone())?;
    let schema = compile_schema(resource)?;
    let schema_source = compile_schema_source(&resource_id, resource)?;
    let cursor = compile_cursor(resource.cursor.as_ref())?;
    let write_disposition = resource
        .write_disposition
        .as_ref()
        .ok_or_else(|| {
            CdfError::contract(format!("resource `{name}` must declare write_disposition"))
        })
        .map(to_write_disposition)??;
    let trust_level = compile_trust(resource)?;
    let contract = resource
        .contract
        .as_ref()
        .map(ContractRef::new)
        .transpose()?;
    let descriptor = ResourceDescriptor {
        resource_id: descriptor_resource_id,
        schema_source,
        primary_key: resource.primary_key.clone(),
        merge_key: resource
            .merge_key
            .clone()
            .unwrap_or_else(|| resource.primary_key.clone()),
        cursor,
        write_disposition,
        contract,
        state_scope: state_scope(resource)?,
        freshness: match &resource.freshness {
            Some(freshness) => Some(FreshnessSpec {
                max_age_ms: parse_duration_ms(&freshness.max_age)?,
            }),
            None => None,
        },
        trust_level,
    };

    let plan = match source {
        SourceDeclaration::Rest(rest) => {
            CompiledResourcePlan::Rest(Box::new(compile_rest_plan(source_name, rest, resource)?))
        }
        SourceDeclaration::Sql(sql) => {
            CompiledResourcePlan::Sql(compile_sql_plan(source_name, sql, resource)?)
        }
        SourceDeclaration::Files(files) => CompiledResourcePlan::Files(compile_file_plan(
            source_name,
            files,
            resource,
            project_root,
        )?),
    };
    let capabilities = capabilities_for(&descriptor, &plan);

    Ok(CompiledResource {
        descriptor,
        schema: Arc::new(schema),
        capabilities,
        plan,
    })
}

fn compile_schema(resource: &ResourceDeclaration) -> Result<Schema> {
    let Some(schema) = &resource.schema else {
        return Ok(Schema::empty());
    };

    let fields = schema
        .fields
        .iter()
        .map(|field| {
            let arrow_field = Field::new(
                &field.name,
                field_type(&field.field_type, field.timezone.clone()),
                field.nullable.unwrap_or(true),
            );
            with_cdf_metadata(
                arrow_field,
                field.source_name.clone(),
                field.semantic.clone(),
                field.null_origin.clone(),
            )
        })
        .collect::<Vec<_>>();
    Ok(Schema::new(fields))
}

fn compile_schema_source(
    resource_id: &str,
    resource: &ResourceDeclaration,
) -> Result<SchemaSource> {
    match &resource.schema {
        Some(schema) => Ok(SchemaSource::Declared {
            schema_hash: schema_hash(schema)?,
            source: format!("declarative:{resource_id}"),
        }),
        None => Ok(SchemaSource::Discovered { schema_hash: None }),
    }
}

fn compile_cursor(cursor: Option<&CursorDeclaration>) -> Result<Option<CursorSpec>> {
    cursor
        .map(|cursor| {
            Ok(CursorSpec {
                field: cursor.field.clone(),
                ordering: match cursor.ordering {
                    CursorOrderingDeclaration::Exact => CursorOrderingClaim::Exact,
                    CursorOrderingDeclaration::Inexact | CursorOrderingDeclaration::BestEffort => {
                        CursorOrderingClaim::Inexact
                    }
                    CursorOrderingDeclaration::Unordered => CursorOrderingClaim::Unordered,
                },
                lag_tolerance_ms: parse_duration_ms(&cursor.lag)?,
            })
        })
        .transpose()
}

fn compile_trust(resource: &ResourceDeclaration) -> Result<TrustLevel> {
    if let Some(trust) = &resource.trust {
        return Ok(to_trust_level(trust));
    }

    match resource.contract.as_deref() {
        Some("experimental") => Ok(TrustLevel::Experimental),
        Some("governed") => Ok(TrustLevel::Governed),
        Some("financial") => Ok(TrustLevel::Financial),
        Some("serving") => Ok(TrustLevel::Serving),
        Some(contract) => Err(CdfError::contract(format!(
            "resource with custom contract `{contract}` must also declare trust"
        ))),
        None => Err(CdfError::contract(
            "resource must declare trust or use a built-in contract preset",
        )),
    }
}

fn compile_rest_plan(
    source_name: &str,
    source: &RestSourceDeclaration,
    resource: &ResourceDeclaration,
) -> Result<RestResourcePlan> {
    let path = resource
        .path
        .clone()
        .ok_or_else(|| CdfError::contract("REST resources must declare path before compilation"))?;
    let record_selector = resource.records.clone().ok_or_else(|| {
        CdfError::contract("REST resources must declare records before compilation")
    })?;
    let auth = source.auth.as_ref().map(compile_auth).transpose()?;
    let (rate_limit, respect_headers) = compile_rate_limit(source.rate_limit.as_ref())?;
    let allowlist = if source.egress_allowlist.is_empty() {
        EgressAllowlist::allow_any()
    } else {
        EgressAllowlist::from_hosts(source.egress_allowlist.clone())
    };
    let cursor_filter_fidelity = resource
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.filter_fidelity.as_ref())
        .map(to_pushdown_fidelity)
        .unwrap_or(PushdownFidelity::Inexact);

    Ok(RestResourcePlan {
        source: source_name.to_owned(),
        base_url: source.base_url.clone(),
        path,
        params: resource
            .params
            .iter()
            .map(|(name, value)| (name.clone(), value.as_query_value()))
            .collect(),
        record_selector,
        pagination: resource
            .paginate
            .as_ref()
            .map(compile_pagination)
            .transpose()?,
        auth,
        rate_limit,
        respect_headers,
        allowlist,
        cursor_param: resource
            .cursor
            .as_ref()
            .and_then(|cursor| cursor.param.clone()),
        cursor_filter_fidelity,
        records_transform: resource.records_transform.clone(),
    })
}

fn compile_sql_plan(
    source_name: &str,
    source: &SqlSourceDeclaration,
    resource: &ResourceDeclaration,
) -> Result<SqlResourcePlan> {
    if resource.query.is_none() && resource.table.is_none() {
        return Err(CdfError::contract(
            "SQL resources must declare query or table before compilation",
        ));
    }
    if resource.query.is_some() && resource.table.is_some() {
        return Err(CdfError::contract(
            "SQL resources must declare only one of query or table",
        ));
    }

    Ok(SqlResourcePlan {
        source: source_name.to_owned(),
        dialect: source.dialect.clone(),
        connection: SecretUri::new(source.connection.clone())?,
        query: resource.query.clone(),
        table: resource.table.clone(),
    })
}

fn compile_file_plan(
    source_name: &str,
    source: &FileSourceDeclaration,
    resource: &ResourceDeclaration,
    project_root: Option<&Path>,
) -> Result<FileResourcePlan> {
    Ok(FileResourcePlan {
        source: source_name.to_owned(),
        root: compile_file_root(&source.root, project_root)?,
        glob: resource.glob.clone().ok_or_else(|| {
            CdfError::contract("file resources must declare glob before compilation")
        })?,
        format: resource.format.clone().ok_or_else(|| {
            CdfError::contract("file resources must declare format before compilation")
        })?,
    })
}

fn compile_file_root(root: &str, project_root: Option<&Path>) -> Result<String> {
    let root_path = PathBuf::from(root);
    if root_path.is_absolute() {
        return path_to_string(&root_path);
    }
    if path_contains_parent_dir(&root_path) {
        return Err(CdfError::contract(
            "relative file source root must stay under the project root and cannot contain `..`",
        ));
    }
    match project_root {
        Some(project_root) => path_to_string(&absolute_project_root(project_root)?.join(root_path)),
        None => Ok(root.to_owned()),
    }
}

fn absolute_project_root(project_root: &Path) -> Result<PathBuf> {
    if project_root.is_absolute() {
        return Ok(project_root.to_path_buf());
    }
    let current_dir = std::env::current_dir().map_err(|error| {
        CdfError::internal(format!(
            "resolve current directory for project root: {error}"
        ))
    })?;
    Ok(current_dir.join(project_root))
}

fn path_contains_parent_dir(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::ParentDir))
}

fn path_to_string(path: &Path) -> Result<String> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        CdfError::contract(format!(
            "file source root path is not valid UTF-8: {}",
            path.display()
        ))
    })
}

fn compile_auth(auth: &AuthDeclaration) -> Result<AuthScheme> {
    match auth {
        AuthDeclaration::Bearer { token } => Ok(AuthScheme::Bearer {
            token_uri: SecretUri::new(token.clone())?,
        }),
        AuthDeclaration::Header { name, value } => Ok(AuthScheme::Header {
            name: name.clone(),
            value_uri: SecretUri::new(value.clone())?,
        }),
    }
}

fn compile_rate_limit(
    rate_limit: Option<&RateLimitDeclaration>,
) -> Result<(RateLimitPolicy, Vec<String>)> {
    let Some(rate_limit) = rate_limit else {
        return Ok((RateLimitPolicy::unrestricted(), Vec::new()));
    };

    Ok((
        RateLimitPolicy {
            requests_per_minute: rate_limit.requests_per_minute,
            quota_headers: rate_limit
                .quota_headers
                .iter()
                .map(|quota| {
                    QuotaHeaderPolicy::remaining_until_reset(
                        quota.remaining_header.clone(),
                        quota.reset_header.clone(),
                        match quota.reset {
                            ResetSemanticsDeclaration::DelaySeconds => {
                                ResetHeaderSemantics::DelaySeconds
                            }
                            ResetSemanticsDeclaration::EpochSeconds => {
                                ResetHeaderSemantics::EpochSeconds
                            }
                        },
                    )
                })
                .collect(),
        },
        rate_limit.respect_headers.clone(),
    ))
}

fn compile_pagination(pagination: &PaginationDeclaration) -> Result<PaginationConfig> {
    Ok(match pagination {
        PaginationDeclaration::LinkHeader => PaginationConfig::LinkHeader,
        PaginationDeclaration::CursorParam {
            query_param,
            response_field,
            initial,
        } => PaginationConfig::Cursor {
            query_param: query_param.clone(),
            response_field: response_field.clone(),
            initial: initial.clone(),
        },
        PaginationDeclaration::PageNumber {
            query_param,
            start_page,
        } => PaginationConfig::Page {
            query_param: query_param.clone(),
            start_page: start_page.unwrap_or(1),
        },
        PaginationDeclaration::Offset {
            offset_param,
            limit_param,
            start_offset,
            limit,
        } => PaginationConfig::Offset {
            offset_param: offset_param.clone(),
            limit_param: limit_param.clone(),
            start_offset: start_offset.unwrap_or(0),
            limit: *limit,
        },
        PaginationDeclaration::NextToken {
            query_param,
            response_field,
            initial,
        } => PaginationConfig::NextToken {
            query_param: query_param.clone(),
            response_field: response_field.clone(),
            initial: initial.clone(),
        },
    })
}

fn capabilities_for(
    descriptor: &ResourceDescriptor,
    plan: &CompiledResourcePlan,
) -> ResourceCapabilities {
    match plan {
        CompiledResourcePlan::Rest(rest) => ResourceCapabilities {
            projection: CapabilitySupport::Unsupported,
            filters: FilterCapabilities {
                default_fidelity: if descriptor.cursor.is_some() {
                    rest.cursor_filter_fidelity.clone()
                } else {
                    PushdownFidelity::Unsupported
                },
                supported_operators: if descriptor.cursor.is_some() {
                    vec![">".to_owned(), ">=".to_owned(), "=".to_owned()]
                } else {
                    Vec::new()
                },
            },
            limits: CapabilitySupport::Unsupported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: partitioning_capabilities(descriptor),
            incremental: if descriptor.cursor.is_some() {
                IncrementalShape::Cursor
            } else {
                IncrementalShape::Full
            },
            replay: if descriptor.cursor.is_some() {
                ReplaySupport::FromPosition
            } else {
                ReplaySupport::None
            },
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::None,
        },
        CompiledResourcePlan::Sql(_) => ResourceCapabilities {
            projection: CapabilitySupport::Supported,
            filters: FilterCapabilities {
                default_fidelity: PushdownFidelity::Exact,
                supported_operators: vec![
                    "=".to_owned(),
                    ">".to_owned(),
                    ">=".to_owned(),
                    "<".to_owned(),
                    "<=".to_owned(),
                ],
            },
            limits: CapabilitySupport::Supported,
            ordering: CapabilitySupport::Supported,
            partitioning: partitioning_capabilities(descriptor),
            incremental: if descriptor.cursor.is_some() {
                IncrementalShape::Cursor
            } else {
                IncrementalShape::Full
            },
            replay: if descriptor.cursor.is_some() {
                ReplaySupport::FromPosition
            } else {
                ReplaySupport::None
            },
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::Rows,
        },
        CompiledResourcePlan::Files(_) => ResourceCapabilities {
            projection: CapabilitySupport::Unsupported,
            filters: FilterCapabilities::default(),
            limits: CapabilitySupport::Unsupported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: PartitioningCapabilities {
                parallel_partitions: true,
                supported_scopes: vec![ScopeKind::File],
            },
            incremental: IncrementalShape::File,
            replay: ReplaySupport::ExactRecordedBatches,
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::Bytes,
        },
    }
}

fn partitioning_capabilities(descriptor: &ResourceDescriptor) -> PartitioningCapabilities {
    match descriptor.state_scope.kind() {
        ScopeKind::Resource => PartitioningCapabilities::default(),
        kind => PartitioningCapabilities {
            parallel_partitions: true,
            supported_scopes: vec![kind],
        },
    }
}

fn partition_for_plan(
    descriptor: &ResourceDescriptor,
    plan: &CompiledResourcePlan,
) -> Result<PartitionPlan> {
    let (partition_id, scope, mut metadata) = match plan {
        CompiledResourcePlan::Rest(rest) => {
            let mut metadata = BTreeMap::new();
            metadata.insert("kind".to_owned(), "rest".to_owned());
            metadata.insert("path".to_owned(), rest.path.clone());
            if let Some(pagination) = &rest.pagination {
                metadata.insert("pagination".to_owned(), pagination.kind().to_string());
            }
            if let Some(cursor) = &descriptor.cursor {
                metadata.insert("cursor_field".to_owned(), cursor.field.clone());
            }
            ("rest".to_owned(), descriptor.state_scope.clone(), metadata)
        }
        CompiledResourcePlan::Sql(sql) => {
            let mut metadata = BTreeMap::new();
            metadata.insert("kind".to_owned(), "sql".to_owned());
            if let Some(table) = &sql.table {
                metadata.insert("table".to_owned(), table.clone());
            }
            ("sql".to_owned(), descriptor.state_scope.clone(), metadata)
        }
        CompiledResourcePlan::Files(files) => {
            let mut metadata = BTreeMap::new();
            metadata.insert("kind".to_owned(), "files".to_owned());
            metadata.insert("glob".to_owned(), files.glob.clone());
            (
                "files".to_owned(),
                ScopeKey::File {
                    path: files.glob.clone(),
                },
                metadata,
            )
        }
    };
    metadata.insert("resource_id".to_owned(), descriptor.resource_id.to_string());

    Ok(PartitionPlan {
        partition_id: PartitionId::new(partition_id)?,
        scope,
        start_position: None,
        metadata,
    })
}

fn state_scope(resource: &ResourceDeclaration) -> Result<ScopeKey> {
    match &resource.partition {
        Some(PartitionDeclaration {
            by: PartitionByDeclaration::CursorWindow,
            width,
        }) => {
            let width = width
                .as_ref()
                .ok_or_else(|| CdfError::contract("cursor_window partitions must declare width"))?;
            parse_duration_ms(width)?;
            Ok(ScopeKey::Window {
                start: "cursor".to_owned(),
                end: format!("cursor+{width}"),
            })
        }
        Some(PartitionDeclaration {
            by: PartitionByDeclaration::File,
            ..
        }) => Ok(ScopeKey::File {
            path: resource.glob.clone().unwrap_or_else(|| "*".to_owned()),
        }),
        Some(PartitionDeclaration {
            by: PartitionByDeclaration::Resource,
            ..
        })
        | None => Ok(ScopeKey::Resource),
    }
}

fn delivery_guarantee(descriptor: &ResourceDescriptor) -> DeliveryGuarantee {
    match descriptor.write_disposition {
        WriteDisposition::Merge if !descriptor.primary_key.is_empty() => {
            DeliveryGuarantee::EffectivelyOncePerKey
        }
        WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
        WriteDisposition::Append | WriteDisposition::Merge => {
            DeliveryGuarantee::AtLeastOnceDuplicateRisk
        }
    }
}

fn validate_fields(name: &str, resource: &ResourceDeclaration) -> Result<()> {
    let mut required = resource.primary_key.clone();
    if let Some(merge_key) = &resource.merge_key {
        required.extend(merge_key.iter().cloned());
    }
    if let Some(cursor) = &resource.cursor {
        required.push(cursor.field.clone());
    }
    required.sort();
    required.dedup();

    if required.is_empty() {
        return Ok(());
    }

    if let Some(schema) = &resource.schema {
        let declared = schema
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<BTreeSet<_>>();
        ensure_fields_exist(name, "declared schema", &declared, &required)?;
    }

    if let Some(sample) = &resource.sample {
        let sample_fields = sample
            .fields
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        ensure_fields_exist(name, "sample", &sample_fields, &required)?;
    }

    Ok(())
}

fn ensure_fields_exist(
    resource_name: &str,
    field_set_name: &str,
    fields: &BTreeSet<&str>,
    required: &[String],
) -> Result<()> {
    let missing = required
        .iter()
        .filter(|field| !fields.contains(field.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }

    Err(CdfError::contract(format!(
        "resource `{resource_name}` is missing required field(s) {} in {field_set_name}",
        missing.join(", ")
    )))
}

fn validate_escape_hatch(resource: &ResourceDeclaration) -> Result<()> {
    let Some(transform) = &resource.records_transform else {
        return Ok(());
    };
    if transform.starts_with("python://") || transform.starts_with("rust://") {
        Ok(())
    } else {
        Err(CdfError::contract(
            "records_transform must use python:// or rust://",
        ))
    }
}

fn schema_hash(schema: &SchemaDeclaration) -> Result<SchemaHash> {
    let bytes =
        serde_json::to_vec(schema).map_err(|error| CdfError::contract(error.to_string()))?;
    let digest = Sha256::digest(bytes);
    SchemaHash::new(format!("sha256:{}", hex::encode(digest)))
}

fn field_type(field_type: &FieldTypeDeclaration, timezone: Option<String>) -> DataType {
    match field_type {
        FieldTypeDeclaration::String | FieldTypeDeclaration::Utf8 | FieldTypeDeclaration::Json => {
            DataType::Utf8
        }
        FieldTypeDeclaration::Int64 => DataType::Int64,
        FieldTypeDeclaration::UInt64 => DataType::UInt64,
        FieldTypeDeclaration::Float64 => DataType::Float64,
        FieldTypeDeclaration::Boolean => DataType::Boolean,
        FieldTypeDeclaration::Date32 => DataType::Date32,
        FieldTypeDeclaration::TimestampMillis => {
            DataType::Timestamp(TimeUnit::Millisecond, timezone.map(Into::into))
        }
        FieldTypeDeclaration::TimestampMicros => {
            DataType::Timestamp(TimeUnit::Microsecond, timezone.map(Into::into))
        }
    }
}

fn to_write_disposition(disposition: &WriteDispositionDeclaration) -> Result<WriteDisposition> {
    Ok(match disposition {
        WriteDispositionDeclaration::Append => WriteDisposition::Append,
        WriteDispositionDeclaration::Replace => WriteDisposition::Replace,
        WriteDispositionDeclaration::Merge => WriteDisposition::Merge,
        WriteDispositionDeclaration::CdcApply => WriteDisposition::CdcApply,
    })
}

fn to_trust_level(trust: &TrustDeclaration) -> TrustLevel {
    match trust {
        TrustDeclaration::Experimental => TrustLevel::Experimental,
        TrustDeclaration::Governed => TrustLevel::Governed,
        TrustDeclaration::Financial => TrustLevel::Financial,
        TrustDeclaration::Serving => TrustLevel::Serving,
    }
}

fn to_pushdown_fidelity(fidelity: &FilterFidelityDeclaration) -> PushdownFidelity {
    match fidelity {
        FilterFidelityDeclaration::Exact => PushdownFidelity::Exact,
        FilterFidelityDeclaration::Inexact => PushdownFidelity::Inexact,
        FilterFidelityDeclaration::Unsupported => PushdownFidelity::Unsupported,
    }
}

fn parse_duration_ms(value: &str) -> Result<u64> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CdfError::contract("duration cannot be empty"));
    }

    let digits = value
        .chars()
        .take_while(|character| character.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        return Err(CdfError::contract(format!(
            "duration `{value}` must start with a number"
        )));
    }
    let unit = &value[digits.len()..];
    let amount = digits.parse::<u64>().map_err(|error| {
        CdfError::contract(format!("duration `{value}` has invalid number: {error}"))
    })?;

    let multiplier = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "" => {
            return Err(CdfError::contract(format!(
                "duration `{value}` must include a unit"
            )));
        }
        _ => {
            return Err(CdfError::contract(format!(
                "duration `{value}` has unsupported unit `{unit}`"
            )));
        }
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| CdfError::contract(format!("duration `{value}` is too large")))
}
